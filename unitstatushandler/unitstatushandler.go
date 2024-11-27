// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2021 Renesas Electronics Corporation.
// Copyright (C) 2021 EPAM Systems, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package unitstatushandler

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/resourcemonitor"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/downloader"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// UnitManager manages unit.
type UnitManager interface {
	GetAllNodeIDs() ([]string, error)
	GetNodeInfo(nodeID string) (cloudprotocol.NodeInfo, error)
	SubscribeNodeInfoChange() <-chan cloudprotocol.NodeInfo
	PauseNode(nodeID string) error
	ResumeNode(nodeID string) error
	GetUnitSubjects() ([]string, error)
	SubscribeUnitSubjectsChanged() <-chan []string
}

// Downloader downloads packages.
type Downloader interface {
	Download(ctx context.Context, packageInfo downloader.PackageInfo) (result downloader.Result, err error)
	Release(filePath string) error
	ReleaseByType(targetType string) error
}

// StatusSender sends unit status to cloud.
type StatusSender interface {
	SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error)
	SendDeltaUnitStatus(deltaUnitStatus cloudprotocol.DeltaUnitStatus) (err error)
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
}

// UnitConfigUpdater updates unit configuration.
type UnitConfigUpdater interface {
	GetStatus() (cloudprotocol.UnitConfigStatus, error)
	CheckUnitConfig(unitConfig cloudprotocol.UnitConfig) error
	UpdateUnitConfig(unitConfig cloudprotocol.UnitConfig) error
}

// FirmwareUpdater updates system components.
type FirmwareUpdater interface {
	GetStatus() (componentsInfo []cloudprotocol.ComponentStatus, err error)
	UpdateComponents(components []cloudprotocol.ComponentInfo, chains []cloudprotocol.CertificateChain,
		certs []cloudprotocol.Certificate) (status []cloudprotocol.ComponentStatus, err error)
	NewComponentsChannel() <-chan []cloudprotocol.ComponentStatus
}

// InstanceRunner instances runner.
type InstanceRunner interface {
	RunInstances(instances []cloudprotocol.InstanceInfo, rebalancing bool) error
}

// SystemQuotaAlertProvider provides system quota alerts.
type SystemQuotaAlertProvider interface {
	GetSystemQuoteAlertChannel() <-chan cloudprotocol.SystemQuotaAlert
}

// SoftwareUpdater updates services, layers.
type SoftwareUpdater interface {
	GetServicesStatus() ([]ServiceStatus, error)
	GetLayersStatus() ([]LayerStatus, error)
	InstallService(serviceInfo cloudprotocol.ServiceInfo,
		chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) error
	RestoreService(serviceID string) error
	RevertService(serviceID string) error
	RemoveService(serviceID string) error
	InstallLayer(layerInfo cloudprotocol.LayerInfo,
		chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) error
	RemoveLayer(digest string) error
	RestoreLayer(digest string) error
}

// Storage used to store unit status handler states.
type Storage interface {
	SetFirmwareUpdateState(state json.RawMessage) (err error)
	GetFirmwareUpdateState() (state json.RawMessage, err error)
	SetSoftwareUpdateState(state json.RawMessage) (err error)
	GetSoftwareUpdateState() (state json.RawMessage, err error)
}

// ServiceStatus represents service status.
type ServiceStatus struct {
	cloudprotocol.ServiceStatus
	Cached bool
}

// LayerStatus represents layer status.
type LayerStatus struct {
	cloudprotocol.LayerStatus
	Cached bool
}

// Instance instance of unit status handler.
type Instance struct {
	sync.Mutex

	unitManager  UnitManager
	statusSender StatusSender

	statusMutex sync.Mutex

	unitStatus       cloudprotocol.UnitStatus
	statusTimer      *time.Timer
	sendStatusPeriod time.Duration

	firmwareManager *firmwareManager
	softwareManager *softwareManager

	newComponentsChannel       <-chan []cloudprotocol.ComponentStatus
	nodeChangedChannel         <-chan cloudprotocol.NodeInfo
	unitSubjectsChangedChannel <-chan []string
	systemQuotaAlertChannel    <-chan cloudprotocol.SystemQuotaAlert

	initDone    bool
	isConnected bool
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new unit status handler instance.
func New(
	cfg *config.Config,
	unitManager UnitManager,
	unitConfigUpdater UnitConfigUpdater,
	firmwareUpdater FirmwareUpdater,
	softwareUpdater SoftwareUpdater,
	instanceRunner InstanceRunner,
	downloader Downloader,
	storage Storage,
	statusSender StatusSender,
	systemQuotaAlertProvider SystemQuotaAlertProvider,
) (instance *Instance, err error) {
	log.Debug("Create unit status handler")

	instance = &Instance{
		unitManager:                unitManager,
		statusSender:               statusSender,
		sendStatusPeriod:           cfg.UnitStatusSendTimeout.Duration,
		newComponentsChannel:       firmwareUpdater.NewComponentsChannel(),
		nodeChangedChannel:         unitManager.SubscribeNodeInfoChange(),
		unitSubjectsChangedChannel: unitManager.SubscribeUnitSubjectsChanged(),
		systemQuotaAlertChannel:    systemQuotaAlertProvider.GetSystemQuoteAlertChannel(),
	}

	instance.resetUnitStatus()

	groupDownloader := newGroupDownloader(downloader)

	if instance.firmwareManager, err = newFirmwareManager(instance, groupDownloader, firmwareUpdater,
		storage, cfg.UMController.UpdateTTL.Duration); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if instance.softwareManager, err = newSoftwareManager(instance, groupDownloader, unitManager, unitConfigUpdater,
		softwareUpdater, instanceRunner, storage, cfg.SMController.UpdateTTL.Duration); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = instance.statusSender.SubscribeForConnectionEvents(instance); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go instance.handleChannels()

	return instance, nil
}

// Close closes unit status handler.
func (instance *Instance) Close() (err error) {
	instance.Lock()
	defer instance.Unlock()

	log.Debug("Close unit status handler")

	instance.statusMutex.Lock()

	if instance.statusTimer != nil {
		instance.statusTimer.Stop()
	}

	instance.statusMutex.Unlock()

	if managerErr := instance.firmwareManager.close(); managerErr != nil {
		if err == nil {
			err = aoserrors.Wrap(managerErr)
		}
	}

	if managerErr := instance.softwareManager.close(); managerErr != nil {
		if err == nil {
			err = aoserrors.Wrap(managerErr)
		}
	}

	return aoserrors.Wrap(err)
}

// SendUnitStatus send unit status.
func (instance *Instance) SendUnitStatus() error {
	instance.Lock()
	defer instance.Unlock()

	if err := instance.initCurrentStatus(); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.sendCurrentStatus(false)

	return nil
}

// ProcessRunStatus process current run instances status.
func (instance *Instance) ProcessRunStatus(instances []cloudprotocol.InstanceStatus) error {
	instance.Lock()
	defer instance.Unlock()

	log.Debug("Process run status")

	if instance.softwareManager.processRunStatus(instances) {
		return nil
	}

	if err := instance.initCurrentStatus(); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.initDone = true

	instance.sendCurrentStatus(false)

	return nil
}

// ProcessUpdateInstanceStatus process update instances status.
func (instance *Instance) ProcessUpdateInstanceStatus(statuses []cloudprotocol.InstanceStatus) {
	instance.Lock()
	defer instance.Unlock()

	for _, status := range statuses {
		instance.updateInstanceStatus(status)
	}
}

// ProcessDesiredStatus processes desired status.
func (instance *Instance) ProcessDesiredStatus(desiredStatus cloudprotocol.DesiredStatus) {
	instance.Lock()
	defer instance.Unlock()

	if err := instance.firmwareManager.processDesiredStatus(desiredStatus); err != nil {
		log.Errorf("Error processing firmware desired status: %s", err)
	}

	if err := instance.softwareManager.processDesiredStatus(desiredStatus); err != nil {
		log.Errorf("Error processing software desired status: %s", err)
	}
}

// GetFOTAStatusChannel returns FOTA status channels.
func (instance *Instance) GetFOTAStatusChannel() (channel <-chan cmserver.UpdateFOTAStatus) {
	instance.Lock()
	defer instance.Unlock()

	return instance.firmwareManager.statusChannel
}

// GetSOTAStatusChannel returns SOTA status channel.
func (instance *Instance) GetSOTAStatusChannel() (channel <-chan cmserver.UpdateSOTAStatus) {
	instance.Lock()
	defer instance.Unlock()

	return instance.softwareManager.statusChannel
}

// GetFOTAStatus returns FOTA current status.
func (instance *Instance) GetFOTAStatus() (status cmserver.UpdateFOTAStatus) {
	instance.Lock()
	defer instance.Unlock()

	return instance.firmwareManager.getCurrentStatus()
}

// GetSOTAStatus returns SOTA current status.
func (instance *Instance) GetSOTAStatus() (status cmserver.UpdateSOTAStatus) {
	instance.Lock()
	defer instance.Unlock()

	return instance.softwareManager.getCurrentStatus()
}

// StartFOTAUpdate triggers FOTA update.
func (instance *Instance) StartFOTAUpdate() (err error) {
	instance.Lock()
	defer instance.Unlock()

	return instance.firmwareManager.startUpdate()
}

// StartSOTAUpdate triggers SOTA update.
func (instance *Instance) StartSOTAUpdate() (err error) {
	instance.Lock()
	defer instance.Unlock()

	return instance.softwareManager.startUpdate()
}

// CloudConnected indicates unit connected to cloud.
func (instance *Instance) CloudConnected() {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	instance.isConnected = true
}

// CloudDisconnected indicates unit disconnected from cloud.
func (instance *Instance) CloudDisconnected() {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	instance.isConnected = false
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Instance) resetUnitStatus() {
	instance.unitStatus = cloudprotocol.UnitStatus{
		MessageType:  cloudprotocol.UnitStatusMessageType,
		UnitConfig:   make([]cloudprotocol.UnitConfigStatus, 0),
		Nodes:        make([]cloudprotocol.NodeInfo, 0),
		Services:     make([]cloudprotocol.ServiceStatus, 0),
		Instances:    make([]cloudprotocol.InstanceStatus, 0),
		Layers:       make([]cloudprotocol.LayerStatus, 0),
		Components:   make([]cloudprotocol.ComponentStatus, 0),
		UnitSubjects: make([]string, 0),
	}
}

func (instance *Instance) initUnitConfigStatus() error {
	unitConfigStatuses, err := instance.softwareManager.getUnitConfigStatuses()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range unitConfigStatuses {
		instance.updateUnitConfigStatus(status)
	}

	return nil
}

func (instance *Instance) initComponentsStatus() error {
	componentStatuses, err := instance.firmwareManager.getComponentStatuses()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range componentStatuses {
		instance.setComponentStatus(status)
	}

	return nil
}

func (instance *Instance) initServicesStatus() error {
	serviceStatuses, err := instance.softwareManager.getServiceStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range serviceStatuses {
		instance.setServiceStatus(status)
	}

	return nil
}

func (instance *Instance) initLayersStatus() error {
	layerStatuses, err := instance.softwareManager.getLayersStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range layerStatuses {
		instance.setLayerStatus(status)
	}

	return nil
}

func (instance *Instance) initInstancesStatus() error {
	instances, err := instance.softwareManager.getInstancesStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range instances {
		instance.setInstanceStatus(status)
	}

	return nil
}

func (instance *Instance) initNodesStatus() error {
	nodesInfo, err := instance.getAllNodesInfo()
	if err != nil {
		log.Errorf("Can't get nodes info: %v", err)
	}

	for _, info := range nodesInfo {
		instance.setNodeInfo(info)
	}

	return nil
}

func (instance *Instance) initUnitSubjects() error {
	subjects, err := instance.unitManager.GetUnitSubjects()
	if err != nil {
		log.Errorf("Can't get unit subjects: %v", err)
	}

	if len(subjects) > 0 {
		instance.setSubjects(subjects)
	}

	return nil
}

func (instance *Instance) initCurrentStatus() error {
	if err := instance.initUnitConfigStatus(); err != nil {
		return err
	}

	if err := instance.initComponentsStatus(); err != nil {
		return err
	}

	if err := instance.initServicesStatus(); err != nil {
		return err
	}

	if err := instance.initLayersStatus(); err != nil {
		return err
	}

	if err := instance.initInstancesStatus(); err != nil {
		return err
	}

	if err := instance.initNodesStatus(); err != nil {
		return err
	}

	if err := instance.initUnitSubjects(); err != nil {
		return err
	}

	return nil
}

func (instance *Instance) setUnitConfigStatus(status cloudprotocol.UnitConfigStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Set unit config status")

	index := slices.IndexFunc(instance.unitStatus.UnitConfig, func(unitConfigStatus cloudprotocol.UnitConfigStatus) bool {
		return unitConfigStatus.Version == status.Version
	})
	if index < 0 {
		instance.unitStatus.UnitConfig = append(instance.unitStatus.UnitConfig, status)
	} else {
		instance.unitStatus.UnitConfig[index] = status
	}

	instance.statusChanged()
}

func (instance *Instance) updateUnitConfigStatus(status cloudprotocol.UnitConfigStatus) {
	instance.setUnitConfigStatus(status)
	instance.statusChanged()
}

func (instance *Instance) setComponentStatus(status cloudprotocol.ComponentStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      status.ComponentID,
		"type":    status.ComponentType,
		"status":  status.Status,
		"version": status.Version,
		"nodeID":  status.NodeID,
		"error":   status.ErrorInfo,
	}).Debug("Set component status")

	index := slices.IndexFunc(instance.unitStatus.Components, func(componentStatus cloudprotocol.ComponentStatus) bool {
		return componentStatus.ComponentID == status.ComponentID && componentStatus.Version == status.Version
	})
	if index < 0 {
		instance.unitStatus.Components = append(instance.unitStatus.Components, status)
	} else {
		instance.unitStatus.Components[index] = status
	}
}

func (instance *Instance) updateComponentStatus(status cloudprotocol.ComponentStatus) {
	instance.setComponentStatus(status)
	instance.statusChanged()
}

func (instance *Instance) setLayerStatus(status cloudprotocol.LayerStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      status.LayerID,
		"digest":  status.Digest,
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Set layer status")

	index := slices.IndexFunc(instance.unitStatus.Layers, func(layerStatus cloudprotocol.LayerStatus) bool {
		return layerStatus.LayerID == status.LayerID && layerStatus.Version == status.Version
	})
	if index < 0 {
		instance.unitStatus.Layers = append(instance.unitStatus.Layers, status)
	} else {
		instance.unitStatus.Layers[index] = status
	}
}

func (instance *Instance) updateLayerStatus(status cloudprotocol.LayerStatus) {
	instance.setLayerStatus(status)
	instance.statusChanged()
}

func (instance *Instance) setServiceStatus(status cloudprotocol.ServiceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      status.ServiceID,
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Set service status")

	index := slices.IndexFunc(instance.unitStatus.Services, func(serviceStatus cloudprotocol.ServiceStatus) bool {
		return serviceStatus.ServiceID == status.ServiceID && serviceStatus.Version == status.Version
	})
	if index < 0 {
		instance.unitStatus.Services = append(instance.unitStatus.Services, status)
	} else {
		instance.unitStatus.Services[index] = status
	}
}

func (instance *Instance) updateServiceStatus(status cloudprotocol.ServiceStatus) {
	instance.setServiceStatus(status)
	instance.statusChanged()
}

func (instance *Instance) setInstanceStatus(status cloudprotocol.InstanceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"serviceID": status.InstanceIdent.ServiceID,
		"subjectID": status.InstanceIdent.SubjectID,
		"instance":  status.InstanceIdent.Instance,
		"version":   status.ServiceVersion,
		"status":    status.Status,
		"error":     status.ErrorInfo,
	}).Debug("Set instance status")

	index := slices.IndexFunc(instance.unitStatus.Instances, func(instanceStatus cloudprotocol.InstanceStatus) bool {
		return instanceStatus.InstanceIdent == status.InstanceIdent
	})
	if index < 0 {
		instance.unitStatus.Instances = append(instance.unitStatus.Instances, status)
	} else {
		instance.unitStatus.Instances[index] = status
	}
}

func (instance *Instance) updateInstanceStatus(status cloudprotocol.InstanceStatus) {
	instance.setInstanceStatus(status)
	instance.statusChanged()
}

func (instance *Instance) setNodeInfo(nodeInfo cloudprotocol.NodeInfo) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"nodeID":   nodeInfo.NodeID,
		"nodeType": nodeInfo.NodeType,
		"status":   nodeInfo.Status,
	}).Debug("Set node info")

	index := slices.IndexFunc(instance.unitStatus.Nodes, func(node cloudprotocol.NodeInfo) bool {
		return node.NodeID == nodeInfo.NodeID
	})
	if index < 0 {
		instance.unitStatus.Nodes = append(instance.unitStatus.Nodes, nodeInfo)
	} else {
		instance.unitStatus.Nodes[index] = nodeInfo
	}
}

func (instance *Instance) updateNodeInfo(nodeInfo cloudprotocol.NodeInfo) {
	instance.setNodeInfo(nodeInfo)
	instance.statusChanged()
}

func (instance *Instance) setSubjects(subjects []string) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithField("subjects", subjects).Debug("Set subjects")

	instance.unitStatus.UnitSubjects = subjects
}

func (instance *Instance) updateSubjects(subjects []string) {
	instance.setSubjects(subjects)
	instance.statusChanged()
}

func (instance *Instance) statusChanged() {
	if instance.statusTimer != nil {
		return
	}

	instance.statusTimer = time.AfterFunc(instance.sendStatusPeriod, func() {
		instance.statusMutex.Lock()
		defer instance.statusMutex.Unlock()

		instance.sendCurrentStatus(true)
	})
}

func (instance *Instance) sendCurrentStatus(deltaStatus bool) {
	if instance.statusTimer != nil {
		instance.statusTimer.Stop()
		instance.statusTimer = nil
	}

	defer instance.resetUnitStatus()

	log.WithField("deltaStatus", deltaStatus).Debug("Send current status")

	if !instance.initDone {
		return
	}

	if !instance.isConnected {
		return
	}

	if !deltaStatus {
		if err := instance.statusSender.SendUnitStatus(
			instance.unitStatus); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
			log.Errorf("Can't send unit status: %s", err)
		}

		return
	}

	deltaUnitStatus := cloudprotocol.DeltaUnitStatus(instance.unitStatus)
	deltaUnitStatus.IsDeltaInfo = true

	if err := instance.statusSender.SendDeltaUnitStatus(
		deltaUnitStatus); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
		log.Errorf("Can't send delta unit status: %s", err)
	}
}

func (instance *Instance) getAllNodesInfo() ([]cloudprotocol.NodeInfo, error) {
	nodeIDs, err := instance.unitManager.GetAllNodeIDs()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	nodesInfo := make([]cloudprotocol.NodeInfo, 0, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		nodeInfo, err := instance.unitManager.GetNodeInfo(nodeID)
		if err != nil {
			log.WithField("nodeID", nodeID).Errorf("Can't get node info: %s", err)
			continue
		}

		nodesInfo = append(nodesInfo, nodeInfo)
	}

	return nodesInfo, nil
}

func (instance *Instance) getNodesStatus() ([]cloudprotocol.NodeStatus, error) {
	nodesInfo, err := instance.getAllNodesInfo()
	if err != nil {
		return nil, err
	}

	nodesStatus := make([]cloudprotocol.NodeStatus, 0, len(nodesInfo))

	for _, nodeInfo := range nodesInfo {
		nodesStatus = append(nodesStatus, cloudprotocol.NodeStatus{NodeID: nodeInfo.NodeID, Status: nodeInfo.Status})
	}

	return nodesStatus, nil
}

func (instance *Instance) handleChannels() {
	for {
		select {
		case newComponents, ok := <-instance.newComponentsChannel:
			if !ok {
				return
			}

			for _, componentStatus := range newComponents {
				instance.updateComponentStatus(componentStatus)
			}

		case nodeInfo, ok := <-instance.nodeChangedChannel:
			if !ok {
				return
			}

			instance.updateNodeInfo(nodeInfo)

			if err := instance.softwareManager.requestRebalancing(); err != nil {
				log.Errorf("Can't perform rebalancing: %v", err)
			}

		case subjects, ok := <-instance.unitSubjectsChangedChannel:
			if !ok {
				return
			}

			instance.updateSubjects(subjects)

		case systemQuotaAlert, ok := <-instance.systemQuotaAlertChannel:
			if !ok {
				return
			}

			if systemQuotaAlert.Status == resourcemonitor.AlertStatusFall {
				continue
			}

			if slices.Contains([]string{"cpu", "ram"}, systemQuotaAlert.Parameter) {
				if err := instance.softwareManager.requestRebalancing(); err != nil {
					log.Errorf("Can't perform rebalancing: %v", err)
				}
			}
		}
	}
}
