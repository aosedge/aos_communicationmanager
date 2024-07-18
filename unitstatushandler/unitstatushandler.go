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
	"sync/atomic"
	"time"

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

// NodeManager manages nodes.
type NodeManager interface {
	GetAllNodeIDs() ([]string, error)
	GetNodeInfo(nodeID string) (cloudprotocol.NodeInfo, error)
	SubscribeNodeInfoChange() <-chan cloudprotocol.NodeInfo
	PauseNode(nodeID string) error
	ResumeNode(nodeID string) error
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
	RunInstances(instances []cloudprotocol.InstanceInfo, newServices []string) error
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

// RunInstancesStatus run instances status.
type RunInstancesStatus struct {
	UnitSubjects  []string
	Instances     []cloudprotocol.InstanceStatus
	ErrorServices []cloudprotocol.ServiceStatus
}

// Instance instance of unit status handler.
type Instance struct {
	sync.Mutex

	nodeManager  NodeManager
	statusSender StatusSender

	statusMutex sync.Mutex

	unitStatus       unitStatus
	statusTimer      *time.Timer
	sendStatusPeriod time.Duration

	firmwareManager *firmwareManager
	softwareManager *softwareManager

	newComponentsChannel    <-chan []cloudprotocol.ComponentStatus
	nodeChangedChannel      <-chan cloudprotocol.NodeInfo
	systemQuotaAlertChannel <-chan cloudprotocol.SystemQuotaAlert

	initDone    bool
	isConnected int32
}

type unitStatus struct {
	subjects   []string
	unitConfig itemStatus
	components map[string]*itemStatus
	layers     map[string]*itemStatus
	services   map[string]*itemStatus
	instances  []cloudprotocol.InstanceStatus
	nodes      []cloudprotocol.NodeInfo
}

type statusDescriptor struct {
	amqpStatus interface{}
}

type itemStatus []statusDescriptor

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new unit status handler instance.
func New(
	cfg *config.Config,
	nodeManager NodeManager,
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
		nodeManager:             nodeManager,
		statusSender:            statusSender,
		sendStatusPeriod:        cfg.UnitStatusSendTimeout.Duration,
		newComponentsChannel:    firmwareUpdater.NewComponentsChannel(),
		nodeChangedChannel:      nodeManager.SubscribeNodeInfoChange(),
		systemQuotaAlertChannel: systemQuotaAlertProvider.GetSystemQuoteAlertChannel(),
	}

	// Initialize maps of statuses for avoiding situation of adding values to uninitialized map on go routine
	instance.unitStatus.components = make(map[string]*itemStatus)
	instance.unitStatus.layers = make(map[string]*itemStatus)
	instance.unitStatus.services = make(map[string]*itemStatus)

	groupDownloader := newGroupDownloader(downloader)

	if instance.firmwareManager, err = newFirmwareManager(instance, groupDownloader, firmwareUpdater,
		storage, cfg.UMController.UpdateTTL.Duration); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if instance.softwareManager, err = newSoftwareManager(instance, groupDownloader, nodeManager, unitConfigUpdater,
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

	instance.sendCurrentStatus()

	return nil
}

// ProcessRunStatus process current run instances status.
func (instance *Instance) ProcessRunStatus(status RunInstancesStatus) error {
	instance.Lock()
	defer instance.Unlock()

	if err := instance.initCurrentStatus(); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.unitStatus.subjects = status.UnitSubjects
	instance.unitStatus.instances = status.Instances

	nodesInfo, err := instance.getAllNodesInfo()
	if err != nil {
		log.Errorf("Can't get nodes info: %v", err)
	}

	instance.unitStatus.nodes = nodesInfo

	instance.softwareManager.processRunStatus(status)
	instance.sendCurrentStatus()

	return nil
}

// ProcessUpdateInstanceStatus process update instances status.
func (instance *Instance) ProcessUpdateInstanceStatus(status []cloudprotocol.InstanceStatus) {
	instance.Lock()
	defer instance.Unlock()

	instance.updateInstanceStatus(status)
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
	atomic.StoreInt32(&instance.isConnected, 1)
}

// CloudDisconnected indicates unit disconnected from cloud.
func (instance *Instance) CloudDisconnected() {
	atomic.StoreInt32(&instance.isConnected, 0)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Instance) initCurrentStatus() error {
	instance.unitStatus.unitConfig = nil
	instance.unitStatus.components = make(map[string]*itemStatus)
	instance.unitStatus.services = make(map[string]*itemStatus)
	instance.unitStatus.layers = make(map[string]*itemStatus)

	// Get initial unit config info

	unitConfigStatuses, err := instance.softwareManager.getUnitConfigStatuses()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range unitConfigStatuses {
		log.WithFields(log.Fields{
			"status":  status.Status,
			"version": status.Version,
			"error":   status.ErrorInfo,
		}).Debug("Initial unit config status")

		instance.processUnitConfigStatus(status)
	}

	// Get initial components info

	componentStatuses, err := instance.firmwareManager.getComponentStatuses()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range componentStatuses {
		log.WithFields(log.Fields{
			"id":      status.ComponentID,
			"type":    status.ComponentType,
			"status":  status.Status,
			"version": status.Version,
			"error":   status.ErrorInfo,
		}).Debug("Initial component status")

		instance.processComponentStatus(status)
	}

	// Get initial services and layers info

	serviceStatuses, err := instance.softwareManager.getServiceStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range serviceStatuses {
		if _, ok := instance.unitStatus.services[status.ServiceID]; !ok {
			instance.unitStatus.services[status.ServiceID] = &itemStatus{}
		}

		log.WithFields(log.Fields{
			"id":      status.ServiceID,
			"status":  status.Status,
			"version": status.Version,
			"error":   status.ErrorInfo,
		}).Debug("Initial service status")

		instance.processServiceStatus(status)
	}

	layerStatuses, err := instance.softwareManager.getLayersStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range layerStatuses {
		if _, ok := instance.unitStatus.layers[status.Digest]; !ok {
			instance.unitStatus.layers[status.Digest] = &itemStatus{}
		}

		log.WithFields(log.Fields{
			"id":      status.LayerID,
			"digest":  status.Digest,
			"status":  status.Status,
			"version": status.Version,
			"error":   status.ErrorInfo,
		}).Debug("Initial layer status")

		instance.processLayerStatus(status)
	}

	instance.initDone = true

	return nil
}

func (descriptor *statusDescriptor) getStatus() (status string) {
	switch amqpStatus := descriptor.amqpStatus.(type) {
	case *cloudprotocol.UnitConfigStatus:
		return amqpStatus.Status

	case *cloudprotocol.ComponentStatus:
		return amqpStatus.Status

	case *cloudprotocol.LayerStatus:
		return amqpStatus.Status

	case *cloudprotocol.ServiceStatus:
		return amqpStatus.Status

	default:
		return cloudprotocol.UnknownStatus
	}
}

func (descriptor *statusDescriptor) getVersion() (version string) {
	switch amqpStatus := descriptor.amqpStatus.(type) {
	case *cloudprotocol.UnitConfigStatus:
		return amqpStatus.Version

	case *cloudprotocol.ComponentStatus:
		return amqpStatus.Version

	case *cloudprotocol.LayerStatus:
		return amqpStatus.Version

	case *cloudprotocol.ServiceStatus:
		return amqpStatus.Version

	default:
		return ""
	}
}

func (instance *Instance) updateUnitConfigStatus(status cloudprotocol.UnitConfigStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Update unit config status")

	instance.processUnitConfigStatus(status)
	instance.statusChanged()
}

func (instance *Instance) processUnitConfigStatus(status cloudprotocol.UnitConfigStatus) {
	instance.updateStatus(&instance.unitStatus.unitConfig, statusDescriptor{&status})
}

func (instance *Instance) updateComponentStatus(status cloudprotocol.ComponentStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      status.ComponentID,
		"type":    status.ComponentType,
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Update component status")

	instance.processComponentStatus(status)
	instance.statusChanged()
}

func (instance *Instance) processComponentStatus(status cloudprotocol.ComponentStatus) {
	componentStatus, ok := instance.unitStatus.components[status.ComponentID]
	if !ok {
		componentStatus = &itemStatus{}
		instance.unitStatus.components[status.ComponentID] = componentStatus
	}

	instance.updateStatus(componentStatus, statusDescriptor{&status})
}

func (instance *Instance) updateLayerStatus(status cloudprotocol.LayerStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      status.LayerID,
		"digest":  status.Digest,
		"status":  status.Status,
		"version": status.Version,
		"error":   status.ErrorInfo,
	}).Debug("Update layer status")

	instance.processLayerStatus(status)
	instance.statusChanged()
}

func (instance *Instance) processLayerStatus(status cloudprotocol.LayerStatus) {
	layerStatus, ok := instance.unitStatus.layers[status.Digest]
	if !ok {
		layerStatus = &itemStatus{}
		instance.unitStatus.layers[status.Digest] = layerStatus
	}

	instance.updateStatus(layerStatus, statusDescriptor{&status})
}

func (instance *Instance) updateServiceStatus(serviceInfo cloudprotocol.ServiceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":      serviceInfo.ServiceID,
		"status":  serviceInfo.Status,
		"version": serviceInfo.Version,
		"error":   serviceInfo.ErrorInfo,
	}).Debug("Update service status")

	instance.processServiceStatus(serviceInfo)
	instance.statusChanged()
}

func (instance *Instance) processServiceStatus(status cloudprotocol.ServiceStatus) {
	serviceStatus, ok := instance.unitStatus.services[status.ServiceID]
	if !ok {
		serviceStatus = &itemStatus{}
		instance.unitStatus.services[status.ServiceID] = serviceStatus
	}

	instance.updateStatus(serviceStatus, statusDescriptor{&status})
}

func (instance *Instance) updateInstanceStatus(status []cloudprotocol.InstanceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	newStatuses := []cloudprotocol.InstanceStatus{}

foundLoop:
	for _, instanceStatus := range status {
		for i := range instance.unitStatus.instances {
			if instanceStatus.InstanceIdent == instance.unitStatus.instances[i].InstanceIdent &&
				instanceStatus.ServiceVersion == instance.unitStatus.instances[i].ServiceVersion {
				log.WithFields(log.Fields{
					"serviceID": instanceStatus.InstanceIdent.ServiceID,
					"subjectID": instanceStatus.InstanceIdent.SubjectID,
					"instance":  instanceStatus.InstanceIdent.Instance,
					"version":   instanceStatus.ServiceVersion,
					"runState":  instanceStatus.RunState,
					"error":     instanceStatus.ErrorInfo,
				}).Debug("Update instance status")

				instance.unitStatus.instances[i].StateChecksum = instanceStatus.StateChecksum
				instance.unitStatus.instances[i].RunState = instanceStatus.RunState
				instance.unitStatus.instances[i].ErrorInfo = instanceStatus.ErrorInfo

				continue foundLoop
			}
		}

		newStatuses = append(newStatuses, instanceStatus)
	}

	instance.unitStatus.instances = append(instance.unitStatus.instances, newStatuses...)

	instance.statusChanged()
}

func (instance *Instance) setInstancesStatus(statuses []cloudprotocol.InstanceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	instance.unitStatus.instances = statuses

	instance.statusChanged()
}

func (instance *Instance) updateNodeInfo(nodeInfo cloudprotocol.NodeInfo) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"nodeID":   nodeInfo.NodeID,
		"nodeType": nodeInfo.NodeType,
		"status":   nodeInfo.Status,
	}).Debug("Node info changed")

	nodeInfoFound := false

	for i, curNodeInfo := range instance.unitStatus.nodes {
		if curNodeInfo.NodeID == nodeInfo.NodeID {
			instance.unitStatus.nodes[i] = nodeInfo
			nodeInfoFound = true

			break
		}
	}

	if !nodeInfoFound {
		instance.unitStatus.nodes = append(instance.unitStatus.nodes, nodeInfo)
	}

	instance.statusChanged()
}

func (instance *Instance) statusChanged() {
	if instance.statusTimer != nil {
		return
	}

	instance.statusTimer = time.AfterFunc(instance.sendStatusPeriod, func() {
		instance.statusMutex.Lock()
		defer instance.statusMutex.Unlock()

		instance.sendCurrentStatus()
	})
}

func (instance *Instance) updateStatus(status *itemStatus, descriptor statusDescriptor) {
	if descriptor.getStatus() == cloudprotocol.InstalledStatus {
		*status = itemStatus{descriptor}
		return
	}

	for i, element := range *status {
		if element.getVersion() == descriptor.getVersion() {
			(*status)[i] = descriptor
			return
		}
	}

	*status = append(*status, descriptor)
}

func (instance *Instance) sendCurrentStatus() {
	if instance.statusTimer != nil {
		instance.statusTimer.Stop()
		instance.statusTimer = nil
	}

	if !instance.initDone {
		return
	}

	if atomic.LoadInt32(&instance.isConnected) != 1 {
		return
	}

	unitStatus := cloudprotocol.UnitStatus{
		UnitSubjects: instance.unitStatus.subjects,
		Components:   make([]cloudprotocol.ComponentStatus, 0, len(instance.unitStatus.components)),
		Layers:       make([]cloudprotocol.LayerStatus, 0, len(instance.unitStatus.layers)),
		Services:     make([]cloudprotocol.ServiceStatus, 0, len(instance.unitStatus.services)),
		Instances:    instance.unitStatus.instances,
		Nodes:        instance.unitStatus.nodes,
	}

	for _, status := range instance.unitStatus.unitConfig {
		unitConfig, ok := status.amqpStatus.(*cloudprotocol.UnitConfigStatus)
		if !ok {
			log.Error("Incorrect unit config type")
			continue
		}

		unitStatus.UnitConfig = append(unitStatus.UnitConfig, *unitConfig)
	}

	for _, componentStatus := range instance.unitStatus.components {
		for _, status := range *componentStatus {
			status, ok := status.amqpStatus.(*cloudprotocol.ComponentStatus)
			if !ok {
				log.Error("Incorrect component status type")
				continue
			}

			unitStatus.Components = append(unitStatus.Components, *status)
		}
	}

	for _, layerStatus := range instance.unitStatus.layers {
		for _, status := range *layerStatus {
			status, ok := status.amqpStatus.(*cloudprotocol.LayerStatus)
			if !ok {
				log.Error("Incorrect layer status type")
				continue
			}

			unitStatus.Layers = append(unitStatus.Layers, *status)
		}
	}

	for _, serviceStatus := range instance.unitStatus.services {
		for _, status := range *serviceStatus {
			status, ok := status.amqpStatus.(*cloudprotocol.ServiceStatus)
			if !ok {
				log.Error("Incorrect service status type")
				continue
			}

			unitStatus.Services = append(unitStatus.Services, *status)
		}
	}

	if err := instance.statusSender.SendUnitStatus(
		unitStatus); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
		log.Errorf("Can't send unit status: %s", err)
	}
}

func (instance *Instance) getAllNodesInfo() ([]cloudprotocol.NodeInfo, error) {
	nodeIDs, err := instance.nodeManager.GetAllNodeIDs()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	nodesInfo := make([]cloudprotocol.NodeInfo, 0, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		nodeInfo, err := instance.nodeManager.GetNodeInfo(nodeID)
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

		case systemQuotaAlert, ok := <-instance.systemQuotaAlertChannel:
			if !ok {
				return
			}

			if systemQuotaAlert.Status == resourcemonitor.AlertStatusFall {
				return
			}

			for _, param := range []string{"cpu", "ram"} {
				if param != systemQuotaAlert.Parameter {
					continue
				}

				if err := instance.softwareManager.requestRebalancing(); err != nil {
					log.Errorf("Can't perform rebalancing: %v", err)
				}

				break
			}
		}
	}
}
