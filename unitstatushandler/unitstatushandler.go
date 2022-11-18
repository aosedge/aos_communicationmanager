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
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/cmserver"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Downloader downloads packages.
type Downloader interface {
	DownloadAndDecrypt(
		ctx context.Context, packageInfo cloudprotocol.DecryptDataStruct,
		chains []cloudprotocol.CertificateChain,
		certs []cloudprotocol.Certificate) (result downloader.Result, err error)
}

// StatusSender sends unit status to cloud.
type StatusSender interface {
	SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error)
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
}

// UnitConfigUpdater updates unit configuration.
type UnitConfigUpdater interface {
	GetStatus() (unitConfigInfo cloudprotocol.UnitConfigStatus, err error)
	GetUnitConfigVersion(configJSON json.RawMessage) (vendorVersion string, err error)
	CheckUnitConfig(configJSON json.RawMessage) (vendorVersion string, err error)
	UpdateUnitConfig(configJSON json.RawMessage) (err error)
}

// FirmwareUpdater updates system components.
type FirmwareUpdater interface {
	GetStatus() (componentsInfo []cloudprotocol.ComponentStatus, err error)
	UpdateComponents(components []cloudprotocol.ComponentInfo) (status []cloudprotocol.ComponentStatus, err error)
}

// SoftwareUpdater updates services, layers, instances runner.
type SoftwareUpdater interface {
	GetServicesStatus() ([]ServiceStatus, error)
	GetLayersStatus() ([]LayerStatus, error)
	InstallService(serviceInfo cloudprotocol.ServiceInfo) error
	RestoreService(serviceID string) error
	RemoveService(serviceID string) error
	InstallLayer(layerInfo cloudprotocol.LayerInfo) error
	RemoveLayer(digest string) error
	RestoreLayer(digest string) error
	RunInstances(instances []cloudprotocol.InstanceInfo) error
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

	downloader   Downloader
	statusSender StatusSender

	statusMutex sync.Mutex

	statusTimer       *time.Timer
	unitSubjects      []string
	unitConfigStatus  itemStatus
	componentStatuses map[string]*itemStatus
	layerStatuses     map[string]*itemStatus
	serviceStatuses   map[string]*itemStatus
	instanceStatuses  []cloudprotocol.InstanceStatus

	sendStatusPeriod time.Duration

	firmwareManager *firmwareManager
	softwareManager *softwareManager

	decryptDir  string
	initDone    bool
	isConnected int32
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
	unitConfigUpdater UnitConfigUpdater,
	firmwareUpdater FirmwareUpdater,
	softwareUpdater SoftwareUpdater,
	downloader Downloader,
	storage Storage,
	statusSender StatusSender,
) (instance *Instance, err error) {
	log.Debug("Create unit status handler")

	instance = &Instance{
		statusSender:     statusSender,
		downloader:       downloader,
		sendStatusPeriod: cfg.UnitStatusSendTimeout.Duration,
		decryptDir:       cfg.Downloader.DecryptDir,
	}

	// Initialize maps of statuses for avoiding situation of adding values to uninitialized map on go routine
	instance.componentStatuses = make(map[string]*itemStatus)
	instance.layerStatuses = make(map[string]*itemStatus)
	instance.serviceStatuses = make(map[string]*itemStatus)

	if instance.firmwareManager, err = newFirmwareManager(instance, firmwareUpdater, unitConfigUpdater,
		storage, cfg.UMController.UpdateTTL.Duration); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if instance.softwareManager, err = newSoftwareManager(instance, softwareUpdater,
		storage, cfg.SMController.UpdateTTL.Duration); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = instance.statusSender.SubscribeForConnectionEvents(instance); err != nil {
		return nil, aoserrors.Wrap(err)
	}

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

	instance.unitSubjects = status.UnitSubjects
	instance.instanceStatuses = status.Instances

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

	if instance.firmwareManager.getCurrentUpdateState() == cmserver.NoUpdate &&
		instance.softwareManager.getCurrentUpdateState() == cmserver.NoUpdate &&
		instance.decryptDir != "" {
		if err := instance.clearDecryptDir(); err != nil {
			log.Errorf("Error clearing decrypt dir: %s", err)
		}
	}

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
	instance.unitConfigStatus = nil
	instance.componentStatuses = make(map[string]*itemStatus)
	instance.serviceStatuses = make(map[string]*itemStatus)
	instance.layerStatuses = make(map[string]*itemStatus)

	// Get initial unit config info

	unitConfigStatuses, err := instance.firmwareManager.getUnitConfigStatuses()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range unitConfigStatuses {
		log.WithFields(log.Fields{
			"status":        status.Status,
			"vendorVersion": status.VendorVersion,
			"error":         status.ErrorInfo,
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
			"id":            status.ID,
			"status":        status.Status,
			"vendorVersion": status.VendorVersion,
			"error":         status.ErrorInfo,
		}).Debug("Initial component status")

		instance.processComponentStatus(status)
	}

	// Get initial services and layers info

	serviceStatuses, err := instance.softwareManager.getServiceStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range serviceStatuses {
		if _, ok := instance.serviceStatuses[status.ID]; !ok {
			instance.serviceStatuses[status.ID] = &itemStatus{}
		}

		log.WithFields(log.Fields{
			"id":         status.ID,
			"status":     status.Status,
			"aosVersion": status.AosVersion,
			"error":      status.ErrorInfo,
		}).Debug("Initial service status")

		instance.processServiceStatus(status)
	}

	layerStatuses, err := instance.softwareManager.getLayersStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, status := range layerStatuses {
		if _, ok := instance.layerStatuses[status.Digest]; !ok {
			instance.layerStatuses[status.Digest] = &itemStatus{}
		}

		log.WithFields(log.Fields{
			"id":         status.ID,
			"digest":     status.Digest,
			"status":     status.Status,
			"aosVersion": status.AosVersion,
			"error":      status.ErrorInfo,
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
		return amqpStatus.VendorVersion

	case *cloudprotocol.ComponentStatus:
		return amqpStatus.VendorVersion

	case *cloudprotocol.LayerStatus:
		return strconv.FormatUint(amqpStatus.AosVersion, 10)

	case *cloudprotocol.ServiceStatus:
		return strconv.FormatUint(amqpStatus.AosVersion, 10)

	default:
		return ""
	}
}

func (instance *Instance) updateUnitConfigStatus(unitConfigInfo cloudprotocol.UnitConfigStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"status":        unitConfigInfo.Status,
		"vendorVersion": unitConfigInfo.VendorVersion,
		"error":         unitConfigInfo.ErrorInfo,
	}).Debug("Update unit config status")

	instance.processUnitConfigStatus(unitConfigInfo)
	instance.statusChanged()
}

func (instance *Instance) processUnitConfigStatus(unitConfigInfo cloudprotocol.UnitConfigStatus) {
	instance.updateStatus(&instance.unitConfigStatus, statusDescriptor{&unitConfigInfo})
}

func (instance *Instance) updateComponentStatus(componentInfo cloudprotocol.ComponentStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":            componentInfo.ID,
		"status":        componentInfo.Status,
		"vendorVersion": componentInfo.VendorVersion,
		"error":         componentInfo.ErrorInfo,
	}).Debug("Update component status")

	instance.processComponentStatus(componentInfo)
	instance.statusChanged()
}

func (instance *Instance) processComponentStatus(componentInfo cloudprotocol.ComponentStatus) {
	componentStatus, ok := instance.componentStatuses[componentInfo.ID]
	if !ok {
		componentStatus = &itemStatus{}
		instance.componentStatuses[componentInfo.ID] = componentStatus
	}

	instance.updateStatus(componentStatus, statusDescriptor{&componentInfo})
}

func (instance *Instance) updateLayerStatus(layerInfo cloudprotocol.LayerStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":         layerInfo.ID,
		"digest":     layerInfo.Digest,
		"status":     layerInfo.Status,
		"aosVersion": layerInfo.AosVersion,
		"error":      layerInfo.ErrorInfo,
	}).Debug("Update layer status")

	if _, ok := instance.layerStatuses[layerInfo.Digest]; !ok {
		instance.layerStatuses[layerInfo.Digest] = &itemStatus{}
	}

	instance.processLayerStatus(layerInfo)
	instance.statusChanged()
}

func (instance *Instance) processLayerStatus(layerInfo cloudprotocol.LayerStatus) {
	layerStatus, ok := instance.layerStatuses[layerInfo.Digest]
	if !ok {
		layerStatus = &itemStatus{}
		instance.layerStatuses[layerInfo.Digest] = layerStatus
	}

	instance.updateStatus(layerStatus, statusDescriptor{&layerInfo})
}

func (instance *Instance) updateServiceStatus(serviceInfo cloudprotocol.ServiceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	log.WithFields(log.Fields{
		"id":         serviceInfo.ID,
		"status":     serviceInfo.Status,
		"aosVersion": serviceInfo.AosVersion,
		"error":      serviceInfo.ErrorInfo,
	}).Debug("Update service status")

	instance.processServiceStatus(serviceInfo)
	instance.statusChanged()
}

func (instance *Instance) processServiceStatus(serviceInfo cloudprotocol.ServiceStatus) {
	serviceStatus, ok := instance.serviceStatuses[serviceInfo.ID]
	if !ok {
		serviceStatus = &itemStatus{}
		instance.serviceStatuses[serviceInfo.ID] = serviceStatus
	}

	instance.updateStatus(serviceStatus, statusDescriptor{&serviceInfo})
}

func (instance *Instance) updateInstanceStatus(status []cloudprotocol.InstanceStatus) {
	instance.statusMutex.Lock()
	defer instance.statusMutex.Unlock()

	newStatuses := []cloudprotocol.InstanceStatus{}

foundloop:
	for _, instanceStauts := range status {
		for i := range instance.instanceStatuses {
			if instanceStauts.InstanceIdent == instance.instanceStatuses[i].InstanceIdent &&
				instanceStauts.AosVersion == instance.instanceStatuses[i].AosVersion {
				log.WithFields(log.Fields{
					"serviceID":  instanceStauts.InstanceIdent.ServiceID,
					"subjectID":  instanceStauts.InstanceIdent.ServiceID,
					"instance":   instanceStauts.InstanceIdent.Instance,
					"aosVersion": instanceStauts.AosVersion,
					"runState":   instanceStauts.RunState,
					"error":      instanceStauts.ErrorInfo,
				}).Debug("Update instance status")

				instance.instanceStatuses[i].StateChecksum = instanceStauts.StateChecksum
				instance.instanceStatuses[i].RunState = instanceStauts.RunState
				instance.instanceStatuses[i].ErrorInfo = instanceStauts.ErrorInfo

				continue foundloop
			}
		}

		newStatuses = append(newStatuses, instanceStauts)
	}

	instance.instanceStatuses = append(instance.instanceStatuses, newStatuses...)

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
		UnitSubjects: instance.unitSubjects,
		Components:   make([]cloudprotocol.ComponentStatus, 0, len(instance.componentStatuses)),
		Layers:       make([]cloudprotocol.LayerStatus, 0, len(instance.layerStatuses)),
		Services:     make([]cloudprotocol.ServiceStatus, 0, len(instance.serviceStatuses)),
		Instances:    instance.instanceStatuses,
	}

	for _, status := range instance.unitConfigStatus {
		unitConfig, ok := status.amqpStatus.(*cloudprotocol.UnitConfigStatus)
		if !ok {
			log.Error("Incorrect unit config type")
			continue
		}

		unitStatus.UnitConfig = append(unitStatus.UnitConfig, *unitConfig)
	}

	for _, componentStatus := range instance.componentStatuses {
		for _, status := range *componentStatus {
			status, ok := status.amqpStatus.(*cloudprotocol.ComponentStatus)
			if !ok {
				log.Error("Incorrect component status type")
				continue
			}

			unitStatus.Components = append(unitStatus.Components, *status)
		}
	}

	for _, layerStatus := range instance.layerStatuses {
		for _, status := range *layerStatus {
			status, ok := status.amqpStatus.(*cloudprotocol.LayerStatus)
			if !ok {
				log.Error("Incorrect layer status type")
				continue
			}

			unitStatus.Layers = append(unitStatus.Layers, *status)
		}
	}

	for _, serviceStatus := range instance.serviceStatuses {
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

func (instance *Instance) clearDecryptDir() (err error) {
	files, err := ioutil.ReadDir(instance.decryptDir)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, file := range files {
		fileName := path.Join(instance.decryptDir, file.Name())

		log.WithFields(log.Fields{"file": fileName}).Debug("Remove outdated decrypt file")

		if err = os.RemoveAll(fileName); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}
