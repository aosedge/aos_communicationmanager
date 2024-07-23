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
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/utils/action"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/downloader"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const maxConcurrentActions = 10

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type softwareDownloader interface {
	download(ctx context.Context, request map[string]downloader.PackageInfo,
		continueOnError bool, notifier statusNotifier) (result map[string]*downloadResult)
	releaseDownloadedSoftware() error
}
type softwareStatusHandler interface {
	updateLayerStatus(status cloudprotocol.LayerStatus)
	updateServiceStatus(status cloudprotocol.ServiceStatus)
	updateUnitConfigStatus(status cloudprotocol.UnitConfigStatus)
	setInstancesStatus(statuses []cloudprotocol.InstanceStatus)
	getNodesStatus() ([]cloudprotocol.NodeStatus, error)
}

type softwareUpdate struct {
	Schedule         cloudprotocol.ScheduleRule       `json:"schedule,omitempty"`
	UnitConfig       *cloudprotocol.UnitConfig        `json:"unitConfig,omitempty"`
	InstallServices  []cloudprotocol.ServiceInfo      `json:"installServices,omitempty"`
	RemoveServices   []cloudprotocol.ServiceStatus    `json:"removeServices,omitempty"`
	RestoreServices  []cloudprotocol.ServiceInfo      `json:"restoreServices,omitempty"`
	InstallLayers    []cloudprotocol.LayerInfo        `json:"installLayers,omitempty"`
	RemoveLayers     []cloudprotocol.LayerStatus      `json:"removeLayers,omitempty"`
	RestoreLayers    []cloudprotocol.LayerStatus      `json:"restoreLayers,omitempty"`
	RunInstances     []cloudprotocol.InstanceInfo     `json:"runInstances,omitempty"`
	CertChains       []cloudprotocol.CertificateChain `json:"certChains,omitempty"`
	Certs            []cloudprotocol.Certificate      `json:"certs,omitempty"`
	NodesStatus      []cloudprotocol.NodeStatus       `json:"nodesStatus,omitempty"`
	RebalanceRequest bool                             `json:"rebalanceRequest,omitempty"`
}

type softwareManager struct {
	sync.Mutex
	runCond *sync.Cond

	statusChannel chan cmserver.UpdateSOTAStatus

	nodeManager       NodeManager
	unitConfigUpdater UnitConfigUpdater
	downloader        softwareDownloader
	statusHandler     softwareStatusHandler
	softwareUpdater   SoftwareUpdater
	instanceRunner    InstanceRunner
	storage           Storage

	stateMachine  *updateStateMachine
	actionHandler *action.Handler
	statusMutex   sync.RWMutex
	pendingUpdate *softwareUpdate

	LayerStatuses    map[string]*cloudprotocol.LayerStatus   `json:"layerStatuses,omitempty"`
	ServiceStatuses  map[string]*cloudprotocol.ServiceStatus `json:"serviceStatuses,omitempty"`
	InstanceStatuses []cloudprotocol.InstanceStatus          `json:"instanceStatuses,omitempty"`
	UnitConfigStatus cloudprotocol.UnitConfigStatus          `json:"unitConfigStatus,omitempty"`
	CurrentUpdate    *softwareUpdate                         `json:"currentUpdate,omitempty"`
	DownloadResult   map[string]*downloadResult              `json:"downloadResult,omitempty"`
	CurrentState     string                                  `json:"currentState,omitempty"`
	UpdateErr        *cloudprotocol.ErrorInfo                `json:"updateErr,omitempty"`
	TTLDate          time.Time                               `json:"ttlDate,omitempty"`
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newSoftwareManager(statusHandler softwareStatusHandler, downloader softwareDownloader, nodeManager NodeManager,
	unitConfigUpdater UnitConfigUpdater, softwareUpdater SoftwareUpdater, instanceRunner InstanceRunner,
	storage Storage, defaultTTL time.Duration,
) (manager *softwareManager, err error) {
	manager = &softwareManager{
		statusChannel:     make(chan cmserver.UpdateSOTAStatus, 1),
		downloader:        downloader,
		statusHandler:     statusHandler,
		nodeManager:       nodeManager,
		unitConfigUpdater: unitConfigUpdater,
		softwareUpdater:   softwareUpdater,
		instanceRunner:    instanceRunner,
		actionHandler:     action.New(maxConcurrentActions),
		storage:           storage,
		CurrentState:      stateNoUpdate,
	}

	manager.runCond = sync.NewCond(&manager.Mutex)

	if err = manager.loadState(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"state": manager.CurrentState, "error": manager.UpdateErr}).Debug("New software manager")

	manager.stateMachine = newUpdateStateMachine(manager.CurrentState, fsm.Events{
		// no update state
		{Name: eventStartDownload, Src: []string{stateNoUpdate}, Dst: stateDownloading},
		{Name: eventReadyToUpdate, Src: []string{stateNoUpdate}, Dst: stateReadyToUpdate},
		// downloading state
		{Name: eventFinishDownload, Src: []string{stateDownloading}, Dst: stateReadyToUpdate},
		{Name: eventCancel, Src: []string{stateDownloading}, Dst: stateNoUpdate},
		// ready to update state
		{Name: eventCancel, Src: []string{stateReadyToUpdate}, Dst: stateNoUpdate},
		{Name: eventStartUpdate, Src: []string{stateReadyToUpdate}, Dst: stateUpdating},
		// updating state
		{Name: eventFinishUpdate, Src: []string{stateUpdating}, Dst: stateNoUpdate},
	}, manager, defaultTTL)

	if err = manager.stateMachine.init(manager.TTLDate); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return manager, nil
}

func (manager *softwareManager) close() (err error) {
	manager.Lock()

	log.Debug("Close software manager")

	manager.runCond.Broadcast()
	close(manager.statusChannel)

	manager.Unlock()

	if err = manager.stateMachine.close(); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) getCurrentStatus() (status cmserver.UpdateSOTAStatus) {
	status.State = convertState(manager.CurrentState)
	status.Error = manager.UpdateErr

	if status.State == cmserver.NoUpdate || manager.CurrentUpdate == nil {
		return status
	}

	status.RebalanceRequest = manager.CurrentUpdate.RebalanceRequest

	for _, layer := range manager.CurrentUpdate.InstallLayers {
		status.InstallLayers = append(status.InstallLayers, cloudprotocol.LayerStatus{
			LayerID: layer.LayerID, Digest: layer.Digest, Version: layer.Version,
		})
	}

	for _, layer := range manager.CurrentUpdate.RemoveLayers {
		status.RemoveLayers = append(status.RemoveLayers, cloudprotocol.LayerStatus{
			LayerID: layer.LayerID, Digest: layer.Digest, Version: layer.Version,
		})
	}

	for _, service := range manager.CurrentUpdate.InstallServices {
		status.InstallServices = append(status.InstallServices, cloudprotocol.ServiceStatus{
			ServiceID: service.ServiceID, Version: service.Version,
		})
	}

	for _, service := range manager.CurrentUpdate.RemoveServices {
		status.RemoveServices = append(status.RemoveServices, cloudprotocol.ServiceStatus{
			ServiceID: service.ServiceID, Version: service.Version,
		})
	}

	if manager.CurrentUpdate.UnitConfig != nil {
		status.UnitConfig = &cloudprotocol.UnitConfigStatus{Version: manager.CurrentUpdate.UnitConfig.Version}
	}

	return status
}

func (manager *softwareManager) processRunStatus(status RunInstancesStatus) {
	manager.Lock()
	defer manager.Unlock()

	manager.InstanceStatuses = status.Instances

	for _, errStatus := range status.ErrorServices {
		if _, ok := manager.ServiceStatuses[errStatus.ServiceID]; !ok {
			status := errStatus
			manager.ServiceStatuses[errStatus.ServiceID] = &status
		}

		manager.updateServiceStatusByID(errStatus.ServiceID, errStatus.Status, errStatus.ErrorInfo)
	}

	manager.runCond.Broadcast()
}

func (manager *softwareManager) processDesiredStatus(desiredStatus cloudprotocol.DesiredStatus) error {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Process desired SOTA")

	update := &softwareUpdate{
		Schedule:        desiredStatus.SOTASchedule,
		UnitConfig:      desiredStatus.UnitConfig,
		InstallServices: make([]cloudprotocol.ServiceInfo, 0),
		RemoveServices:  make([]cloudprotocol.ServiceStatus, 0),
		InstallLayers:   make([]cloudprotocol.LayerInfo, 0),
		RemoveLayers:    make([]cloudprotocol.LayerStatus, 0),
		RunInstances:    desiredStatus.Instances,
		CertChains:      desiredStatus.CertificateChains, Certs: desiredStatus.Certificates,
		NodesStatus: make([]cloudprotocol.NodeStatus, 0),
	}

	allServices, err := manager.softwareUpdater.GetServicesStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	allLayers, err := manager.softwareUpdater.GetLayersStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	manager.processDesiredServices(update, allServices, desiredStatus.Services)
	manager.processDesiredLayers(update, allLayers, desiredStatus.Layers)
	manager.processNodesStatus(update, desiredStatus.Nodes)

	if len(update.InstallServices) != 0 || len(update.RemoveServices) != 0 ||
		len(update.InstallLayers) != 0 || len(update.RemoveLayers) != 0 || len(update.RestoreServices) != 0 ||
		len(update.RestoreLayers) != 0 || manager.needRunInstances(desiredStatus.Instances) ||
		update.UnitConfig != nil || len(update.NodesStatus) != 0 {
		if err := manager.newUpdate(update); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		log.Debug("No SOTA update required")
	}

	return nil
}

func (manager *softwareManager) requestRebalancing() error {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Request rebalancing")

	if manager.CurrentUpdate == nil || len(manager.CurrentUpdate.RunInstances) == 0 {
		return aoserrors.New("no previous update")
	}

	update := &softwareUpdate{
		Schedule:         manager.CurrentUpdate.Schedule,
		InstallServices:  make([]cloudprotocol.ServiceInfo, 0),
		RemoveServices:   make([]cloudprotocol.ServiceStatus, 0),
		InstallLayers:    make([]cloudprotocol.LayerInfo, 0),
		RemoveLayers:     make([]cloudprotocol.LayerStatus, 0),
		RunInstances:     manager.CurrentUpdate.RunInstances,
		NodesStatus:      make([]cloudprotocol.NodeStatus, 0),
		RebalanceRequest: true,
	}

	if err := manager.newUpdate(update); err != nil {
		return err
	}

	return nil
}

func (manager *softwareManager) processDesiredServices(
	update *softwareUpdate, allServices []ServiceStatus, desiredServices []cloudprotocol.ServiceInfo,
) {
downloadServiceLoop:
	for _, desiredService := range desiredServices {
		for _, service := range allServices {
			if desiredService.ServiceID == service.ServiceID && desiredService.Version == service.Version &&
				service.Status != cloudprotocol.ErrorStatus {
				if service.Cached {
					update.RestoreServices = append(update.RestoreServices, desiredService)
				}

				continue downloadServiceLoop
			}
		}

		update.InstallServices = append(update.InstallServices, desiredService)
	}

removeServiceLoop:
	for _, service := range allServices {
		if service.Status != cloudprotocol.InstalledStatus {
			continue
		}

		if service.Cached {
			continue
		}

		for _, desiredService := range desiredServices {
			if service.ServiceID == desiredService.ServiceID {
				continue removeServiceLoop
			}
		}

		update.RemoveServices = append(update.RemoveServices, service.ServiceStatus)
	}
}

func (manager *softwareManager) processDesiredLayers(
	update *softwareUpdate, allLayers []LayerStatus, desiredLayers []cloudprotocol.LayerInfo,
) {
downloadLayersLoop:
	for _, desiredLayer := range desiredLayers {
		for _, layer := range allLayers {
			if desiredLayer.Digest == layer.Digest && layer.Status == cloudprotocol.InstalledStatus {
				if layer.Cached {
					update.RestoreLayers = append(update.RestoreLayers, layer.LayerStatus)
				}

				continue downloadLayersLoop
			}
		}

		update.InstallLayers = append(update.InstallLayers, desiredLayer)
	}

removeLayersLoop:
	for _, installedLayer := range allLayers {
		if installedLayer.Status != cloudprotocol.InstalledStatus {
			continue
		}

		if installedLayer.Cached {
			continue
		}

		for _, desiredLayer := range desiredLayers {
			if installedLayer.Digest == desiredLayer.Digest {
				continue removeLayersLoop
			}
		}

		update.RemoveLayers = append(update.RemoveLayers, installedLayer.LayerStatus)
	}
}

func (manager *softwareManager) processNodesStatus(
	update *softwareUpdate, desiredNodesStatus []cloudprotocol.NodeStatus,
) {
	curNodesStatus, err := manager.statusHandler.getNodesStatus()
	if err != nil {
		log.Errorf("Can't get nodes status: %v", err)
		return
	}

desiredNodesLoop:
	for _, desiredNodeStatus := range desiredNodesStatus {
		for _, curNodeStatus := range curNodesStatus {
			if desiredNodeStatus.NodeID == curNodeStatus.NodeID {
				if desiredNodeStatus.Status != curNodeStatus.Status {
					update.NodesStatus = append(update.NodesStatus, desiredNodeStatus)
				}

				continue desiredNodesLoop
			}
		}

		update.NodesStatus = append(update.NodesStatus, desiredNodeStatus)
	}
}

func (manager *softwareManager) needRunInstances(desiredInstances []cloudprotocol.InstanceInfo) bool {
	currentIdents := make([]aostypes.InstanceIdent, len(manager.InstanceStatuses))
	desiredIdents := []aostypes.InstanceIdent{}

	for i, ident := range manager.InstanceStatuses {
		currentIdents[i] = ident.InstanceIdent
	}

	for _, instance := range desiredInstances {
		ident := aostypes.InstanceIdent{
			ServiceID: instance.ServiceID, SubjectID: instance.SubjectID,
		}

		for i := uint64(0); i < instance.NumInstances; i++ {
			ident.Instance = i

			desiredIdents = append(desiredIdents, ident)
		}
	}

	if len(currentIdents) != len(desiredIdents) {
		return true
	}

loopFound:
	for _, desIdent := range desiredIdents {
		for _, curIdent := range currentIdents {
			if desIdent == curIdent {
				continue loopFound
			}
		}

		return true
	}

	return false
}

func (manager *softwareManager) startUpdate() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Start software update")

	if err = manager.stateMachine.sendEvent(eventStartUpdate, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) getServiceStatus() (serviceStatuses []cloudprotocol.ServiceStatus, err error) {
	manager.Lock()
	defer manager.Unlock()

	manager.statusMutex.RLock()
	defer manager.statusMutex.RUnlock()

	servicesStatus, err := manager.softwareUpdater.GetServicesStatus()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	// Get installed info

	for _, service := range servicesStatus {
		if service.Status == cloudprotocol.InstalledStatus && !service.Cached {
			serviceStatuses = append(serviceStatuses, service.ServiceStatus)
		}
	}

	// Append currently processing info

	for _, service := range manager.ServiceStatuses {
		serviceStatuses = append(serviceStatuses, *service)
	}

	return serviceStatuses, nil
}

func (manager *softwareManager) getLayersStatus() (layerStatuses []cloudprotocol.LayerStatus, err error) {
	layersStatus, err := manager.softwareUpdater.GetLayersStatus()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, layer := range layersStatus {
		if layer.Status == cloudprotocol.InstalledStatus && !layer.Cached {
			layerStatuses = append(layerStatuses, layer.LayerStatus)
		}
	}

	if manager.CurrentState == stateNoUpdate {
		return layerStatuses, nil
	}

	for _, layer := range manager.LayerStatuses {
		layerStatuses = append(layerStatuses, *layer)
	}

	return layerStatuses, nil
}

func (manager *softwareManager) getUnitConfigStatuses() (status []cloudprotocol.UnitConfigStatus, err error) {
	manager.Lock()
	defer manager.Unlock()

	manager.statusMutex.RLock()
	defer manager.statusMutex.RUnlock()

	info, err := manager.unitConfigUpdater.GetStatus()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	status = append(status, info)

	// Append currently processing info

	if manager.CurrentState == stateNoUpdate || manager.CurrentUpdate.UnitConfig == nil {
		return status, nil
	}

	status = append(status, manager.UnitConfigStatus)

	return status, nil
}

/***********************************************************************************************************************
 * Implementer
 **********************************************************************************************************************/

func (manager *softwareManager) stateChanged(event, state string, updateErr error) {
	var errorInfo *cloudprotocol.ErrorInfo

	if updateErr != nil {
		errorInfo = &cloudprotocol.ErrorInfo{Message: updateErr.Error()}
	}

	if event == eventCancel {
		for id, status := range manager.LayerStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateLayerStatusByID(id, cloudprotocol.ErrorStatus, errorInfo)
			}
		}

		for id, status := range manager.ServiceStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateServiceStatusByID(id, cloudprotocol.ErrorStatus, errorInfo)
			}
		}

		if manager.CurrentUpdate.UnitConfig != nil {
			if manager.UnitConfigStatus.Status != cloudprotocol.ErrorStatus {
				manager.updateUnitConfigStatus(cloudprotocol.ErrorStatus, errorInfo)
			}
		}
	}

	if state == stateDownloading || state == stateReadyToUpdate {
		if manager.CurrentUpdate.UnitConfig != nil {
			manager.UnitConfigStatus.Version = manager.CurrentUpdate.UnitConfig.Version
			manager.updateUnitConfigStatus(cloudprotocol.PendingStatus, nil)
		}
	}

	manager.CurrentState = state
	manager.UpdateErr = errorInfo

	log.WithFields(log.Fields{
		"state": state,
		"event": event,
	}).Debug("Software manager state changed")

	if updateErr != nil {
		log.Errorf("Software update error: %v", updateErr)
	}

	manager.sendCurrentStatus()

	if err := manager.saveState(); err != nil {
		log.Errorf("Can't save current software manager state: %v", err)
	}
}

func (manager *softwareManager) noUpdate() {
	log.Debug("Release downloaded software")

	if err := manager.downloader.releaseDownloadedSoftware(); err != nil {
		log.Errorf("Error release downloading software: %v", err)
	}

	if manager.pendingUpdate != nil {
		log.Debug("Schedule pending software update")

		manager.CurrentUpdate = manager.pendingUpdate
		manager.pendingUpdate = nil

		go func() {
			manager.Lock()
			defer manager.Unlock()

			var err error

			if manager.TTLDate, err = manager.stateMachine.startNewUpdate(
				time.Duration(manager.CurrentUpdate.Schedule.TTL)*time.Second,
				manager.isDownloadRequired()); err != nil {
				log.Errorf("Can't start new software update: %v", err)
			}
		}()
	}
}

func (manager *softwareManager) download(ctx context.Context) {
	var (
		downloadErr error
		finishEvent = eventFinishDownload
	)

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			manager.stateMachine.finishOperation(ctx, finishEvent, downloadErr)
		}()
	}()

	manager.DownloadResult = nil

	request := manager.prepareDownloadRequest()

	// Nothing to download
	if len(request) == 0 {
		return
	}

	manager.DownloadResult = manager.downloader.download(ctx, request, true, manager.updateStatusByID)

	// Set pending state

	for id := range manager.DownloadResult {
		if layerStatus, ok := manager.LayerStatuses[id]; ok {
			if layerStatus.Status == cloudprotocol.ErrorStatus {
				log.WithFields(log.Fields{
					"id":      layerStatus.LayerID,
					"digest":  layerStatus.Digest,
					"version": layerStatus.Version,
				}).Errorf("Error downloading layer: %s", layerStatus.ErrorInfo.Message)

				continue
			}

			log.WithFields(log.Fields{
				"id":      layerStatus.LayerID,
				"digest":  layerStatus.Digest,
				"version": layerStatus.Version,
			}).Debug("Layer successfully downloaded")

			manager.updateLayerStatusByID(id, cloudprotocol.PendingStatus, nil)
		} else if serviceStatus, ok := manager.ServiceStatuses[id]; ok {
			if serviceStatus.Status == cloudprotocol.ErrorStatus {
				log.WithFields(log.Fields{
					"id":      serviceStatus.ServiceID,
					"version": serviceStatus.Version,
				}).Errorf("Error downloading service: %s", serviceStatus.ErrorInfo.Message)

				continue
			}

			log.WithFields(log.Fields{
				"id":      serviceStatus.ServiceID,
				"version": serviceStatus.Version,
			}).Debug("Service successfully downloaded")

			manager.updateServiceStatusByID(id, cloudprotocol.PendingStatus, nil)
		}
	}

	downloadErr = getDownloadError(manager.DownloadResult)

	numDownloadErrors := 0

	for _, item := range manager.DownloadResult {
		if item.Error != "" {
			numDownloadErrors++
		}
	}

	// All downloads failed and there is nothing to update (not counting remove layers) then cancel
	if numDownloadErrors == len(manager.DownloadResult) && len(manager.CurrentUpdate.RemoveServices) == 0 {
		finishEvent = eventCancel
	}
}

func (manager *softwareManager) prepareDownloadRequest() (request map[string]downloader.PackageInfo) {
	request = make(map[string]downloader.PackageInfo)

	manager.statusMutex.Lock()

	manager.LayerStatuses = make(map[string]*cloudprotocol.LayerStatus)
	manager.ServiceStatuses = make(map[string]*cloudprotocol.ServiceStatus)

	for _, service := range manager.CurrentUpdate.InstallServices {
		log.WithFields(log.Fields{
			"id":      service.ServiceID,
			"version": service.Version,
		}).Debug("Download service")

		request[service.ServiceID] = downloader.PackageInfo{
			URLs:          service.URLs,
			Sha256:        service.Sha256,
			Size:          service.Size,
			TargetType:    cloudprotocol.DownloadTargetService,
			TargetID:      service.ServiceID,
			TargetVersion: service.Version,
		}
		manager.ServiceStatuses[service.ServiceID] = &cloudprotocol.ServiceStatus{
			ServiceID: service.ServiceID,
			Version:   service.Version,
			Status:    cloudprotocol.DownloadingStatus,
		}
	}

	for _, layer := range manager.CurrentUpdate.InstallLayers {
		log.WithFields(log.Fields{
			"id":      layer.LayerID,
			"digest":  layer.Digest,
			"version": layer.Version,
		}).Debug("Download layer")

		request[layer.Digest] = downloader.PackageInfo{
			URLs:          layer.URLs,
			Sha256:        layer.Sha256,
			Size:          layer.Size,
			TargetType:    cloudprotocol.DownloadTargetLayer,
			TargetID:      layer.Digest,
			TargetVersion: layer.Version,
		}
		manager.LayerStatuses[layer.Digest] = &cloudprotocol.LayerStatus{
			LayerID: layer.LayerID,
			Version: layer.Version,
			Digest:  layer.Digest,
			Status:  cloudprotocol.DownloadingStatus,
		}
	}

	manager.statusMutex.Unlock()

	return request
}

func (manager *softwareManager) readyToUpdate() {
	manager.stateMachine.scheduleUpdate(manager.CurrentUpdate.Schedule)
}

func (manager *softwareManager) update(ctx context.Context) {
	manager.Lock()
	defer manager.Unlock()

	var updateErr error

	if manager.LayerStatuses == nil {
		manager.LayerStatuses = make(map[string]*cloudprotocol.LayerStatus)
	}

	if manager.ServiceStatuses == nil {
		manager.ServiceStatuses = make(map[string]*cloudprotocol.ServiceStatus)
	}

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			manager.stateMachine.finishOperation(ctx, eventFinishUpdate, updateErr)
		}()
	}()

	if err := manager.updateNodes(); err != nil {
		updateErr = err
	}

	if manager.CurrentUpdate.UnitConfig != nil {
		if err := manager.updateUnitConfig(); err != nil {
			updateErr = err
		}
	}

	if err := manager.removeServices(); err != nil && updateErr == nil {
		updateErr = err
	}

	if err := manager.installLayers(); err != nil && updateErr == nil {
		updateErr = err
	}

	if err := manager.restoreServices(); err != nil && updateErr == nil {
		updateErr = err
	}

	newServices, err := manager.installServices()
	if err != nil && updateErr == nil {
		updateErr = err
	}

	if err := manager.removeLayers(); err != nil && updateErr == nil {
		updateErr = err
	}

	if err := manager.restoreLayers(); err != nil && updateErr == nil {
		updateErr = err
	}

	if err := manager.runInstances(newServices); err != nil && updateErr == nil {
		updateErr = err
	}

	manager.runCond.Wait()
}

func (manager *softwareManager) updateTimeout() {
	manager.Lock()
	defer manager.Unlock()

	if manager.stateMachine.canTransit(eventCancel) {
		if err := manager.stateMachine.sendEvent(eventCancel, aoserrors.New("update timeout")); err != nil {
			log.Errorf("Can't cancel update: %s", err)
		}
	}

	manager.runCond.Broadcast()
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (manager *softwareManager) newUpdate(update *softwareUpdate) (err error) {
	log.Debug("New software update")

	// Set default schedule type
	switch update.Schedule.Type {
	case "":
		update.Schedule.Type = cloudprotocol.ForceUpdate

	case cloudprotocol.TimetableUpdate:
		if err = validateTimetable(update.Schedule.Timetable); err != nil {
			return aoserrors.Wrap(err)
		}

	case cloudprotocol.ForceUpdate, cloudprotocol.TriggerUpdate:

	default:
		return aoserrors.New("wrong update type")
	}

	switch manager.CurrentState {
	case stateNoUpdate:
		manager.CurrentUpdate = update

		if manager.TTLDate, err = manager.stateMachine.startNewUpdate(
			time.Duration(manager.CurrentUpdate.Schedule.TTL)*time.Second, manager.isDownloadRequired()); err != nil {
			return aoserrors.Wrap(err)
		}

	default:
		if update.RebalanceRequest {
			log.Debug("Skip rebalancing during update")

			return nil
		}

		if manager.isUpdateEquals(update) {
			if reflect.DeepEqual(update.Schedule, manager.CurrentUpdate.Schedule) {
				log.Debug("Skip update without changes")

				return nil
			}

			// Schedule changed: in ready to update state we can reschedule update. Except current update is forced type,
			// because in this case force update is already scheduled
			if manager.CurrentState == stateReadyToUpdate && (manager.CurrentUpdate.Schedule.Type != cloudprotocol.ForceUpdate) {
				manager.CurrentUpdate.Schedule = update.Schedule

				manager.stateMachine.scheduleUpdate(manager.CurrentUpdate.Schedule)

				return nil
			}
		}

		log.Debugf("Pending software update")

		manager.pendingUpdate = update

		// If current state can't be canceled, wait until it is finished
		if !manager.stateMachine.canTransit(eventCancel) {
			return nil
		}

		if err = manager.stateMachine.sendEvent(eventCancel, nil); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *softwareManager) sendCurrentStatus() {
	manager.statusChannel <- manager.getCurrentStatus()
}

func (manager *softwareManager) updateStatusByID(id string, status string, errorInfo *cloudprotocol.ErrorInfo) {
	if _, ok := manager.LayerStatuses[id]; ok {
		manager.updateLayerStatusByID(id, status, errorInfo)
	} else if _, ok := manager.ServiceStatuses[id]; ok {
		manager.updateServiceStatusByID(id, status, errorInfo)
	} else {
		log.Errorf("Software update ID not found: %s", id)
	}
}

func (manager *softwareManager) updateLayerStatusByID(id, status string, layerErr *cloudprotocol.ErrorInfo) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.LayerStatuses[id]
	if !ok {
		log.Errorf("Can't update software layer status: id %s not found", id)
		return
	}

	info.Status = status
	info.ErrorInfo = layerErr

	manager.statusHandler.updateLayerStatus(*info)
}

func (manager *softwareManager) updateServiceStatusByID(id, status string, serviceErr *cloudprotocol.ErrorInfo) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.ServiceStatuses[id]
	if !ok {
		log.Errorf("Can't update software service status: id %s not found", id)
		return
	}

	info.Status = status
	info.ErrorInfo = serviceErr

	manager.statusHandler.updateServiceStatus(*info)
}

func (manager *softwareManager) loadState() (err error) {
	stateJSON, err := manager.storage.GetSoftwareUpdateState()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if len(stateJSON) == 0 {
		return nil
	}

	if err = json.Unmarshal(stateJSON, manager); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) saveState() (err error) {
	stateJSON, err := json.Marshal(manager)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = manager.storage.SetSoftwareUpdateState(stateJSON); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) installLayers() (installErr error) {
	var mutex sync.Mutex

	handleError := func(layer cloudprotocol.LayerInfo, layerErr error) {
		log.WithFields(log.Fields{
			"digest":  layer.Digest,
			"id":      layer.LayerID,
			"version": layer.Version,
		}).Errorf("Can't install layer: %s", layerErr)

		if errors.Is(layerErr, context.Canceled) {
			return
		}

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.ErrorStatus,
			&cloudprotocol.ErrorInfo{Message: layerErr.Error()})

		mutex.Lock()
		defer mutex.Unlock()

		if installErr == nil {
			installErr = layerErr
		}
	}

	installLayers := []cloudprotocol.LayerInfo{}

	for _, layer := range manager.CurrentUpdate.InstallLayers {
		downloadInfo, ok := manager.DownloadResult[layer.Digest]
		if !ok {
			handleError(layer, aoserrors.New("can't get download result"))
			continue
		}

		// Do not install not downloaded layers
		if downloadInfo.Error != "" {
			continue
		}

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		layer.DownloadInfo.URLs = []string{url.String()}

		installLayers = append(installLayers, layer)
	}

	for _, layer := range installLayers {
		log.WithFields(log.Fields{
			"id":         layer.LayerID,
			"aosVersion": layer.Version,
			"digest":     layer.Digest,
		}).Debug("Install layer")

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.InstallingStatus, nil)

		// Create new variable to be captured by action function
		layerInfo := layer

		manager.actionHandler.Execute(layerInfo.Digest, func(digest string) error {
			if err := manager.softwareUpdater.InstallLayer(layerInfo,
				manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs); err != nil {
				handleError(layerInfo, aoserrors.Wrap(err))
				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":         layerInfo.LayerID,
				"aosVersion": layerInfo.Version,
				"digest":     layerInfo.Digest,
			}).Info("Layer successfully installed")

			manager.updateLayerStatusByID(layerInfo.Digest, cloudprotocol.InstalledStatus, nil)

			return nil
		})
	}

	manager.actionHandler.Wait()

	return installErr
}

func (manager *softwareManager) removeLayers() (removeErr error) {
	return manager.processRemoveRestoreLayers(
		manager.CurrentUpdate.RemoveLayers, "remove", cloudprotocol.RemovedStatus, manager.softwareUpdater.RemoveLayer)
}

func (manager *softwareManager) restoreLayers() (restoreErr error) {
	return manager.processRemoveRestoreLayers(
		manager.CurrentUpdate.RestoreLayers, "restore", cloudprotocol.InstalledStatus, manager.softwareUpdater.RestoreLayer)
}

func (manager *softwareManager) processRemoveRestoreLayers(
	layers []cloudprotocol.LayerStatus, operationStr, successStatus string, operation func(digest string) error,
) (processError error) {
	var mutex sync.Mutex

	handleError := func(layer cloudprotocol.LayerStatus, layerErr error) {
		log.WithFields(log.Fields{
			"digest":     layer.Digest,
			"id":         layer.LayerID,
			"aosVersion": layer.Version,
		}).Errorf("Can't %s layer: %s", operationStr, layerErr)

		if errors.Is(layerErr, context.Canceled) {
			return
		}

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.ErrorStatus,
			&cloudprotocol.ErrorInfo{Message: layerErr.Error()})

		mutex.Lock()
		defer mutex.Unlock()

		if processError == nil {
			processError = layerErr
		}
	}

	for _, layer := range layers {
		log.WithFields(log.Fields{
			"id":         layer.LayerID,
			"aosVersion": layer.Version,
			"digest":     layer.Digest,
		}).Debugf("%s layer", operationStr)

		manager.statusMutex.Lock()
		manager.LayerStatuses[layer.Digest] = &cloudprotocol.LayerStatus{
			LayerID: layer.LayerID,
			Version: layer.Version,
			Digest:  layer.Digest,
		}
		manager.statusMutex.Unlock()

		// Create new variable to be captured by action function
		layerInfo := layer

		manager.actionHandler.Execute(layerInfo.Digest, func(digest string) error {
			if err := operation(layerInfo.Digest); err != nil {
				handleError(layerInfo, aoserrors.Wrap(err))
				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":         layerInfo.LayerID,
				"aosVersion": layerInfo.Version,
				"digest":     layerInfo.Digest,
			}).Infof("Layer successfully %sd", operationStr)

			manager.updateLayerStatusByID(layerInfo.Digest, successStatus, nil)

			return nil
		})
	}

	manager.actionHandler.Wait()

	return nil
}

func (manager *softwareManager) installServices() (newServices []string, installErr error) {
	var mutex sync.Mutex

	handleError := func(service cloudprotocol.ServiceInfo, serviceErr error) {
		log.WithFields(log.Fields{
			"id":      service.ServiceID,
			"version": service.Version,
		}).Errorf("Can't install service: %s", serviceErr)

		if errors.Is(serviceErr, context.Canceled) {
			return
		}

		manager.updateStatusByID(service.ServiceID, cloudprotocol.ErrorStatus,
			&cloudprotocol.ErrorInfo{Message: serviceErr.Error()})

		mutex.Lock()
		defer mutex.Unlock()

		if installErr == nil {
			installErr = serviceErr
		}
	}

	installServices := []cloudprotocol.ServiceInfo{}

	for _, service := range manager.CurrentUpdate.InstallServices {
		downloadInfo, ok := manager.DownloadResult[service.ServiceID]
		if !ok {
			handleError(service, aoserrors.New("can't get download result"))
			continue
		}

		// Skip not downloaded services
		if downloadInfo.Error != "" {
			continue
		}

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		service.DownloadInfo.URLs = []string{url.String()}

		installServices = append(installServices, service)
	}

	for _, service := range installServices {
		log.WithFields(log.Fields{
			"id":      service.ServiceID,
			"version": service.Version,
		}).Debug("Install service")

		manager.updateServiceStatusByID(service.ServiceID, cloudprotocol.InstallingStatus, nil)

		// Create new variable to be captured by action function
		serviceInfo := service

		manager.actionHandler.Execute(serviceInfo.ServiceID, func(serviceID string) error {
			err := manager.softwareUpdater.InstallService(serviceInfo,
				manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs)
			if err != nil {
				handleError(serviceInfo, aoserrors.Wrap(err))
				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":         serviceInfo.ServiceID,
				"aosVersion": serviceInfo.Version,
			}).Info("Service successfully installed")

			newServices = append(newServices, serviceInfo.ServiceID)

			manager.updateServiceStatusByID(serviceInfo.ServiceID, cloudprotocol.InstalledStatus, nil)

			return nil
		})
	}

	manager.actionHandler.Wait()

	return newServices, installErr
}

func (manager *softwareManager) restoreServices() (restoreErr error) {
	var mutex sync.Mutex

	handleError := func(service cloudprotocol.ServiceInfo, serviceErr error) {
		log.WithFields(log.Fields{
			"id":         service.ServiceID,
			"aosVersion": service.Version,
		}).Errorf("Can't restore service: %v", serviceErr)

		if errors.Is(serviceErr, context.Canceled) {
			return
		}

		manager.updateStatusByID(service.ServiceID, cloudprotocol.ErrorStatus,
			&cloudprotocol.ErrorInfo{Message: serviceErr.Error()})

		mutex.Lock()
		defer mutex.Unlock()

		if restoreErr == nil {
			restoreErr = serviceErr
		}
	}

	for _, service := range manager.CurrentUpdate.RestoreServices {
		log.WithFields(log.Fields{
			"id":         service.ServiceID,
			"aosVersion": service.Version,
		}).Debug("Restore service")

		manager.ServiceStatuses[service.ServiceID] = &cloudprotocol.ServiceStatus{
			ServiceID: service.ServiceID,
			Version:   service.Version,
			Status:    cloudprotocol.InstallingStatus,
		}

		// Create new variable to be captured by action function
		serviceInfo := service

		manager.actionHandler.Execute(serviceInfo.ServiceID, func(serviceID string) error {
			if err := manager.softwareUpdater.RestoreService(serviceInfo.ServiceID); err != nil {
				handleError(serviceInfo, aoserrors.Wrap(err))
				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":      serviceInfo.ServiceID,
				"version": serviceInfo.Version,
			}).Info("Service successfully restored")

			manager.updateServiceStatusByID(serviceInfo.ServiceID, cloudprotocol.InstalledStatus, nil)

			return nil
		})
	}

	manager.actionHandler.Wait()

	return restoreErr
}

func (manager *softwareManager) removeServices() (removeErr error) {
	var mutex sync.Mutex

	handleError := func(service cloudprotocol.ServiceStatus, serviceErr error) {
		log.WithFields(log.Fields{
			"id":      service.ServiceID,
			"version": service.Version,
		}).Errorf("Can't install service: %v", serviceErr)

		if errors.Is(serviceErr, context.Canceled) {
			return
		}

		manager.updateStatusByID(service.ServiceID, cloudprotocol.ErrorStatus,
			&cloudprotocol.ErrorInfo{Message: serviceErr.Error()})

		mutex.Lock()
		defer mutex.Unlock()

		if removeErr == nil {
			removeErr = serviceErr
		}
	}

	for _, service := range manager.CurrentUpdate.RemoveServices {
		log.WithFields(log.Fields{
			"id":         service.ServiceID,
			"aosVersion": service.Version,
		}).Debug("Remove service")

		// Create status for remove layers. For install layer it is created in download function.
		manager.statusMutex.Lock()
		manager.ServiceStatuses[service.ServiceID] = &cloudprotocol.ServiceStatus{
			ServiceID: service.ServiceID,
			Version:   service.Version,
			Status:    cloudprotocol.RemovingStatus,
		}
		manager.statusMutex.Unlock()

		manager.updateServiceStatusByID(service.ServiceID, cloudprotocol.RemovingStatus, nil)

		// Create new variable to be captured by action function
		serviceStatus := service

		manager.actionHandler.Execute(serviceStatus.ServiceID, func(serviceID string) error {
			if err := manager.softwareUpdater.RemoveService(serviceStatus.ServiceID); err != nil {
				err = aoserrors.Wrap(err)
				handleError(serviceStatus, err)

				return err
			}

			log.WithFields(log.Fields{
				"id":         serviceStatus.ServiceID,
				"aosVersion": serviceStatus.Version,
			}).Info("Service successfully removed")

			manager.updateServiceStatusByID(serviceStatus.ServiceID, cloudprotocol.RemovedStatus, nil)

			return nil
		})
	}

	manager.actionHandler.Wait()

	return removeErr
}

func (manager *softwareManager) runInstances(newServices []string) (runErr error) {
	manager.InstanceStatuses = []cloudprotocol.InstanceStatus{}

	for _, instance := range manager.CurrentUpdate.RunInstances {
		var version string

		if serviceInfo, ok := manager.ServiceStatuses[instance.ServiceID]; ok {
			version = serviceInfo.Version
		}

		ident := aostypes.InstanceIdent{
			ServiceID: instance.ServiceID, SubjectID: instance.SubjectID,
		}

		for i := uint64(0); i < instance.NumInstances; i++ {
			ident.Instance = i

			manager.InstanceStatuses = append(manager.InstanceStatuses, cloudprotocol.InstanceStatus{
				InstanceIdent:  ident,
				ServiceVersion: version,
				Status:         cloudprotocol.InstanceStateActivating,
			})
		}
	}

	manager.statusHandler.setInstancesStatus(manager.InstanceStatuses)

	if err := manager.instanceRunner.RunInstances(manager.CurrentUpdate.RunInstances, newServices); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) updateNodes() (nodesErr error) {
	for _, nodeStatus := range manager.CurrentUpdate.NodesStatus {
		if nodeStatus.Status == cloudprotocol.NodeStatusPaused {
			log.WithField("nodeID", nodeStatus.NodeID).Debug("Pause node")

			if err := manager.nodeManager.PauseNode(nodeStatus.NodeID); err != nil && nodesErr == nil {
				log.WithField("nodeID", nodeStatus.NodeID).Errorf("Can't pause node: %v", err)

				nodesErr = aoserrors.Wrap(err)
			}
		}

		if nodeStatus.Status == cloudprotocol.NodeStatusProvisioned {
			log.WithField("nodeID", nodeStatus.NodeID).Debug("Resume node")

			if err := manager.nodeManager.ResumeNode(nodeStatus.NodeID); err != nil && nodesErr == nil {
				log.WithField("nodeID", nodeStatus.NodeID).Errorf("Can't resume node: %v", err)

				nodesErr = aoserrors.Wrap(err)
			}
		}
	}

	return nodesErr
}

func (manager *softwareManager) updateUnitConfig() (unitConfigErr error) {
	log.Debug("Update unit config")

	defer func() {
		if unitConfigErr != nil {
			manager.updateUnitConfigStatus(cloudprotocol.ErrorStatus,
				&cloudprotocol.ErrorInfo{Message: unitConfigErr.Error()})
		}
	}()

	if err := manager.unitConfigUpdater.CheckUnitConfig(*manager.CurrentUpdate.UnitConfig); err != nil {
		if errors.Is(err, unitconfig.ErrAlreadyInstalled) {
			log.Error("Unit config already installed")

			manager.updateUnitConfigStatus(cloudprotocol.InstalledStatus, nil)

			return nil
		}

		return aoserrors.Wrap(err)
	}

	manager.updateUnitConfigStatus(cloudprotocol.InstallingStatus, nil)

	if err := manager.unitConfigUpdater.UpdateUnitConfig(*manager.CurrentUpdate.UnitConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	manager.updateUnitConfigStatus(cloudprotocol.InstalledStatus, nil)

	return nil
}

func (manager *softwareManager) updateUnitConfigStatus(status string, errorInfo *cloudprotocol.ErrorInfo) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	manager.UnitConfigStatus.Status = status
	manager.UnitConfigStatus.ErrorInfo = errorInfo

	manager.statusHandler.updateUnitConfigStatus(manager.UnitConfigStatus)
}

func (manager *softwareManager) isDownloadRequired() bool {
	if manager.CurrentUpdate != nil &&
		(len(manager.CurrentUpdate.InstallLayers) > 0 ||
			len(manager.CurrentUpdate.InstallServices) > 0) {
		return true
	}

	return false
}

func (manager *softwareManager) isUpdateEquals(update *softwareUpdate) bool {
	if reflect.DeepEqual(update.NodesStatus, manager.CurrentUpdate.NodesStatus) &&
		reflect.DeepEqual(update.UnitConfig, manager.CurrentUpdate.UnitConfig) &&
		reflect.DeepEqual(update.InstallLayers, manager.CurrentUpdate.InstallLayers) &&
		reflect.DeepEqual(update.RemoveLayers, manager.CurrentUpdate.RemoveLayers) &&
		reflect.DeepEqual(update.InstallServices, manager.CurrentUpdate.InstallServices) &&
		reflect.DeepEqual(update.RemoveServices, manager.CurrentUpdate.RemoveServices) &&
		reflect.DeepEqual(update.RunInstances, manager.CurrentUpdate.RunInstances) &&
		reflect.DeepEqual(update.RestoreServices, manager.CurrentUpdate.RestoreServices) &&
		reflect.DeepEqual(update.RestoreLayers, manager.CurrentUpdate.RestoreLayers) {
		return true
	}

	return false
}
