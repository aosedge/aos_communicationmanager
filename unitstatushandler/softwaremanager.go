// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/action"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/cmserver"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const maxConcurrentActions = 10

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type softwareStatusHandler interface {
	download(ctx context.Context, request map[string]cloudprotocol.DecryptDataStruct,
		continueOnError bool, notifier statusNotifier,
		chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result map[string]*downloadResult)
	updateLayerStatus(layerInfo cloudprotocol.LayerInfo)
	updateServiceStatus(serviceInfo cloudprotocol.ServiceInfo)
}

type softwareUpdate struct {
	Schedule        cloudprotocol.ScheduleRule           `json:"schedule,omitempty"`
	InstallLayers   []cloudprotocol.LayerInfoFromCloud   `json:"installLayers,omitempty"`
	RemoveLayers    []cloudprotocol.LayerInfo            `json:"removeLayers,omitempty"`
	InstallServices []cloudprotocol.ServiceInfoFromCloud `json:"installServices,omitempty"`
	RemoveServices  []cloudprotocol.ServiceInfo          `json:"removeServices,omitempty"`
	CertChains      []cloudprotocol.CertificateChain     `json:"certChains,omitempty"`
	Certs           []cloudprotocol.Certificate          `json:"certs,omitempty"`
}

type softwareManager struct {
	sync.Mutex

	statusChannel chan cmserver.UpdateStatus

	statusHandler   softwareStatusHandler
	softwareUpdater SoftwareUpdater
	storage         Storage

	stateMachine  *updateStateMachine
	actionHandler *action.Handler
	statusMutex   sync.RWMutex
	pendingUpdate *softwareUpdate
	currentUsers  []string

	LayerStatuses   map[string]*cloudprotocol.LayerInfo   `json:"layerStatuses,omitempty"`
	ServiceStatuses map[string]*cloudprotocol.ServiceInfo `json:"serviceStatuses,omitempty"`
	CurrentUpdate   *softwareUpdate                       `json:"currentUpdate,omitempty"`
	DownloadResult  map[string]*downloadResult            `json:"downloadResult,omitempty"`
	CurrentState    string                                `json:"currentState,omitempty"`
	UpdateErr       string                                `json:"updateErr,omitempty"`
	TTLDate         time.Time                             `json:"ttlDate,omitempty"`
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newSoftwareManager(statusHandler softwareStatusHandler,
	softwareUpdater SoftwareUpdater, storage Storage, defaultTTL time.Duration) (manager *softwareManager, err error) {
	manager = &softwareManager{
		statusChannel:   make(chan cmserver.UpdateStatus, 1),
		statusHandler:   statusHandler,
		softwareUpdater: softwareUpdater,
		actionHandler:   action.New(maxConcurrentActions),
		storage:         storage,
		CurrentState:    stateNoUpdate,
	}

	if err = manager.loadState(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"state": manager.CurrentState, "error": manager.UpdateErr}).Debug("New software manager")

	manager.stateMachine = newUpdateStateMachine(manager.CurrentState, fsm.Events{
		// no update state
		{Name: eventStartDownload, Src: []string{stateNoUpdate}, Dst: stateDownloading},
		// downloading state
		{Name: eventFinishDownload, Src: []string{stateDownloading}, Dst: stateReadyToUpdate},
		{Name: eventCancel, Src: []string{stateDownloading}, Dst: stateNoUpdate},
		// ready to update state
		{Name: eventCancel, Src: []string{stateReadyToUpdate}, Dst: stateNoUpdate},
		{Name: eventStartUpdate, Src: []string{stateReadyToUpdate}, Dst: stateUpdating},
		// updating state
		{Name: eventFinishUpdate, Src: []string{stateUpdating}, Dst: stateNoUpdate},
		{Name: eventCancel, Src: []string{stateUpdating}, Dst: stateNoUpdate},
	}, manager, defaultTTL)

	if err = manager.stateMachine.init(manager.TTLDate); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return manager, nil
}

func (manager *softwareManager) close() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Close software manager")

	close(manager.statusChannel)

	if err = manager.stateMachine.close(); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) getCurrentStatus() (status cmserver.UpdateStatus) {
	manager.Lock()
	defer manager.Unlock()

	return cmserver.UpdateStatus{State: convertState(manager.CurrentState), Error: manager.UpdateErr}
}

func (manager *softwareManager) processDesiredStatus(desiredStatus cloudprotocol.DecodedDesiredStatus) (err error) {
	manager.Lock()
	defer manager.Unlock()

	installedServices, installedLayers, err := manager.softwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	installServices := make([]cloudprotocol.ServiceInfoFromCloud, 0)
	removeServices := make([]cloudprotocol.ServiceInfo, 0)

desiredServiceLoop:
	for _, desiredService := range desiredStatus.Services {
		for _, installedService := range installedServices {
			if desiredService.ID == installedService.ID && desiredService.AosVersion == installedService.AosVersion &&
				installedService.Status == cloudprotocol.InstalledStatus {
				continue desiredServiceLoop
			}
		}

		installServices = append(installServices, desiredService)
	}

installedServiceLoop:
	for _, installedService := range installedServices {
		if installedService.Status != cloudprotocol.InstalledStatus {
			continue
		}

		for _, desiredService := range desiredStatus.Services {
			if installedService.ID == desiredService.ID {
				continue installedServiceLoop
			}
		}

		removeServices = append(removeServices, installedService)
	}

	installLayers := make([]cloudprotocol.LayerInfoFromCloud, 0)
	removeLayers := make([]cloudprotocol.LayerInfo, 0)

desiredLayerLoop:
	for _, desiredLayer := range desiredStatus.Layers {
		for _, installedLayer := range installedLayers {
			if desiredLayer.Digest == installedLayer.Digest && installedLayer.Status == cloudprotocol.InstalledStatus {
				continue desiredLayerLoop
			}
		}

		installLayers = append(installLayers, desiredLayer)
	}

installedLayerLoop:
	for _, installedLayer := range installedLayers {
		if installedLayer.Status != cloudprotocol.InstalledStatus {
			continue
		}

		for _, desiredLayer := range desiredStatus.Layers {
			if installedLayer.Digest == desiredLayer.Digest {
				continue installedLayerLoop
			}
		}

		removeLayers = append(removeLayers, installedLayer)
	}

	if len(installLayers) != 0 || len(removeLayers) != 0 || len(installServices) != 0 || len(removeServices) != 0 {
		if err := manager.newUpdate(desiredStatus.SOTASchedule, installServices, removeServices,
			installLayers, removeLayers, desiredStatus.CertificateChains, desiredStatus.Certificates); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *softwareManager) startUpdate() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Start software update")

	if err = manager.stateMachine.sendEvent(eventStartUpdate, ""); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *softwareManager) getItemStatuses() (serviceStatuses []cloudprotocol.ServiceInfo,
	layerStatuses []cloudprotocol.LayerInfo, err error) {
	manager.Lock()
	defer manager.Unlock()

	manager.statusMutex.RLock()
	defer manager.statusMutex.RUnlock()

	serviceInfo, layerInfo, err := manager.softwareUpdater.GetStatus()
	if err != nil {
		return nil, nil, aoserrors.Wrap(err)
	}

	// Get installed info

	for _, service := range serviceInfo {
		if service.Status == cloudprotocol.InstalledStatus {
			serviceStatuses = append(serviceStatuses, service)
		}
	}

	for _, layer := range layerInfo {
		if layer.Status == cloudprotocol.InstalledStatus {
			layerStatuses = append(layerStatuses, layer)
		}
	}

	// Append currently processing info

	if manager.CurrentState == stateNoUpdate {
		return serviceStatuses, layerStatuses, nil
	}

	for _, service := range manager.ServiceStatuses {
		serviceStatuses = append(serviceStatuses, *service)
	}

	for _, layer := range manager.LayerStatuses {
		layerStatuses = append(layerStatuses, *layer)
	}

	return serviceStatuses, layerStatuses, nil
}

func (manager *softwareManager) setUsers(users []string) (err error) {
	manager.Lock()
	defer manager.Unlock()

	if isUsersEqual(manager.currentUsers, users) {
		return nil
	}

	if manager.stateMachine.canTransit(eventCancel) {
		if err = manager.stateMachine.sendEvent(eventCancel, ""); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if err = manager.softwareUpdater.SetUsers(users); err != nil {
		return aoserrors.Wrap(err)
	}

	manager.currentUsers = users

	return nil
}

/***********************************************************************************************************************
 * Implementer
 **********************************************************************************************************************/

func (manager *softwareManager) stateChanged(event, state string, updateErr string) {
	if event == eventCancel {
		for id, status := range manager.LayerStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateLayerStatusByID(id, cloudprotocol.ErrorStatus, updateErr)
			}
		}

		for id, status := range manager.ServiceStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateServiceStatusByID(id, cloudprotocol.ErrorStatus, updateErr, "")
			}
		}
	}

	manager.CurrentState = state
	manager.UpdateErr = updateErr

	log.WithFields(log.Fields{
		"state": state,
		"event": event}).Debug("Software manager state changed")

	if updateErr != "" {
		log.Errorf("Software update error: %s", updateErr)
	}

	manager.sendCurrentStatus()

	if err := manager.saveState(); err != nil {
		log.Errorf("Can't save current software manager state: %s", err)
	}
}

func (manager *softwareManager) noUpdate() {
	// Remove downloaded files
	for _, result := range manager.DownloadResult {
		if result.FileName != "" {
			log.WithField("file", result.FileName).Debug("Remove software update file")

			if err := os.RemoveAll(result.FileName); err != nil {
				log.WithField("file", result.FileName).Errorf("Can't remove update file: %s", err)
			}
		}
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
				time.Duration(manager.CurrentUpdate.Schedule.TTL) * time.Second); err != nil {
				log.Errorf("Can't start new software update: %s", err)
			}
		}()
	}
}

func (manager *softwareManager) download(ctx context.Context) {
	var (
		downloadErr string
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

	manager.statusMutex.Lock()

	manager.LayerStatuses = make(map[string]*cloudprotocol.LayerInfo)
	manager.ServiceStatuses = make(map[string]*cloudprotocol.ServiceInfo)

	request := make(map[string]cloudprotocol.DecryptDataStruct)

	for _, service := range manager.CurrentUpdate.InstallServices {
		log.WithFields(log.Fields{
			"id":      service.ID,
			"version": service.AosVersion,
		}).Debug("Download service")

		request[service.ID] = service.DecryptDataStruct
		manager.ServiceStatuses[service.ID] = &cloudprotocol.ServiceInfo{
			ID:         service.ID,
			AosVersion: service.AosVersion,
			Status:     cloudprotocol.DownloadingStatus,
		}
	}

	for _, layer := range manager.CurrentUpdate.InstallLayers {
		log.WithFields(log.Fields{
			"id":      layer.ID,
			"digest":  layer.Digest,
			"version": layer.AosVersion,
		}).Debug("Download layer")

		request[layer.Digest] = layer.DecryptDataStruct
		manager.LayerStatuses[layer.Digest] = &cloudprotocol.LayerInfo{
			ID:         layer.ID,
			AosVersion: layer.AosVersion,
			Digest:     layer.Digest,
			Status:     cloudprotocol.DownloadingStatus,
		}
	}

	manager.statusMutex.Unlock()

	// Nothing to download
	if len(request) == 0 {
		return
	}

	manager.DownloadResult = manager.statusHandler.download(ctx, request, true, manager.updateStatusByID,
		manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs)

	// Set pending state

	for id, layer := range manager.LayerStatuses {
		if layer.Status == cloudprotocol.ErrorStatus {
			log.WithFields(log.Fields{
				"id":      layer.ID,
				"digest":  layer.Digest,
				"version": layer.AosVersion,
			}).Errorf("Error downloading layer: %s", layer.Error)
			continue
		}

		log.WithFields(log.Fields{
			"id":      layer.ID,
			"digest":  layer.Digest,
			"version": layer.AosVersion,
		}).Debug("Layer successfully downloaded")

		manager.updateLayerStatusByID(id, cloudprotocol.PendingStatus, "")
	}

	for id, service := range manager.ServiceStatuses {
		if service.Status == cloudprotocol.ErrorStatus {
			log.WithFields(log.Fields{
				"id":      service.ID,
				"version": service.AosVersion,
			}).Errorf("Error downloading service: %s", service.Error)
			continue
		}

		log.WithFields(log.Fields{
			"id":      service.ID,
			"version": service.AosVersion,
		}).Debug("Service successfully downloaded")

		manager.updateServiceStatusByID(id, cloudprotocol.PendingStatus, "", "")
	}

	downloadErr = getDownloadError(manager.DownloadResult)

	numDownloadErrors := 0

	for _, item := range manager.DownloadResult {
		if item.Error != "" {
			numDownloadErrors++
		}
	}

	// All downloads failed and there is nothing to update then cancel
	if numDownloadErrors == len(manager.DownloadResult) &&
		len(manager.CurrentUpdate.RemoveLayers) == 0 && len(manager.CurrentUpdate.RemoveServices) == 0 {
		finishEvent = eventCancel
	}
}

func (manager *softwareManager) readyToUpdate() {
	manager.stateMachine.scheduleUpdate(manager.CurrentUpdate.Schedule)
}

func (manager *softwareManager) update(ctx context.Context) {
	var updateErr string

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			manager.stateMachine.finishOperation(ctx, eventFinishUpdate, updateErr)
		}()
	}()

	if errorStr := manager.installLayers(ctx); errorStr != "" {
		if updateErr == "" {
			updateErr = errorStr
		}
	}

	if errorStr := manager.installServices(ctx); errorStr != "" {
		if updateErr == "" {
			updateErr = errorStr
		}
	}

	if errorStr := manager.removeServices(ctx); errorStr != "" {
		if updateErr == "" {
			updateErr = errorStr
		}
	}

	if errorStr := manager.removeLayers(ctx); errorStr != "" {
		if updateErr == "" {
			updateErr = errorStr
		}
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (manager *softwareManager) newUpdate(schedule cloudprotocol.ScheduleRule,
	installServices []cloudprotocol.ServiceInfoFromCloud, removeServices []cloudprotocol.ServiceInfo,
	installLayers []cloudprotocol.LayerInfoFromCloud, removeLayers []cloudprotocol.LayerInfo,
	certChains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (err error) {
	log.Debug("New software update")

	update := &softwareUpdate{
		Schedule:        schedule,
		InstallLayers:   installLayers,
		RemoveLayers:    removeLayers,
		InstallServices: installServices,
		RemoveServices:  removeServices,
		CertChains:      certChains,
		Certs:           certs,
	}

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
			time.Duration(manager.CurrentUpdate.Schedule.TTL) * time.Second); err != nil {
			return aoserrors.Wrap(err)
		}

	default:
		if reflect.DeepEqual(update.InstallLayers, manager.CurrentUpdate.InstallLayers) &&
			reflect.DeepEqual(update.RemoveLayers, manager.CurrentUpdate.RemoveLayers) &&
			reflect.DeepEqual(update.InstallServices, manager.CurrentUpdate.InstallServices) &&
			reflect.DeepEqual(update.RemoveServices, manager.CurrentUpdate.RemoveServices) {
			if reflect.DeepEqual(update.Schedule, manager.CurrentUpdate.Schedule) {
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

		manager.pendingUpdate = update

		// If current state can't be canceled, wait until it is finished
		if !manager.stateMachine.canTransit(eventCancel) {
			return nil
		}

		if err = manager.stateMachine.sendEvent(eventCancel, ""); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *softwareManager) sendCurrentStatus() {
	manager.statusChannel <- cmserver.UpdateStatus{State: convertState(manager.CurrentState), Error: manager.UpdateErr}
}

func (manager *softwareManager) updateStatusByID(id string, status string, errorStr string) {
	if _, ok := manager.LayerStatuses[id]; ok {
		manager.updateLayerStatusByID(id, status, errorStr)
	} else if _, ok := manager.ServiceStatuses[id]; ok {
		manager.updateServiceStatusByID(id, status, errorStr, "")
	} else {
		log.Errorf("Software update ID not found: %s", id)
	}
}

func (manager *softwareManager) updateLayerStatusByID(id, status, layerErr string) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.LayerStatuses[id]
	if !ok {
		log.Errorf("Can't update software layer status: id %s not found", id)
		return
	}

	info.Status = status
	info.Error = layerErr

	manager.statusHandler.updateLayerStatus(*info)
}

func (manager *softwareManager) updateServiceStatusByID(id, status, serviceErr, stateChecksum string) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.ServiceStatuses[id]
	if !ok {
		log.Errorf("Can't update software service status: id %s not found", id)
		return
	}

	info.Status = status
	info.Error = serviceErr
	info.StateChecksum = stateChecksum

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

func (manager *softwareManager) installLayers(ctx context.Context) (installErr string) {
	var mutex sync.Mutex

	handleError := func(layer cloudprotocol.LayerInfoFromCloud, layerErr string) {
		log.WithFields(log.Fields{
			"digest":     layer.Digest,
			"id":         layer.ID,
			"aosVersion": layer.AosVersion,
		}).Errorf("Can't install layer: %s", layerErr)

		if isCancelError(layerErr) {
			return
		}

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.ErrorStatus, layerErr)

		mutex.Lock()
		defer mutex.Unlock()

		if installErr == "" {
			installErr = layerErr
		}
	}

	for _, layer := range manager.CurrentUpdate.InstallLayers {
		downloadInfo, ok := manager.DownloadResult[layer.Digest]
		if !ok {
			handleError(layer, aoserrors.New("can't get download result").Error())
			continue
		}

		// Do not install not downloaded layers
		if downloadInfo.Error != "" {
			continue
		}

		log.WithFields(log.Fields{
			"id":         layer.ID,
			"aosVersion": layer.AosVersion,
			"digest":     layer.Digest,
		}).Debug("Install layer")

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.InstallingStatus, "")

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		// Create new variable to be captured by action function
		layerInfo := layer

		layerInfo.DecryptDataStruct = cloudprotocol.DecryptDataStruct{
			URLs:   []string{url.String()},
			Size:   downloadInfo.FileInfo.Size,
			Sha256: downloadInfo.FileInfo.Sha256,
			Sha512: downloadInfo.FileInfo.Sha512,
		}

		manager.actionHandler.PutInQueue(layerInfo.Digest, func(digest string) {
			if err := manager.softwareUpdater.InstallLayer(layerInfo); err != nil {
				handleError(layerInfo, aoserrors.Wrap(err).Error())
				return
			}

			log.WithFields(log.Fields{
				"id":         layerInfo.ID,
				"aosVersion": layerInfo.AosVersion,
				"digest":     layerInfo.Digest,
			}).Info("Layer successfully installed")

			manager.updateLayerStatusByID(layerInfo.Digest, cloudprotocol.InstalledStatus, "")
		})
	}

	manager.actionHandler.Wait()

	return installErr
}

func (manager *softwareManager) removeLayers(ctx context.Context) (removeErr string) {
	var mutex sync.Mutex

	handleError := func(layer cloudprotocol.LayerInfo, layerErr string) {
		log.WithFields(log.Fields{
			"id":         layer.ID,
			"aosVersion": layer.AosVersion,
			"digest":     layer.Digest,
		}).Errorf("Can't remove layer: %s", layerErr)

		if isCancelError(layerErr) {
			return
		}

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.ErrorStatus, layerErr)

		mutex.Lock()
		defer mutex.Unlock()

		if removeErr == "" {
			removeErr = layerErr
		}
	}

	for _, layer := range manager.CurrentUpdate.RemoveLayers {
		log.WithFields(log.Fields{
			"id":         layer.ID,
			"aosVersion": layer.AosVersion,
			"digest":     layer.Digest,
		}).Debug("Remove layer")

		// Create status for remove layers. For install layer it is created in download function.
		manager.statusMutex.Lock()
		manager.LayerStatuses[layer.Digest] = &cloudprotocol.LayerInfo{
			ID:         layer.ID,
			AosVersion: layer.AosVersion,
			Digest:     layer.Digest,
			Status:     cloudprotocol.RemovingStatus,
		}
		manager.statusMutex.Unlock()

		manager.updateLayerStatusByID(layer.Digest, cloudprotocol.RemovingStatus, "")

		// Create new variable to be captured by action function
		layerStatus := layer

		manager.actionHandler.PutInQueue(layerStatus.Digest, func(digest string) {
			if err := manager.softwareUpdater.RemoveLayer(layerStatus); err != nil {
				handleError(layerStatus, err.Error())
				return
			}

			log.WithFields(log.Fields{
				"id":         layerStatus.ID,
				"aosVersion": layerStatus.AosVersion,
				"digest":     layerStatus.Digest,
			}).Info("Layer successfully removed")

			manager.updateLayerStatusByID(layerStatus.Digest, cloudprotocol.RemovedStatus, "")
		})
	}

	manager.actionHandler.Wait()

	return removeErr
}

func (manager *softwareManager) installServices(ctx context.Context) (installErr string) {
	var mutex sync.Mutex

	handleError := func(service cloudprotocol.ServiceInfoFromCloud, serviceErr string) {
		log.WithFields(log.Fields{
			"id":         service.ID,
			"aosVersion": service.AosVersion,
		}).Errorf("Can't install service: %s", serviceErr)

		if isCancelError(serviceErr) {
			return
		}

		manager.updateStatusByID(service.ID, cloudprotocol.ErrorStatus, serviceErr)

		mutex.Lock()
		defer mutex.Unlock()

		if installErr == "" {
			installErr = serviceErr
		}
	}

	for _, service := range manager.CurrentUpdate.InstallServices {
		downloadInfo, ok := manager.DownloadResult[service.ID]
		if !ok {
			handleError(service, aoserrors.New("can't get download result").Error())
			continue
		}

		// Skip not downloaded services
		if downloadInfo.Error != "" {
			continue
		}

		log.WithFields(log.Fields{
			"id":         service.ID,
			"aosVersion": service.AosVersion,
		}).Debug("Install service")

		manager.updateServiceStatusByID(service.ID, cloudprotocol.InstallingStatus, "", "")

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		// Create new variable to be captured by action function
		serviceInfo := service

		serviceInfo.DecryptDataStruct = cloudprotocol.DecryptDataStruct{
			URLs:   []string{url.String()},
			Size:   downloadInfo.FileInfo.Size,
			Sha256: downloadInfo.FileInfo.Sha256,
			Sha512: downloadInfo.FileInfo.Sha512,
		}

		manager.actionHandler.PutInQueue(serviceInfo.ID, func(serviceID string) {
			stateChecksum, err := manager.softwareUpdater.InstallService(serviceInfo)
			if err != nil {
				handleError(serviceInfo, aoserrors.Wrap(err).Error())
				return
			}

			log.WithFields(log.Fields{
				"id":            serviceInfo.ID,
				"aosVersion":    serviceInfo.AosVersion,
				"stateChecksum": stateChecksum,
			}).Info("Service successfully installed")

			manager.updateServiceStatusByID(serviceInfo.ID, cloudprotocol.InstalledStatus, "", stateChecksum)
		})
	}

	manager.actionHandler.Wait()

	return installErr
}

func (manager *softwareManager) removeServices(ctx context.Context) (removeErr string) {
	var mutex sync.Mutex

	handleError := func(service cloudprotocol.ServiceInfo, serviceErr string) {
		log.WithFields(log.Fields{
			"id":         service.ID,
			"aosVersion": service.AosVersion,
		}).Errorf("Can't install service: %s", serviceErr)

		if isCancelError(serviceErr) {
			return
		}

		manager.updateStatusByID(service.ID, cloudprotocol.ErrorStatus, serviceErr)

		mutex.Lock()
		defer mutex.Unlock()

		if removeErr == "" {
			removeErr = serviceErr
		}
	}

	for _, service := range manager.CurrentUpdate.RemoveServices {
		log.WithFields(log.Fields{
			"id":         service.ID,
			"aosVersion": service.AosVersion,
		}).Debug("Remove service")

		// Create status for remove layers. For install layer it is created in download function.
		manager.statusMutex.Lock()
		manager.ServiceStatuses[service.ID] = &cloudprotocol.ServiceInfo{
			ID:         service.ID,
			AosVersion: service.AosVersion,
			Status:     cloudprotocol.RemovingStatus,
		}
		manager.statusMutex.Unlock()

		manager.updateServiceStatusByID(service.ID, cloudprotocol.RemovingStatus, "", "")

		// Create new variable to be captured by action function
		serviceStatus := service

		manager.actionHandler.PutInQueue(serviceStatus.ID, func(serviceID string) {
			if err := manager.softwareUpdater.RemoveService(serviceStatus); err != nil {
				handleError(serviceStatus, err.Error())
				return
			}

			log.WithFields(log.Fields{
				"id":         serviceStatus.ID,
				"aosVersion": serviceStatus.AosVersion,
			}).Info("Service successfully removed")

			manager.updateServiceStatusByID(serviceStatus.ID, cloudprotocol.RemovedStatus, "", "")
		})
	}

	manager.actionHandler.Wait()

	return removeErr
}

func getLayerUpdateID(layer cloudprotocol.LayerInfoFromCloud) (id string) {
	return base64.URLEncoding.EncodeToString(layer.DecryptDataStruct.Sha256)
}

func getServiceUpdateID(service cloudprotocol.ServiceInfoFromCloud) (id string) {
	return base64.URLEncoding.EncodeToString(service.DecryptDataStruct.Sha256)
}

func isUsersEqual(users1, users2 []string) (result bool) {
	if users1 == nil && users2 == nil {
		return true
	}

	if users1 == nil || users2 == nil {
		return false
	}

	if len(users1) != len(users2) {
		return false
	}

	for i := range users1 {
		if users1[i] != users2[i] {
			return false
		}
	}

	return true
}
