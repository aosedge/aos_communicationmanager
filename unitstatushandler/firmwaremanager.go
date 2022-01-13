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
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/cmserver"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type firmwareStatusHandler interface {
	download(ctx context.Context, request map[string]cloudprotocol.DecryptDataStruct,
		continueOnError bool, notifier statusNotifier,
		chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result map[string]*downloadResult)
	updateComponentStatus(componentInfo cloudprotocol.ComponentInfo)
	updateBoardConfigStatus(boardConfigInfo cloudprotocol.BoardConfigInfo)
}

type firmwareUpdate struct {
	Schedule    cloudprotocol.ScheduleRule             `json:"schedule,omitempty"`
	BoardConfig json.RawMessage                        `json:"boardConfig,omitempty"`
	Components  []cloudprotocol.ComponentInfoFromCloud `json:"components,omitempty"`
	CertChains  []cloudprotocol.CertificateChain       `json:"certChains,omitempty"`
	Certs       []cloudprotocol.Certificate            `json:"certs,omitempty"`
}

type firmwareManager struct {
	sync.Mutex

	statusChannel chan cmserver.UpdateFOTAStatus

	statusHandler      firmwareStatusHandler
	firmwareUpdater    FirmwareUpdater
	boardConfigUpdater BoardConfigUpdater
	storage            Storage

	stateMachine  *updateStateMachine
	statusMutex   sync.RWMutex
	pendingUpdate *firmwareUpdate

	ComponentStatuses map[string]*cloudprotocol.ComponentInfo `json:"componentStatuses,omitempty"`
	BoardConfigStatus cloudprotocol.BoardConfigInfo           `json:"boardConfigStatus,omitempty"`
	CurrentUpdate     *firmwareUpdate                         `json:"currentUpdate,omitempty"`
	DownloadResult    map[string]*downloadResult              `json:"downloadResult,omitempty"`
	CurrentState      string                                  `json:"currentState,omitempty"`
	UpdateErr         string                                  `json:"updateErr,omitempty"`
	TTLDate           time.Time                               `json:"ttlDate,omitempty"`
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newFirmwareManager(statusHandler firmwareStatusHandler,
	firmwareUpdater FirmwareUpdater, boardConfigUpdater BoardConfigUpdater,
	storage Storage, defaultTTL time.Duration) (manager *firmwareManager, err error) {
	manager = &firmwareManager{
		statusChannel:      make(chan cmserver.UpdateFOTAStatus, 1),
		statusHandler:      statusHandler,
		firmwareUpdater:    firmwareUpdater,
		boardConfigUpdater: boardConfigUpdater,
		storage:            storage,
		CurrentState:       stateNoUpdate,
	}

	if err = manager.loadState(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"state": manager.CurrentState, "error": manager.UpdateErr}).Debug("New firmware manager")

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
	}, manager, defaultTTL)

	if err = manager.stateMachine.init(manager.TTLDate); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return manager, nil
}

func (manager *firmwareManager) close() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Close firmware manager")

	close(manager.statusChannel)

	if err = manager.stateMachine.close(); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *firmwareManager) getCurrentStatus() (status cmserver.UpdateFOTAStatus) {
	status.State = convertState(manager.CurrentState)
	status.Error = manager.UpdateErr

	if status.State == cmserver.NoUpdate || manager.CurrentUpdate == nil {
		return status
	}

	for _, component := range manager.CurrentUpdate.Components {
		status.Components = append(status.Components, cloudprotocol.ComponentInfo{
			ID: component.ID, AosVersion: component.AosVersion, VendorVersion: component.VendorVersion,
		})
	}

	if len(manager.CurrentUpdate.BoardConfig) != 0 {
		version, _ := manager.boardConfigUpdater.GetBoardConfigVersion(manager.CurrentUpdate.BoardConfig)
		status.BoardConfig = &cloudprotocol.BoardConfigInfo{VendorVersion: version}
	}

	return status
}

func (manager *firmwareManager) getCurrentUpdateState() (status cmserver.UpdateState) {
	manager.Lock()
	defer manager.Unlock()

	return convertState(manager.CurrentState)
}

func (manager *firmwareManager) processDesiredStatus(desiredStatus cloudprotocol.DecodedDesiredStatus) (err error) {
	manager.Lock()
	defer manager.Unlock()

	update := &firmwareUpdate{
		Schedule:    desiredStatus.FOTASchedule,
		BoardConfig: desiredStatus.BoardConfig,
		Components:  make([]cloudprotocol.ComponentInfoFromCloud, 0),
		CertChains:  desiredStatus.CertificateChains,
		Certs:       desiredStatus.Certificates,
	}

	installedComponents, err := manager.firmwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

desiredLoop:
	for _, desiredComponent := range desiredStatus.Components {
		for _, installedComponent := range installedComponents {
			if desiredComponent.ID == installedComponent.ID {
				if desiredComponent.VendorVersion == installedComponent.VendorVersion &&
					installedComponent.Status == cloudprotocol.InstalledStatus {
					continue desiredLoop
				} else {
					update.Components = append(update.Components, desiredComponent)
					continue desiredLoop
				}
			}
		}

		log.WithFields(log.Fields{
			"id":            desiredComponent.ID,
			"vendorVersion": desiredComponent.VendorVersion,
		}).Error("Desired component not found")
	}

	if len(update.BoardConfig) != 0 || len(update.Components) != 0 {
		if err = manager.newUpdate(update); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *firmwareManager) startUpdate() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Start firmware update")

	if err = manager.stateMachine.sendEvent(eventStartUpdate, ""); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *firmwareManager) getComponentStatuses() (status []cloudprotocol.ComponentInfo, err error) {
	manager.Lock()
	defer manager.Unlock()

	manager.statusMutex.RLock()
	defer manager.statusMutex.RUnlock()

	info, err := manager.firmwareUpdater.GetStatus()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if manager.CurrentState == stateNoUpdate {
		return info, nil
	}

	// Get installed info

	for _, item := range info {
		if item.Status == cloudprotocol.InstalledStatus {
			status = append(status, item)
		}
	}

	// Append currently processing info

	for _, item := range manager.ComponentStatuses {
		status = append(status, *item)
	}

	return status, nil
}

func (manager *firmwareManager) getBoardConfigStatuses() (status []cloudprotocol.BoardConfigInfo, err error) {
	manager.Lock()
	defer manager.Unlock()

	manager.statusMutex.RLock()
	defer manager.statusMutex.RUnlock()

	info, err := manager.boardConfigUpdater.GetStatus()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	status = append(status, info)

	// Append currently processing info

	if manager.CurrentState == stateNoUpdate || len(manager.CurrentUpdate.BoardConfig) == 0 {
		return status, nil
	}

	status = append(status, manager.BoardConfigStatus)

	return status, nil
}

/***********************************************************************************************************************
 * Implementer
 **********************************************************************************************************************/

func (manager *firmwareManager) stateChanged(event, state string, updateErr string) {
	if event == eventCancel {
		for id, status := range manager.ComponentStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateComponentStatusByID(id, cloudprotocol.ErrorStatus, updateErr)
			}
		}

		if len(manager.CurrentUpdate.BoardConfig) != 0 {
			if manager.BoardConfigStatus.Status != cloudprotocol.ErrorStatus {
				manager.updateBoardConfigStatus(cloudprotocol.ErrorStatus, updateErr)
			}
		}
	}

	manager.CurrentState = state
	manager.UpdateErr = updateErr

	log.WithFields(log.Fields{
		"state": state,
		"event": event,
	}).Debug("Firmware manager state changed")

	if updateErr != "" {
		log.Errorf("Firmware update error: %s", updateErr)
	}

	manager.sendCurrentStatus()

	if err := manager.saveState(); err != nil {
		log.Errorf("Can't save current firmware manager state: %s", err)
	}
}

func (manager *firmwareManager) noUpdate() {
	// Remove downloaded files
	for _, result := range manager.DownloadResult {
		if result.FileName != "" {
			log.WithField("file", result.FileName).Debug("Remove firmware update file")

			if err := os.RemoveAll(result.FileName); err != nil {
				log.WithField("file", result.FileName).Errorf("Can't remove update file: %s", err)
			}
		}
	}

	if manager.pendingUpdate != nil {
		log.Debug("Handle pending firmware update")

		manager.CurrentUpdate = manager.pendingUpdate
		manager.pendingUpdate = nil

		go func() {
			manager.Lock()
			defer manager.Unlock()

			var err error

			if manager.TTLDate, err = manager.stateMachine.startNewUpdate(
				time.Duration(manager.CurrentUpdate.Schedule.TTL) * time.Second); err != nil {
				log.Errorf("Can't start new firmware update: %s", err)
			}
		}()
	}
}

func (manager *firmwareManager) download(ctx context.Context) {
	var downloadErr string

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			if downloadErr != "" {
				manager.stateMachine.finishOperation(ctx, eventCancel, downloadErr)
			} else {
				manager.stateMachine.finishOperation(ctx, eventFinishDownload, "")
			}
		}()
	}()

	manager.DownloadResult = nil

	if len(manager.CurrentUpdate.BoardConfig) != 0 {
		manager.BoardConfigStatus.VendorVersion = ""

		version, err := manager.boardConfigUpdater.GetBoardConfigVersion(manager.CurrentUpdate.BoardConfig)

		manager.BoardConfigStatus.VendorVersion = version

		if err != nil {
			log.Errorf("Error getting board config version: %s", err)

			downloadErr = aoserrors.Wrap(err).Error()
			manager.updateBoardConfigStatus(cloudprotocol.ErrorStatus, downloadErr)

			return
		}

		log.WithFields(log.Fields{"version": version}).Debug("Get board config version")

		manager.updateBoardConfigStatus(cloudprotocol.PendingStatus, "")
	}

	manager.statusMutex.Lock()

	manager.ComponentStatuses = make(map[string]*cloudprotocol.ComponentInfo)
	request := make(map[string]cloudprotocol.DecryptDataStruct)

	for _, component := range manager.CurrentUpdate.Components {
		log.WithFields(log.Fields{
			"id":      component.ID,
			"version": component.VendorVersion,
		}).Debug("Download component")

		request[component.ID] = component.DecryptDataStruct
		manager.ComponentStatuses[component.ID] = &cloudprotocol.ComponentInfo{
			ID:            component.ID,
			AosVersion:    component.AosVersion,
			VendorVersion: component.VendorVersion,
			Status:        cloudprotocol.DownloadingStatus,
		}
	}

	manager.statusMutex.Unlock()

	// Nothing to download
	if len(request) == 0 {
		return
	}

	manager.DownloadResult = manager.statusHandler.download(ctx, request, false, manager.updateComponentStatusByID,
		manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs)

	downloadErr = getDownloadError(manager.DownloadResult)

	for id, item := range manager.ComponentStatuses {
		if item.Error != "" {
			log.WithFields(log.Fields{
				"id":      item.ID,
				"version": item.VendorVersion,
			}).Errorf("Error downloading component: %s", item.Error)

			continue
		}

		log.WithFields(log.Fields{
			"id":      item.ID,
			"version": item.VendorVersion,
		}).Debug("Component successfully downloaded")

		manager.updateComponentStatusByID(id, cloudprotocol.PendingStatus, "")
	}
}

func (manager *firmwareManager) readyToUpdate() {
	manager.stateMachine.scheduleUpdate(manager.CurrentUpdate.Schedule)
}

func (manager *firmwareManager) update(ctx context.Context) {
	var updateErr string

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			manager.stateMachine.finishOperation(ctx, eventFinishUpdate, updateErr)
		}()
	}()

	if len(manager.CurrentUpdate.Components) != 0 {
		if err := manager.updateComponents(ctx); err != "" {
			updateErr = err
			return
		}
	}

	if len(manager.CurrentUpdate.BoardConfig) != 0 {
		if err := manager.updateBoardConfig(ctx); err != "" {
			updateErr = err
			return
		}
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (manager *firmwareManager) newUpdate(update *firmwareUpdate) (err error) {
	log.Debug("New firmware update")

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
		if reflect.DeepEqual(update.Components, manager.CurrentUpdate.Components) &&
			boardConfigsEqual(update.BoardConfig, manager.CurrentUpdate.BoardConfig) {
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

		if err = manager.stateMachine.sendEvent(eventCancel, aoserrors.Wrap(context.Canceled).Error()); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *firmwareManager) updateComponents(ctx context.Context) (componentsErr string) {
	defer func() {
		switch {
		case strings.Contains(componentsErr, context.Canceled.Error()):

		case componentsErr == "":
			for _, status := range manager.ComponentStatuses {
				log.WithFields(log.Fields{
					"id":      status.ID,
					"version": status.VendorVersion,
				}).Info("Component successfully updated")
			}

		default:
			for id, status := range manager.ComponentStatuses {
				if status.Status != cloudprotocol.ErrorStatus {
					manager.updateComponentStatusByID(id, cloudprotocol.ErrorStatus,
						fmt.Sprintf("update aborted due to error: %s", componentsErr))
				}

				log.WithFields(log.Fields{
					"id":      status.ID,
					"version": status.VendorVersion,
				}).Errorf("Error updating component: %s", status.Error)
			}
		}
	}()

	updateComponents := make([]cloudprotocol.ComponentInfoFromCloud, 0, len(manager.CurrentUpdate.Components))

	for _, component := range manager.CurrentUpdate.Components {
		log.WithFields(log.Fields{"id": component.ID, "version": component.VendorVersion}).Debug("Update component")

		manager.updateComponentStatusByID(component.ID, cloudprotocol.InstallingStatus, "")

		downloadInfo, ok := manager.DownloadResult[component.ID]
		if !ok {
			err := aoserrors.New("update ID not found").Error()

			manager.updateComponentStatusByID(component.ID, cloudprotocol.ErrorStatus, err)

			return err
		}

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		component.URLs = []string{url.String()}
		component.Size = downloadInfo.FileInfo.Size
		component.Sha256 = downloadInfo.FileInfo.Sha256
		component.Sha512 = downloadInfo.FileInfo.Sha512

		updateComponents = append(updateComponents, component)
	}

	select {
	case errStr := <-manager.asyncUpdate(updateComponents):
		return errStr

	case <-ctx.Done():
		errStr := ""

		if err := ctx.Err(); err != nil {
			errStr = err.Error()
		}

		return errStr
	}
}

func (manager *firmwareManager) sendCurrentStatus() {
	manager.statusChannel <- manager.getCurrentStatus()
}

func (manager *firmwareManager) updateComponentStatusByID(id, status, componentErr string) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.ComponentStatuses[id]
	if !ok {
		log.Errorf("Can't update firmware component status: id %s not found", id)
		return
	}

	info.Status = status
	info.Error = componentErr

	manager.statusHandler.updateComponentStatus(*info)
}

func (manager *firmwareManager) loadState() (err error) {
	stateJSON, err := manager.storage.GetFirmwareUpdateState()
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

func (manager *firmwareManager) saveState() (err error) {
	stateJSON, err := json.Marshal(manager)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = manager.storage.SetFirmwareUpdateState(stateJSON); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (manager *firmwareManager) updateBoardConfig(ctx context.Context) (boardConfigErr string) {
	log.Debug("Update board config")

	defer func() {
		if boardConfigErr != "" {
			manager.updateBoardConfigStatus(cloudprotocol.ErrorStatus, boardConfigErr)
		}
	}()

	if _, err := manager.boardConfigUpdater.CheckBoardConfig(manager.CurrentUpdate.BoardConfig); err != nil {
		return aoserrors.Wrap(err).Error()
	}

	manager.updateBoardConfigStatus(cloudprotocol.InstallingStatus, "")

	if err := manager.boardConfigUpdater.UpdateBoardConfig(manager.CurrentUpdate.BoardConfig); err != nil {
		return aoserrors.Wrap(err).Error()
	}

	manager.updateBoardConfigStatus(cloudprotocol.InstalledStatus, "")

	return ""
}

func (manager *firmwareManager) asyncUpdate(updateComponents []cloudprotocol.ComponentInfoFromCloud) (channel <-chan string) {
	finishChannel := make(chan string, 1)

	go func() (errorStr string) {
		defer func() { finishChannel <- errorStr }()

		updateResult, err := manager.firmwareUpdater.UpdateComponents(updateComponents)
		if err != nil {
			errorStr = aoserrors.Wrap(err).Error()
		}

		for id, status := range manager.ComponentStatuses {
			for _, item := range updateResult {
				if item.ID == status.ID && item.VendorVersion == status.VendorVersion {
					if errorStr == "" {
						errorStr = item.Error
					}

					manager.updateComponentStatusByID(id, item.Status, item.Error)
				}
			}
		}

		return errorStr
	}()

	return finishChannel
}

func (manager *firmwareManager) updateBoardConfigStatus(status, errorStr string) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	manager.BoardConfigStatus.Status = status
	manager.BoardConfigStatus.Error = errorStr

	manager.statusHandler.updateBoardConfigStatus(manager.BoardConfigStatus)
}

func boardConfigsEqual(config1, config2 json.RawMessage) (equal bool) {
	var configData1, configData2 interface{}

	if config1 == nil && config2 == nil {
		return true
	}

	if (config1 == nil && config2 != nil) || (config1 != nil && config2 == nil) {
		return false
	}

	if err := json.Unmarshal(config1, &configData1); err != nil {
		log.Errorf("Can't marshal board config: %s", err)
	}

	if err := json.Unmarshal(config2, &configData2); err != nil {
		log.Errorf("Can't marshal board config: %s", err)
	}

	return reflect.DeepEqual(configData1, configData2)
}
