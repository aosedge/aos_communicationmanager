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
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/downloader"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type firmwareDownloader interface {
	download(ctx context.Context, request map[string]downloader.PackageInfo,
		continueOnError bool, notifier statusNotifier) (result map[string]*downloadResult)
	releaseDownloadedFirmware() error
}

type firmwareStatusHandler interface {
	updateComponentStatus(componentInfo cloudprotocol.ComponentStatus)
	updateUnitConfigStatus(unitConfigInfo cloudprotocol.UnitConfigStatus)
}

type firmwareUpdate struct {
	Schedule   cloudprotocol.ScheduleRule       `json:"schedule,omitempty"`
	UnitConfig json.RawMessage                  `json:"unitConfig,omitempty"`
	Components []cloudprotocol.ComponentInfo    `json:"components,omitempty"`
	CertChains []cloudprotocol.CertificateChain `json:"certChains,omitempty"`
	Certs      []cloudprotocol.Certificate      `json:"certs,omitempty"`
}

type firmwareManager struct {
	sync.Mutex

	statusChannel chan cmserver.UpdateFOTAStatus

	downloader        firmwareDownloader
	statusHandler     firmwareStatusHandler
	firmwareUpdater   FirmwareUpdater
	unitConfigUpdater UnitConfigUpdater
	storage           Storage
	runner            InstanceRunner

	stateMachine  *updateStateMachine
	statusMutex   sync.RWMutex
	pendingUpdate *firmwareUpdate

	ComponentStatuses map[string]*cloudprotocol.ComponentStatus `json:"componentStatuses,omitempty"`
	UnitConfigStatus  cloudprotocol.UnitConfigStatus            `json:"unitConfigStatus,omitempty"`
	CurrentUpdate     *firmwareUpdate                           `json:"currentUpdate,omitempty"`
	DownloadResult    map[string]*downloadResult                `json:"downloadResult,omitempty"`
	CurrentState      string                                    `json:"currentState,omitempty"`
	UpdateErr         string                                    `json:"updateErr,omitempty"`
	TTLDate           time.Time                                 `json:"ttlDate,omitempty"`
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newFirmwareManager(statusHandler firmwareStatusHandler, downloader firmwareDownloader,
	firmwareUpdater FirmwareUpdater, unitConfigUpdater UnitConfigUpdater,
	storage Storage, runner InstanceRunner, defaultTTL time.Duration,
) (manager *firmwareManager, err error) {
	manager = &firmwareManager{
		statusChannel:     make(chan cmserver.UpdateFOTAStatus, 1),
		downloader:        downloader,
		statusHandler:     statusHandler,
		firmwareUpdater:   firmwareUpdater,
		unitConfigUpdater: unitConfigUpdater,
		storage:           storage,
		runner:            runner,
		CurrentState:      stateNoUpdate,
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
		if component.ComponentID != nil {
			status.Components = append(status.Components, cloudprotocol.ComponentStatus{
				ComponentID: *component.ComponentID, Version: component.Version,
			})
		}
	}

	if len(manager.CurrentUpdate.UnitConfig) != 0 {
		version, _ := manager.unitConfigUpdater.GetUnitConfigVersion(manager.CurrentUpdate.UnitConfig)
		status.UnitConfig = &cloudprotocol.UnitConfigStatus{Version: version}
	}

	return status
}

func (manager *firmwareManager) processDesiredStatus(desiredStatus cloudprotocol.DesiredStatus) error {
	manager.Lock()
	defer manager.Unlock()

	update := &firmwareUpdate{
		Schedule:   desiredStatus.FOTASchedule,
		UnitConfig: desiredStatus.UnitConfig,
		Components: make([]cloudprotocol.ComponentInfo, 0),
		CertChains: desiredStatus.CertificateChains,
		Certs:      desiredStatus.Certificates,
	}

	installedComponents, err := manager.firmwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

desiredLoop:
	for _, desiredComponent := range desiredStatus.Components {
		if desiredComponent.ComponentID == nil {
			continue
		}

		for _, installedComponent := range installedComponents {
			if *desiredComponent.ComponentID == installedComponent.ComponentID {
				if desiredComponent.Version == installedComponent.Version &&
					installedComponent.Status == cloudprotocol.InstalledStatus {
					continue desiredLoop
				} else {
					update.Components = append(update.Components, desiredComponent)
					continue desiredLoop
				}
			}
		}

		log.WithFields(log.Fields{
			"id":      desiredComponent.ComponentID,
			"version": desiredComponent.Version,
		}).Error("Desired component not found")
	}

	if len(update.UnitConfig) != 0 || len(update.Components) != 0 {
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

func (manager *firmwareManager) getComponentStatuses() (status []cloudprotocol.ComponentStatus, err error) {
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

func (manager *firmwareManager) getUnitConfigStatuses() (status []cloudprotocol.UnitConfigStatus, err error) {
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

	if manager.CurrentState == stateNoUpdate || len(manager.CurrentUpdate.UnitConfig) == 0 {
		return status, nil
	}

	status = append(status, manager.UnitConfigStatus)

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

		if len(manager.CurrentUpdate.UnitConfig) != 0 {
			if manager.UnitConfigStatus.Status != cloudprotocol.ErrorStatus {
				manager.updateUnitConfigStatus(cloudprotocol.ErrorStatus, updateErr)
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
	log.Debug("Release downloaded firmware")

	if err := manager.downloader.releaseDownloadedFirmware(); err != nil {
		log.Errorf("Error release downloading firmware: %v", err)
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

	if len(manager.CurrentUpdate.UnitConfig) != 0 {
		manager.UnitConfigStatus.Version = ""

		version, err := manager.unitConfigUpdater.GetUnitConfigVersion(manager.CurrentUpdate.UnitConfig)

		manager.UnitConfigStatus.Version = version

		if err != nil {
			log.Errorf("Error getting unit config version: %s", err)

			downloadErr = aoserrors.Wrap(err).Error()
			manager.updateUnitConfigStatus(cloudprotocol.ErrorStatus, downloadErr)

			return
		}

		log.WithFields(log.Fields{"version": version}).Debug("Get unit config version")

		manager.updateUnitConfigStatus(cloudprotocol.PendingStatus, "")
	}

	manager.statusMutex.Lock()

	manager.ComponentStatuses = make(map[string]*cloudprotocol.ComponentStatus)
	request := make(map[string]downloader.PackageInfo)

	for _, component := range manager.CurrentUpdate.Components {
		if component.ComponentID == nil {
			continue
		}

		log.WithFields(log.Fields{
			"id":      component.ComponentID,
			"version": component.Version,
		}).Debug("Download component")

		request[*component.ComponentID] = downloader.PackageInfo{
			URLs:          component.URLs,
			Sha256:        component.Sha256,
			Size:          component.Size,
			TargetType:    cloudprotocol.DownloadTargetComponent,
			TargetID:      *component.ComponentID,
			TargetVersion: component.Version,
		}
		manager.ComponentStatuses[*component.ComponentID] = &cloudprotocol.ComponentStatus{
			ComponentID: *component.ComponentID,
			Version:     component.Version,
			Status:      cloudprotocol.DownloadingStatus,
		}
	}

	manager.statusMutex.Unlock()

	// Nothing to download
	if len(request) == 0 {
		return
	}

	manager.DownloadResult = manager.downloader.download(ctx, request, false, manager.updateComponentStatusByID)

	downloadErr = getDownloadError(manager.DownloadResult)

	for id, item := range manager.ComponentStatuses {
		if item.ErrorInfo != nil {
			log.WithFields(log.Fields{
				"id":      item.ComponentID,
				"version": item.Version,
			}).Errorf("Error downloading component: %s", item.ErrorInfo.Message)

			continue
		}

		log.WithFields(log.Fields{
			"id":      item.ComponentID,
			"version": item.Version,
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

	if len(manager.CurrentUpdate.UnitConfig) != 0 {
		if err := manager.updateUnitConfig(ctx); err != "" {
			updateErr = err
			return
		}

		if err := manager.runner.RestartInstances(); err != nil {
			updateErr = err.Error()
			return
		}
	}
}

func (manager *firmwareManager) updateTimeout() {
	manager.Lock()
	defer manager.Unlock()

	if manager.stateMachine.canTransit(eventCancel) {
		if err := manager.stateMachine.sendEvent(eventCancel, aoserrors.New("update timeout").Error()); err != nil {
			log.Errorf("Can't cancel update: %s", err)
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
			unitConfigsEqual(update.UnitConfig, manager.CurrentUpdate.UnitConfig) {
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
					"id":      status.ComponentID,
					"version": status.Version,
				}).Info("Component successfully updated")
			}

		default:
			for id, status := range manager.ComponentStatuses {
				if status.Status != cloudprotocol.ErrorStatus {
					manager.updateComponentStatusByID(id, cloudprotocol.ErrorStatus,
						fmt.Sprintf("update aborted due to error: %s", componentsErr))
				}

				log.WithFields(log.Fields{
					"id":      status.ComponentID,
					"version": status.Version,
				}).Errorf("Error updating component: %v", status.ErrorInfo)
			}
		}
	}()

	updateComponents := make([]cloudprotocol.ComponentInfo, 0, len(manager.CurrentUpdate.Components))

	for _, component := range manager.CurrentUpdate.Components {
		if component.ComponentID == nil {
			continue
		}

		log.WithFields(log.Fields{"id": component.ComponentID, "version": component.Version}).Debug("Update component")

		manager.updateComponentStatusByID(*component.ComponentID, cloudprotocol.InstallingStatus, "")

		downloadInfo, ok := manager.DownloadResult[*component.ComponentID]
		if !ok {
			err := aoserrors.New("update ID not found").Error()

			manager.updateComponentStatusByID(*component.ComponentID, cloudprotocol.ErrorStatus, err)

			return err
		}

		url := url.URL{
			Scheme: "file",
			Path:   downloadInfo.FileName,
		}

		component.URLs = []string{url.String()}

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

	if componentErr != "" {
		info.ErrorInfo = &cloudprotocol.ErrorInfo{Message: componentErr}
	}

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

func (manager *firmwareManager) updateUnitConfig(ctx context.Context) (unitConfigErr string) {
	log.Debug("Update unit config")

	defer func() {
		if unitConfigErr != "" {
			manager.updateUnitConfigStatus(cloudprotocol.ErrorStatus, unitConfigErr)
		}
	}()

	if _, err := manager.unitConfigUpdater.CheckUnitConfig(manager.CurrentUpdate.UnitConfig); err != nil {
		if errors.Is(err, unitconfig.ErrAlreadyInstalled) {
			log.Error("Unit config already installed")

			manager.updateUnitConfigStatus(cloudprotocol.InstalledStatus, "")

			return ""
		}

		return aoserrors.Wrap(err).Error()
	}

	manager.updateUnitConfigStatus(cloudprotocol.InstallingStatus, "")

	if err := manager.unitConfigUpdater.UpdateUnitConfig(manager.CurrentUpdate.UnitConfig); err != nil {
		return aoserrors.Wrap(err).Error()
	}

	manager.updateUnitConfigStatus(cloudprotocol.InstalledStatus, "")

	return ""
}

func (manager *firmwareManager) asyncUpdate(
	updateComponents []cloudprotocol.ComponentInfo,
) (channel <-chan string) {
	finishChannel := make(chan string, 1)

	go func() (errorStr string) {
		defer func() { finishChannel <- errorStr }()

		updateResult, err := manager.firmwareUpdater.UpdateComponents(
			updateComponents, manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs)
		if err != nil {
			errorStr = aoserrors.Wrap(err).Error()
		}

		for id, status := range manager.ComponentStatuses {
			for _, item := range updateResult {
				if item.ComponentID == status.ComponentID && item.Version == status.Version {
					if errorStr == "" {
						if item.ErrorInfo != nil {
							errorStr = item.ErrorInfo.Message
						}
					}

					manager.updateComponentStatusByID(id, item.Status, errorStr)
				}
			}
		}

		return errorStr
	}()

	return finishChannel
}

func (manager *firmwareManager) updateUnitConfigStatus(status, errorStr string) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	manager.UnitConfigStatus.Status = status

	if errorStr != "" {
		manager.UnitConfigStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: errorStr}
	}

	manager.statusHandler.updateUnitConfigStatus(manager.UnitConfigStatus)
}

func unitConfigsEqual(config1, config2 json.RawMessage) (equal bool) {
	var configData1, configData2 interface{}

	if config1 == nil && config2 == nil {
		return true
	}

	if (config1 == nil && config2 != nil) || (config1 != nil && config2 == nil) {
		return false
	}

	if err := json.Unmarshal(config1, &configData1); err != nil {
		log.Errorf("Can't marshal unit config: %s", err)
	}

	if err := json.Unmarshal(config2, &configData2); err != nil {
		log.Errorf("Can't marshal unit config: %s", err)
	}

	return reflect.DeepEqual(configData1, configData2)
}
