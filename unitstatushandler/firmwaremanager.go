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
	"slices"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	semver "github.com/hashicorp/go-version"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_common/utils/semverutils"
	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/downloader"
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
}

type firmwareUpdate struct {
	Schedule   cloudprotocol.ScheduleRule       `json:"schedule,omitempty"`
	Components []cloudprotocol.ComponentInfo    `json:"components,omitempty"`
	CertChains []cloudprotocol.CertificateChain `json:"certChains,omitempty"`
	Certs      []cloudprotocol.Certificate      `json:"certs,omitempty"`
}

type firmwareManager struct {
	sync.Mutex

	statusChannel chan cmserver.UpdateFOTAStatus

	downloader      firmwareDownloader
	statusHandler   firmwareStatusHandler
	firmwareUpdater FirmwareUpdater
	storage         Storage

	stateMachine  *updateStateMachine
	statusMutex   sync.RWMutex
	pendingUpdate *firmwareUpdate

	ComponentStatuses map[string]*cloudprotocol.ComponentStatus `json:"componentStatuses,omitempty"`
	CurrentUpdate     *firmwareUpdate                           `json:"currentUpdate,omitempty"`
	DownloadResult    map[string]*downloadResult                `json:"downloadResult,omitempty"`
	CurrentState      string                                    `json:"currentState,omitempty"`
	UpdateErr         *cloudprotocol.ErrorInfo                  `json:"updateErr,omitempty"`
	TTLDate           time.Time                                 `json:"ttlDate,omitempty"`
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newFirmwareManager(statusHandler firmwareStatusHandler, downloader firmwareDownloader,
	firmwareUpdater FirmwareUpdater, storage Storage, defaultTTL time.Duration,
) (manager *firmwareManager, err error) {
	manager = &firmwareManager{
		statusChannel:   make(chan cmserver.UpdateFOTAStatus, 1),
		downloader:      downloader,
		statusHandler:   statusHandler,
		firmwareUpdater: firmwareUpdater,
		storage:         storage,
		CurrentState:    stateNoUpdate,
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
				ComponentID:   *component.ComponentID,
				ComponentType: component.ComponentType,
				Version:       component.Version,
			})
		}
	}

	return status
}

func (manager *firmwareManager) processDesiredStatus(desiredStatus cloudprotocol.DesiredStatus) error {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Process desired FOTA")

	update := &firmwareUpdate{
		Schedule:   desiredStatus.FOTASchedule,
		Components: make([]cloudprotocol.ComponentInfo, 0),
		CertChains: desiredStatus.CertificateChains,
		Certs:      desiredStatus.Certificates,
	}

	installedComponents, err := manager.firmwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	var desiredComponentsWithNoID []cloudprotocol.ComponentInfo

desiredLoop:
	for _, desiredComponent := range desiredStatus.Components {
		if err := validateComponent(desiredComponent); err != nil {
			log.WithFields(log.Fields{
				"id":      desiredComponent.ComponentID,
				"type":    desiredComponent.ComponentType,
				"version": desiredComponent.Version,
			}).Warnf("Skip invalid component: %v", err)
			continue
		}

		if desiredComponent.ComponentID == nil {
			desiredComponentsWithNoID = append(desiredComponentsWithNoID, desiredComponent)
			continue
		}

		for _, installedComponent := range installedComponents {
			if *desiredComponent.ComponentID == installedComponent.ComponentID &&
				desiredComponent.ComponentType == installedComponent.ComponentType {
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
			"type":    desiredComponent.ComponentType,
			"version": desiredComponent.Version,
		}).Error("Desired component not found")
	}

	handleDesiredComponentsWithNoID(desiredComponentsWithNoID, installedComponents, update)

	if len(update.Components) != 0 {
		log.WithField("components", update.Components).Debug("FOTA update required")

		if err = manager.newUpdate(update); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		log.Debug("No FOTA update required")
	}

	return nil
}

func (manager *firmwareManager) startUpdate() (err error) {
	manager.Lock()
	defer manager.Unlock()

	log.Debug("Start firmware update")

	if err = manager.stateMachine.sendEvent(eventStartUpdate, nil); err != nil {
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

/***********************************************************************************************************************
 * Implementer
 **********************************************************************************************************************/

func (manager *firmwareManager) stateChanged(event, state string, updateErr error) {
	var errorInfo *cloudprotocol.ErrorInfo

	if updateErr != nil {
		errorInfo = &cloudprotocol.ErrorInfo{Message: updateErr.Error()}
	}

	if event == eventCancel {
		for id, status := range manager.ComponentStatuses {
			if status.Status != cloudprotocol.ErrorStatus {
				manager.updateComponentStatusByID(id, cloudprotocol.ErrorStatus, errorInfo)
			}
		}
	}

	manager.CurrentState = state
	manager.UpdateErr = errorInfo

	log.WithFields(log.Fields{
		"state": state,
		"event": event,
	}).Debug("Firmware manager state changed")

	if updateErr != nil {
		log.Errorf("Firmware update error: %v", updateErr)
	}

	manager.sendCurrentStatus()

	if err := manager.saveState(); err != nil {
		log.Errorf("Can't save current firmware manager state: %v", err)
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
				log.Errorf("Can't start new firmware update: %v", err)
			}
		}()
	}
}

func createDownloadRequest(components []cloudprotocol.ComponentInfo) map[string]downloader.PackageInfo {
	request := make(map[string]downloader.PackageInfo)

	for _, component := range components {
		if component.ComponentID == nil {
			continue
		}

		downloadID := getDownloadID(component)

		if _, ok := request[downloadID]; ok {
			log.WithFields(log.Fields{
				"id":      downloadID,
				"version": component.Version,
			}).Debug("Skip duplicate download component")

			continue
		}

		log.WithFields(log.Fields{
			"id":      downloadID,
			"version": component.Version,
		}).Debug("Download component")

		request[downloadID] = downloader.PackageInfo{
			URLs:          component.URLs,
			Sha256:        component.Sha256,
			Size:          component.Size,
			TargetType:    cloudprotocol.DownloadTargetComponent,
			TargetID:      downloadID,
			TargetVersion: component.Version,
		}
	}

	return request
}

func (manager *firmwareManager) download(ctx context.Context) {
	var downloadErr error

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			if downloadErr != nil {
				manager.stateMachine.finishOperation(ctx, eventCancel, downloadErr)
			} else {
				manager.stateMachine.finishOperation(ctx, eventFinishDownload, nil)
			}
		}()
	}()

	manager.DownloadResult = nil

	manager.statusMutex.Lock()

	manager.ComponentStatuses = make(map[string]*cloudprotocol.ComponentStatus)
	request := createDownloadRequest(manager.CurrentUpdate.Components)

	for _, component := range manager.CurrentUpdate.Components {
		if component.ComponentID == nil {
			continue
		}

		if _, ok := request[getDownloadID(component)]; ok {
			manager.ComponentStatuses[*component.ComponentID] = &cloudprotocol.ComponentStatus{
				ComponentID:   *component.ComponentID,
				ComponentType: component.ComponentType,
				Version:       component.Version,
				Status:        cloudprotocol.DownloadingStatus,
			}
		}
	}

	manager.statusMutex.Unlock()

	// Nothing to download
	if len(request) == 0 {
		return
	}

	manager.DownloadResult = manager.downloader.download(ctx, request, false, manager.updateComponentStatusByDownloadID)

	downloadErr = getDownloadError(manager.DownloadResult)

	for _, item := range manager.ComponentStatuses {
		if item.ErrorInfo != nil {
			log.WithFields(log.Fields{
				"id":      item.ComponentID,
				"type":    item.ComponentType,
				"version": item.Version,
			}).Errorf("Error downloading component: %s", item.ErrorInfo.Message)

			continue
		}

		downloadID := getDownloadID(cloudprotocol.ComponentInfo{
			ComponentID:   &item.ComponentID,
			ComponentType: item.ComponentType,
			Version:       item.Version,
		})

		log.WithFields(log.Fields{
			"downloadID":  downloadID,
			"componentID": item.ComponentID,
			"type":        item.ComponentType,
			"version":     item.Version,
		}).Debug("Component successfully downloaded")

		manager.updateComponentStatusByDownloadID(downloadID, cloudprotocol.PendingStatus, nil)
	}
}

func (manager *firmwareManager) readyToUpdate() {
	manager.stateMachine.scheduleUpdate(manager.CurrentUpdate.Schedule)
}

func (manager *firmwareManager) update(ctx context.Context) {
	var updateErr error

	defer func() {
		go func() {
			manager.Lock()
			defer manager.Unlock()

			manager.stateMachine.finishOperation(ctx, eventFinishUpdate, updateErr)
		}()
	}()

	if len(manager.CurrentUpdate.Components) != 0 {
		if err := manager.updateComponents(ctx); err != nil {
			updateErr = err
		}
	}
}

func (manager *firmwareManager) updateTimeout() {
	manager.Lock()
	defer manager.Unlock()

	if manager.stateMachine.canTransit(eventCancel) {
		if err := manager.stateMachine.sendEvent(eventCancel, aoserrors.New("update timeout")); err != nil {
			log.Errorf("Can't cancel update: %v", err)
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
		if reflect.DeepEqual(update.Components, manager.CurrentUpdate.Components) {
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

		if err = manager.stateMachine.sendEvent(eventCancel, aoserrors.Wrap(context.Canceled)); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (manager *firmwareManager) updateComponents(ctx context.Context) (componentsErr error) {
	defer func() {
		switch {
		case errors.Is(ctx.Err(), context.Canceled):

		case componentsErr == nil:
			for _, status := range manager.ComponentStatuses {
				log.WithFields(log.Fields{
					"id":      status.ComponentID,
					"type":    status.ComponentType,
					"version": status.Version,
				}).Info("Component successfully updated")
			}

		default:
			for id, status := range manager.ComponentStatuses {
				if status.Status != cloudprotocol.ErrorStatus {
					manager.updateComponentStatusByID(id, cloudprotocol.ErrorStatus, &cloudprotocol.ErrorInfo{
						Message: "update aborted due to error: " + componentsErr.Error(),
					})
				}

				log.WithFields(log.Fields{
					"id":      status.ComponentID,
					"type":    status.ComponentType,
					"version": status.Version,
				}).Errorf("Error updating component: %s", status.ErrorInfo.Message)
			}
		}
	}()

	updateComponents := make([]cloudprotocol.ComponentInfo, 0, len(manager.CurrentUpdate.Components))

	for _, component := range manager.CurrentUpdate.Components {
		if component.ComponentID == nil {
			continue
		}

		log.WithFields(log.Fields{
			"id":      component.ComponentID,
			"type":    component.ComponentType,
			"version": component.Version,
		}).Debug("Update component")

		manager.updateComponentStatusByID(*component.ComponentID, cloudprotocol.InstallingStatus, nil)

		downloadInfo, ok := manager.DownloadResult[getDownloadID(component)]
		if !ok {
			err := aoserrors.New("update ID not found")

			manager.updateComponentStatusByID(*component.ComponentID, cloudprotocol.ErrorStatus,
				&cloudprotocol.ErrorInfo{Message: err.Error()})

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
	case err := <-manager.asyncUpdate(updateComponents):
		return err

	case <-ctx.Done():
		if err := ctx.Err(); err != nil {
			return err
		}

		return nil
	}
}

func (manager *firmwareManager) sendCurrentStatus() {
	manager.statusChannel <- manager.getCurrentStatus()
}

func (manager *firmwareManager) updateComponentStatusByDownloadID(id, status string,
	componentErr *cloudprotocol.ErrorInfo,
) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	found := false

	for _, info := range manager.ComponentStatuses {
		downloadID := getDownloadID(cloudprotocol.ComponentInfo{
			ComponentID:   &info.ComponentID,
			ComponentType: info.ComponentType,
			Version:       info.Version,
		})

		if downloadID != id {
			continue
		}

		info.Status = status
		info.ErrorInfo = componentErr

		manager.statusHandler.updateComponentStatus(*info)

		found = true
	}

	if !found {
		log.Errorf("Can't update firmware component status: id %s not found", id)

		return
	}
}

func (manager *firmwareManager) updateComponentStatusByID(id, status string, componentErr *cloudprotocol.ErrorInfo) {
	manager.statusMutex.Lock()
	defer manager.statusMutex.Unlock()

	info, ok := manager.ComponentStatuses[id]
	if !ok {
		log.Errorf("Can't update firmware component status: id %s not found", id)
		return
	}

	info.Status = status
	info.ErrorInfo = componentErr

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

func (manager *firmwareManager) asyncUpdate(updateComponents []cloudprotocol.ComponentInfo) (channel <-chan error) {
	finishChannel := make(chan error, 1)

	go func() {
		var err error

		updateResult, updateErr := manager.firmwareUpdater.UpdateComponents(
			updateComponents, manager.CurrentUpdate.CertChains, manager.CurrentUpdate.Certs)
		if updateErr != nil {
			err = aoserrors.Wrap(updateErr)
		}

		for id, status := range manager.ComponentStatuses {
			for _, item := range updateResult {
				if item.ComponentID == status.ComponentID && item.Version == status.Version {
					if err == nil {
						if item.ErrorInfo != nil {
							err = aoserrors.New(item.ErrorInfo.Message)
						}
					}

					manager.updateComponentStatusByID(id, item.Status, item.ErrorInfo)
				}
			}
		}

		finishChannel <- err
	}()

	return finishChannel
}

func getDownloadID(component cloudprotocol.ComponentInfo) string {
	return component.ComponentType + ":" + component.Version
}

func validateComponent(component cloudprotocol.ComponentInfo) error {
	if component.ComponentType == "" {
		return aoserrors.New("component type is empty")
	}

	if err := validateSemver(component.Version); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func validateSemver(version string) error {
	_, err := semver.NewSemver(version)

	return aoserrors.Wrap(err)
}

func handleDesiredComponentsWithNoID(componentsWithNoID []cloudprotocol.ComponentInfo,
	installedComponents []cloudprotocol.ComponentStatus, update *firmwareUpdate,
) {
	log.Debug("Handle desired components with no ID")

	newComponents := make(map[string]cloudprotocol.ComponentInfo)

	for _, installedComponent := range installedComponents {
		componentID := installedComponent.ComponentID

		if slices.ContainsFunc(update.Components, func(component cloudprotocol.ComponentInfo) bool {
			return *component.ComponentID == componentID
		}) {
			continue
		}

		for _, component := range componentsWithNoID {
			if component.ComponentType != installedComponent.ComponentType {
				continue
			}

			component.ComponentID = &componentID

			newComponent, ok := newComponents[*component.ComponentID]
			if !ok {
				if err := validateSemver(component.Version); err != nil {
					log.Errorf("Error validating version: %v", err)
					continue
				}

				newComponents[*component.ComponentID] = component

				continue
			}

			greater, err := semverutils.GreaterThan(component.Version, newComponent.Version)
			if err != nil {
				log.Errorf("Error comparing versions: %v", err)
				continue
			}

			if greater {
				newComponents[*component.ComponentID] = component
			}
		}
	}

	for _, component := range newComponents {
		log.WithFields(log.Fields{
			"id":      *component.ComponentID,
			"type":    component.ComponentType,
			"version": component.Version,
		}).Debug("Deducted component ID from installed components")

		update.Components = append(update.Components, component)
	}
}
