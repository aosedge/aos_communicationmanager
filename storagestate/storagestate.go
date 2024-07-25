// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

package storagestate

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/utils/fs"
	"github.com/fsnotify/fsnotify"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	stateFileFormat  = "%s_state.dat"
	stateChannelSize = 32
)

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// These global variables are used to be able to mocking the functionality in tests.
//
//nolint:gochecknoglobals
var (
	SetUserFSQuota     = fs.SetUserFSQuota
	StateChangeTimeout = 1 * time.Second
)

// ErrNotExist is returned when requested entry not exist in DB.
var (
	ErrNotExist = errors.New("entry does not exist")
	ErrNotFound = errors.New("instance not found")
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// StorageStateInstanceInfo storage state instance info.
type StorageStateInstanceInfo struct {
	aostypes.InstanceIdent
	InstanceID    string
	StorageQuota  uint64
	StateQuota    uint64
	StateChecksum []byte
}

// MessageSender sends messages to the cloud.
type MessageSender interface {
	SendInstanceNewState(newState cloudprotocol.NewState) error
	SendInstanceStateRequest(request cloudprotocol.StateRequest) error
}

// Storage storage interface.
type Storage interface {
	GetAllStorageStateInfo() ([]StorageStateInstanceInfo, error)
	GetStorageStateInfo(instanceIdent aostypes.InstanceIdent) (StorageStateInstanceInfo, error)
	SetStorageStateQuotas(instanceIdent aostypes.InstanceIdent, storageQuota, stateQuota uint64) error
	AddStorageStateInfo(storageStateInfo StorageStateInstanceInfo) error
	SetStateChecksum(instanceIdent aostypes.InstanceIdent, checksum []byte) error
	RemoveStorageStateInfo(instanceIdent aostypes.InstanceIdent) error
}

// SetupParams setup storage state instance params.
type SetupParams struct {
	aostypes.InstanceIdent
	UID          int
	GID          int
	StateQuota   uint64
	StorageQuota uint64
}

// StateChangedInfo contains state changed information.
type StateChangedInfo struct {
	aostypes.InstanceIdent
	Checksum []byte
}

// StorageState storage state instance.
type StorageState struct {
	sync.Mutex
	messageSender       MessageSender
	storageDir          string
	stateDir            string
	storage             Storage
	statesMap           map[aostypes.InstanceIdent]*stateParams
	watcher             *fsnotify.Watcher
	newStateChannel     chan cloudprotocol.NewState
	stateRequestChannel chan cloudprotocol.StateRequest
	isSamePartition     bool
}

type stateParams struct {
	instanceID         string
	stateFilePath      string
	quota              uint64
	checksum           []byte
	changeTimer        *time.Timer
	changeTimerChannel chan bool
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates storagestate instance.
func New(cfg *config.Config, messageSender MessageSender, storage Storage) (storageState *StorageState, err error) {
	log.Debug("Create storagestate")

	storageState = &StorageState{
		storage:             storage,
		storageDir:          cfg.StorageDir,
		stateDir:            cfg.StateDir,
		messageSender:       messageSender,
		statesMap:           make(map[aostypes.InstanceIdent]*stateParams),
		newStateChannel:     make(chan cloudprotocol.NewState, stateChannelSize),
		stateRequestChannel: make(chan cloudprotocol.StateRequest, stateChannelSize),
	}

	if err = os.MkdirAll(storageState.storageDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = os.MkdirAll(storageState.stateDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if storageState.watcher, err = fsnotify.NewWatcher(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	storageMountPoint, err := fs.GetMountPoint(cfg.StorageDir)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	stateMountPoint, err := fs.GetMountPoint(cfg.StateDir)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	storageState.isSamePartition = storageMountPoint == stateMountPoint

	if err = storageState.initStateWatching(); err != nil {
		log.Errorf("Can't init state watching: %v", err)
	}

	go storageState.processWatcher()

	return storageState, nil
}

// Close closes storagestate instance.
func (storageState *StorageState) Close() {
	log.Debug("Close storagestate")

	storageState.watcher.Close()
}

// Setup setups storagestate instance.
func (storageState *StorageState) Setup(
	params SetupParams,
) (storagePath string, statePath string, err error) {
	storageState.Lock()
	defer storageState.Unlock()

	log.WithFields(log.Fields{
		"instance":     params.Instance,
		"serviceID":    params.ServiceID,
		"subjectID":    params.SubjectID,
		"storageQuota": params.StorageQuota,
		"stateQuota":   params.StateQuota,
	}).Debug("Setup storage and state")

	storageStateInfo, err := storageState.storage.GetStorageStateInfo(params.InstanceIdent)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return "", "", aoserrors.Wrap(err)
		}

		storageStateInfo = StorageStateInstanceInfo{
			InstanceID:    uuid.New().String(),
			InstanceIdent: params.InstanceIdent,
		}

		if err = storageState.storage.AddStorageStateInfo(storageStateInfo); err != nil {
			return "", "", aoserrors.Wrap(err)
		}
	}

	instanceID := storageStateInfo.InstanceID

	if storagePath, err = storageState.prepareStorage(instanceID, params); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	if err := storageState.stopStateWatching(params.InstanceIdent); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	if storageStateInfo.StateQuota != params.StateQuota || storageStateInfo.StorageQuota != params.StorageQuota {
		if err = storageState.setQuotasFS(params); err != nil {
			return "", "", aoserrors.Wrap(err)
		}

		if err = storageState.storage.SetStorageStateQuotas(
			params.InstanceIdent, params.StorageQuota, params.StateQuota); err != nil {
			return "", "", aoserrors.Wrap(err)
		}
	}

	if statePath, err = storageState.prepareState(
		instanceID, params, storageStateInfo.StateChecksum); err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	return storagePath, statePath, nil
}

// Cleanup cleans storagestate instance.
func (storageState *StorageState) Cleanup(instanceIdent aostypes.InstanceIdent) error {
	storageState.Lock()
	defer storageState.Unlock()

	log.WithFields(log.Fields{
		"instance":  instanceIdent.Instance,
		"serviceID": instanceIdent.ServiceID,
		"subjectID": instanceIdent.SubjectID,
	}).Debug("Clean storage and state")

	if err := storageState.stopStateWatching(instanceIdent); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storageState *StorageState) RemoveServiceInstance(instanceIdent aostypes.InstanceIdent) error {
	stateStorageInfos, err := storageState.storage.GetAllStorageStateInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, stateStorageInfo := range stateStorageInfos {
		if stateStorageInfo.ServiceID != instanceIdent.ServiceID ||
			stateStorageInfo.SubjectID != instanceIdent.SubjectID ||
			stateStorageInfo.Instance != instanceIdent.Instance {
			continue
		}

		log.WithFields(log.Fields{
			"instance":  stateStorageInfo.Instance,
			"serviceID": stateStorageInfo.ServiceID,
			"subjectID": stateStorageInfo.SubjectID,
		}).Debug("Remove storage and state")

		if err := storageState.Cleanup(stateStorageInfo.InstanceIdent); err != nil {
			return aoserrors.Wrap(err)
		}

		if err := storageState.remove(stateStorageInfo.InstanceIdent, stateStorageInfo.InstanceID); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// UpdateState updates state.
func (storageState *StorageState) UpdateState(updateState cloudprotocol.UpdateState) error {
	storageState.Lock()
	defer storageState.Unlock()

	state, ok := storageState.statesMap[updateState.InstanceIdent]
	if !ok {
		return ErrNotFound
	}

	log.WithFields(log.Fields{
		"instance":  updateState.InstanceIdent.Instance,
		"serviceID": updateState.InstanceIdent.ServiceID,
		"subjectID": updateState.InstanceIdent.SubjectID,
	}).Debug("Update state")

	if len(updateState.State) > int(state.quota) {
		return aoserrors.New("update state is too big")
	}

	sumBytes, err := hex.DecodeString(updateState.Checksum)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := checkChecksum([]byte(updateState.State), sumBytes); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := storageState.storage.SetStateChecksum(updateState.InstanceIdent, sumBytes); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := os.WriteFile(state.stateFilePath, []byte(updateState.State), 0o600); err != nil {
		return aoserrors.Wrap(err)
	}

	state.checksum = sumBytes

	return nil
}

// StateAcceptance acceptance state.
func (storageState *StorageState) StateAcceptance(updateState cloudprotocol.StateAcceptance) error {
	storageState.Lock()
	defer storageState.Unlock()

	state, ok := storageState.statesMap[updateState.InstanceIdent]
	if !ok {
		return ErrNotFound
	}

	sumBytes, err := hex.DecodeString(updateState.Checksum)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if !bytes.Equal(state.checksum, sumBytes) {
		return aoserrors.New("unexpected checksum")
	}

	if strings.ToLower(updateState.Result) == "accepted" {
		log.WithFields(log.Fields{
			"instance":  updateState.InstanceIdent.Instance,
			"serviceID": updateState.InstanceIdent.ServiceID,
			"subjectID": updateState.InstanceIdent.SubjectID,
		}).Debug("State is accepted")

		if err := storageState.storage.SetStateChecksum(updateState.InstanceIdent, sumBytes); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		log.WithFields(log.Fields{
			"instance":  updateState.InstanceIdent.Instance,
			"serviceID": updateState.InstanceIdent.ServiceID,
			"subjectID": updateState.InstanceIdent.SubjectID,
		}).Errorf("State is rejected due to: %v", updateState.Reason)

		if err = storageState.pushStateRequestMessage(updateState.InstanceIdent, false); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (storageState *StorageState) GetInstanceCheckSum(instanceIdent aostypes.InstanceIdent) string {
	state, ok := storageState.statesMap[instanceIdent]
	if !ok {
		return ""
	}

	return string(state.checksum)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (storageState *StorageState) initStateWatching() error {
	infos, err := storageState.storage.GetAllStorageStateInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, info := range infos {
		if err = storageState.startStateWatching(info.InstanceIdent, info.InstanceID,
			storageState.getStatePath(info.InstanceID), info.StateQuota); err != nil {
			log.WithField("instanceID", info.InstanceID).Errorf("Can't setup state watching: %v", err)
		}
	}

	return nil
}

func (storageState *StorageState) prepareState(
	instanceID string, params SetupParams, checksum []byte,
) (stateFilePath string, err error) {
	if params.StateQuota == 0 {
		if err := os.RemoveAll(storageState.getStatePath(instanceID)); err != nil {
			return "", aoserrors.Wrap(err)
		}

		return "", nil
	}

	stateFilePath = storageState.getStatePath(instanceID)

	if err = storageState.setupStateWatching(instanceID, stateFilePath, params); err != nil {
		return "", aoserrors.Wrap(err)
	}

	defer func() {
		if err != nil {
			if err := storageState.stopStateWatching(params.InstanceIdent); err != nil {
				log.Warnf("Can't stop watching state file: %v", err)
			}
		}
	}()

	storageState.statesMap[params.InstanceIdent].checksum = checksum

	if err = storageState.checkChecksumAndSendUpdateRequest(
		stateFilePath, params.InstanceIdent); err != nil {
		return "", aoserrors.Wrap(err)
	}

	rel, err := filepath.Rel(storageState.stateDir, stateFilePath)

	return rel, aoserrors.Wrap(err)
}

func (storageState *StorageState) remove(instanceIdent aostypes.InstanceIdent, instanceID string) error {
	log.WithFields(log.Fields{
		"instanceID": instanceID, "serviceID": instanceIdent.ServiceID,
		"storagePath": storageState.getStoragePath(instanceID),
		"statePath":   storageState.getStatePath(instanceID),
	}).Debug("Remove storage and state")

	if err := os.RemoveAll(storageState.getStoragePath(instanceID)); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := os.RemoveAll(storageState.getStatePath(instanceID)); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := storageState.storage.RemoveStorageStateInfo(instanceIdent); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storageState *StorageState) checkChecksumAndSendUpdateRequest(
	stateFilePath string, instanceIdent aostypes.InstanceIdent,
) (err error) {
	state, ok := storageState.statesMap[instanceIdent]
	if !ok {
		return ErrNotFound
	}

	_, checksum, err := getFileDataChecksum(stateFilePath)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if !bytes.Equal(checksum, state.checksum) {
		if err = storageState.pushStateRequestMessage(instanceIdent, false); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (storageState *StorageState) setupStateWatching(instanceID, stateFilePath string, params SetupParams) error {
	if err := createStateFileIfNotExist(stateFilePath, params.UID, params.GID); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := storageState.startStateWatching(
		params.InstanceIdent, instanceID, stateFilePath, params.StateQuota); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storageState *StorageState) startStateWatching(
	instanceIdent aostypes.InstanceIdent, instanceID string, stateFilePath string, quota uint64,
) (err error) {
	if err = storageState.watcher.Add(stateFilePath); err != nil {
		return aoserrors.Wrap(err)
	}

	storageState.statesMap[instanceIdent] = &stateParams{
		instanceID:         instanceID,
		stateFilePath:      stateFilePath,
		quota:              quota,
		changeTimerChannel: make(chan bool, 1),
	}

	return nil
}

func (storageState *StorageState) stopStateWatching(instanceIdent aostypes.InstanceIdent) (err error) {
	if state, ok := storageState.statesMap[instanceIdent]; ok {
		if err = storageState.watcher.Remove(state.stateFilePath); err != nil {
			return aoserrors.Wrap(err)
		}

		if state.changeTimer != nil {
			if state.changeTimer.Stop() {
				state.changeTimerChannel <- false
			}
		}

		delete(storageState.statesMap, instanceIdent)
	}

	return nil
}

func (storageState *StorageState) pushNewStateMessage(
	instanceIdent aostypes.InstanceIdent, checksum, stateData string,
) (err error) {
	if err := storageState.messageSender.SendInstanceNewState(cloudprotocol.NewState{
		InstanceIdent: instanceIdent,
		Checksum:      checksum,
		State:         stateData,
	}); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storageState *StorageState) pushStateRequestMessage(
	instanceIdent aostypes.InstanceIdent, defaultState bool,
) (err error) {
	if err := storageState.messageSender.SendInstanceStateRequest(cloudprotocol.StateRequest{
		InstanceIdent: instanceIdent,
		Default:       defaultState,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storageState *StorageState) processWatcher() {
	for {
		select {
		case event, ok := <-storageState.watcher.Events:
			if !ok {
				return
			}

			storageState.processStateChanged(event.Name)

		case err, ok := <-storageState.watcher.Errors:
			if !ok {
				return
			}

			log.Errorf("FS watcher error: %v", err)
		}
	}
}

func (storageState *StorageState) processStateChanged(stateFile string) {
	storageState.Lock()
	defer storageState.Unlock()

	for instanceIdent, state := range storageState.statesMap {
		if state.stateFilePath == stateFile {
			log.WithFields(log.Fields{
				"file":      stateFile,
				"instance":  instanceIdent.Instance,
				"serviceID": instanceIdent.ServiceID,
				"subjectID": instanceIdent.SubjectID,
			}).Debug("State file changed")

			if state.changeTimer == nil {
				state.changeTimer = time.NewTimer(StateChangeTimeout)

				go func() {
					select {
					case <-state.changeTimer.C:
						storageState.stateChanged(stateFile, instanceIdent, state)

					case <-state.changeTimerChannel:
						log.WithFields(log.Fields{
							"instance":  instanceIdent.Instance,
							"serviceID": instanceIdent.ServiceID,
							"subjectID": instanceIdent.SubjectID,
						}).Debug("Send state change cancel")
					}
				}()
			} else {
				state.changeTimer.Reset(StateChangeTimeout)
			}

			break
		}
	}
}

func (storageState *StorageState) stateChanged(
	fileName string, instanceIdent aostypes.InstanceIdent, state *stateParams,
) {
	storageState.Lock()
	defer storageState.Unlock()

	state.changeTimer = nil

	stateData, checksum, err := getFileDataChecksum(fileName)
	if err != nil {
		log.Errorf("Can't get state and checksum: %v", err)

		return
	}

	// This check is necessary because in UpdateState the data is saved to the file
	// and at the same time the file change event occurs
	if bytes.Equal(checksum, state.checksum) {
		return
	}

	state.checksum = checksum

	if err := storageState.pushNewStateMessage(
		instanceIdent, hex.EncodeToString(checksum), string(stateData)); err != nil {
		log.Errorf("Can't send new state message: %v", err)
	}
}

func (storageState *StorageState) prepareStorage(
	instanceID string, params SetupParams,
) (string, error) {
	storagePath := storageState.getStoragePath(instanceID)

	if params.StorageQuota == 0 {
		if err := os.RemoveAll(storagePath); err != nil {
			return "", aoserrors.Wrap(err)
		}

		return "", nil
	}

	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return "", aoserrors.Wrap(err)
	}

	if err := os.Chown(storagePath, params.UID, params.GID); err != nil {
		return "", aoserrors.Wrap(err)
	}

	rel, err := filepath.Rel(storageState.storageDir, storagePath)

	return rel, aoserrors.Wrap(err)
}

func (storageState *StorageState) setQuotasFS(params SetupParams) error {
	if storageState.isSamePartition {
		if err := SetUserFSQuota(
			storageState.storageDir, params.StorageQuota+params.StateQuota,
			uint32(params.UID), uint32(params.GID)); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		if err := SetUserFSQuota(
			storageState.storageDir, params.StorageQuota, uint32(params.UID), uint32(params.GID)); err != nil {
			return aoserrors.Wrap(err)
		}

		if err := SetUserFSQuota(
			storageState.stateDir, params.StateQuota, uint32(params.UID), uint32(params.GID)); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (storageState *StorageState) getStatePath(instanceID string) string {
	return path.Join(storageState.stateDir, fmt.Sprintf(stateFileFormat, instanceID))
}

func (storageState *StorageState) getStoragePath(instanceID string) string {
	return path.Join(storageState.storageDir, instanceID)
}

func createStateFileIfNotExist(path string, uid, gid int) error {
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return aoserrors.Wrap(err)
		}

		if err := createStateFile(path, uid, gid); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func createStateFile(path string, uid, gid int) (err error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer file.Close()

	if err = os.Chown(path, uid, gid); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func getFileDataChecksum(fileName string) (data []byte, checksum []byte, err error) {
	if _, err = os.Stat(fileName); err != nil {
		if !os.IsNotExist(err) {
			return nil, nil, aoserrors.Wrap(err)
		}

		return nil, nil, nil
	}

	if data, err = os.ReadFile(fileName); err != nil {
		return nil, nil, aoserrors.Wrap(err)
	}

	calcSum := sha3.Sum224(data)

	return data, calcSum[:], nil
}

func checkChecksum(state []byte, checksum []byte) (err error) {
	calcSum := sha3.Sum224(state)

	if !bytes.Equal(calcSum[:], checksum) {
		return aoserrors.New("wrong checksum")
	}

	return nil
}
