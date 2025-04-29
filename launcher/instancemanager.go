// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2024 Renesas Electronics Corporation.
// Copyright (C) 2024 EPAM Systems, Inc.
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

package launcher

import (
	"context"
	"errors"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
	"github.com/aosedge/aos_communicationmanager/utils/uidgidpool"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

const removePeriod = time.Hour * 24

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Instance state.
const (
	InstanceActive = iota
	InstanceCached
)

// InstanceInfo instance info.
type InstanceInfo struct {
	aostypes.InstanceIdent
	NodeID     string
	PrevNodeID string
	UID        int
	Timestamp  time.Time
	State      int
}

// Storage storage interface.
type Storage interface {
	AddInstance(instanceInfo InstanceInfo) error
	UpdateInstance(instanceInfo InstanceInfo) error
	RemoveInstance(instanceIdent aostypes.InstanceIdent) error
	GetInstance(instanceIdent aostypes.InstanceIdent) (InstanceInfo, error)
	GetInstances() ([]InstanceInfo, error)
}

type instanceManager struct {
	config                           *config.Config
	imageProvider                    ImageProvider
	storageStateProvider             StorageStateProvider
	storage                          Storage
	cancelFunc                       context.CancelFunc
	uidPool                          *uidgidpool.IdentifierPool
	errorStatus                      map[aostypes.InstanceIdent]cloudprotocol.InstanceStatus
	instances                        map[aostypes.InstanceIdent]aostypes.InstanceInfo
	removeServiceChannel             <-chan string
	curInstances                     []InstanceInfo
	availableStorage, availableState uint64
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newInstanceManager(config *config.Config, imageProvider ImageProvider, storageStateProvider StorageStateProvider,
	storage Storage, removeServiceChannel <-chan string,
) (im *instanceManager, err error) {
	im = &instanceManager{
		config:               config,
		imageProvider:        imageProvider,
		storageStateProvider: storageStateProvider,
		storage:              storage,
		removeServiceChannel: removeServiceChannel,
		uidPool:              uidgidpool.NewUserIDPool(),
	}

	if err := im.fillUIDPool(); err != nil {
		log.Errorf("Can't fill UID pool: %v", err)

		return nil, err
	}

	if err := im.clearInstancesWithDeletedService(); err != nil {
		log.Errorf("Can't clear instances with deleted service: %v", err)

		return nil, err
	}

	if err := im.removeOutdatedInstances(); err != nil {
		log.Errorf("Can't remove outdated instances: %v", err)

		return nil, err
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	im.cancelFunc = cancelFunction

	go im.instanceRemover(ctx)

	return im, nil
}

func (im *instanceManager) initInstances() {
	im.instances = make(map[aostypes.InstanceIdent]aostypes.InstanceInfo)
	im.errorStatus = make(map[aostypes.InstanceIdent]cloudprotocol.InstanceStatus)

	var err error

	instances, err := im.storage.GetInstances()
	if err != nil {
		log.Errorf("Can't get current instances: %v", err)
	}

	im.curInstances = make([]InstanceInfo, 0, len(instances))

	for _, instance := range instances {
		if instance.State == InstanceCached {
			continue
		}

		im.curInstances = append(im.curInstances, instance)
	}
}

func (im *instanceManager) getCurrentInstances() []InstanceInfo {
	return im.curInstances
}

func (im *instanceManager) getCurrentInstance(instanceIdent aostypes.InstanceIdent) (InstanceInfo, error) {
	curIndex := slices.IndexFunc(im.curInstances, func(curInstance InstanceInfo) bool {
		return curInstance.InstanceIdent == instanceIdent
	})

	if curIndex == -1 {
		return InstanceInfo{}, aoserrors.Wrap(ErrNotExist)
	}

	return im.curInstances[curIndex], nil
}

func (im *instanceManager) resetStorageStateUsage(storageSize, stateSize uint64) {
	log.WithFields(log.Fields{
		"availableState": im.availableState, "availableStorage": im.availableStorage,
	}).Debug("Available storage and state")

	im.availableStorage, im.availableState = storageSize, stateSize
}

func (im *instanceManager) setupInstance(
	instance cloudprotocol.InstanceInfo, index uint64, node *nodeHandler, service imagemanager.ServiceInfo,
	rebalancing bool,
) (aostypes.InstanceInfo, error) {
	instanceInfo := aostypes.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{
			ServiceID: instance.ServiceID, SubjectID: instance.SubjectID, Instance: index,
		},
		Priority: instance.Priority,
	}

	if _, ok := im.instances[instanceInfo.InstanceIdent]; ok {
		return aostypes.InstanceInfo{}, aoserrors.Errorf("instance already set up")
	}

	storedInstance, err := im.storage.GetInstance(instanceInfo.InstanceIdent)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return aostypes.InstanceInfo{}, aoserrors.Wrap(err)
		}

		uid, err := im.acquireUID()
		if err != nil {
			return aostypes.InstanceInfo{}, err
		}

		storedInstance = InstanceInfo{
			InstanceIdent: instanceInfo.InstanceIdent,
			NodeID:        node.nodeInfo.NodeID,
			UID:           uid,
			Timestamp:     time.Now(),
		}

		if err := im.storage.AddInstance(storedInstance); err != nil {
			log.Errorf("Can't add instance: %v", err)
		}
	} else {
		if rebalancing {
			storedInstance.PrevNodeID = storedInstance.NodeID
		} else {
			storedInstance.PrevNodeID = ""
		}

		storedInstance.NodeID = node.nodeInfo.NodeID
		storedInstance.Timestamp = time.Now()
		storedInstance.State = InstanceActive

		if err := im.storage.UpdateInstance(storedInstance); err != nil {
			log.Errorf("Can't update instance: %v", err)
		}
	}

	log.WithFields(instanceIdentLogFields(instanceInfo.InstanceIdent,
		log.Fields{
			"curNodeID":  storedInstance.NodeID,
			"prevNodeID": storedInstance.PrevNodeID,
		})).Debug("Setup instance")

	instanceInfo.UID = uint32(storedInstance.UID)
	requestedState, requestedStorage := getReqDiskSize(service.Config, node.nodeConfig.ResourceRatios)

	log.WithFields(instanceIdentLogFields(instanceInfo.InstanceIdent,
		log.Fields{
			"requestedStorage": requestedStorage,
			"requestedState":   requestedState,
		})).Debug("Requested storage and state")

	if err = im.setupInstanceStateStorage(&instanceInfo, service, requestedState, requestedStorage); err != nil {
		return aostypes.InstanceInfo{}, err
	}

	im.instances[instanceInfo.InstanceIdent] = instanceInfo

	return instanceInfo, nil
}

func createInstanceIdent(instance cloudprotocol.InstanceInfo, instanceIndex uint64) aostypes.InstanceIdent {
	return aostypes.InstanceIdent{
		ServiceID: instance.ServiceID, SubjectID: instance.SubjectID, Instance: instanceIndex,
	}
}

func (im *instanceManager) setInstanceError(
	instanceIdent aostypes.InstanceIdent, serviceVersion string, err error,
) {
	instanceStatus := cloudprotocol.InstanceStatus{
		InstanceIdent:  instanceIdent,
		ServiceVersion: serviceVersion,
		Status:         cloudprotocol.InstanceStateFailed,
	}

	if err != nil {
		log.WithFields(instanceIdentLogFields(instanceStatus.InstanceIdent, nil)).Errorf(
			"Schedule instance error: %v", err)

		instanceStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: err.Error()}
	}

	im.errorStatus[instanceStatus.InstanceIdent] = instanceStatus
}

func (im *instanceManager) setAllInstanceError(
	instance cloudprotocol.InstanceInfo, serviceVersion string, err error,
) {
	for i := range instance.NumInstances {
		im.setInstanceError(createInstanceIdent(instance, i), serviceVersion, err)
	}
}

func (im *instanceManager) isInstanceScheduled(instanceIdent aostypes.InstanceIdent) bool {
	if _, ok := im.instances[instanceIdent]; ok {
		return true
	}

	if _, ok := im.errorStatus[instanceIdent]; ok {
		return true
	}

	return false
}

func (im *instanceManager) getInstanceCheckSum(instance aostypes.InstanceIdent) string {
	return im.storageStateProvider.GetInstanceCheckSum(instance)
}

func (im *instanceManager) setupInstanceStateStorage(
	instanceInfo *aostypes.InstanceInfo, serviceInfo imagemanager.ServiceInfo,
	requestedState, requestedStorage uint64,
) error {
	stateStorageParams := storagestate.SetupParams{
		InstanceIdent: instanceInfo.InstanceIdent,
		UID:           int(instanceInfo.UID), GID: int(serviceInfo.GID),
	}

	if serviceInfo.Config.Quotas.StateLimit != nil {
		stateStorageParams.StateQuota = *serviceInfo.Config.Quotas.StateLimit
	}

	if serviceInfo.Config.Quotas.StorageLimit != nil {
		stateStorageParams.StorageQuota = *serviceInfo.Config.Quotas.StorageLimit
	}

	if requestedStorage > im.availableStorage && !serviceInfo.Config.SkipResourceLimits {
		return aoserrors.Errorf("not enough storage space")
	}

	if requestedState > im.availableState && !serviceInfo.Config.SkipResourceLimits {
		return aoserrors.Errorf("not enough state space")
	}

	var err error

	instanceInfo.StoragePath, instanceInfo.StatePath, err = im.storageStateProvider.Setup(stateStorageParams)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if !serviceInfo.Config.SkipResourceLimits {
		im.availableStorage -= requestedStorage
		im.availableState -= requestedState
	}

	log.WithFields(log.Fields{
		"remainingState": im.availableState, "remainingStorage": im.availableStorage,
	}).Debug("Remaining storage and state")

	return nil
}

func (im *instanceManager) cacheInstance(instanceInfo InstanceInfo) error {
	log.WithFields(instanceIdentLogFields(instanceInfo.InstanceIdent, nil)).Debug("Cache instance")

	instanceInfo.State = InstanceCached
	instanceInfo.NodeID = ""

	if err := im.storage.UpdateInstance(instanceInfo); err != nil {
		log.Errorf("Can't update instance: %v", err)
	}

	if err := im.storageStateProvider.Cleanup(instanceInfo.InstanceIdent); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (im *instanceManager) acquireUID() (int, error) {
	uid, err := im.uidPool.GetFreeID()
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	return uid, nil
}

func (im *instanceManager) close() {
	if im.cancelFunc != nil {
		im.cancelFunc()
	}
}

func (im *instanceManager) fillUIDPool() error {
	instances, err := im.storage.GetInstances()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, instance := range instances {
		if err = im.uidPool.AddID(instance.UID); err != nil {
			log.Warnf("Can't add UID to pool: %v", err)
		}
	}

	return nil
}

func (im *instanceManager) releaseUID(uid int) error {
	if err := im.uidPool.RemoveID(uid); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (im *instanceManager) removeOutdatedInstances() error {
	instances, err := im.storage.GetInstances()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, instance := range instances {
		if instance.State != InstanceCached ||
			time.Since(instance.Timestamp) < im.config.ServiceTTL.Duration {
			continue
		}

		if errRem := im.removeInstance(instance); errRem != nil {
			log.Errorf("Can't remove instance: %v", errRem)

			if err == nil {
				err = errRem
			}
		}
	}

	return err
}

func (im *instanceManager) removeInstance(instanceInfo InstanceInfo) error {
	err := im.storageStateProvider.RemoveServiceInstance(instanceInfo.InstanceIdent)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	if err = im.storage.RemoveInstance(instanceInfo.InstanceIdent); err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	if err = im.releaseUID(instanceInfo.UID); err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (im *instanceManager) clearInstancesWithDeletedService() error {
	instances, err := im.storage.GetInstances()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, instance := range instances {
		if _, err := im.imageProvider.GetServiceInfo(instance.ServiceID); err == nil {
			continue
		}

		if err := im.removeInstance(instance); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (im *instanceManager) getErrorInstanceStatuses() []cloudprotocol.InstanceStatus {
	return maps.Values(im.errorStatus)
}

func (im *instanceManager) instanceRemover(ctx context.Context) {
	removeTicker := time.NewTicker(removePeriod)
	defer removeTicker.Stop()

	for {
		select {
		case <-removeTicker.C:
			if err := im.removeOutdatedInstances(); err != nil {
				log.Errorf("Can't remove outdated instances: %v", err)
			}

		case serviceID, ok := <-im.removeServiceChannel:
			if !ok {
				continue
			}

			if err := im.removeInstance(InstanceInfo{
				InstanceIdent: aostypes.InstanceIdent{ServiceID: serviceID},
			}); err != nil && !errors.Is(err, ErrNotExist) {
				log.Errorf("Can't remove instance: %v", err)
			}

		case <-ctx.Done():
			return
		}
	}
}
