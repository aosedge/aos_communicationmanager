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
	"slices"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

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

type InstanceInfo struct {
	aostypes.InstanceIdent
	UID       int
	Timestamp time.Time
	Cached    bool
}

// Storage storage interface.
type Storage interface {
	AddInstance(instanceInfo InstanceInfo) error
	RemoveInstance(instanceIdent aostypes.InstanceIdent) error
	SetInstanceCached(instance aostypes.InstanceIdent, cached bool) error
	GetInstanceUID(instance aostypes.InstanceIdent) (int, error)
	GetInstances() ([]InstanceInfo, error)
}

type instanceManager struct {
	config               *config.Config
	imageProvider        ImageProvider
	storageStateProvider StorageStateProvider
	storage              Storage
	cancelFunc           context.CancelFunc
	uidPool              *uidgidpool.IdentifierPool
	removeServiceChannel <-chan string
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

func (im *instanceManager) getCurrentInstances() ([]InstanceInfo, error) {
	instances, err := im.storage.GetInstances()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return slices.DeleteFunc(instances, func(instance InstanceInfo) bool {
		return instance.Cached
	}), nil
}

func (im *instanceManager) setupInstance(
	instance cloudprotocol.InstanceInfo, index uint64, service imagemanager.ServiceInfo,
) (aostypes.InstanceInfo, error) {
	instanceInfo := aostypes.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{
			ServiceID: instance.ServiceID, SubjectID: instance.SubjectID, Instance: index,
		},
		Priority: instance.Priority,
	}

	uid, err := im.storage.GetInstanceUID(instanceInfo.InstanceIdent)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			return aostypes.InstanceInfo{}, aoserrors.Wrap(err)
		}

		uid, err = im.acquireUID()
		if err != nil {
			return aostypes.InstanceInfo{}, err
		}

		if err := im.storage.AddInstance(InstanceInfo{
			InstanceIdent: instanceInfo.InstanceIdent,
			UID:           uid,
			Timestamp:     time.Now(),
		}); err != nil {
			log.Errorf("Can't store uid: %v", err)
		}
	}

	instanceInfo.UID = uint32(uid)

	if err = im.setupInstanceStateStorage(&instanceInfo, service); err != nil {
		return aostypes.InstanceInfo{}, err
	}

	return instanceInfo, nil
}

func (im *instanceManager) getInstanceCheckSum(instance aostypes.InstanceIdent) string {
	return im.storageStateProvider.GetInstanceCheckSum(instance)
}

func (im *instanceManager) setupInstanceStateStorage(
	instanceInfo *aostypes.InstanceInfo, serviceInfo imagemanager.ServiceInfo,
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

	var err error

	instanceInfo.StoragePath, instanceInfo.StatePath, err = im.storageStateProvider.Setup(stateStorageParams)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (im *instanceManager) cacheInstance(instanceInfo InstanceInfo) error {
	log.WithFields(instanceIdentLogFields(instanceInfo.InstanceIdent, nil)).Debug("Cache instance")

	if err := im.storageStateProvider.Cleanup(instanceInfo.InstanceIdent); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := im.storage.SetInstanceCached(instanceInfo.InstanceIdent, true); err != nil {
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
		if !instance.Cached ||
			time.Since(instance.Timestamp) < time.Hour*24*time.Duration(im.config.ServiceTTLDays) {
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
