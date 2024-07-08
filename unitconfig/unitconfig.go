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

package unitconfig

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/aosedge/aos_common/aoserrors"
	semver "github.com/hashicorp/go-version"

	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Instance unit config instance.
type Instance struct {
	sync.Mutex

	client          Client
	unitConfigFile  string
	unitConfig      cloudprotocol.UnitConfig
	unitConfigError error
}

// Client client unit config interface.
type Client interface {
	CheckUnitConfig(unitConfig cloudprotocol.UnitConfig) (err error)
	SetUnitConfig(unitConfig cloudprotocol.UnitConfig) (err error)
}

// ErrAlreadyInstalled error to detect that unit config with the same version already installed.
var ErrAlreadyInstalled = errors.New("already installed")

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new unit config instance.
func New(cfg *config.Config, client Client) (instance *Instance, err error) {
	instance = &Instance{
		client:         client,
		unitConfigFile: cfg.UnitConfigFile,
	}

	if err := instance.load(); err != nil {
		instance.unitConfigError = err
	}

	return instance, nil
}

// GetStatus returns unit config status.
func (instance *Instance) GetStatus() (unitConfigInfo cloudprotocol.UnitConfigStatus, err error) {
	instance.Lock()
	defer instance.Unlock()

	unitConfigInfo.Version = instance.unitConfig.Version
	unitConfigInfo.Status = cloudprotocol.InstalledStatus

	if instance.unitConfigError != nil {
		unitConfigInfo.Status = cloudprotocol.ErrorStatus
		unitConfigInfo.ErrorInfo = &cloudprotocol.ErrorInfo{Message: instance.unitConfigError.Error()}
	}

	return unitConfigInfo, nil
}

// CheckUnitConfig checks unit config.
func (instance *Instance) CheckUnitConfig(unitConfig cloudprotocol.UnitConfig) error {
	instance.Lock()
	defer instance.Unlock()

	if err := instance.checkVersion(unitConfig.Version); err != nil {
		return err
	}

	if err := instance.client.CheckUnitConfig(unitConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// GetUnitConfig returns unit config for node or node type.
func (instance *Instance) GetUnitConfig(nodeID, nodeType string) cloudprotocol.NodeConfig {
	for _, node := range instance.unitConfig.Nodes {
		if node.NodeID != nil && *node.NodeID == nodeID {
			return node
		}
	}

	for _, node := range instance.unitConfig.Nodes {
		if node.NodeType == nodeType {
			return node
		}
	}

	return cloudprotocol.NodeConfig{}
}

// UpdateUnitConfig updates unit config.
func (instance *Instance) UpdateUnitConfig(unitConfig cloudprotocol.UnitConfig) (err error) {
	instance.Lock()
	defer instance.Unlock()

	if err := instance.checkVersion(unitConfig.Version); err != nil {
		return err
	}

	instance.unitConfig = unitConfig

	if err = instance.client.SetUnitConfig(instance.unitConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	configJSON, err := json.Marshal(instance.unitConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = os.WriteFile(instance.unitConfigFile, configJSON, 0o600); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Instance) load() (err error) {
	defer func() {
		instance.unitConfigError = aoserrors.Wrap(err)
	}()

	byteValue, err := os.ReadFile(instance.unitConfigFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(byteValue, &instance.unitConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *Instance) checkVersion(version string) error {
	curVer, err := semver.NewVersion(instance.unitConfig.Version)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	newVer, err := semver.NewVersion(version)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if newVer.Equal(curVer) {
		return ErrAlreadyInstalled
	}

	if newVer.LessThan(curVer) {
		return aoserrors.New("wrong version")
	}

	return nil
}
