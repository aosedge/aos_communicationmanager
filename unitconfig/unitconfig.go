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
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/config"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

// ErrNotFound error to detect that item not found.
var ErrNotFound = errors.New("not found")

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Instance unit config instance.
type Instance struct {
	sync.Mutex

	client                     Client
	curNodeID                  string
	curNodeType                string
	unitConfigFile             string
	unitConfig                 cloudprotocol.UnitConfig
	currentNodeConfigListeners []chan cloudprotocol.NodeConfig
	unitConfigError            error
}

// NodeInfoProvider node info provider interface.
type NodeInfoProvider interface {
	GetCurrentNodeInfo() (cloudprotocol.NodeInfo, error)
}

// Client client unit config interface.
type Client interface {
	CheckNodeConfig(nodeID, version string, nodeConfig cloudprotocol.NodeConfig) error
	SetNodeConfig(nodeID, version string, nodeConfig cloudprotocol.NodeConfig) error
	GetNodeConfigStatuses() ([]NodeConfigStatus, error)
	NodeConfigStatusChannel() <-chan NodeConfigStatus
}

// NodeConfigStatus node config status.
type NodeConfigStatus struct {
	NodeID   string
	NodeType string
	Version  string
	Error    *cloudprotocol.ErrorInfo
}

// ErrAlreadyInstalled error to detect that unit config with the same version already installed.
var ErrAlreadyInstalled = errors.New("already installed")

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new unit config instance.
func New(cfg *config.Config, nodeInfoProvider NodeInfoProvider, client Client) (instance *Instance, err error) {
	instance = &Instance{
		client:                     client,
		unitConfigFile:             cfg.UnitConfigFile,
		currentNodeConfigListeners: make([]chan cloudprotocol.NodeConfig, 0),
	}

	var nodeInfo cloudprotocol.NodeInfo

	if nodeInfo, err = nodeInfoProvider.GetCurrentNodeInfo(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	instance.curNodeID = nodeInfo.NodeID
	instance.curNodeType = nodeInfo.NodeType

	if err := instance.load(); err != nil {
		instance.unitConfigError = err
	}

	go instance.handleNodeConfigStatus()

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

	if instance.unitConfigError != nil && instance.unitConfig.Version == "" {
		log.Warnf("Skip unit config version check due to error: %v", instance.unitConfigError)
	} else if err := instance.checkVersion(unitConfig.Version); err != nil {
		return aoserrors.Wrap(err)
	}

	nodeConfigStatuses, err := instance.client.GetNodeConfigStatuses()
	if err != nil {
		log.Errorf("Error getting node config statuses: %v", err)
	}

	for i, nodeConfigStatus := range nodeConfigStatuses {
		if nodeConfigStatus.Version != unitConfig.Version || nodeConfigStatus.Error != nil {
			nodeConfig := findNodeConfig(nodeConfigStatus.NodeID, nodeConfigStatus.NodeType, unitConfig)

			nodeConfig.NodeID = &nodeConfigStatuses[i].NodeID

			if err := instance.client.CheckNodeConfig(
				nodeConfigStatus.NodeID, unitConfig.Version, nodeConfig); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

// GetNodeConfig returns node config for node or node type.
func (instance *Instance) GetNodeConfig(nodeID, nodeType string) (cloudprotocol.NodeConfig, error) {
	for _, node := range instance.unitConfig.Nodes {
		if node.NodeID != nil && *node.NodeID == nodeID {
			return node, nil
		}
	}

	for _, node := range instance.unitConfig.Nodes {
		if node.NodeType == nodeType {
			return node, nil
		}
	}

	return cloudprotocol.NodeConfig{}, ErrNotFound
}

// GetNodeConfig returns node config of the node with given id and type.
func (instance *Instance) GetCurrentNodeConfig() (cloudprotocol.NodeConfig, error) {
	return instance.GetNodeConfig(instance.curNodeID, instance.curNodeType)
}

// SubscribeCurrentNodeConfigChange subscribes new current node config listener.
func (instance *Instance) SubscribeCurrentNodeConfigChange() <-chan cloudprotocol.NodeConfig {
	instance.Lock()
	defer instance.Unlock()

	log.Debug("Subscribe to current node config change event")

	ch := make(chan cloudprotocol.NodeConfig, 1)
	instance.currentNodeConfigListeners = append(instance.currentNodeConfigListeners, ch)

	return ch
}

// UpdateUnitConfig updates unit config.
func (instance *Instance) UpdateUnitConfig(unitConfig cloudprotocol.UnitConfig) (err error) {
	instance.Lock()
	defer instance.Unlock()

	defer func() {
		instance.unitConfigError = err
	}()

	if instance.unitConfigError != nil && instance.unitConfig.Version == "" {
		log.Warnf("Skip unit config version check due to error: %v", instance.unitConfigError)
	} else if err := instance.checkVersion(unitConfig.Version); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.unitConfig = unitConfig

	nodeConfigStatuses, err := instance.client.GetNodeConfigStatuses()
	if err != nil {
		log.Errorf("Error getting node config statuses: %v", err)
	}

	for _, nodeConfigStatus := range nodeConfigStatuses {
		if nodeConfigStatus.Version != unitConfig.Version || nodeConfigStatus.Error != nil {
			nodeConfig := findNodeConfig(nodeConfigStatus.NodeID, nodeConfigStatus.NodeType, unitConfig)

			if err := instance.client.SetNodeConfig(
				nodeConfigStatus.NodeID, unitConfig.Version, nodeConfig); err != nil {
				return aoserrors.Wrap(err)
			}

			if nodeConfigStatus.NodeID == instance.curNodeID {
				instance.updateCurrentNodeConfigListeners(nodeConfig)
			}
		}
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
		if os.IsNotExist(err) {
			// Don't treat absent config as an error.
			instance.unitConfig = cloudprotocol.UnitConfig{Version: "0.0.0"}

			return nil
		}

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

func (instance *Instance) handleNodeConfigStatus() {
	for {
		nodeConfigStatus, ok := <-instance.client.NodeConfigStatusChannel()
		if !ok {
			return
		}

		if instance.unitConfigError != nil {
			log.WithField("NodeID", nodeConfigStatus.NodeID).Warnf(
				"Can't update node config due to: %v", instance.unitConfigError)
		}

		if nodeConfigStatus.Version == instance.unitConfig.Version && nodeConfigStatus.Error == nil {
			continue
		}

		nodeConfig := findNodeConfig(nodeConfigStatus.NodeID, nodeConfigStatus.NodeType, instance.unitConfig)

		if err := instance.client.SetNodeConfig(
			nodeConfigStatus.NodeID, instance.unitConfig.Version, nodeConfig); err != nil {
			log.WithField("NodeID", nodeConfigStatus.NodeID).Errorf("Can't update node config: %v", err)
		}

		if nodeConfigStatus.NodeID == instance.curNodeID {
			instance.updateCurrentNodeConfigListeners(nodeConfig)
		}
	}
}

func findNodeConfig(nodeID, nodeType string, unitConfig cloudprotocol.UnitConfig) cloudprotocol.NodeConfig {
	for i, nodeConfig := range unitConfig.Nodes {
		if nodeConfig.NodeID != nil && *nodeConfig.NodeID == nodeID {
			return unitConfig.Nodes[i]
		}
	}

	for i, nodeConfig := range unitConfig.Nodes {
		if nodeConfig.NodeType == nodeType {
			return unitConfig.Nodes[i]
		}
	}

	return cloudprotocol.NodeConfig{}
}

func (instance *Instance) updateCurrentNodeConfigListeners(curNodeConfig cloudprotocol.NodeConfig) {
	for _, listener := range instance.currentNodeConfigListeners {
		listener <- curNodeConfig
	}
}
