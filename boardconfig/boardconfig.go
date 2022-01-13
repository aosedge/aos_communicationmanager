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

package boardconfig

import (
	"encoding/json"
	"io/ioutil"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// FileSystemMount specifies a mount instructions.
type FileSystemMount struct {
	Destination string   `json:"destination"`
	Type        string   `json:"type,omitempty"`
	Source      string   `json:"source,omitempty"`
	Options     []string `json:"options,omitempty"`
}

// DeviceResource describes Device available resource
type DeviceResource struct {
	Name        string   `json:"name"`
	SharedCount int      `json:"sharedCount,omitempty"`
	Groups      []string `json:"groups,omitempty"`
	HostDevices []string `json:"hostDevices"`
}

// Host struct represent entry in /etc/hosts
type Host struct {
	IP       string `json:"ip"`
	Hostname string `json:"hostname"`
}

// BoardResource describes other board resource
type BoardResource struct {
	Name   string            `json:"name"`
	Groups []string          `json:"groups,omitempty"`
	Mounts []FileSystemMount `json:"mounts,omitempty"`
	Env    []string          `json:"env,omitempty"`
	Hosts  []Host            `json:"hosts,omitempty"`
}

// BoardConfig resources that are proviced by Cloud for using at AOS services
type BoardConfig struct {
	FormatVersion uint64           `json:"formatVersion"`
	VendorVersion string           `json:"vendorVersion"`
	Devices       []DeviceResource `json:"devices"`
	Resources     []BoardResource  `json:"resources"`
}

// Instance board config instance
type Instance struct {
	sync.Mutex

	client           Client
	boardConfigFile  string
	boardConfig      BoardConfig
	boardConfigError error
}

// Client client board config interface
type Client interface {
	CheckBoardConfig(boardConfig BoardConfig) (err error)
	SetBoardConfig(boardConfig BoardConfig) (err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new board config instance
func New(cfg *config.Config, client Client) (instance *Instance, err error) {
	instance = &Instance{
		client:          client,
		boardConfigFile: cfg.BoardConfigFile,
	}

	_ = instance.load()

	return instance, nil
}

// GetStatus returns board config status
func (instance *Instance) GetStatus() (boardConfigInfo cloudprotocol.BoardConfigInfo, err error) {
	instance.Lock()
	defer instance.Unlock()

	boardConfigInfo.VendorVersion = instance.boardConfig.VendorVersion
	boardConfigInfo.Status = cloudprotocol.InstalledStatus

	if instance.boardConfigError == nil {
		if err = instance.client.SetBoardConfig(instance.boardConfig); err != nil {
			instance.boardConfigError = err
		}
	}

	if instance.boardConfigError != nil {
		boardConfigInfo.Status = cloudprotocol.ErrorStatus
		boardConfigInfo.Error = instance.boardConfigError.Error()
	}

	return boardConfigInfo, nil
}

// GetBoardConfigVersion returns board config version
func (instance *Instance) GetBoardConfigVersion(configJSON json.RawMessage) (vendorVersion string, err error) {
	boardConfig := BoardConfig{VendorVersion: "unknown"}

	if err = json.Unmarshal(configJSON, &boardConfig); err != nil {
		return boardConfig.VendorVersion, aoserrors.Wrap(err)
	}

	return boardConfig.VendorVersion, nil
}

// CheckBoardConfig checks board config
func (instance *Instance) CheckBoardConfig(configJSON json.RawMessage) (vendorVersion string, err error) {
	instance.Lock()
	defer instance.Unlock()

	boardConfig := BoardConfig{VendorVersion: "unknown"}

	if err = json.Unmarshal(configJSON, &boardConfig); err != nil {
		return boardConfig.VendorVersion, aoserrors.Wrap(err)
	}

	if vendorVersion, err = instance.checkBoardConfig(boardConfig); err != nil {
		return vendorVersion, aoserrors.Wrap(err)
	}

	return vendorVersion, nil
}

// UpdateBoardConfig updates board config
func (instance *Instance) UpdateBoardConfig(configJSON json.RawMessage) (err error) {
	instance.Lock()
	defer instance.Unlock()

	boardConfig := BoardConfig{VendorVersion: "unknown"}

	if err = json.Unmarshal(configJSON, &boardConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	if boardConfig.VendorVersion == instance.boardConfig.VendorVersion {
		return aoserrors.New("invalid vendor version")
	}

	instance.boardConfig = boardConfig

	if err = instance.client.SetBoardConfig(instance.boardConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = ioutil.WriteFile(instance.boardConfigFile, configJSON, 0644); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = instance.load(); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Instance) load() (err error) {
	defer func() {
		instance.boardConfigError = aoserrors.Wrap(err)
	}()

	byteValue, err := ioutil.ReadFile(instance.boardConfigFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(byteValue, &instance.boardConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *Instance) checkBoardConfig(boardConfig BoardConfig) (vendorVersion string, err error) {
	if boardConfig.VendorVersion == instance.boardConfig.VendorVersion {
		return boardConfig.VendorVersion, aoserrors.New("invalid vendor version")
	}

	if err = instance.client.CheckBoardConfig(boardConfig); err != nil {
		return boardConfig.VendorVersion, aoserrors.Wrap(err)
	}

	return boardConfig.VendorVersion, nil
}
