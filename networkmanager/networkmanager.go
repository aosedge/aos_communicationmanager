// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2023 Renesas Electronics Corporation.
// Copyright (C) 2023 EPAM Systems, Inc.
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

// Package networkmanager provides set of API to configure network

package networkmanager

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_communicationmanager/config"
	log "github.com/sirupsen/logrus"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

const vlanIDCapacity = 4096

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Storage provides API to create, remove or access information from DB.
type Storage interface {
	AddNetworkInstanceInfo(NetworkInfo) error
	RemoveNetworkInstanceInfo(aostypes.InstanceIdent) error
	GetNetworkInstancesInfo() ([]NetworkInfo, error)
}

// NetworkManager networks manager instance.
type NetworkManager struct {
	sync.RWMutex
	instancesData map[string]map[aostypes.InstanceIdent]aostypes.NetworkParameters
	vlanIDs       map[string]uint64
	ipamSubnet    *ipSubnet
	dns           *dnsServer
	storage       Storage
}

// NetworkInfo represents network info for instance.
type NetworkInfo struct {
	aostypes.InstanceIdent
	aostypes.NetworkParameters
	NetworkID string
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// These global variable is used to be able to mocking the functionality of networking in tests.
// nolint:gochecknoglobals
var (
	GetIPSubnet func(networkID string) (allocIPNet *net.IPNet, ip net.IP, err error)
	GetVlanID   func(networkID string) (uint64, error)
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates network manager instance.
func New(storage Storage, config *config.Config) (*NetworkManager, error) {
	log.Debug("Create network manager")

	ipamSubnet, err := newIPam()
	if err != nil {
		return nil, err
	}

	dns, err := newDNSServer(filepath.Join(config.WorkingDir, "network"))
	if err != nil {
		return nil, err
	}

	if GetIPSubnet == nil {
		GetIPSubnet = ipamSubnet.prepareSubnet
	}

	networkManager := &NetworkManager{
		instancesData: make(map[string]map[aostypes.InstanceIdent]aostypes.NetworkParameters),
		vlanIDs:       make(map[string]uint64),
		ipamSubnet:    ipamSubnet,
		dns:           dns,
		storage:       storage,
	}

	if GetVlanID == nil {
		GetVlanID = networkManager.getVlanID
	}

	networkInfos, err := storage.GetNetworkInstancesInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, networkInfo := range networkInfos {
		if len(networkManager.instancesData[networkInfo.NetworkID]) == 0 {
			networkManager.instancesData[networkInfo.NetworkID] = make(
				map[aostypes.InstanceIdent]aostypes.NetworkParameters)
		}

		networkManager.instancesData[networkInfo.NetworkID][networkInfo.InstanceIdent] = networkInfo.NetworkParameters
	}

	return networkManager, nil
}

// RemoveInstanceNetworkConf removes stored instance network parameters.
func (manager *NetworkManager) RemoveInstanceNetworkParameters(instanceIdent aostypes.InstanceIdent, networkID string) {
	networkParameters, found := manager.getNetworkParametersToCache(networkID, instanceIdent)
	if !found {
		return
	}

	manager.deleteNetworkParametersFromCache(networkID, instanceIdent, net.ParseIP(networkParameters.IP))

	if err := manager.storage.RemoveNetworkInstanceInfo(instanceIdent); err != nil {
		log.Errorf("Can't remove network info: %v", err)
	}
}

// GetInstances gets instances.
func (manager *NetworkManager) GetInstances() []aostypes.InstanceIdent {
	manager.Lock()
	defer manager.Unlock()

	var instances []aostypes.InstanceIdent

	for _, instancesData := range manager.instancesData {
		for instanceIdent := range instancesData {
			instances = append(instances, instanceIdent)
		}
	}

	return instances
}

// Restart restarts DNS server.
func (manager *NetworkManager) RestartDNSServer() error {
	if err := manager.dns.rewriteHostsFile(); err != nil {
		return err
	}

	manager.dns.cleanCacheHosts()

	return manager.dns.restart()
}

// PrepareInstanceNetworkParameters prepares network parameters for instance.
func (manager *NetworkManager) PrepareInstanceNetworkParameters(
	instanceIdent aostypes.InstanceIdent, networkID string, hosts []string,
) (networkParameters aostypes.NetworkParameters, err error) {
	if instanceIdent.ServiceID != "" && instanceIdent.SubjectID != "" {
		hosts = append(
			hosts, fmt.Sprintf("%d.%s.%s", instanceIdent.Instance, instanceIdent.SubjectID, instanceIdent.ServiceID))

		if instanceIdent.Instance == 0 {
			hosts = append(hosts, fmt.Sprintf("%s.%s", instanceIdent.SubjectID, instanceIdent.ServiceID))
		}
	}

	networkParameters, found := manager.getNetworkParametersToCache(networkID, instanceIdent)
	if found {
		if err := manager.dns.addHosts(hosts, networkParameters.IP); err != nil {
			return networkParameters, err
		}

		return networkParameters, err
	}

	var (
		ip     net.IP
		subnet *net.IPNet
	)

	defer func() {
		if err != nil {
			manager.deleteNetworkParametersFromCache(networkID, instanceIdent, ip)
		}
	}()

	subnet, ip, err = GetIPSubnet(networkID)
	if err != nil {
		return networkParameters, err
	}

	networkParameters.IP = ip.String()
	networkParameters.Subnet = subnet.String()
	networkParameters.DNSServers = []string{manager.dns.IPAddress}

	if networkParameters.VlanID, err = GetVlanID(networkID); err != nil {
		return networkParameters, err
	}

	if err := manager.dns.addHosts(hosts, ip.String()); err != nil {
		return networkParameters, err
	}

	if err := manager.storage.AddNetworkInstanceInfo(NetworkInfo{
		InstanceIdent:     instanceIdent,
		NetworkID:         networkID,
		NetworkParameters: networkParameters,
	}); err != nil {
		return networkParameters, aoserrors.Wrap(err)
	}

	manager.addNetworkParametersToCache(networkID, instanceIdent, networkParameters)

	return networkParameters, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (manager *NetworkManager) deleteNetworkParametersFromCache(
	networkID string, instanceIdent aostypes.InstanceIdent, ip net.IP,
) {
	manager.Lock()
	defer manager.Unlock()

	delete(manager.instancesData[networkID], instanceIdent)
	delete(manager.dns.hosts, ip.String())

	if len(manager.instancesData[networkID]) == 0 {
		manager.ipamSubnet.releaseIPNetPool(networkID)

		return
	}

	manager.ipamSubnet.releaseIPToSubnet(networkID, ip)
}

func (manager *NetworkManager) addNetworkParametersToCache(
	networkID string, instanceIdent aostypes.InstanceIdent, networkParameters aostypes.NetworkParameters,
) {
	manager.Lock()
	defer manager.Unlock()

	if _, ok := manager.instancesData[networkID]; !ok {
		manager.instancesData[networkID] = make(map[aostypes.InstanceIdent]aostypes.NetworkParameters)
	}

	manager.instancesData[networkID][instanceIdent] = networkParameters
}

func (manager *NetworkManager) getNetworkParametersToCache(
	networkID string, instanceIdent aostypes.InstanceIdent,
) (aostypes.NetworkParameters, bool) {
	manager.RLock()
	defer manager.RUnlock()

	if instances, ok := manager.instancesData[networkID]; ok {
		if networkParameter, ok := instances[instanceIdent]; ok {
			return networkParameter, true
		}
	}

	return aostypes.NetworkParameters{}, false
}

func (manager *NetworkManager) getVlanID(networkID string) (uint64, error) {
	manager.Lock()
	defer manager.Unlock()

	if vlanID, ok := manager.vlanIDs[networkID]; ok {
		return vlanID, nil
	}

	vlanID, err := generateVlanID()
	if err != nil {
		return 0, err
	}

	manager.vlanIDs[networkID] = vlanID

	return vlanID, nil
}

func generateVlanID() (uint64, error) {
	vlanID, err := rand.Int(rand.Reader, big.NewInt(vlanIDCapacity))
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	return vlanID.Uint64() + 1, nil
}
