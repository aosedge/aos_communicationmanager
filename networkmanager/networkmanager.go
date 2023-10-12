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
	"errors"
	"fmt"
	"math/big"
	"net"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/apparentlymart/go-cidr/cidr"
	log "github.com/sirupsen/logrus"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

const (
	vlanIDCapacity                = 4096
	allowedConnectionsExpectedLen = 3
	exposePortConfigExpectedLen   = 2
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Storage provides API to create, remove or access information from DB.
type Storage interface {
	AddNetworkInstanceInfo(InstanceNetworkInfo) error
	RemoveNetworkInstanceInfo(aostypes.InstanceIdent) error
	GetNetworkInstancesInfo() ([]InstanceNetworkInfo, error)
	RemoveNetworkInfo(networkID string) error
	AddNetworkInfo(NetworkInfo) error
	GetNetworksInfo() ([]NetworkInfo, error)
}

// NodeManager nodes controller.
type NodeManager interface {
	UpdateNetwork(nodeID string, networkParameters []aostypes.NetworkParameters) error
}

// NetworkManager networks manager instance.
type NetworkManager struct {
	sync.RWMutex
	instancesData    map[string]map[aostypes.InstanceIdent]InstanceNetworkInfo
	providerNetworks map[string]aostypes.NetworkParameters
	ipamSubnet       *ipSubnet
	dns              *dnsServer
	storage          Storage
	nodeManager      NodeManager
}

// NetworkInfo represents network info for instance.
type NetworkInfo struct {
	aostypes.NetworkParameters
	NetworkID string
}

// FirewallRule represents firewall rule.
type FirewallRule struct {
	Protocol string
	Port     string
}

// InstanceNetworkInfo represents network info for instance.
type InstanceNetworkInfo struct {
	aostypes.InstanceIdent
	NetworkInfo
	Rules []FirewallRule
}

// NetworkParameters represents network parameters.
type NetworkParameters struct {
	Hosts            []string
	AllowConnections []string
	ExposePorts      []string
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// These global variable is used to be able to mocking the functionality of networking in tests.
//
//nolint:gochecknoglobals
var (
	GetIPSubnet func(networkID string) (allocIPNet *net.IPNet, ip net.IP, err error)
	GetSubnet   func(networkID string) (*net.IPNet, error)
	GetVlanID   func(networkID string) (uint64, error)
)

var errRuleNotFound = aoserrors.New("rule not found")

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates network manager instance.
func New(storage Storage, nodeManager NodeManager, config *config.Config) (*NetworkManager, error) {
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

	if GetSubnet == nil {
		GetSubnet = ipamSubnet.getAvailableSubnet
	}

	networkManager := &NetworkManager{
		instancesData:    make(map[string]map[aostypes.InstanceIdent]InstanceNetworkInfo),
		providerNetworks: make(map[string]aostypes.NetworkParameters),
		ipamSubnet:       ipamSubnet,
		dns:              dns,
		storage:          storage,
		nodeManager:      nodeManager,
	}

	if GetVlanID == nil {
		GetVlanID = networkManager.getVlanID
	}

	networksInfo, err := storage.GetNetworksInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, networkInfo := range networksInfo {
		networkInfo.NetworkParameters.NetworkID = networkInfo.NetworkID
		networkManager.providerNetworks[networkInfo.NetworkID] = networkInfo.NetworkParameters
	}

	networkInfos, err := storage.GetNetworkInstancesInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, networkInfo := range networkInfos {
		if len(networkManager.instancesData[networkInfo.NetworkID]) == 0 {
			networkManager.instancesData[networkInfo.NetworkID] = make(
				map[aostypes.InstanceIdent]InstanceNetworkInfo)
		}

		networkInfo.DNSServers = []string{networkManager.dns.IPAddress}
		networkManager.instancesData[networkInfo.NetworkID][networkInfo.InstanceIdent] = networkInfo
	}

	return networkManager, nil
}

// RemoveInstanceNetworkConf removes stored instance network parameters.
func (manager *NetworkManager) RemoveInstanceNetworkParameters(instanceIdent aostypes.InstanceIdent, networkID string) {
	networkParameters, found := manager.getNetworkParametersToCache(networkID, instanceIdent)
	if !found {
		return
	}

	manager.deleteNetworkParametersFromCache(networkID, instanceIdent, net.IP(networkParameters.IP))

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

// UpdateProviderNetwork updates provider network.
func (manager *NetworkManager) UpdateProviderNetwork(providers []string, nodeID string) error {
	manager.Lock()
	defer manager.Unlock()

	manager.removeProviderNetworks(providers)

	networkParameters, err := manager.addProviderNetworks(providers)
	if err != nil {
		return err
	}

	return aoserrors.Wrap(manager.nodeManager.UpdateNetwork(nodeID, networkParameters))
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
	instanceIdent aostypes.InstanceIdent, networkID string, params NetworkParameters,
) (networkParameters aostypes.NetworkParameters, err error) {
	if instanceIdent.ServiceID != "" && instanceIdent.SubjectID != "" {
		params.Hosts = append(
			params.Hosts, fmt.Sprintf("%d.%s.%s", instanceIdent.Instance, instanceIdent.SubjectID, instanceIdent.ServiceID))

		if instanceIdent.Instance == 0 {
			params.Hosts = append(params.Hosts, fmt.Sprintf("%s.%s", instanceIdent.SubjectID, instanceIdent.ServiceID))
		}
	}

	networkParameters, found := manager.getNetworkParametersToCache(networkID, instanceIdent)
	if !found {
		if networkParameters, err = manager.createNetwork(instanceIdent, networkID, params); err != nil {
			return networkParameters, err
		}
	}

	if err := manager.dns.addHosts(params.Hosts, networkParameters.IP); err != nil {
		return networkParameters, err
	}

	if len(params.AllowConnections) > 0 {
		firewallRules, err := manager.prepareFirewallRules(
			networkParameters.Subnet, networkParameters.IP, params.AllowConnections)
		if err != nil {
			return networkParameters, err
		}

		networkParameters.FirewallRules = firewallRules
	}

	return networkParameters, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (manager *NetworkManager) createNetwork(
	instanceIdent aostypes.InstanceIdent, networkID string, params NetworkParameters,
) (networkParameters aostypes.NetworkParameters, err error) {
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

	instanceNetworkInfo := InstanceNetworkInfo{
		InstanceIdent: instanceIdent,
		NetworkInfo: NetworkInfo{
			NetworkID:         networkID,
			NetworkParameters: networkParameters,
		},
	}

	if len(params.ExposePorts) > 0 {
		instanceNetworkInfo.Rules, err = parseExposedPorts(params.ExposePorts)
		if err != nil {
			return networkParameters, err
		}
	}

	if err := manager.storage.AddNetworkInstanceInfo(instanceNetworkInfo); err != nil {
		return networkParameters, aoserrors.Wrap(err)
	}

	manager.addNetworkParametersToCache(instanceNetworkInfo)

	return networkParameters, nil
}

func (manager *NetworkManager) prepareFirewallRules(
	subnet, ip string, allowConnection []string,
) (rules []aostypes.FirewallRule, err error) {
	for _, connection := range allowConnection {
		serviceID, port, protocol, err := parseAllowConnection(connection)
		if err != nil {
			return nil, err
		}

		instanceRule, err := manager.getInstanceRule(serviceID, subnet, port, protocol, ip)
		if err != nil {
			if !errors.Is(err, errRuleNotFound) {
				return nil, err
			}

			continue
		}

		rules = append(rules, instanceRule)
	}

	return rules, nil
}

func (manager *NetworkManager) getInstanceRule(
	serviceID, subnet, port, protocol, ip string,
) (rule aostypes.FirewallRule, err error) {
	for _, instances := range manager.instancesData {
		for _, instanceNetworkInfo := range instances {
			if instanceNetworkInfo.ServiceID != serviceID {
				continue
			}

			same, err := checkIPInSubnet(subnet, instanceNetworkInfo.NetworkParameters.IP)
			if err != nil {
				return rule, err
			}

			if same {
				continue
			}

			if ruleExists(instanceNetworkInfo, port, protocol) {
				return aostypes.FirewallRule{
					DstIP:   instanceNetworkInfo.NetworkParameters.IP,
					SrcIP:   ip,
					Proto:   protocol,
					DstPort: port,
				}, nil
			}
		}
	}

	return rule, errRuleNotFound
}

func checkIPInSubnet(subnet, ip string) (bool, error) {
	_, ipnet, err := net.ParseCIDR(subnet)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false, aoserrors.Errorf("invalid IP %s", ip)
	}

	return ipnet.Contains(parsedIP), nil
}

func (manager *NetworkManager) deleteNetworkParametersFromCache(
	networkID string, instanceIdent aostypes.InstanceIdent, ip net.IP,
) {
	manager.Lock()
	defer manager.Unlock()

	delete(manager.instancesData[networkID], instanceIdent)
	delete(manager.dns.hosts, ip.String())

	manager.ipamSubnet.releaseIPToSubnet(networkID, ip)
}

func (manager *NetworkManager) addNetworkParametersToCache(instanceNetworkInfo InstanceNetworkInfo) {
	manager.Lock()
	defer manager.Unlock()

	if _, ok := manager.instancesData[instanceNetworkInfo.NetworkID]; !ok {
		manager.instancesData[instanceNetworkInfo.NetworkID] = make(map[aostypes.InstanceIdent]InstanceNetworkInfo)
	}

	manager.instancesData[instanceNetworkInfo.NetworkID][instanceNetworkInfo.InstanceIdent] = instanceNetworkInfo
}

func (manager *NetworkManager) getNetworkParametersToCache(
	networkID string, instanceIdent aostypes.InstanceIdent,
) (aostypes.NetworkParameters, bool) {
	manager.RLock()
	defer manager.RUnlock()

	if instances, ok := manager.instancesData[networkID]; ok {
		if networkParameter, ok := instances[instanceIdent]; ok {
			return networkParameter.NetworkParameters, true
		}
	}

	return aostypes.NetworkParameters{}, false
}

func (manager *NetworkManager) removeProviderNetworks(providers []string) {
next:
	for networkID := range manager.providerNetworks {
		for _, providerID := range providers {
			if networkID == providerID {
				continue next
			}
		}

		delete(manager.providerNetworks, networkID)

		manager.ipamSubnet.releaseIPNetPool(networkID)

		if err := manager.storage.RemoveNetworkInfo(networkID); err != nil {
			log.Errorf("Can't remove network info: %v", err)
		}
	}
}

func (manager *NetworkManager) addProviderNetworks(
	providers []string,
) (networkParameters []aostypes.NetworkParameters, err error) {
	for _, providerID := range providers {
		if networkParameter, ok := manager.providerNetworks[providerID]; ok {
			networkParameters = append(networkParameters, networkParameter)

			continue
		}

		networkParameter := aostypes.NetworkParameters{
			NetworkID: providerID,
		}

		if networkParameter.VlanID, err = GetVlanID(providerID); err != nil {
			return nil, err
		}

		subnet, err := GetSubnet(providerID)
		if err != nil {
			return nil, err
		}

		networkParameter.Subnet = subnet.String()
		networkParameter.IP = cidr.Inc(subnet.IP).String()
		manager.providerNetworks[providerID] = networkParameter

		if err := manager.storage.AddNetworkInfo(NetworkInfo{
			NetworkID:         providerID,
			NetworkParameters: networkParameter,
		}); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		networkParameters = append(networkParameters, networkParameter)
	}

	return networkParameters, err
}

func (manager *NetworkManager) getVlanID(networkID string) (uint64, error) {
	vlanID, err := rand.Int(rand.Reader, big.NewInt(vlanIDCapacity))
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	return vlanID.Uint64() + 1, nil
}

func parseAllowConnection(connection string) (serviceID, port, protocol string, err error) {
	connConf := strings.Split(connection, "/")
	if len(connConf) > allowedConnectionsExpectedLen || len(connConf) < 2 {
		return "", "", "", aoserrors.Errorf("unsupported AllowedConnections format %s", connConf)
	}

	serviceID = connConf[0]
	port = connConf[1]
	protocol = "tcp"

	if len(connConf) == allowedConnectionsExpectedLen {
		protocol = connConf[2]
	}

	return serviceID, port, protocol, nil
}

func ruleExists(info InstanceNetworkInfo, port, protocol string) bool {
	for _, rule := range info.Rules {
		if rule.Port == port && protocol == rule.Protocol {
			return true
		}
	}

	return false
}

func parseExposedPorts(exposePorts []string) ([]FirewallRule, error) {
	rules := make([]FirewallRule, len(exposePorts))

	for i, exposePort := range exposePorts {
		portConfig := strings.Split(exposePort, "/")
		if len(portConfig) > exposePortConfigExpectedLen || len(portConfig) == 0 {
			return nil, aoserrors.Errorf("unsupported ExposedPorts format %s", exposePort)
		}

		protocol := "tcp"
		if len(portConfig) == exposePortConfigExpectedLen {
			protocol = portConfig[1]
		}

		rules[i] = FirewallRule{
			Protocol: protocol,
			Port:     portConfig[0],
		}
	}

	return rules, nil
}
