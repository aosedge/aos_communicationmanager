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
	"net"
	"sync"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/apparentlymart/go-cidr/cidr"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type subnetwork struct {
	ipNet *net.IPNet
	ips   []net.IP
}

type ipSubnet struct {
	sync.Mutex
	predefinedPrivateNetworks []*net.IPNet
	usedIPSubnets             map[string]subnetwork
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newIPam() (ipam *ipSubnet, err error) {
	log.Debug("Create ipam allocator")

	ipam = &ipSubnet{}

	if ipam.predefinedPrivateNetworks, err = makeNetPools(); err != nil {
		return nil, err
	}

	ipam.usedIPSubnets = make(map[string]subnetwork)

	return ipam, nil
}

func (ipam *ipSubnet) requestIPNetPool(networkID string) (allocIPNet *net.IPNet, err error) {
	if len(ipam.predefinedPrivateNetworks) == 0 {
		return nil, aoserrors.Errorf("IP subnet pool is empty")
	}

	allocIPNet, err = ipam.findUnusedIPSubnet()
	if err != nil {
		return nil, err
	}

	ipam.usedIPSubnets[networkID] = subnetwork{
		ipNet: allocIPNet,
		ips:   generateSubnetIPs(allocIPNet),
	}

	return allocIPNet, nil
}

func (ipam *ipSubnet) findAvailableIP(networkID string) (ip net.IP, err error) {
	subnet, ok := ipam.usedIPSubnets[networkID]
	if !ok {
		return ip, aoserrors.Errorf("incorrect subnet %s", networkID)
	}

	if len(subnet.ips) == 0 {
		return ip, aoserrors.Errorf("no available ip")
	}

	ip, subnet.ips = subnet.ips[0], subnet.ips[1:]

	ipam.usedIPSubnets[networkID] = subnet

	return ip, nil
}

func (ipam *ipSubnet) releaseIPToSubnet(networkID string, ip net.IP) {
	ipam.Lock()
	defer ipam.Unlock()

	subnet, exist := ipam.usedIPSubnets[networkID]
	if !exist {
		return
	}

	subnet.ips = append(subnet.ips, ip)

	ipam.usedIPSubnets[networkID] = subnet
}

func (ipam *ipSubnet) releaseIPNetPool(networkID string) {
	ipam.Lock()
	defer ipam.Unlock()

	subnet, exist := ipam.usedIPSubnets[networkID]
	if !exist {
		return
	}

	delete(ipam.usedIPSubnets, networkID)

	ipam.predefinedPrivateNetworks = append(ipam.predefinedPrivateNetworks, subnet.ipNet)
}

func (ipam *ipSubnet) findUnusedIPSubnet() (unusedIPNet *net.IPNet, err error) {
	networks, err := getNetworkRoutes()
	if err != nil {
		return nil, err
	}

	for i, nw := range ipam.predefinedPrivateNetworks {
		if !checkRouteOverlaps(nw, networks) {
			ipam.predefinedPrivateNetworks = append(
				ipam.predefinedPrivateNetworks[:i], ipam.predefinedPrivateNetworks[i+1:]...)
			return nw, nil
		}
	}

	return nil, aoserrors.Errorf("no available network")
}

func (ipam *ipSubnet) prepareSubnet(networkID string) (allocIPNet *net.IPNet, ip net.IP, err error) {
	ipam.Lock()
	defer ipam.Unlock()

	ipSubnet, err := ipam.getAvailableSubnet(networkID)
	if err != nil {
		return nil, ip, err
	}

	ip, err = ipam.findAvailableIP(networkID)
	if err != nil {
		return nil, ip, err
	}

	return ipSubnet, ip, err
}

func (ipam *ipSubnet) getAvailableSubnet(networkID string) (*net.IPNet, error) {
	subnet, exist := ipam.usedIPSubnets[networkID]
	if !exist {
		ipSubnet, err := ipam.requestIPNetPool(networkID)
		if err != nil {
			return nil, err
		}

		return ipSubnet, nil
	}

	return subnet.ipNet, nil
}

func (ipam *ipSubnet) removeAllocatedSubnets(networks []aostypes.NetworkParameters,
	networkInstances []InstanceNetworkInfo,
) {
	ipam.Lock()
	defer ipam.Unlock()

	for _, network := range networks {
		_, ipNet, err := net.ParseCIDR(network.Subnet)
		if err != nil {
			log.Errorf("Failed to parse subnet %s: %v", network.Subnet, err)

			continue
		}

		for i, ipNetPool := range ipam.predefinedPrivateNetworks {
			if ipNetPool.String() == ipNet.String() {
				ipam.usedIPSubnets[network.NetworkID] = subnetwork{
					ipNet: ipNetPool,
					ips:   generateSubnetIPs(ipNetPool),
				}

				ipam.predefinedPrivateNetworks = append(
					ipam.predefinedPrivateNetworks[:i], ipam.predefinedPrivateNetworks[i+1:]...)

				log.Debugf("Allocated subnet %s was removed", ipNet.String())

				break
			}
		}
	}

	for _, networkInstance := range networkInstances {
		subnet, ok := ipam.usedIPSubnets[networkInstance.NetworkID]
		if !ok {
			continue
		}

		for i, ip := range subnet.ips {
			if ip.String() == networkInstance.IP {
				subnet.ips = append(subnet.ips[:i], subnet.ips[i+1:]...)

				log.Debugf("Allocated ip %s was removed", ip.String())

				break
			}
		}
	}
}

func generateSubnetIPs(ipNet *net.IPNet) []net.IP {
	var (
		minIPRange, _ = cidr.AddressRange(ipNet)

		// Three is subtracted because the first address is zero, the second address is reserved for the network address,
		// and the last address is reserved for the broadcast address.
		addressCount = cidr.AddressCount(ipNet) - 3

		ips = make([]net.IP, addressCount)

		ip = cidr.Inc(minIPRange)
	)

	for i := range addressCount {
		ip = cidr.Inc(ip)
		ips[i] = ip
	}

	return ips
}
