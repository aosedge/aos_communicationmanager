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

package networkmanager_test

import (
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/apparentlymart/go-cidr/cidr"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type ipam struct {
	ip     net.IP
	subnet net.IPNet
}

type ipamTest struct {
	ipamData map[string]*ipam
}

type testStore struct {
	networkInfos map[aostypes.InstanceIdent]networkmanager.InstanceNetworkInfo
}

type testNodeManager struct {
	network   map[string][]aostypes.NetworkParameters
	chanReady chan struct{}
}

type testVlan struct {
	vlanID int
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var tmpDir string

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	cleanup()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestBaseNetwork(t *testing.T) {
	ipam, err := newIpam()
	if err != nil {
		t.Fatalf("Can't init ipam management: %v", err)
	}

	networkmanager.GetIPSubnet = ipam.getIPSubnet
	networkmanager.LookPath = lookPath
	networkmanager.DiscoverInterface = discoverInterface
	networkmanager.ExecContext = newTestShellCommander

	storage := &testStore{
		networkInfos: make(map[aostypes.InstanceIdent]networkmanager.InstanceNetworkInfo),
	}

	manager, err := networkmanager.New(storage, nil, &config.Config{
		WorkingDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Can't create network manager: %v", err)
	}

	testData := []struct {
		instance          aostypes.InstanceIdent
		removeConfig      bool
		networkParameters aostypes.NetworkParameters
		hosts             []string
	}{
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.17.0.1"),
				Subnet: ("172.17.0.0/16"),
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  1,
			},
			hosts: []string{"hosts1"},
		},
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.17.0.2"),
				Subnet: ("172.17.0.0/16"),
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  2,
			},
			hosts: []string{"hosts2"},
		},
	}

	for _, data := range testData {
		if data.removeConfig {
			manager.RemoveInstanceNetworkParameters(data.instance)

			continue
		}

		networkParameters, err := manager.PrepareInstanceNetworkParameters(
			data.instance, "network1", networkmanager.NetworkParameters{
				Hosts: data.hosts,
			})
		if err != nil {
			t.Fatalf("Can't prepare instance network configuration: %v", err)
		}

		if networkParameters.IP != data.networkParameters.IP {
			t.Errorf("Wrong IP: %v", data.networkParameters.IP)
		}

		if networkParameters.Subnet != data.networkParameters.Subnet {
			t.Errorf("Wrong subnet: %v", data.networkParameters.Subnet)
		}

		if networkParameters.VlanID != data.networkParameters.VlanID {
			t.Errorf("Wrong vlan id: %v", data.networkParameters.VlanID)
		}

		if len(networkParameters.DNSServers) != 1 {
			t.Errorf("Wrong dns servers: %v", networkParameters.DNSServers)
		}

		if networkParameters.DNSServers[0] != "10.10.0.1" {
			t.Errorf("Wrong dns servers: %v", networkParameters.DNSServers)
		}
	}

	if err = manager.RestartDNSServer(); err != nil {
		t.Fatalf("Can't restart dns server: %v", err)
	}

	expected := []string{
		"172.17.0.1\thosts1\t1.subject1.service1",
		"172.17.0.2\thosts2\t2.subject1.service1",
	}
	sort.Strings(expected)

	rawHosts, err := os.ReadFile(filepath.Join(tmpDir, "network", "addnhosts"))
	if err != nil {
		t.Fatalf("Can't read hosts file: %v", err)
	}

	hosts := strings.TrimSpace(string(rawHosts))
	hostsLines := strings.Split(hosts, "\n")
	sort.Strings(hostsLines)

	if !reflect.DeepEqual(hostsLines, expected) {
		t.Errorf("Unexpected hosts file content: %v", hostsLines)
	}

	expectedInstancesIdent := []aostypes.InstanceIdent{
		{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  1,
		},
		{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  2,
		},
	}

	instances := manager.GetInstances()
	if !compareInstancesIdent(instances, expectedInstancesIdent) {
		t.Error("Unexpected instances ident")
	}
}

func TestAllowConnection(t *testing.T) {
	ipam, err := newIpam()
	if err != nil {
		t.Fatalf("Can't init ipam management: %v", err)
	}

	networkmanager.GetIPSubnet = ipam.getIPSubnet
	networkmanager.LookPath = lookPath
	networkmanager.DiscoverInterface = discoverInterface
	networkmanager.ExecContext = newTestShellCommander

	storage := &testStore{
		networkInfos: make(map[aostypes.InstanceIdent]networkmanager.InstanceNetworkInfo),
	}

	manager, err := networkmanager.New(storage, nil, &config.Config{
		WorkingDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Can't create network manager: %v", err)
	}

	testData := []struct {
		instance          aostypes.InstanceIdent
		network           string
		networkParameters aostypes.NetworkParameters
		exposePorts       []string
		allowConnections  []string
	}{
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.17.0.1"),
				Subnet: ("172.17.0.0/16"),
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  1,
			},
			network:     "network1",
			exposePorts: []string{"10001/udp"},
		},
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.18.0.1"),
				Subnet: ("172.18.0.0/16"),
				FirewallRules: []aostypes.FirewallRule{
					{
						Proto:   "udp",
						DstPort: "10001",
						SrcIP:   "172.18.0.1",
						DstIP:   "172.17.0.1",
					},
				},
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service2",
				SubjectID: "subject2",
				Instance:  1,
			},
			network:          "network2",
			allowConnections: []string{"service1/10001/udp"},
		},
	}

	for _, data := range testData {
		networkParameters, err := manager.PrepareInstanceNetworkParameters(
			data.instance, data.network, networkmanager.NetworkParameters{
				AllowConnections: data.allowConnections,
				ExposePorts:      data.exposePorts,
			})
		if err != nil {
			t.Fatalf("Can't prepare instance network configuration: %v", err)
		}

		if networkParameters.IP != data.networkParameters.IP {
			t.Errorf("Wrong IP: %v", data.networkParameters.IP)
		}

		if networkParameters.Subnet != data.networkParameters.Subnet {
			t.Errorf("Wrong subnet: %v", data.networkParameters.Subnet)
		}

		if len(data.networkParameters.FirewallRules) != 0 {
			if len(networkParameters.FirewallRules) != 1 {
				t.Errorf("Wrong firewall rules: %v", networkParameters.FirewallRules)
			}

			if networkParameters.FirewallRules[0] != data.networkParameters.FirewallRules[0] {
				t.Errorf("Wrong firewall rules: %v", networkParameters.FirewallRules)
			}
		}
	}
}

func TestNetworkStorage(t *testing.T) {
	ipam, err := newIpam()
	if err != nil {
		t.Fatalf("Can't init ipam management: %v", err)
	}

	networkmanager.GetIPSubnet = ipam.getIPSubnet
	networkmanager.LookPath = lookPath
	networkmanager.DiscoverInterface = discoverInterface
	networkmanager.ExecContext = newTestShellCommander

	storage := &testStore{
		networkInfos: make(map[aostypes.InstanceIdent]networkmanager.InstanceNetworkInfo),
	}

	manager, err := networkmanager.New(storage, nil, &config.Config{
		WorkingDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Can't create network manager: %v", err)
	}

	testData := []struct {
		networkParameters aostypes.NetworkParameters
		instance          aostypes.InstanceIdent
		hosts             []string
	}{
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.17.0.1"),
				Subnet: ("172.17.0.0/16"),
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  1,
			},
			hosts: []string{"hosts1"},
		},
		{
			networkParameters: aostypes.NetworkParameters{
				IP:     ("172.17.0.2"),
				Subnet: ("172.17.0.0/16"),
			},
			instance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  2,
			},
			hosts: []string{"hosts2"},
		},
	}

	for _, data := range testData {
		if _, err := manager.PrepareInstanceNetworkParameters(
			data.instance, "network1", networkmanager.NetworkParameters{
				Hosts: data.hosts,
			}); err != nil {
			t.Fatalf("Can't prepare instance network configuration: %v", err)
		}
	}

	manager1, err := networkmanager.New(storage, nil, &config.Config{
		WorkingDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Can't create network manager: %v", err)
	}

	expectedInstancesIdent := []aostypes.InstanceIdent{
		{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  1,
		},
		{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  2,
		},
	}

	instances := manager1.GetInstances()
	if !compareInstancesIdent(instances, expectedInstancesIdent) {
		t.Error("Unexpected instances ident")
	}
}

func TestNetworkUpdates(t *testing.T) {
	ipam, err := newIpam()
	if err != nil {
		t.Fatalf("Can't init ipam management: %v", err)
	}

	vlan := &testVlan{}

	networkmanager.GetIPSubnet = ipam.getIPSubnet
	networkmanager.LookPath = lookPath
	networkmanager.DiscoverInterface = discoverInterface
	networkmanager.ExecContext = newTestShellCommander
	networkmanager.GetVlanID = vlan.getVlanID

	storage := &testStore{
		networkInfos: make(map[aostypes.InstanceIdent]networkmanager.InstanceNetworkInfo),
	}

	nodeManager := &testNodeManager{
		network:   make(map[string][]aostypes.NetworkParameters),
		chanReady: make(chan struct{}, 2),
	}

	manager, err := networkmanager.New(storage, nodeManager, &config.Config{
		WorkingDir: tmpDir,
	})
	if err != nil {
		t.Fatalf("Can't create network manager: %v", err)
	}

	testData := []struct {
		providers                 []string
		nodeID                    string
		expectedNetworkParameters []aostypes.NetworkParameters
	}{
		{
			providers: []string{"network1", "network2"},
			nodeID:    "node1",
			expectedNetworkParameters: []aostypes.NetworkParameters{
				{
					NetworkID: "network1",
					IP:        "172.17.0.1",
					Subnet:    "172.17.0.0/16",
					VlanID:    1,
				},
				{
					NetworkID: "network2",
					IP:        "172.18.0.1",
					Subnet:    "172.18.0.0/16",
					VlanID:    2,
				},
			},
		},
		{
			providers: []string{"network1"},
			nodeID:    "node1",
			expectedNetworkParameters: []aostypes.NetworkParameters{
				{
					NetworkID: "network1",
					IP:        "172.17.0.1",
					Subnet:    "172.17.0.0/16",
					VlanID:    1,
				},
			},
		},
	}

	for _, data := range testData {
		if err := manager.UpdateProviderNetwork(data.providers, data.nodeID); err != nil {
			t.Fatalf("Can't update node network parameters: %v", err)
		}

		select {
		case <-nodeManager.chanReady:
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for node manager")
		}

		networkParameters := nodeManager.network[data.nodeID]

		if !reflect.DeepEqual(networkParameters, data.expectedNetworkParameters) {
			t.Error("Unexpected network parameters")
		}
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newIpam() (*ipamTest, error) {
	ipamInfo := &ipamTest{
		ipamData: make(map[string]*ipam),
	}

	ip, ipnet, err := net.ParseCIDR("172.17.0.0/16")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	ipamInfo.ipamData["network1"] = &ipam{
		subnet: *ipnet,
		ip:     ip,
	}

	if ip, ipnet, err = net.ParseCIDR("172.18.0.0/16"); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	ipamInfo.ipamData["network2"] = &ipam{
		subnet: *ipnet,
		ip:     ip,
	}

	return ipamInfo, nil
}

func (ipam *ipamTest) getIPSubnet(networkID string) (*net.IPNet, net.IP, error) {
	ipamInfo, ok := ipam.ipamData[networkID]
	if !ok {
		return nil, nil, aoserrors.Errorf("Can't find network %v", networkID)
	}

	ipamInfo.ip = cidr.Inc(ipamInfo.ip)

	return &ipamInfo.subnet, ipamInfo.ip, nil
}

func (storage *testStore) AddNetworkInstanceInfo(networkInfo networkmanager.InstanceNetworkInfo) error {
	storage.networkInfos[networkInfo.InstanceIdent] = networkInfo

	return nil
}

func (storage *testStore) RemoveNetworkInstanceInfo(instanceIdent aostypes.InstanceIdent) error {
	delete(storage.networkInfos, instanceIdent)

	return nil
}

func (storage *testStore) GetNetworkInstancesInfo() (networkInfos []networkmanager.InstanceNetworkInfo, err error) {
	for _, networkInfo := range storage.networkInfos {
		networkInfos = append(networkInfos, networkInfo)
	}

	return networkInfos, err
}

func (storage *testStore) RemoveNetworkInfo(networkID string, nodeID string) error {
	return nil
}

func (storage *testStore) AddNetworkInfo(networkInfo networkmanager.NetworkParametersStorage) error {
	return nil
}

func (storage *testStore) GetNetworksInfo() (networkInfos []networkmanager.NetworkParametersStorage, err error) {
	return nil, nil
}

func (node *testNodeManager) UpdateNetwork(nodeID string, networkParameters []aostypes.NetworkParameters) error {
	node.network[nodeID] = networkParameters

	node.chanReady <- struct{}{}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func compareInstancesIdent(instances1, instances2 []aostypes.InstanceIdent) bool {
	if len(instances1) != len(instances2) {
		return false
	}

nextInstance:
	for _, inst1 := range instances1 {
		for _, inst2 := range instances2 {
			if inst1 == inst2 {
				continue nextInstance
			}
		}

		return false
	}

	return true
}

func (vlan *testVlan) getVlanID(networkID string) (uint64, error) {
	vlan.vlanID++

	return uint64(vlan.vlanID), nil
}

func setup() (err error) {
	if tmpDir, err = os.MkdirTemp("", "aos_"); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func cleanup() {
	if err := os.RemoveAll(tmpDir); err != nil {
		log.Errorf("Can't remove tmp folder: %s", err)
	}
}

func lookPath(file string) (string, error) {
	return tmpDir, nil
}

func discoverInterface() (ip net.IP, err error) {
	return net.ParseIP("10.10.0.1"), nil
}

func newTestShellCommander(name string, arg ...string) (string, error) {
	return "", nil
}
