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

package launcher_test

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/apparentlymart/go-cidr/cidr"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/networkmanager"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	magicSum   = "magicSum"
	runnerRunc = "runc"
	runnerRunx = "runx"
)

const (
	nodeIDLocalSM    = "localSM"
	nodeIDRemoteSM1  = "remoteSM1"
	nodeIDRemoteSM2  = "remoteSM2"
	nodeIDRunxSM     = "runxSM"
	nodeTypeLocalSM  = "localSMType"
	nodeTypeRemoteSM = "remoteSMType"
	nodeTypeRunxSM   = "runxSMType"
)

const (
	subject1          = "subject1"
	service1          = "service1"
	service1LocalURL  = "service1LocalURL"
	service1RemoteURL = "service1RemoteURL"
	service2          = "service2"
	service2LocalURL  = "service2LocalURL"
	service2RemoteURL = "service2RemoteURL"
	service3          = "service3"
	service3LocalURL  = "service3LocalURL"
	service3RemoteURL = "service3RemoteURL"
	layer1            = "layer1"
	layer1LocalURL    = "layer1LocalURL"
	layer1RemoteURL   = "layer1RemoteURL"
	layer2            = "layer2"
	layer2LocalURL    = "layer2LocalURL"
	layer2RemoteURL   = "layer2RemoteURL"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type runRequest struct {
	services     []aostypes.ServiceInfo
	layers       []aostypes.LayerInfo
	instances    []aostypes.InstanceInfo
	forceRestart bool
}

type testNodeManager struct {
	runStatusChan   chan launcher.NodeRunInstanceStatus
	alertsChannel   chan cloudprotocol.SystemQuotaAlert
	nodeInformation map[string]launcher.NodeInfo
	runRequest      map[string]runRequest
}

type testImageProvider struct {
	services         map[string]imagemanager.ServiceInfo
	layers           map[string]imagemanager.LayerInfo
	revertedServices []string
}

type testResourceManager struct {
	nodeResources map[string]aostypes.NodeUnitConfig
}

type testStorage struct {
	instanceInfo     []launcher.InstanceInfo
	desiredInstances json.RawMessage
}

type testStateStorage struct {
	cleanedInstances []aostypes.InstanceIdent
}

type testNetworkManager struct {
	currentIP   net.IP
	subnet      net.IPNet
	networkInfo map[string]map[aostypes.InstanceIdent]struct{}
}

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
 * Tests
 **********************************************************************************************************************/

func TestInitialStatus(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{"localSM", "remoteSM"},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager       = newTestNodeManager()
		expectedRunStatus = unitstatushandler.RunInstancesStatus{}
		expectedNodeInfo  = []cloudprotocol.NodeInfo{}
	)

	launcherInstance, err := launcher.New(cfg, newTestStorage(), nodeManager, nil, &testResourceManager{},
		&testStateStorage{}, newTestNetworkManager(""))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for i, id := range cfg.SMController.NodeIDs {
		instances := []cloudprotocol.InstanceStatus{{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "s1", SubjectID: "subj1", Instance: uint64(i)},
			AosVersion:    1, StateChecksum: magicSum, RunState: "running",
			NodeID: id,
		}}

		nodeInfo := cloudprotocol.NodeInfo{
			NodeID: id, NodeType: "nodeType", SystemInfo: cloudprotocol.SystemInfo{
				NumCPUs: 1, TotalRAM: 100,
				Partitions: []cloudprotocol.PartitionInfo{
					{Name: "id", TotalSize: 200},
				},
			},
		}

		nodeManager.nodeInformation[id] = launcher.NodeInfo{NodeInfo: nodeInfo}

		expectedNodeInfo = append(expectedNodeInfo, nodeInfo)
		expectedRunStatus.Instances = append(expectedRunStatus.Instances, instances...)

		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{NodeID: id, Instances: instances}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	nodesInfo := launcherInstance.GetNodesConfiguration()
	if !reflect.DeepEqual(expectedNodeInfo, nodesInfo) {
		t.Error("Incorrect nodes info")
	}
}

func TestBalancing(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{nodeIDLocalSM, nodeIDRemoteSM1, nodeIDRemoteSM2, nodeIDRunxSM},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager     = newTestNodeManager()
		resourceManager = newTestResourceManager()
		imageManager    = &testImageProvider{}
	)

	nodeManager.nodeInformation = map[string]launcher.NodeInfo{
		nodeIDLocalSM: {
			NodeInfo:   cloudprotocol.NodeInfo{NodeID: nodeIDLocalSM, NodeType: nodeTypeLocalSM},
			RemoteNode: false, RunnerFeature: []string{runnerRunc},
		},
		nodeIDRemoteSM1: {
			NodeInfo:   cloudprotocol.NodeInfo{NodeID: nodeIDRemoteSM1, NodeType: nodeTypeRemoteSM},
			RemoteNode: true, RunnerFeature: []string{runnerRunc},
		},
		nodeIDRemoteSM2: {
			NodeInfo:   cloudprotocol.NodeInfo{NodeID: nodeIDRemoteSM2, NodeType: nodeTypeRemoteSM},
			RemoteNode: true, RunnerFeature: []string{runnerRunc},
		},
		nodeIDRunxSM: {
			NodeInfo:   cloudprotocol.NodeInfo{NodeID: nodeIDRunxSM, NodeType: nodeTypeRunxSM},
			RemoteNode: true, RunnerFeature: []string{runnerRunx},
		},
	}

	imageManager.services = map[string]imagemanager.ServiceInfo{
		service1: {
			ServiceInfo: createServiceInfo(service1, 5000, service1LocalURL),
			RemoteURL:   service1RemoteURL,
			Layers:      []string{layer1, layer2},
		},
		service2: {
			ServiceInfo: createServiceInfo(service2, 5001, service2LocalURL),
			RemoteURL:   service2RemoteURL,
			Layers:      []string{layer1},
		},
		service3: {
			ServiceInfo: createServiceInfo(service3, 5002, service3LocalURL),
			RemoteURL:   service3RemoteURL,
		},
	}

	imageManager.layers = map[string]imagemanager.LayerInfo{
		layer1: {
			LayerInfo: createLayerInfo(layer1, layer1LocalURL),
			RemoteURL: layer1RemoteURL,
		},
		layer2: {
			LayerInfo: createLayerInfo(layer2, layer2LocalURL),
			RemoteURL: layer2RemoteURL,
		},
	}

	type testData struct {
		nodeResources       map[string]aostypes.NodeUnitConfig
		serviceConfigs      map[string]aostypes.ServiceConfig
		desiredInstances    []cloudprotocol.InstanceInfo
		expectedRunRequests map[string]runRequest
		expectedRunStatus   unitstatushandler.RunInstancesStatus
	}

	testItems := []testData{
		// Check node priority and runner: all service instances should be start on higher priority node according to
		// supported runner
		{
			nodeResources: map[string]aostypes.NodeUnitConfig{
				nodeTypeLocalSM:  {NodeType: nodeTypeLocalSM, Priority: 100},
				nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50},
				nodeTypeRunxSM:   {NodeType: nodeTypeRunxSM, Priority: 0},
			},
			serviceConfigs: map[string]aostypes.ServiceConfig{
				service1: {Runner: runnerRunc},
				service2: {Runner: runnerRunc},
				service3: {Runner: runnerRunx},
			},
			desiredInstances: []cloudprotocol.InstanceInfo{
				{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 2},
				{ServiceID: service2, SubjectID: subject1, Priority: 50, NumInstances: 2},
				{ServiceID: service3, SubjectID: subject1, Priority: 0, NumInstances: 2},
			},
			expectedRunRequests: map[string]runRequest{
				nodeIDLocalSM: {
					services: []aostypes.ServiceInfo{
						createServiceInfo(service1, 5000, service1LocalURL),
						createServiceInfo(service2, 5001, service2LocalURL),
					},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1LocalURL),
						createLayerInfo(layer2, layer2LocalURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5000, 2, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 0,
						}, 100),
						createInstanceInfo(5001, 3, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 1,
						}, 100),
						createInstanceInfo(5002, 4, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 0,
						}, 50),
						createInstanceInfo(5003, 5, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 1,
						}, 50),
					},
				},
				nodeIDRemoteSM1: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
				nodeIDRemoteSM2: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
				nodeIDRunxSM: {
					services: []aostypes.ServiceInfo{createServiceInfo(service3, 5002, service3RemoteURL)},
					layers:   []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5004, 6, aostypes.InstanceIdent{
							ServiceID: service3, SubjectID: subject1, Instance: 0,
						}, 0),
						createInstanceInfo(5005, 7, aostypes.InstanceIdent{
							ServiceID: service3, SubjectID: subject1, Instance: 1,
						}, 0),
					},
				},
			},
			expectedRunStatus: unitstatushandler.RunInstancesStatus{
				Instances: []cloudprotocol.InstanceStatus{
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 1,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 1,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 0,
					}, nodeIDRunxSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 1,
					}, nodeIDRunxSM, nil),
				},
			},
		},
		// Check labels: label low priority service to run on high priority node
		{
			nodeResources: map[string]aostypes.NodeUnitConfig{
				nodeTypeLocalSM:  {NodeType: nodeTypeLocalSM, Priority: 100, Labels: []string{"label1"}},
				nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50, Labels: []string{"label2"}},
				nodeTypeRunxSM:   {NodeType: nodeTypeRunxSM, Priority: 0},
			},
			serviceConfigs: map[string]aostypes.ServiceConfig{
				service1: {Runner: runnerRunc},
				service2: {Runner: runnerRunc},
				service3: {Runner: runnerRunx},
			},
			desiredInstances: []cloudprotocol.InstanceInfo{
				{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 2, Labels: []string{"label2"}},
				{ServiceID: service2, SubjectID: subject1, Priority: 50, NumInstances: 2, Labels: []string{"label1"}},
				{ServiceID: service3, SubjectID: subject1, Priority: 0, NumInstances: 2, Labels: []string{"label1"}},
			},
			expectedRunRequests: map[string]runRequest{
				nodeIDLocalSM: {
					services: []aostypes.ServiceInfo{createServiceInfo(service2, 5001, service2LocalURL)},
					layers:   []aostypes.LayerInfo{createLayerInfo(layer1, layer1LocalURL)},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5002, 2, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 0,
						}, 50),
						createInstanceInfo(5003, 3, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 1,
						}, 50),
					},
				},
				nodeIDRemoteSM1: {
					services: []aostypes.ServiceInfo{createServiceInfo(service1, 5000, service1RemoteURL)},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1RemoteURL),
						createLayerInfo(layer2, layer2RemoteURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5000, 4, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 0,
						}, 100),
						createInstanceInfo(5001, 5, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 1,
						}, 100),
					},
				},
				nodeIDRemoteSM2: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
				nodeIDRunxSM: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
			},
			expectedRunStatus: unitstatushandler.RunInstancesStatus{
				Instances: []cloudprotocol.InstanceStatus{
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 0,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 1,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 1,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 0,
					}, "", errors.New("no node with labels [label1]")), //nolint:goerr113
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 1,
					}, "", errors.New("no node with labels [label1]")), //nolint:goerr113
				},
			},
		},
		// Check available resources
		{
			nodeResources: map[string]aostypes.NodeUnitConfig{
				nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100, Resources: []aostypes.ResourceInfo{
					{Name: "resource1"},
					{Name: "resource3"},
				}},
				nodeTypeRemoteSM: {
					NodeType: nodeTypeRemoteSM, Priority: 50, Labels: []string{"label2"},
					Resources: []aostypes.ResourceInfo{
						{Name: "resource1"},
						{Name: "resource2"},
					},
				},
				nodeTypeRunxSM: {NodeType: nodeTypeRunxSM, Priority: 0},
			},
			serviceConfigs: map[string]aostypes.ServiceConfig{
				service1: {Runner: runnerRunc, Resources: []string{"resource1", "resource2"}},
				service2: {Runner: runnerRunc, Resources: []string{"resource1"}},
				service3: {Runner: runnerRunc, Resources: []string{"resource3"}},
			},
			desiredInstances: []cloudprotocol.InstanceInfo{
				{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 2},
				{ServiceID: service2, SubjectID: subject1, Priority: 50, NumInstances: 2},
				{ServiceID: service3, SubjectID: subject1, Priority: 0, NumInstances: 2, Labels: []string{"label2"}},
			},
			expectedRunRequests: map[string]runRequest{
				nodeIDLocalSM: {
					services: []aostypes.ServiceInfo{createServiceInfo(service2, 5001, service2LocalURL)},
					layers:   []aostypes.LayerInfo{createLayerInfo(layer1, layer1LocalURL)},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5002, 2, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 0,
						}, 50),
						createInstanceInfo(5003, 3, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 1,
						}, 50),
					},
				},
				nodeIDRemoteSM1: {
					services: []aostypes.ServiceInfo{createServiceInfo(service1, 5000, service1RemoteURL)},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1RemoteURL),
						createLayerInfo(layer2, layer2RemoteURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5000, 4, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 0,
						}, 100),
						createInstanceInfo(5001, 5, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 1,
						}, 100),
					},
				},
				nodeIDRemoteSM2: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
				nodeIDRunxSM: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
			},
			expectedRunStatus: unitstatushandler.RunInstancesStatus{
				Instances: []cloudprotocol.InstanceStatus{
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 0,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 1,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 1,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 0,
					}, "", errors.New("no node with resources [resource3]")), //nolint:goerr113
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 1,
					}, "", errors.New("no node with resources [resource3]")), //nolint:goerr113
				},
			},
		},
		// Check available devices
		{
			nodeResources: map[string]aostypes.NodeUnitConfig{
				nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100, Devices: []aostypes.DeviceInfo{
					{Name: "dev1", SharedCount: 1},
					{Name: "dev2", SharedCount: 2},
					{Name: "dev3"},
				}},
				nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50, Devices: []aostypes.DeviceInfo{
					{Name: "dev1", SharedCount: 1},
					{Name: "dev2", SharedCount: 3},
				}, Labels: []string{"label2"}},
				nodeTypeRunxSM: {NodeType: nodeTypeRunxSM, Priority: 0, Devices: []aostypes.DeviceInfo{
					{Name: "dev1", SharedCount: 1},
					{Name: "dev2", SharedCount: 2},
				}},
			},
			serviceConfigs: map[string]aostypes.ServiceConfig{
				service1: {Runner: runnerRunc, Devices: []aostypes.ServiceDevice{{Name: "dev1"}, {Name: "dev2"}}},
				service2: {Runner: runnerRunc, Devices: []aostypes.ServiceDevice{{Name: "dev2"}}},
				service3: {Runner: runnerRunc, Devices: []aostypes.ServiceDevice{{Name: "dev3"}}},
			},
			desiredInstances: []cloudprotocol.InstanceInfo{
				{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 4},
				{ServiceID: service2, SubjectID: subject1, Priority: 50, NumInstances: 3},
				{ServiceID: service3, SubjectID: subject1, Priority: 0, NumInstances: 2, Labels: []string{"label2"}},
			},
			expectedRunRequests: map[string]runRequest{
				nodeIDLocalSM: {
					services: []aostypes.ServiceInfo{
						createServiceInfo(service1, 5000, service1LocalURL),
						createServiceInfo(service2, 5001, service2LocalURL),
					},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1LocalURL),
						createLayerInfo(layer2, layer2LocalURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5000, 2, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 0,
						}, 100),
						createInstanceInfo(5003, 3, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 0,
						}, 50),
					},
				},
				nodeIDRemoteSM1: {
					services: []aostypes.ServiceInfo{
						createServiceInfo(service1, 5000, service1RemoteURL),
						createServiceInfo(service2, 5001, service2RemoteURL),
					},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1RemoteURL),
						createLayerInfo(layer2, layer2RemoteURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5001, 4, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 1,
						}, 100),
						createInstanceInfo(5004, 5, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 1,
						}, 50),
						createInstanceInfo(5005, 6, aostypes.InstanceIdent{
							ServiceID: service2, SubjectID: subject1, Instance: 2,
						}, 50),
					},
				},
				nodeIDRemoteSM2: {
					services: []aostypes.ServiceInfo{createServiceInfo(service1, 5000, service1RemoteURL)},
					layers: []aostypes.LayerInfo{
						createLayerInfo(layer1, layer1RemoteURL),
						createLayerInfo(layer2, layer2RemoteURL),
					},
					instances: []aostypes.InstanceInfo{
						createInstanceInfo(5002, 7, aostypes.InstanceIdent{
							ServiceID: service1, SubjectID: subject1, Instance: 2,
						}, 100),
					},
				},
				nodeIDRunxSM: {
					services:  []aostypes.ServiceInfo{},
					layers:    []aostypes.LayerInfo{},
					instances: []aostypes.InstanceInfo{},
				},
			},
			expectedRunStatus: unitstatushandler.RunInstancesStatus{
				Instances: []cloudprotocol.InstanceStatus{
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 1,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 2,
					}, nodeIDRemoteSM2, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 3,
					}, "", errors.New("no available device found")), //nolint:goerr113
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 0,
					}, nodeIDLocalSM, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 1,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service2, SubjectID: subject1, Instance: 2,
					}, nodeIDRemoteSM1, nil),
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 0,
					}, "", errors.New("no available device found")), //nolint:goerr113
					createInstanceStatus(aostypes.InstanceIdent{
						ServiceID: service3, SubjectID: subject1, Instance: 1,
					}, "", errors.New("no available device found")), //nolint:goerr113
				},
			},
		},
	}

	for _, testItem := range testItems {
		resourceManager.nodeResources = testItem.nodeResources

		for serviceID, config := range testItem.serviceConfigs {
			service := imageManager.services[serviceID]
			service.Config = config
			imageManager.services[serviceID] = service
		}

		launcherInstance, err := launcher.New(cfg, newTestStorage(), nodeManager, imageManager, resourceManager,
			&testStateStorage{}, newTestNetworkManager("172.17.0.1/16"))
		if err != nil {
			t.Fatalf("Can't create launcher %v", err)
		}

		// Wait initial run status

		for nodeID, info := range nodeManager.nodeInformation {
			nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
				NodeID: nodeID, NodeType: info.NodeType, Instances: []cloudprotocol.InstanceStatus{},
			}
		}

		if err := waitRunInstancesStatus(
			launcherInstance.GetRunStatusesChannel(), unitstatushandler.RunInstancesStatus{}, time.Second); err != nil {
			t.Errorf("Incorrect run status: %v", err)
		}

		// Run instances

		if err := launcherInstance.RunInstances(testItem.desiredInstances, nil); err != nil {
			t.Fatalf("Can't run instances %v", err)
		}

		if err := waitRunInstancesStatus(
			launcherInstance.GetRunStatusesChannel(), testItem.expectedRunStatus, time.Second); err != nil {
			t.Errorf("Incorrect run status: %v", err)
		}

		if err := nodeManager.compareRunRequests(testItem.expectedRunRequests); err != nil {
			t.Errorf("Incorrect run request: %v", err)
		}

		launcherInstance.Close()
	}
}

func TestServiceRevert(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{nodeIDLocalSM},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager     = newTestNodeManager()
		imageManager    = &testImageProvider{}
		resourceManager = newTestResourceManager()
	)

	nodeManager.nodeInformation[nodeIDLocalSM] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: nodeIDLocalSM, NodeType: nodeTypeLocalSM},
		RemoteNode: false,
	}
	resourceManager.nodeResources[nodeTypeLocalSM] = aostypes.NodeUnitConfig{NodeType: nodeTypeLocalSM, Priority: 100}

	imageManager.services = map[string]imagemanager.ServiceInfo{
		service1: {
			ServiceInfo: createServiceInfo(service1, 5000, service1LocalURL),
			RemoteURL:   service1RemoteURL,
			Layers:      []string{layer1},
		},
		service2: {
			ServiceInfo: createServiceInfo(service2, 5001, service2LocalURL),
			RemoteURL:   service2RemoteURL,
			Layers:      []string{layer2},
		},
	}

	imageManager.layers = map[string]imagemanager.LayerInfo{
		layer1: {
			LayerInfo: createLayerInfo(layer1, layer1LocalURL),
			RemoteURL: layer1RemoteURL,
		},
	}

	launcherInstance, err := launcher.New(cfg, newTestStorage(), nodeManager, imageManager, resourceManager,
		&testStateStorage{}, newTestNetworkManager("172.17.0.1/16"))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}

	// Wait initial run status

	for nodeID, info := range nodeManager.nodeInformation {
		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: info.NodeType, Instances: []cloudprotocol.InstanceStatus{},
		}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), unitstatushandler.RunInstancesStatus{}, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	// Run instances

	desiredInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 2},
		{ServiceID: service2, SubjectID: subject1, Priority: 50, NumInstances: 2},
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{service2}); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	expectedRunStatus := unitstatushandler.RunInstancesStatus{
		Instances: []cloudprotocol.InstanceStatus{
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 0,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 1,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service2, SubjectID: subject1, Instance: 0,
			}, "", errors.New("layer does't exist")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service2, SubjectID: subject1, Instance: 1,
			}, "", errors.New("layer does't exist")), //nolint:goerr113
		},
		ErrorServices: []cloudprotocol.ServiceStatus{
			{ID: service2, AosVersion: 1, Status: cloudprotocol.ErrorStatus},
		},
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	if !reflect.DeepEqual([]string{service2}, imageManager.revertedServices) {
		t.Errorf("Incorrect reverted services: %v", imageManager.revertedServices)
	}
}

func TestRebalancing(t *testing.T) {
	const nodeTypeSuffix = "Type"
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{"localSM1", "localSM2", "remoteSM1", "remoteSM2"},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager     = newTestNodeManager()
		resourceManager = newTestResourceManager()
		imageManager    = &testImageProvider{}
	)

	resourceManager.nodeResources["localSM"+nodeTypeSuffix] = aostypes.NodeUnitConfig{
		Priority: 100,
		NodeType: "localSM" + nodeTypeSuffix, Devices: []aostypes.DeviceInfo{
			{Name: "commonDevice", SharedCount: 1},
		},
	}
	resourceManager.nodeResources["remoteSM"+nodeTypeSuffix] = aostypes.NodeUnitConfig{
		Priority: 50,
		NodeType: "remoteSM" + nodeTypeSuffix,
		Devices: []aostypes.DeviceInfo{
			{Name: "commonDevice", SharedCount: 2},
		},
		Resources: []aostypes.ResourceInfo{{Name: "res1"}},
	}

	nodeManager.nodeInformation["localSM1"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "localSM1", NodeType: "localSM" + nodeTypeSuffix},
		RemoteNode: false, RunnerFeature: []string{runnerRunc, "crun"},
	}
	nodeManager.nodeInformation["localSM2"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM2", NodeType: "localSM" + nodeTypeSuffix},
		RemoteNode: false, RunnerFeature: []string{runnerRunc, "crun"},
	}

	nodeManager.nodeInformation["remoteSM1"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM1", NodeType: "remoteSM" + nodeTypeSuffix},
		RemoteNode: true, RunnerFeature: []string{runnerRunc, "crun"},
	}
	nodeManager.nodeInformation["remoteSM2"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM2", NodeType: "remoteSM" + nodeTypeSuffix},
		RemoteNode: true, RunnerFeature: []string{runnerRunc, "crun"},
	}

	launcherInstance, err := launcher.New(cfg, newTestStorage(), nodeManager, imageManager, resourceManager,
		&testStateStorage{}, newTestNetworkManager("172.17.0.1/16"))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for nodeID, nodeInfo := range nodeManager.nodeInformation {
		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: nodeInfo.NodeType, Instances: []cloudprotocol.InstanceStatus{},
		}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), unitstatushandler.RunInstancesStatus{}, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	desiredInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: "servRes1", SubjectID: "subj1", Priority: 100, NumInstances: 1},
		{ServiceID: "servNoDev", SubjectID: "subj1", Priority: 90, NumInstances: 1},
		{ServiceID: "servCommonDev", SubjectID: "subj1", Priority: 50, NumInstances: 3},
	}

	imageManager.services = map[string]imagemanager.ServiceInfo{
		"servRes1": {
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servRes1", URL: "servRes1LocalUrl", GID: 5000,
			},
			RemoteURL: "servRes1RemoteUrl",
			Config: aostypes.ServiceConfig{
				Runner:    runnerRunc,
				Resources: []string{"res1"},
			},
		},
		"servNoDev": {
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servNoDev", URL: "servNoDevLocalUrl", GID: 5001,
			},
			RemoteURL: "servNoDevRemoteUrl",
			Config: aostypes.ServiceConfig{
				Runner: runnerRunc,
			},
		},
		"servCommonDev": {
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev", URL: "servCommonDevLocalUrl", GID: 5002,
			},
			RemoteURL: "servCommonDevRemoteUrl",
			Config: aostypes.ServiceConfig{
				Runner: runnerRunc,
				Devices: []aostypes.ServiceDevice{
					{Name: "commonDevice"},
				},
			},
		},
	}

	expectedRunRequests := map[string]runRequest{
		"localSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servNoDev",
					URL: "servNoDevLocalUrl", GID: 5001,
				},
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevLocalUrl", GID: 5002,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servNoDev", SubjectID: "subj1", Instance: 0},
					UID:           5001, Priority: 90, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.4",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 0},
					UID:           5002, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.5",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"localSM2": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevLocalUrl", GID: 5002,
				},
			},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 1},
					UID:           5003, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.6",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"remoteSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servRes1",
					URL: "servRes1RemoteUrl", GID: 5000,
				},
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevRemoteUrl", GID: 5002,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servRes1", SubjectID: "subj1", Instance: 0},
					UID:           5000, Priority: 100, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.2",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 2},
					UID:           5004, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.3",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"remoteSM2": {
			services:  []aostypes.ServiceInfo{},
			layers:    []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{},
		},
	}

	var expectedRunStatus unitstatushandler.RunInstancesStatus

	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servRes1", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 2},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servNoDev", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 1},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM2", StateChecksum: magicSum,
		},
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{}); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	if err := nodeManager.compareRunRequests(expectedRunRequests); err != nil {
		t.Errorf("Incorrect run request: %v", err)
	}

	// cpu alert
	nodeManager.alertsChannel <- cloudprotocol.SystemQuotaAlert{NodeID: "localSM1", Parameter: "cpu"}

	expectedRunRequests = map[string]runRequest{
		"localSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servNoDev",
					URL: "servNoDevLocalUrl", GID: 5001,
				},
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevLocalUrl", GID: 5002,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servNoDev", SubjectID: "subj1", Instance: 0},
					UID:           5001, Priority: 90, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.9",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"localSM2": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevLocalUrl", GID: 5002,
				},
			},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 1},
					UID:           5003, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.11",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"remoteSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servRes1",
					URL: "servRes1RemoteUrl", GID: 5000,
				},
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "servCommonDev",
					URL: "servCommonDevRemoteUrl", GID: 5002,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servRes1", SubjectID: "subj1", Instance: 0},
					UID:           5000, Priority: 100, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.7",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 2},
					UID:           5004, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.8",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 0},
					UID:           5002, Priority: 50, StoragePath: "", StatePath: "",
					NetworkParameters: aostypes.NetworkParameters{
						IP:         "172.17.0.10",
						Subnet:     "172.17.0.0/16",
						DNSServers: []string{"10.10.0.1"},
					},
				},
			},
		},
		"remoteSM2": {
			services:  []aostypes.ServiceInfo{},
			layers:    []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{},
		},
	}

	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servRes1", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 2},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servNoDev", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "servCommonDev", SubjectID: "subj1", Instance: 1},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM2", StateChecksum: magicSum,
		},
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	if err := nodeManager.compareRunRequests(expectedRunRequests); err != nil {
		t.Errorf("incorrect run request: %v", err)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

// testNodeManager

func newTestNodeManager() *testNodeManager {
	nodeManager := &testNodeManager{
		runStatusChan:   make(chan launcher.NodeRunInstanceStatus, 10),
		nodeInformation: make(map[string]launcher.NodeInfo),
		runRequest:      make(map[string]runRequest),
		alertsChannel:   make(chan cloudprotocol.SystemQuotaAlert, 10),
	}

	return nodeManager
}

func (nodeManager *testNodeManager) GetNodeConfiguration(nodeID string) (launcher.NodeInfo, error) {
	config, ok := nodeManager.nodeInformation[nodeID]
	if !ok {
		return launcher.NodeInfo{}, aoserrors.New("node config doesn't exist")
	}

	config.NodeID = nodeID

	return config, nil
}

func (nodeManager *testNodeManager) RunInstances(nodeID string,
	services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo, forceRestart bool,
) error {
	nodeManager.runRequest[nodeID] = runRequest{
		services: services, layers: layers, instances: instances,
		forceRestart: forceRestart,
	}

	successStatus := launcher.NodeRunInstanceStatus{
		NodeID:    nodeID,
		Instances: make([]cloudprotocol.InstanceStatus, len(instances)),
	}

	for i, instance := range instances {
		successStatus.Instances[i] = cloudprotocol.InstanceStatus{
			InstanceIdent: instance.InstanceIdent,
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive, NodeID: nodeID,
		}
	}

	nodeManager.runStatusChan <- successStatus

	return nil
}

func (nodeManager *testNodeManager) GetRunInstancesStatusChannel() <-chan launcher.NodeRunInstanceStatus {
	return nodeManager.runStatusChan
}

func (nodeManager *testNodeManager) GetUpdateInstancesStatusChannel() <-chan []cloudprotocol.InstanceStatus {
	return nil
}

func (nodeManager *testNodeManager) GetSystemLimitAlertChannel() <-chan cloudprotocol.SystemQuotaAlert {
	return nodeManager.alertsChannel
}

func (nodeManager *testNodeManager) GetNodeMonitoringData(nodeID string) (cloudprotocol.NodeMonitoringData, error) {
	return cloudprotocol.NodeMonitoringData{}, nil
}

func (nodeManager *testNodeManager) compareRunRequests(expectedRunRequests map[string]runRequest) error {
	for nodeID, runRequest := range nodeManager.runRequest {
		if err := deepSlicesCompare(expectedRunRequests[nodeID].services, runRequest.services); err != nil {
			return aoserrors.Errorf("incorrect services for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].layers, runRequest.layers); err != nil {
			return aoserrors.Errorf("incorrect layers for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].instances, runRequest.instances); err != nil {
			return aoserrors.Errorf("incorrect instances for node %s: %v", nodeID, err)
		}

		if expectedRunRequests[nodeID].forceRestart {
			return aoserrors.Errorf("incorrect force restart flag")
		}
	}

	return nil
}

// testResourceManager

func newTestResourceManager() *testResourceManager {
	resourceManager := &testResourceManager{
		nodeResources: make(map[string]aostypes.NodeUnitConfig),
	}

	return resourceManager
}

func (resourceManager *testResourceManager) GetUnitConfiguration(nodeType string) aostypes.NodeUnitConfig {
	resource := resourceManager.nodeResources[nodeType]
	resource.NodeType = nodeType

	return resource
}

// testStorage

func newTestStorage() *testStorage {
	return &testStorage{
		desiredInstances: json.RawMessage("[]"),
	}
}

func (storage *testStorage) AddInstance(instanceInfo launcher.InstanceInfo) error {
	for _, uid := range storage.instanceInfo {
		if uid.InstanceIdent == instanceInfo.InstanceIdent {
			return aoserrors.New("uid for instance already exist")
		}
	}

	storage.instanceInfo = append(storage.instanceInfo, instanceInfo)

	return nil
}

func (storage *testStorage) GetInstanceUID(instance aostypes.InstanceIdent) (int, error) {
	for _, instanceInfo := range storage.instanceInfo {
		if instanceInfo.InstanceIdent == instance {
			return instanceInfo.UID, nil
		}
	}

	return 0, launcher.ErrNotExist
}

func (storage *testStorage) GetInstances() ([]launcher.InstanceInfo, error) {
	return storage.instanceInfo, nil
}

func (storage *testStorage) RemoveInstance(instanceIdent aostypes.InstanceIdent) error {
	for i, instanceInfo := range storage.instanceInfo {
		if instanceInfo.InstanceIdent == instanceIdent {
			storage.instanceInfo = append(storage.instanceInfo[:i], storage.instanceInfo[i+1:]...)

			return nil
		}
	}

	return launcher.ErrNotExist
}

func (storage *testStorage) SetDesiredInstances(instances json.RawMessage) error {
	storage.desiredInstances = instances
	return nil
}

func (storage *testStorage) GetDesiredInstances() (instances json.RawMessage, err error) {
	return storage.desiredInstances, nil
}

// testStateStorage

func (provider *testStateStorage) Setup(
	params storagestate.SetupParams,
) (storagePath string, statePath string, err error) {
	return "", "", nil
}

func (provider *testStateStorage) Cleanup(instanceIdent aostypes.InstanceIdent) error {
	provider.cleanedInstances = append(provider.cleanedInstances, instanceIdent)

	return nil
}

func (provider *testStateStorage) GetInstanceCheckSum(instance aostypes.InstanceIdent) string {
	return magicSum
}

// testImageProvider

func (testProvider *testImageProvider) GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error) {
	if service, ok := testProvider.services[serviceID]; ok {
		return service, nil
	}

	return imagemanager.ServiceInfo{}, errors.New("service does't exist") //nolint:goerr113
}

func (testProvider *testImageProvider) GetLayerInfo(digest string) (imagemanager.LayerInfo, error) {
	if layer, ok := testProvider.layers[digest]; ok {
		return layer, nil
	}

	return imagemanager.LayerInfo{}, errors.New("layer does't exist") //nolint:goerr113
}

func (testProvider *testImageProvider) RevertService(serviceID string) error {
	testProvider.revertedServices = append(testProvider.revertedServices, serviceID)

	return nil
}

// testNetworkManager

func newTestNetworkManager(network string) *testNetworkManager {
	networkManager := &testNetworkManager{
		networkInfo: make(map[string]map[aostypes.InstanceIdent]struct{}),
	}

	if len(network) != 0 {
		ip, ipNet, err := net.ParseCIDR(network)
		if err != nil {
			log.Fatalf("Can't parse CIDR: %v", err)
		}

		networkManager.currentIP = ip
		networkManager.subnet = *ipNet
	}

	return networkManager
}

func (network *testNetworkManager) UpdateProviderNetwork(providers []string, nodeID string) error {
	return nil
}

func (network *testNetworkManager) PrepareInstanceNetworkParameters(
	instanceIdent aostypes.InstanceIdent, networkID string,
	params networkmanager.NetworkParameters,
) (aostypes.NetworkParameters, error) {
	if len(network.networkInfo[networkID]) == 0 {
		network.networkInfo[networkID] = make(map[aostypes.InstanceIdent]struct{})
	}

	network.currentIP = cidr.Inc(network.currentIP)

	network.networkInfo[networkID][instanceIdent] = struct{}{}

	return aostypes.NetworkParameters{
		IP:         network.currentIP.String(),
		Subnet:     network.subnet.String(),
		DNSServers: []string{"10.10.0.1"},
	}, nil
}

func (network *testNetworkManager) RemoveInstanceNetworkParameters(
	instanceIdent aostypes.InstanceIdent, networkID string,
) {
	delete(network.networkInfo[networkID], instanceIdent)
}

func (network *testNetworkManager) GetInstances() (instances []aostypes.InstanceIdent) {
	for networkID := range network.networkInfo {
		for instanceIdent := range network.networkInfo[networkID] {
			instances = append(instances, instanceIdent)
		}
	}

	return instances
}

func (network *testNetworkManager) RestartDNSServer() error {
	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createServiceInfo(id string, gid uint32, url string) aostypes.ServiceInfo {
	return aostypes.ServiceInfo{
		ID:          id,
		VersionInfo: aostypes.VersionInfo{AosVersion: 1},
		URL:         url,
		GID:         gid,
	}
}

func createLayerInfo(digest string, url string) aostypes.LayerInfo {
	return aostypes.LayerInfo{
		Digest:      digest,
		VersionInfo: aostypes.VersionInfo{AosVersion: 1},
		URL:         url,
	}
}

func createInstanceStatus(ident aostypes.InstanceIdent, nodeID string, err error) cloudprotocol.InstanceStatus {
	status := cloudprotocol.InstanceStatus{
		InstanceIdent: ident,
		RunState:      cloudprotocol.InstanceStateActive,
		AosVersion:    1,
		NodeID:        nodeID,
	}

	if err != nil {
		status.RunState = cloudprotocol.InstanceStateFailed
		status.ErrorInfo = &cloudprotocol.ErrorInfo{
			Message: err.Error(),
		}
	} else {
		status.StateChecksum = magicSum
	}

	return status
}

func createInstanceInfo(uid uint32, ip int, ident aostypes.InstanceIdent, priority uint64) aostypes.InstanceInfo {
	return aostypes.InstanceInfo{
		InstanceIdent: ident,
		NetworkParameters: aostypes.NetworkParameters{
			IP:         "172.17.0." + strconv.Itoa(ip),
			Subnet:     "172.17.0.0/16",
			DNSServers: []string{"10.10.0.1"},
		},
		UID:      uid,
		Priority: priority,
	}
}

func waitRunInstancesStatus(
	messageChannel <-chan unitstatushandler.RunInstancesStatus, expectedMsg unitstatushandler.RunInstancesStatus,
	timeout time.Duration,
) (err error) {
	var message unitstatushandler.RunInstancesStatus

	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message = <-messageChannel:
		if len(message.Instances) != len(expectedMsg.Instances) {
			return aoserrors.New("incorrect length")
		}

	topLoop:
		for _, receivedEl := range message.Instances {
			for _, expectedEl := range expectedMsg.Instances {
				if receivedEl.ErrorInfo == nil && expectedEl.ErrorInfo != nil {
					continue
				}

				if receivedEl.ErrorInfo != nil && expectedEl.ErrorInfo == nil {
					continue
				}

				if receivedEl.ErrorInfo != nil && expectedEl.ErrorInfo != nil {
					if receivedEl.ErrorInfo.AosCode != expectedEl.ErrorInfo.AosCode ||
						receivedEl.ErrorInfo.ExitCode != expectedEl.ErrorInfo.ExitCode ||
						!strings.Contains(receivedEl.ErrorInfo.Message, expectedEl.ErrorInfo.Message) {
						continue
					}
				}

				receivedForCheck := receivedEl

				receivedForCheck.ErrorInfo = nil
				expectedEl.ErrorInfo = nil

				if reflect.DeepEqual(receivedForCheck, expectedEl) {
					continue topLoop
				}
			}

			return aoserrors.New("incorrect instances in run status")
		}

		if err := deepSlicesCompare(expectedMsg.UnitSubjects, message.UnitSubjects); err != nil {
			return aoserrors.New("incorrect subjects in run status")
		}

		for i := range message.ErrorServices {
			message.ErrorServices[i].ErrorInfo = nil
		}

		if err := deepSlicesCompare(expectedMsg.ErrorServices, message.ErrorServices); err != nil {
			return aoserrors.New("incorrect error services in run status")
		}

		return nil
	}
}

func deepSlicesCompare[T any](sliceA, sliceB []T) error {
	if len(sliceA) != len(sliceB) {
		return aoserrors.New("incorrect length")
	}

topLabel:
	for _, elementA := range sliceA {
		for _, elementB := range sliceB {
			if reflect.DeepEqual(elementA, elementB) {
				continue topLabel
			}
		}

		return aoserrors.New("slices are not equals")
	}

	return nil
}
