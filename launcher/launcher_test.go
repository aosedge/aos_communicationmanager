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
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/apparentlymart/go-cidr/cidr"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
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

type testNodeInfoProvider struct {
	nodeID   string
	nodeInfo map[string]cloudprotocol.NodeInfo
}

type testNodeManager struct {
	runStatusChan chan launcher.NodeRunInstanceStatus
	runRequest    map[string]runRequest
}

type testImageProvider struct {
	services                      map[string]imagemanager.ServiceInfo
	layers                        map[string]imagemanager.LayerInfo
	removeServiceInstancesChannel chan string
}

type testResourceManager struct {
	nodeConfigs map[string]cloudprotocol.NodeConfig
}

type testStorage struct {
	instanceInfo map[aostypes.InstanceIdent]*launcher.InstanceInfo
}

type testStateStorage struct {
	cleanedInstances []aostypes.InstanceIdent
	removedInstances []aostypes.InstanceIdent
}

type testNetworkManager struct {
	currentIP   net.IP
	subnet      net.IPNet
	networkInfo map[string]map[aostypes.InstanceIdent]struct{}
}

type testData struct {
	testCaseName        string
	nodeConfigs         map[string]cloudprotocol.NodeConfig
	serviceConfigs      map[string]aostypes.ServiceConfig
	desiredInstances    []cloudprotocol.InstanceInfo
	storedInstances     []launcher.InstanceInfo
	expectedRunRequests map[string]runRequest
	expectedRunStatus   []cloudprotocol.InstanceStatus
	rebalancing         bool
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

func TestInstancesWithRemovedServiceInfoAreRemovedOnStart(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeInfoProvider = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager      = newTestNodeManager()
		imageManager     = newTestImageProvider()
		testStorage      = newTestStorage(nil)
	)

	err := testStorage.AddInstance(launcher.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{ServiceID: "SubjectId"},
	})
	if err != nil {
		t.Fatalf("Can't add instance %v", err)
	}

	launcherInstance, err := launcher.New(cfg, testStorage, nodeInfoProvider, nodeManager, imageManager,
		&testResourceManager{}, &testStateStorage{}, newTestNetworkManager(""))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	instances, err := testStorage.GetInstances()
	if err != nil {
		t.Fatalf("Can't get instances %v", err)
	}

	if len(instances) != 0 {
		t.Fatalf("Instances should be removed, but found %v", instances)
	}
}

func TestInstancesWithOutdatedTTLRemovedOnStart(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
			ServiceTTLDays: 1,
		}
		nodeInfoProvider = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager      = newTestNodeManager()
		imageManager     = newTestImageProvider()
		testStorage      = newTestStorage(nil)
		testStateStorage = &testStateStorage{}
	)

	err := testStorage.AddInstance(launcher.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{ServiceID: service1},
		Cached:        true,
		Timestamp:     time.Now().Add(-time.Hour * 25).UTC(),
	})
	if err != nil {
		t.Fatalf("Can't add instance %v", err)
	}

	err = testStorage.AddInstance(launcher.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{ServiceID: service2},
		Cached:        true,
		Timestamp:     time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("Can't add instance %v", err)
	}

	// add a service to the storage

	imageManager.services[service1] = imagemanager.ServiceInfo{
		ServiceInfo: createServiceInfo(service1, 0, ""),
	}
	imageManager.services[service2] = imagemanager.ServiceInfo{
		ServiceInfo: createServiceInfo(service2, 0, ""),
	}

	launcherInstance, err := launcher.New(cfg, testStorage, nodeInfoProvider, nodeManager, imageManager,
		&testResourceManager{}, testStateStorage, newTestNetworkManager(""))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	instances, err := testStorage.GetInstances()
	if err != nil {
		t.Fatalf("Can't get instances %v", err)
	}

	if len(instances) != 1 {
		t.Fatalf("Instances should be removed, but found %v", instances)
	}

	if instances[0].ServiceID != service2 {
		t.Fatalf("Unexpected service ID: %v", instances[0].ServiceID)
	}

	if removedLen := len(testStateStorage.removedInstances); removedLen != 1 {
		t.Fatalf("Expected exactly 1 instance to be removed, but got %v", removedLen)
	}

	if testStateStorage.removedInstances[0].ServiceID != service1 {
		t.Fatalf("Unexpected serviceID: %v", testStateStorage.removedInstances[0].ServiceID)
	}
}

func TestInstancesAreRemovedViaChannel(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
			ServiceTTLDays: 1,
		}
		nodeInfoProvider = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager      = newTestNodeManager()
		testImageManager = newTestImageProvider()
		testStorage      = newTestStorage(nil)
		testStateStorage = &testStateStorage{}
	)

	err := testStorage.AddInstance(launcher.InstanceInfo{
		InstanceIdent: aostypes.InstanceIdent{ServiceID: service1},
		Cached:        false,
	})
	if err != nil {
		t.Fatalf("Can't add instance %v", err)
	}

	launcherInstance, err := launcher.New(cfg, testStorage, nodeInfoProvider, nodeManager, testImageManager,
		&testResourceManager{}, testStateStorage, newTestNetworkManager(""))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	testImageManager.removeServiceInstancesChannel <- service1

	instancesWereRemoved := false

	for i := 0; i < 3; i++ {
		time.Sleep(time.Duration(i) * time.Second)

		instances, err := testStorage.GetInstances()
		if err != nil {
			t.Logf("Can't get instances %v", err)

			continue
		}

		if len(instances) != 0 {
			t.Logf("Instances should be removed, but found %v", instances)

			continue
		}

		if removedLen := len(testStateStorage.removedInstances); removedLen != 1 {
			t.Logf("Expected exactly 1 instance to be removed, but got %v", removedLen)

			continue
		}

		if testStateStorage.removedInstances[0].ServiceID != service1 {
			t.Logf("Unexpected service ID: %v", testStateStorage.removedInstances[0].ServiceID)

			continue
		}

		instancesWereRemoved = true
	}

	if !instancesWereRemoved {
		t.Error("Instances were not removed")
	}
}

func TestInitialStatus(t *testing.T) {
	var (
		nodeIDs = []string{nodeIDLocalSM, nodeIDRemoteSM1}
		cfg     = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeInfoProvider  = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager       = newTestNodeManager()
		expectedRunStatus = []cloudprotocol.InstanceStatus{}
		imageManager      = newTestImageProvider()
	)

	for _, id := range nodeIDs {
		nodeInfo := cloudprotocol.NodeInfo{
			NodeID: id, NodeType: "nodeType",
			Status:   cloudprotocol.NodeStatusProvisioned,
			TotalRAM: 100,
			CPUs: []cloudprotocol.CPUInfo{
				{ModelName: "Intel(R) Core(TM) i7-1185G7"},
			},
			Partitions: []cloudprotocol.PartitionInfo{
				{Name: "id", TotalSize: 200},
			},
		}

		nodeInfoProvider.nodeInfo[id] = nodeInfo
	}

	launcherInstance, err := launcher.New(cfg, newTestStorage(nil), nodeInfoProvider, nodeManager, imageManager,
		&testResourceManager{}, &testStateStorage{}, newTestNetworkManager(""))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for i, id := range nodeIDs {
		instances := []cloudprotocol.InstanceStatus{{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "s1", SubjectID: "subj1", Instance: uint64(i)},
			ServiceVersion: "1.0", StateChecksum: magicSum, Status: "running",
			NodeID: id,
		}}

		expectedRunStatus = append(expectedRunStatus, instances...)

		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{NodeID: id, Instances: instances}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}
}

func TestBalancing(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeInfoProvider = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager      = newTestNodeManager()
		resourceManager  = newTestResourceManager()
		imageManager     = newTestImageProvider()
	)

	nodeInfoProvider.nodeInfo = map[string]cloudprotocol.NodeInfo{
		nodeIDLocalSM: {
			NodeID: nodeIDLocalSM, NodeType: nodeTypeLocalSM,
			Status:   cloudprotocol.NodeStatusProvisioned,
			Attrs:    map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunc},
			MaxDMIPs: 1000,
			TotalRAM: 1024,
			Partitions: []cloudprotocol.PartitionInfo{
				{Name: "storages", Types: []string{aostypes.StoragesPartition}, TotalSize: 1024},
				{Name: "states", Types: []string{aostypes.StatesPartition}, TotalSize: 1024},
			},
		},
		nodeIDRemoteSM1: {
			NodeID: nodeIDRemoteSM1, NodeType: nodeTypeRemoteSM,
			Status:   cloudprotocol.NodeStatusProvisioned,
			Attrs:    map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunc},
			MaxDMIPs: 1000,
			TotalRAM: 1024,
		},
		nodeIDRemoteSM2: {
			NodeID: nodeIDRemoteSM2, NodeType: nodeTypeRemoteSM,
			Status:   cloudprotocol.NodeStatusProvisioned,
			Attrs:    map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunc},
			MaxDMIPs: 1000,
			TotalRAM: 1024,
		},
		nodeIDRunxSM: {
			NodeID: nodeIDRunxSM, NodeType: nodeTypeRunxSM,
			Status:   cloudprotocol.NodeStatusProvisioned,
			Attrs:    map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunx},
			MaxDMIPs: 1000,
			TotalRAM: 1024,
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

	testItems := []testData{
		testItemNodePriority(),
		testItemLabels(),
		testItemResources(),
		testItemDevices(),
		testItemStorageRatio(),
		testItemStateRatio(),
		testItemCPURatio(),
		testItemRAMRatio(),
	}

	for _, testItem := range testItems {
		t.Logf("Test case: %s", testItem.testCaseName)

		resourceManager.nodeConfigs = testItem.nodeConfigs

		for serviceID, config := range testItem.serviceConfigs {
			service := imageManager.services[serviceID]
			service.Config = config
			imageManager.services[serviceID] = service
		}

		storage := newTestStorage(testItem.storedInstances)

		launcherInstance, err := launcher.New(cfg, storage, nodeInfoProvider, nodeManager, imageManager,
			resourceManager, &testStateStorage{}, newTestNetworkManager("172.17.0.1/16"))
		if err != nil {
			t.Fatalf("Can't create launcher %v", err)
		}

		// Wait initial run status

		for nodeID, info := range nodeInfoProvider.nodeInfo {
			nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
				NodeID: nodeID, NodeType: info.NodeType, Instances: []cloudprotocol.InstanceStatus{},
			}
		}

		if err := waitRunInstancesStatus(
			launcherInstance.GetRunStatusesChannel(), []cloudprotocol.InstanceStatus{}, time.Second); err != nil {
			t.Errorf("Incorrect run status: %v", err)
		}

		// Run instances

		if err := launcherInstance.RunInstances(testItem.desiredInstances, testItem.rebalancing); err != nil {
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

func TestStorageCleanup(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeInfoProvider     = newTestNodeInfoProvider(nodeIDLocalSM)
		nodeManager          = newTestNodeManager()
		resourceManager      = newTestResourceManager()
		imageManager         = newTestImageProvider()
		stateStorageProvider = &testStateStorage{}
	)

	nodeInfoProvider.nodeInfo[nodeIDLocalSM] = cloudprotocol.NodeInfo{
		NodeID: nodeIDLocalSM, NodeType: nodeTypeLocalSM,
		Status: cloudprotocol.NodeStatusProvisioned,
		Attrs:  map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunc},
	}
	resourceManager.nodeConfigs[nodeTypeLocalSM] = cloudprotocol.NodeConfig{Priority: 100}

	nodeInfoProvider.nodeInfo[nodeIDRunxSM] = cloudprotocol.NodeInfo{
		NodeID: nodeIDRunxSM, NodeType: nodeTypeRunxSM,
		Status: cloudprotocol.NodeStatusProvisioned,
		Attrs:  map[string]interface{}{cloudprotocol.NodeAttrRunners: runnerRunx},
	}
	resourceManager.nodeConfigs[nodeTypeRunxSM] = cloudprotocol.NodeConfig{Priority: 0}

	imageManager.services = map[string]imagemanager.ServiceInfo{
		service1: {
			ServiceInfo: createServiceInfo(service1, 5000, service1LocalURL),
			RemoteURL:   service1RemoteURL, Config: aostypes.ServiceConfig{Runners: []string{runnerRunc}},
		},
		service2: {
			ServiceInfo: createServiceInfo(service2, 5001, service2LocalURL),
			RemoteURL:   service2RemoteURL, Config: aostypes.ServiceConfig{Runners: []string{runnerRunc}},
		},
		service3: {
			ServiceInfo: createServiceInfo(service3, 5002, service3LocalURL),
			RemoteURL:   service3RemoteURL, Config: aostypes.ServiceConfig{Runners: []string{runnerRunx}},
		},
	}

	launcherInstance, err := launcher.New(cfg, newTestStorage(nil), nodeInfoProvider, nodeManager, imageManager,
		resourceManager, stateStorageProvider, newTestNetworkManager("172.17.0.1/16"))
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for nodeID, info := range nodeInfoProvider.nodeInfo {
		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: info.NodeType, Instances: []cloudprotocol.InstanceStatus{},
		}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), []cloudprotocol.InstanceStatus{}, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	desiredInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 2},
		{ServiceID: service2, SubjectID: subject1, Priority: 100, NumInstances: 1},
		{ServiceID: service3, SubjectID: subject1, Priority: 100, NumInstances: 1},
	}

	if err := launcherInstance.RunInstances(desiredInstances, false); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	expectedRunRequests := map[string]runRequest{
		nodeIDLocalSM: {
			services: []aostypes.ServiceInfo{
				createServiceInfo(service1, 5000, service1LocalURL),
				createServiceInfo(service2, 5001, service2LocalURL),
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
				}, 100),
			},
		},
		nodeIDRunxSM: {
			services: []aostypes.ServiceInfo{
				createServiceInfo(service3, 5002, service3RemoteURL),
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				createInstanceInfo(5003, 5, aostypes.InstanceIdent{
					ServiceID: service3, SubjectID: subject1, Instance: 0,
				}, 100),
			},
		},
	}

	expectedRunStatus := []cloudprotocol.InstanceStatus{
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
			ServiceID: service3, SubjectID: subject1, Instance: 0,
		}, nodeIDRunxSM, nil),
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Error waiting for run instances status: %v", err)
	}

	if err := nodeManager.compareRunRequests(expectedRunRequests); err != nil {
		t.Errorf("incorrect run request: %v", err)
	}

	desiredInstances = []cloudprotocol.InstanceInfo{
		{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 1},
	}

	expectedRunStatus = []cloudprotocol.InstanceStatus{
		createInstanceStatus(aostypes.InstanceIdent{
			ServiceID: service1, SubjectID: subject1, Instance: 0,
		}, nodeIDLocalSM, nil),
	}

	expectedCleanInstances := []aostypes.InstanceIdent{
		{ServiceID: service1, SubjectID: subject1, Instance: 1},
		{ServiceID: service2, SubjectID: subject1, Instance: 0},
		{ServiceID: service3, SubjectID: subject1, Instance: 0},
	}

	if err := launcherInstance.RunInstances(desiredInstances, false); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	if err := deepSlicesCompare(expectedCleanInstances, stateStorageProvider.cleanedInstances); err != nil {
		t.Errorf("Incorrect state storage cleanup: %v", err)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

// testNodeInfoProvider

func newTestNodeInfoProvider(nodeID string) *testNodeInfoProvider {
	return &testNodeInfoProvider{
		nodeID:   nodeID,
		nodeInfo: make(map[string]cloudprotocol.NodeInfo),
	}
}

func (provider *testNodeInfoProvider) GetAllNodeIDs() (nodeIDs []string, err error) {
	if provider.nodeInfo == nil {
		return nil, aoserrors.New("node info not found")
	}

	nodeIDs = make([]string, 0, len(provider.nodeInfo))
	for nodeID := range provider.nodeInfo {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

func (provider *testNodeInfoProvider) GetNodeID() string {
	return provider.nodeID
}

func (provider *testNodeInfoProvider) GetNodeInfo(nodeID string) (cloudprotocol.NodeInfo, error) {
	nodeInfo, ok := provider.nodeInfo[nodeID]
	if !ok {
		return cloudprotocol.NodeInfo{}, aoserrors.New("node info not found")
	}

	return nodeInfo, nil
}

// testNodeManager

func newTestNodeManager() *testNodeManager {
	nodeManager := &testNodeManager{
		runStatusChan: make(chan launcher.NodeRunInstanceStatus, 10),
		runRequest:    make(map[string]runRequest),
	}

	return nodeManager
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
			InstanceIdent:  instance.InstanceIdent,
			ServiceVersion: "1.0",
			Status:         cloudprotocol.InstanceStateActive, NodeID: nodeID,
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

func (nodeManager *testNodeManager) GetAverageMonitoring(nodeID string) (aostypes.NodeMonitoring, error) {
	return aostypes.NodeMonitoring{}, nil
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
		nodeConfigs: make(map[string]cloudprotocol.NodeConfig),
	}

	return resourceManager
}

func (resourceManager *testResourceManager) GetNodeConfig(nodeID, nodeType string) (cloudprotocol.NodeConfig, error) {
	resource := resourceManager.nodeConfigs[nodeType]
	resource.NodeType = nodeType

	return resource, nil
}

// testStorage

func newTestStorage(instances []launcher.InstanceInfo) *testStorage {
	storage := &testStorage{
		instanceInfo: make(map[aostypes.InstanceIdent]*launcher.InstanceInfo),
	}

	for _, instance := range instances {
		storage.instanceInfo[instance.InstanceIdent] = &launcher.InstanceInfo{}
		*storage.instanceInfo[instance.InstanceIdent] = instance
	}

	return storage
}

func (storage *testStorage) AddInstance(instanceInfo launcher.InstanceInfo) error {
	if _, ok := storage.instanceInfo[instanceInfo.InstanceIdent]; ok {
		return aoserrors.New("instance already exist")
	}

	storage.instanceInfo[instanceInfo.InstanceIdent] = &instanceInfo

	return nil
}

func (storage *testStorage) UpdateInstance(instanceInfo launcher.InstanceInfo) error {
	if _, ok := storage.instanceInfo[instanceInfo.InstanceIdent]; !ok {
		return launcher.ErrNotExist
	}

	storage.instanceInfo[instanceInfo.InstanceIdent] = &instanceInfo

	return nil
}

func (storage *testStorage) GetInstance(instanceIdent aostypes.InstanceIdent) (launcher.InstanceInfo, error) {
	instanceInfo, ok := storage.instanceInfo[instanceIdent]
	if !ok {
		return launcher.InstanceInfo{}, launcher.ErrNotExist
	}

	return *instanceInfo, nil
}

func (storage *testStorage) GetInstances() ([]launcher.InstanceInfo, error) {
	instances := make([]launcher.InstanceInfo, 0, len(storage.instanceInfo))

	for _, instanceInfo := range storage.instanceInfo {
		instances = append(instances, *instanceInfo)
	}

	return instances, nil
}

func (storage *testStorage) RemoveInstance(instanceIdent aostypes.InstanceIdent) error {
	if _, ok := storage.instanceInfo[instanceIdent]; !ok {
		return launcher.ErrNotExist
	}

	delete(storage.instanceInfo, instanceIdent)

	return nil
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

func (provider *testStateStorage) RemoveServiceInstance(instanceIdent aostypes.InstanceIdent) error {
	provider.removedInstances = append(provider.removedInstances, instanceIdent)

	return nil
}

// testImageProvider

func newTestImageProvider() *testImageProvider {
	return &testImageProvider{
		services:                      make(map[string]imagemanager.ServiceInfo),
		layers:                        make(map[string]imagemanager.LayerInfo),
		removeServiceInstancesChannel: make(chan string, 1),
	}
}

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

func (testProvider *testImageProvider) GetRemoveServiceChannel() (channel <-chan string) {
	return testProvider.removeServiceInstancesChannel
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
 * Balancing test items
 **********************************************************************************************************************/

func testItemNodePriority() testData {
	return testData{
		testCaseName: "node priority",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM:  {NodeType: nodeTypeLocalSM, Priority: 100},
			nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50},
			nodeTypeRunxSM:   {NodeType: nodeTypeRunxSM, Priority: 0},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {Runners: []string{runnerRunc}},
			service2: {Runners: []string{runnerRunc}},
			service3: {Runners: []string{runnerRunx}},
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
		expectedRunStatus: []cloudprotocol.InstanceStatus{
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
	}
}

func testItemLabels() testData {
	return testData{
		testCaseName: "labels",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM:  {NodeType: nodeTypeLocalSM, Priority: 100, Labels: []string{"label1"}},
			nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50, Labels: []string{"label2"}},
			nodeTypeRunxSM:   {NodeType: nodeTypeRunxSM, Priority: 0},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {Runners: []string{runnerRunc}},
			service2: {Runners: []string{runnerRunc}},
			service3: {Runners: []string{runnerRunx}},
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
		expectedRunStatus: []cloudprotocol.InstanceStatus{
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
			}, "", errors.New("no nodes with labels [label1]")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service3, SubjectID: subject1, Instance: 1,
			}, "", errors.New("no nodes with labels [label1]")), //nolint:goerr113
		},
	}
}

func testItemResources() testData {
	return testData{
		testCaseName: "resources",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100, Resources: []cloudprotocol.ResourceInfo{
				{Name: "resource1"},
				{Name: "resource3"},
			}},
			nodeTypeRemoteSM: {
				NodeType: nodeTypeRemoteSM, Priority: 50, Labels: []string{"label2"},
				Resources: []cloudprotocol.ResourceInfo{
					{Name: "resource1"},
					{Name: "resource2"},
				},
			},
			nodeTypeRunxSM: {NodeType: nodeTypeRunxSM, Priority: 0},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {Runners: []string{runnerRunc}, Resources: []string{"resource1", "resource2"}},
			service2: {Runners: []string{runnerRunc}, Resources: []string{"resource1"}},
			service3: {Runners: []string{runnerRunc}, Resources: []string{"resource3"}},
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
		expectedRunStatus: []cloudprotocol.InstanceStatus{
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
			}, "", errors.New("no nodes with resources [resource3]")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service3, SubjectID: subject1, Instance: 1,
			}, "", errors.New("no nodes with resources [resource3]")), //nolint:goerr113
		},
	}
}

func testItemDevices() testData {
	return testData{
		testCaseName: "devices",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100, Devices: []cloudprotocol.DeviceInfo{
				{Name: "dev1", SharedCount: 1},
				{Name: "dev2", SharedCount: 2},
				{Name: "dev3"},
			}},
			nodeTypeRemoteSM: {NodeType: nodeTypeRemoteSM, Priority: 50, Devices: []cloudprotocol.DeviceInfo{
				{Name: "dev1", SharedCount: 1},
				{Name: "dev2", SharedCount: 3},
			}, Labels: []string{"label2"}},
			nodeTypeRunxSM: {NodeType: nodeTypeRunxSM, Priority: 0, Devices: []cloudprotocol.DeviceInfo{
				{Name: "dev1", SharedCount: 1},
				{Name: "dev2", SharedCount: 2},
			}},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {
				Runners: []string{runnerRunc},
				Devices: []aostypes.ServiceDevice{{Name: "dev1"}, {Name: "dev2"}},
			},
			service2: {
				Runners: []string{runnerRunc},
				Devices: []aostypes.ServiceDevice{{Name: "dev2"}},
			},
			service3: {
				Runners: []string{runnerRunc},
				Devices: []aostypes.ServiceDevice{{Name: "dev3"}},
			},
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
		expectedRunStatus: []cloudprotocol.InstanceStatus{
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
			}, "", errors.New("no nodes with devices")), //nolint:goerr113
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
			}, "", errors.New("no nodes with devices")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service3, SubjectID: subject1, Instance: 1,
			}, "", errors.New("no nodes with devices")), //nolint:goerr113
		},
	}
}

func testItemStorageRatio() testData {
	return testData{
		testCaseName: "storage ratio",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {
				Quotas: aostypes.ServiceQuotas{
					StorageLimit: newQuota(500),
				},
				Runners: []string{runnerRunc},
			},
		},
		desiredInstances: []cloudprotocol.InstanceInfo{
			{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 5},
		},
		expectedRunRequests: map[string]runRequest{
			nodeIDLocalSM: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1LocalURL),
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
						ServiceID: service1, SubjectID: subject1, Instance: 2,
					}, 100),
					createInstanceInfo(5003, 5, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 3,
					}, 100),
				},
			},
		},
		expectedRunStatus: []cloudprotocol.InstanceStatus{
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 0,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 1,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 2,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 3,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 4,
			}, "", errors.New("not enough storage space")), //nolint:goerr113
		},
	}
}

func testItemStateRatio() testData {
	return testData{
		testCaseName: "state ratio",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {
				Quotas: aostypes.ServiceQuotas{
					StateLimit: newQuota(500),
				},
				Runners: []string{runnerRunc},
			},
		},
		desiredInstances: []cloudprotocol.InstanceInfo{
			{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 5},
		},
		expectedRunRequests: map[string]runRequest{
			nodeIDLocalSM: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1LocalURL),
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
						ServiceID: service1, SubjectID: subject1, Instance: 2,
					}, 100),
					createInstanceInfo(5003, 5, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 3,
					}, 100),
				},
			},
		},
		expectedRunStatus: []cloudprotocol.InstanceStatus{
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 0,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 1,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 2,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 3,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 4,
			}, "", errors.New("not enough state space")), //nolint:goerr113
		},
	}
}

func testItemCPURatio() testData {
	return testData{
		testCaseName: "CPU ratio",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {
				Quotas: aostypes.ServiceQuotas{
					CPULimit: newQuota(1000),
				},
				Runners: []string{runnerRunc},
			},
		},
		desiredInstances: []cloudprotocol.InstanceInfo{
			{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 8},
		},
		expectedRunRequests: map[string]runRequest{
			nodeIDLocalSM: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1LocalURL),
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
				},
			},
			nodeIDRemoteSM1: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1RemoteURL),
				},
				layers: []aostypes.LayerInfo{
					createLayerInfo(layer1, layer1RemoteURL),
					createLayerInfo(layer2, layer2RemoteURL),
				},
				instances: []aostypes.InstanceInfo{
					createInstanceInfo(5002, 4, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 2,
					}, 100),
					createInstanceInfo(5004, 5, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 4,
					}, 100),
				},
			},
			nodeIDRemoteSM2: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1RemoteURL),
				},
				layers: []aostypes.LayerInfo{
					createLayerInfo(layer1, layer1RemoteURL),
					createLayerInfo(layer2, layer2RemoteURL),
				},
				instances: []aostypes.InstanceInfo{
					createInstanceInfo(5003, 6, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 3,
					}, 100),
					createInstanceInfo(5005, 7, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 5,
					}, 100),
				},
			},
		},
		expectedRunStatus: []cloudprotocol.InstanceStatus{
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 0,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 1,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 2,
			}, nodeIDRemoteSM1, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 3,
			}, nodeIDRemoteSM2, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 4,
			}, nodeIDRemoteSM1, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 5,
			}, nodeIDRemoteSM2, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 6,
			}, "", errors.New("no nodes with available CPU")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 7,
			}, "", errors.New("no nodes with available CPU")), //nolint:goerr113
		},
	}
}

func testItemRAMRatio() testData {
	return testData{
		testCaseName: "RAM ratio",
		nodeConfigs: map[string]cloudprotocol.NodeConfig{
			nodeTypeLocalSM: {NodeType: nodeTypeLocalSM, Priority: 100},
		},
		serviceConfigs: map[string]aostypes.ServiceConfig{
			service1: {
				Quotas: aostypes.ServiceQuotas{
					RAMLimit: newQuota(1024),
				},
				Runners: []string{runnerRunc},
			},
		},
		desiredInstances: []cloudprotocol.InstanceInfo{
			{ServiceID: service1, SubjectID: subject1, Priority: 100, NumInstances: 8},
		},
		expectedRunRequests: map[string]runRequest{
			nodeIDLocalSM: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1LocalURL),
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
				},
			},
			nodeIDRemoteSM1: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1RemoteURL),
				},
				layers: []aostypes.LayerInfo{
					createLayerInfo(layer1, layer1RemoteURL),
					createLayerInfo(layer2, layer2RemoteURL),
				},
				instances: []aostypes.InstanceInfo{
					createInstanceInfo(5002, 4, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 2,
					}, 100),
					createInstanceInfo(5003, 5, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 3,
					}, 100),
				},
			},
			nodeIDRemoteSM2: {
				services: []aostypes.ServiceInfo{
					createServiceInfo(service1, 5000, service1RemoteURL),
				},
				layers: []aostypes.LayerInfo{
					createLayerInfo(layer1, layer1RemoteURL),
					createLayerInfo(layer2, layer2RemoteURL),
				},
				instances: []aostypes.InstanceInfo{
					createInstanceInfo(5004, 6, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 4,
					}, 100),
					createInstanceInfo(5005, 7, aostypes.InstanceIdent{
						ServiceID: service1, SubjectID: subject1, Instance: 5,
					}, 100),
				},
			},
		},
		expectedRunStatus: []cloudprotocol.InstanceStatus{
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 0,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 1,
			}, nodeIDLocalSM, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 2,
			}, nodeIDRemoteSM1, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 3,
			}, nodeIDRemoteSM1, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 4,
			}, nodeIDRemoteSM2, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 5,
			}, nodeIDRemoteSM2, nil),
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 6,
			}, "", errors.New("no nodes with available RAM")), //nolint:goerr113
			createInstanceStatus(aostypes.InstanceIdent{
				ServiceID: service1, SubjectID: subject1, Instance: 7,
			}, "", errors.New("no nodes with available RAM")), //nolint:goerr113
		},
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createServiceInfo(id string, gid uint32, url string) aostypes.ServiceInfo {
	return aostypes.ServiceInfo{
		ServiceID: id,
		Version:   "1.0",
		URL:       url,
		GID:       gid,
	}
}

func createLayerInfo(digest string, url string) aostypes.LayerInfo {
	return aostypes.LayerInfo{
		Digest:  digest,
		Version: "1.0",
		URL:     url,
	}
}

func createInstanceStatus(ident aostypes.InstanceIdent, nodeID string, err error) cloudprotocol.InstanceStatus {
	status := cloudprotocol.InstanceStatus{
		InstanceIdent:  ident,
		Status:         cloudprotocol.InstanceStateActive,
		ServiceVersion: "1.0",
		NodeID:         nodeID,
	}

	if err != nil {
		status.Status = cloudprotocol.InstanceStateFailed
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

func waitRunInstancesStatus(runStatusChannel <-chan []cloudprotocol.InstanceStatus,
	expectedStatus []cloudprotocol.InstanceStatus, timeout time.Duration,
) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case receivedStatus := <-runStatusChannel:
		if len(receivedStatus) != len(expectedStatus) {
			return aoserrors.New("incorrect length")
		}

	topLoop:
		for _, receivedItem := range receivedStatus {
			for _, expectedItem := range expectedStatus {
				if receivedItem.ErrorInfo == nil && expectedItem.ErrorInfo != nil {
					continue
				}

				if receivedItem.ErrorInfo != nil && expectedItem.ErrorInfo == nil {
					continue
				}

				if receivedItem.ErrorInfo != nil && expectedItem.ErrorInfo != nil {
					if receivedItem.ErrorInfo.AosCode != expectedItem.ErrorInfo.AosCode ||
						receivedItem.ErrorInfo.ExitCode != expectedItem.ErrorInfo.ExitCode ||
						!strings.Contains(receivedItem.ErrorInfo.Message, expectedItem.ErrorInfo.Message) {
						continue
					}
				}

				receivedForCheck := receivedItem

				receivedForCheck.ErrorInfo = nil
				expectedItem.ErrorInfo = nil

				if reflect.DeepEqual(receivedForCheck, expectedItem) {
					continue topLoop
				}
			}

			return aoserrors.New("incorrect instances in run status")
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

func newQuota(value uint64) *uint64 {
	return &value
}
