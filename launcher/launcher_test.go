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
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const magicSum = "magicSum"

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
	nodeInformation map[string]launcher.NodeInfo
	runRequest      map[string]runRequest
}

type testImageProvider struct {
	services         []imagemanager.ServiceInfo
	layers           []imagemanager.LayerInfo
	revertedServices []string
}

type testResourceManager struct {
	nodeResources map[string]aostypes.NodeUnitConfig
}

type instanceUID struct {
	aostypes.InstanceIdent
	uid int
}
type testStorage struct {
	uids             []instanceUID
	desiredInstances json.RawMessage
}

type testStateStorage struct {
	cleanedInstances []aostypes.InstanceIdent
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
		nodeManager          = createTestNodeManager()
		expectedRunStatus    = unitstatushandler.RunInstancesStatus{}
		expectedNodeInfo     = []cloudprotocol.NodeInfo{}
		stateStorageProvider = &testStateStorage{}
		testStorage          = &testStorage{}
	)

	launcherInstance, err := launcher.New(
		cfg, testStorage, nodeManager, nil, &testResourceManager{}, stateStorageProvider)
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for _, id := range cfg.SMController.NodeIDs {
		instances := []cloudprotocol.InstanceStatus{{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "s1", SubjectID: "subj1", Instance: 1},
			AosVersion:    1, StateChecksum: magicSum, RunState: "running",
			NodeID: id,
		}}

		nodeInfo := cloudprotocol.NodeInfo{
			NodeID: id, NodeType: id + "Type", SystemInfo: cloudprotocol.SystemInfo{
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
		log.Error("Incorrect nodes info")
	}
}

func TestBalancing(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{"localSM", "remoteSM", "runxSM"},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager          = createTestNodeManager()
		resourceManager      = createTestResourceManager()
		imageManager         = &testImageProvider{}
		stateStorageProvider = &testStateStorage{}
	)

	nodeManager.nodeInformation["localSM"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "localSM", NodeType: "localSMType"},
		RemoteNode: false, RunnerFeature: []string{"runc"},
	}
	resourceManager.nodeResources["localSMType"] = aostypes.NodeUnitConfig{Priority: 100}

	nodeManager.nodeInformation["remoteSM"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM", NodeType: "remoteSMType"},
		RemoteNode: true, RunnerFeature: []string{"runc"},
	}
	resourceManager.nodeResources["remoteSMType"] = aostypes.NodeUnitConfig{Priority: 50}

	nodeManager.nodeInformation["runxSM"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "runxSM", NodeType: "runxSMType"},
		RemoteNode: true, RunnerFeature: []string{"runx"},
	}
	resourceManager.nodeResources["runxSMType"] = aostypes.NodeUnitConfig{Priority: 100}

	launcherInstance, err := launcher.New(
		cfg, &testStorage{}, nodeManager, imageManager, resourceManager, stateStorageProvider)
	if err != nil {
		t.Fatalf("Can't create launcher %v", err)
	}
	defer launcherInstance.Close()

	for nodeID := range nodeManager.nodeInformation {
		nodeManager.runStatusChan <- launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: nodeID + "Type", Instances: []cloudprotocol.InstanceStatus{},
		}
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), unitstatushandler.RunInstancesStatus{}, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	desiredInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: "serv1", SubjectID: "subj1", Priority: 100, NumInstances: 2},
		{ServiceID: "serv2", SubjectID: "subj1", Priority: 100, NumInstances: 1},
		{ServiceID: "runxServ1", SubjectID: "subj2", Priority: 100, NumInstances: 1},
		{ServiceID: "serviceNotExist", SubjectID: "subj1", Priority: 100, NumInstances: 1},
		{ServiceID: "serviceNoLayer", SubjectID: "subj1", Priority: 100, NumInstances: 1},
	}

	imageManager.services = []imagemanager.ServiceInfo{
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv1", URL: "serv1LocalUrl", GID: 5000,
			},
			RemoteURL: "serv1RemoteUrl", Config: aostypes.ServiceConfig{Runner: "runc"},
			Layers: []string{"digest1", "digest2"},
		},
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv2", URL: "serv2LocalUrl", GID: 5001,
			},
			RemoteURL: "serv2RemoteUrl", Config: aostypes.ServiceConfig{Runner: "runc"},
			Layers: []string{"digest1"},
		},
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "runxServ1", URL: "runxServ1LocalUrl", GID: 5002,
			},
			RemoteURL: "runxServ1RemoteUrl", Config: aostypes.ServiceConfig{Runner: "runx"},
		},
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serviceNoLayer", URL: "LocalUrl", GID: 5003,
			},
			RemoteURL: "RemoteUrl", Config: aostypes.ServiceConfig{Runner: "runx"},
			Layers: []string{"LayerNotExist"},
		},
	}

	imageManager.layers = []imagemanager.LayerInfo{
		{
			LayerInfo: aostypes.LayerInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, Digest: "digest1", URL: "digest1LocalUrl",
			},
			RemoteURL: "digest1RemoteUrl",
		},
		{
			LayerInfo: aostypes.LayerInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, Digest: "digest2", URL: "digest2LocalUrl",
			},
			RemoteURL: "digest2RemoteUrl",
		},
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{"serv1", "serviceNoLayer"}); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	expectedRevertedServices := []string{"serviceNoLayer"}

	expectedRunRequests := map[string]runRequest{
		"localSM": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv1",
					URL: "serv1LocalUrl", GID: 5000,
				}, {
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv2",
					URL: "serv2LocalUrl", GID: 5001,
				},
			},
			layers: []aostypes.LayerInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, Digest: "digest1",
					URL: "digest1LocalUrl",
				},
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, Digest: "digest2",
					URL: "digest2LocalUrl",
				},
			},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
					UID:           5000, Priority: 100, StoragePath: "", StatePath: "",
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
					UID:           5001, Priority: 100, StoragePath: "", StatePath: "",
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
					UID:           5002, Priority: 100, StoragePath: "", StatePath: "",
				},
			},
		},
		"remoteSM": {
			services:  []aostypes.ServiceInfo{},
			layers:    []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{},
		},
		"runxSM": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "runxServ1",
					URL: "runxServ1RemoteUrl", GID: 5002,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "runxServ1", SubjectID: "subj2", Instance: 0},
					UID:           5003, Priority: 100, StoragePath: "", StatePath: "",
				},
			},
		},
	}

	var expectedRunStatus unitstatushandler.RunInstancesStatus

	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
			RunState:      cloudprotocol.InstanceStateActive, AosVersion: 1,
			NodeID: "localSM", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
			RunState:      cloudprotocol.InstanceStateActive, AosVersion: 1,
			NodeID: "localSM", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
			RunState:      cloudprotocol.InstanceStateActive, AosVersion: 1,
			NodeID: "localSM", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "runxServ1", SubjectID: "subj2", Instance: 0},
			RunState:      cloudprotocol.InstanceStateActive, AosVersion: 1,
			NodeID: "runxSM", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serviceNotExist", SubjectID: "subj1", Instance: 0},
			RunState:      cloudprotocol.InstanceStateFailed,
			ErrorInfo:     &cloudprotocol.ErrorInfo{Message: "service does't exist"},
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serviceNoLayer", SubjectID: "subj1", Instance: 0},
			RunState:      cloudprotocol.InstanceStateFailed, AosVersion: 1,
			ErrorInfo: &cloudprotocol.ErrorInfo{Message: "layer does't exist"},
		},
	}

	expectedRunStatus.ErrorServices = append(expectedRunStatus.ErrorServices,
		cloudprotocol.ServiceStatus{ID: "serviceNoLayer", AosVersion: 1, Status: cloudprotocol.ErrorStatus})

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	for nodeID, runRequest := range nodeManager.runRequest {
		if err := deepSlicesCompare(expectedRunRequests[nodeID].services, runRequest.services); err != nil {
			t.Errorf("Incorrect services for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].layers, runRequest.layers); err != nil {
			t.Errorf("Incorrect layers for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].instances, runRequest.instances); err != nil {
			t.Errorf("Incorrect instances for node %s: %v", nodeID, err)
		}
	}

	if !reflect.DeepEqual(expectedRevertedServices, imageManager.revertedServices) {
		t.Errorf("Incorrect reverted services: %v", imageManager.revertedServices)
	}

	stateStorageProvider.cleanedInstances = []aostypes.InstanceIdent{}

	desiredInstances = []cloudprotocol.InstanceInfo{
		{ServiceID: "serv1", SubjectID: "subj1", Priority: 100, NumInstances: 1},
	}

	expectedRunStatus.ErrorServices = []cloudprotocol.ServiceStatus{}
	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
			RunState:      cloudprotocol.InstanceStateActive, AosVersion: 1,
			NodeID: "localSM", StateChecksum: magicSum,
		},
	}

	expectedCleanInstances := []aostypes.InstanceIdent{
		{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
		{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
		{ServiceID: "runxServ1", SubjectID: "subj2", Instance: 0},
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{}); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	if !reflect.DeepEqual(expectedCleanInstances, stateStorageProvider.cleanedInstances) {
		t.Errorf("Incorrect state storage cleanup: %v", stateStorageProvider.cleanedInstances)
	}
}

func TestBalancingByUnitConfiguration(t *testing.T) {
	var (
		cfg = &config.Config{
			SMController: config.SMController{
				NodeIDs:                []string{"localSM1", "localSM2", "remoteSM1"},
				NodesConnectionTimeout: aostypes.Duration{Duration: time.Second},
			},
		}
		nodeManager     = createTestNodeManager()
		resourceManager = createTestResourceManager()
		imageManager    = &testImageProvider{}
	)

	resourceManager.nodeResources["localSMType"] = aostypes.NodeUnitConfig{
		Priority: 100,
		NodeType: "localSM", Devices: []aostypes.DeviceInfo{
			{Name: "devSpeaker", SharedCount: 0},
			{Name: "devMic", SharedCount: 2},
			{Name: "devTest", SharedCount: 1},
		},
	}
	nodeManager.nodeInformation["localSM1"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "localSM1", NodeType: "localSMType"},
		RemoteNode: false, RunnerFeature: []string{"runc", "crun"},
	}
	nodeManager.nodeInformation["localSM2"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM2", NodeType: "localSMType"},
		RemoteNode: false, RunnerFeature: []string{"runc", "crun"},
	}

	resourceManager.nodeResources["remoteSMType"] = aostypes.NodeUnitConfig{
		Priority: 50,
		NodeType: "remoteSMType",
		Devices: []aostypes.DeviceInfo{
			{Name: "devTest", SharedCount: 1},
			{Name: "devSpeaker", SharedCount: 0},
			{Name: "devUniq", SharedCount: 0},
			{Name: "devRemote", SharedCount: 5},
		},
	}
	nodeManager.nodeInformation["remoteSM1"] = launcher.NodeInfo{
		NodeInfo:   cloudprotocol.NodeInfo{NodeID: "remoteSM1", NodeType: "remoteSMType"},
		RemoteNode: true, RunnerFeature: []string{"runc", "crun"},
	}

	launcherInstance, err := launcher.New(
		cfg, &testStorage{}, nodeManager, imageManager, resourceManager, &testStateStorage{})
	if err != nil {
		t.Fatalf("Can't create launcher: %v", err)
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
		{ServiceID: "serv1", SubjectID: "subj1", Priority: 100, NumInstances: 1},
		{ServiceID: "serv2", SubjectID: "subj1", Priority: 90, NumInstances: 3},
	}

	imageManager.services = []imagemanager.ServiceInfo{
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv1", URL: "serv1LocalUrl", GID: 5000,
			},
			RemoteURL: "serv1RemoteUrl",
			Config: aostypes.ServiceConfig{
				Runner: "runc",
				Devices: []aostypes.ServiceDevice{
					{Name: "devSpeaker"},
					{Name: "devUniq"},
					{Name: "devTest"},
				},
			},
		},
		{
			ServiceInfo: aostypes.ServiceInfo{
				VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv2", URL: "serv2LocalUrl", GID: 5001,
			},
			RemoteURL: "serv2RemoteUrl",
			Config: aostypes.ServiceConfig{
				Runner: "runc",
				Devices: []aostypes.ServiceDevice{
					{Name: "devTest"},
				},
			},
		},
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{}); err != nil {
		t.Fatalf("Can't run instances %v", err)
	}

	expectedRunRequests := map[string]runRequest{
		"localSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv2",
					URL: "serv2LocalUrl", GID: 5001,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
					UID:           5001, Priority: 90, StoragePath: "", StatePath: "",
				},
			},
		},
		"localSM2": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv2",
					URL: "serv2LocalUrl", GID: 5001,
				},
			},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 1},
					UID:           5002, Priority: 90, StoragePath: "", StatePath: "",
				},
			},
		},
		"remoteSM1": {
			services: []aostypes.ServiceInfo{
				{
					VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv1",
					URL: "serv1RemoteUrl", GID: 5000,
				},
			},
			layers: []aostypes.LayerInfo{},
			instances: []aostypes.InstanceInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
					UID:           5000, Priority: 100, StoragePath: "", StatePath: "",
				},
			},
		},
	}

	var expectedRunStatus unitstatushandler.RunInstancesStatus

	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 1},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM2", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 2},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateFailed,
			ErrorInfo:     &cloudprotocol.ErrorInfo{Message: "no devices for instance"},
		},
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status: %v", err)
	}

	for nodeID, runRequest := range nodeManager.runRequest {
		if err := deepSlicesCompare(expectedRunRequests[nodeID].services, runRequest.services); err != nil {
			t.Errorf("Incorrect services for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].layers, runRequest.layers); err != nil {
			t.Errorf("Incorrect layers for node %s: %v", nodeID, err)
		}

		if err := deepSlicesCompare(expectedRunRequests[nodeID].instances, runRequest.instances); err != nil {
			t.Errorf("Incorrect instances for node %s: %v", nodeID, err)
		}

		if expectedRunRequests[nodeID].forceRestart {
			t.Error("Incorrect force restart flag")
		}
	}

	resourceManager.nodeResources["remoteSMType"] = aostypes.NodeUnitConfig{
		Priority: 50,
		NodeType: "remoteSMType",
		Devices: []aostypes.DeviceInfo{
			{Name: "devTest", SharedCount: 2},
			{Name: "devSpeaker", SharedCount: 0},
			{Name: "devUniq", SharedCount: 0},
			{Name: "devRemote", SharedCount: 5},
		},
	}

	expectedRunStatus.Instances = []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 0},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM1", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 1},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "localSM2", StateChecksum: magicSum,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj1", Instance: 2},
			AosVersion:    1,
			RunState:      cloudprotocol.InstanceStateActive,
			NodeID:        "remoteSM1", StateChecksum: magicSum,
		},
	}

	if err := launcherInstance.RestartInstances(); err != nil {
		t.Fatalf("Can't restart instances: %v", err)
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status after update unitconfig: %v", err)
	}

	if err := launcherInstance.RunInstances(desiredInstances, []string{}); err != nil {
		t.Fatalf("Can't run instances: %v", err)
	}

	if err := waitRunInstancesStatus(
		launcherInstance.GetRunStatusesChannel(), expectedRunStatus, time.Second); err != nil {
		t.Errorf("Incorrect run status after the same desired status: %v", err)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func createTestNodeManager() *testNodeManager {
	nodeManager := &testNodeManager{
		runStatusChan:   make(chan launcher.NodeRunInstanceStatus, 10),
		nodeInformation: make(map[string]launcher.NodeInfo),
		runRequest:      make(map[string]runRequest),
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

func createTestResourceManager() *testResourceManager {
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

func (storage *testStorage) AddInstance(instance aostypes.InstanceIdent, uid int) error {
	for _, uid := range storage.uids {
		if uid.InstanceIdent == instance {
			return aoserrors.New("uid for instacne already exist")
		}
	}

	storage.uids = append(storage.uids, instanceUID{InstanceIdent: instance, uid: uid})

	return nil
}

func (storage *testStorage) GetInstanceUID(instance aostypes.InstanceIdent) (int, error) {
	for _, uid := range storage.uids {
		if uid.InstanceIdent == instance {
			return uid.uid, nil
		}
	}

	return 0, launcher.ErrNotExist
}

func (storage *testStorage) GetAllUIDs() ([]int, error) {
	return []int{}, nil
}

func (storage *testStorage) SetDesiredInstances(instances json.RawMessage) error {
	storage.desiredInstances = instances
	return nil
}

func (storage *testStorage) GetDesiredInstances() (instances json.RawMessage, err error) {
	return storage.desiredInstances, nil
}

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

func (testProvider *testImageProvider) GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error) {
	for _, service := range testProvider.services {
		if service.ID == serviceID {
			return service, nil
		}
	}

	return imagemanager.ServiceInfo{}, errors.New("service does't exist") // nolint:goerr113
}

func (testProvider *testImageProvider) GetLayerInfo(digest string) (imagemanager.LayerInfo, error) {
	for _, layer := range testProvider.layers {
		if layer.Digest == digest {
			return layer, nil
		}
	}

	return imagemanager.LayerInfo{}, errors.New("layer does't exist") // nolint:goerr113
}

func (testProvider *testImageProvider) RevertService(serviceID string) error {
	testProvider.revertedServices = append(testProvider.revertedServices, serviceID)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

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
					if !reflect.DeepEqual(*receivedEl.ErrorInfo, *expectedEl.ErrorInfo) {
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
			return aoserrors.New("incorrect subejects in run status")
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

toplabel:
	for _, elementA := range sliceA {
		for _, elementB := range sliceB {
			if reflect.DeepEqual(elementA, elementB) {
				continue toplabel
			}
		}

		return aoserrors.New("slices are not equals")
	}

	return nil
}
