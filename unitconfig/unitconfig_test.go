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

package unitconfig_test

import (
	"encoding/json"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/unitconfig"

	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const validTestUnitConfig = `
 {
	 "formatVersion": "1",
	 "version": "1.0.0",
	 "nodes": [
		{
			"nodeType" : "type1"
		}
	 ]
 }`

const node0TestUnitConfig = `
 {
	 "formatVersion": "1",
	 "version": "1.0.0",
	 "nodes": [
		{
			"nodeId" : "node0",
			"nodeType" : "type1"
		}
	 ]
 }`

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testNodeConfig struct {
	NodeID   string
	NodeType string
	Version  string
}

type testClient struct {
	nodeConfigStatuses        []unitconfig.NodeConfigStatus
	nodeConfigStatusChannel   chan unitconfig.NodeConfigStatus
	nodeConfigSetCheckChannel chan testNodeConfig
}

type testNodeInfoProvider struct {
	nodeID   string
	nodeType string
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
	var err error

	if tmpDir, err = os.MkdirTemp("", "cm_"); err != nil {
		log.Fatalf("Can't create tmp dir: %s", err)
	}

	ret := m.Run()

	if err = os.RemoveAll(tmpDir); err != nil {
		log.Fatalf("Can't remove tmp dir: %s", err)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestValidGetStatus(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(validTestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %s", err)
	}

	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	unitConfig, err := unitconfig.New(
		&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, newTestClient())
	if err != nil {
		t.Fatalf("Can't create unit config instance: %s", err)
	}

	info, err := unitConfig.GetStatus()
	if err != nil {
		t.Fatalf("Can't get unit config status: %s", err)
	}

	if info.Status != cloudprotocol.InstalledStatus {
		t.Errorf("Wrong unit config status: %s", info.Status)
	}

	if info.Version != "1.0.0" {
		t.Errorf("Wrong unit config version: %s", info.Version)
	}

	nodeUnitConfig, err := unitConfig.GetNodeConfig("id1", "type1")
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if nodeUnitConfig.NodeType != "type1" {
		t.Error("Unexpected node type")
	}
}

func TestInvalidGetStatus(t *testing.T) {
	testUnitConfig := `
{
	"formatVersion": 1,
	"vendorVersion": "1.0.0",
	something not valid
}`

	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(testUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %s", err)
	}

	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	unitConfig, err := unitconfig.New(
		&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, newTestClient())
	if err != nil {
		t.Fatalf("Can't create unit config instance: %s", err)
	}

	info, err := unitConfig.GetStatus()
	if err != nil {
		t.Fatalf("Can't get unit config status: %s", err)
	}

	if info.Status != cloudprotocol.ErrorStatus {
		t.Errorf("Wrong unit config status: %s", info.Status)
	}
}

func TestCheckUnitConfig(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(validTestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %s", err)
	}

	client := newTestClient()
	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	unitConfig, err := unitconfig.New(
		&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, client)
	if err != nil {
		t.Fatalf("Can't create unit config instance: %s", err)
	}

	validUnitConfig := cloudprotocol.UnitConfig{
		FormatVersion: "1",
		Version:       "2.0.0",
		Nodes:         []cloudprotocol.NodeConfig{{NodeType: "type1"}},
	}

	client.nodeConfigStatuses = []unitconfig.NodeConfigStatus{
		{NodeID: "id1", NodeType: "type1", Version: "1.0.0"},
		{NodeID: "id2", NodeType: "type1", Version: "1.0.0"},
		{NodeID: "id3", NodeType: "type1", Version: "1.0.0"},
	}

	if err := unitConfig.CheckUnitConfig(validUnitConfig); err != nil {
		t.Errorf("Check unit config error: %v", err)
	}

	for i := 0; i < len(client.nodeConfigStatuses); i++ {
		select {
		case nodeConfig := <-client.nodeConfigSetCheckChannel:
			if !reflect.DeepEqual(nodeConfig, testNodeConfig{
				NodeID:   client.nodeConfigStatuses[i].NodeID,
				NodeType: "type1",
				Version:  "2.0.0",
			}) {
				t.Errorf("Wrong node config: %v", nodeConfig)
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("Set node config timeout")
		}
	}

	invalidUnitConfig := cloudprotocol.UnitConfig{
		FormatVersion: "1",
		Version:       "1.0.0",
	}

	if err := unitConfig.CheckUnitConfig(invalidUnitConfig); err == nil {
		t.Error("Error expected")
	}
}

func TestUpdateUnitConfig(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(validTestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %v", err)
	}

	client := newTestClient()
	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	unitConfig, err := unitconfig.New(
		&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, client)
	if err != nil {
		t.Fatalf("Can't create unit config instance: %v", err)
	}

	newUnitConfig := cloudprotocol.UnitConfig{
		FormatVersion: "1",
		Version:       "2.0.0",
		Nodes:         []cloudprotocol.NodeConfig{{NodeType: "type1"}},
	}

	client.nodeConfigStatuses = []unitconfig.NodeConfigStatus{
		{NodeID: "id1", NodeType: "type1", Version: "1.0.0"},
		{NodeID: "id2", NodeType: "type1", Version: "1.0.0"},
		{NodeID: "id3", NodeType: "type1", Version: "1.0.0"},
	}

	if err = unitConfig.UpdateUnitConfig(newUnitConfig); err != nil {
		t.Fatalf("Can't update unit config: %v", err)
	}

	for i := 0; i < len(client.nodeConfigStatuses); i++ {
		select {
		case nodeConfig := <-client.nodeConfigSetCheckChannel:
			if !reflect.DeepEqual(nodeConfig, testNodeConfig{
				NodeID:   client.nodeConfigStatuses[i].NodeID,
				NodeType: "type1",
				Version:  "2.0.0",
			}) {
				t.Errorf("Wrong node config: %v", nodeConfig)
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("Set node config timeout")
		}
	}

	status, err := unitConfig.GetStatus()
	if err != nil {
		t.Errorf("Get unit config status error: %v", err)
	}

	if status.Version != "2.0.0" {
		t.Errorf("Wrong unit config version: %s", status.Version)
	}

	readUnitConfig, err := os.ReadFile(path.Join(tmpDir, "aos_unit.cfg"))
	if err != nil {
		t.Fatalf("Can't read unit config file: %s", err)
	}

	newUnitConfigJSON, err := json.Marshal(newUnitConfig)
	if err != nil {
		t.Fatalf("Can't marshal new unit config: %s", err)
	}

	if string(readUnitConfig) != string(newUnitConfigJSON) {
		t.Errorf("Read wrong unit config: %s", readUnitConfig)
	}
}

func TestNodeConfigStatus(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(validTestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %v", err)
	}

	client := newTestClient()
	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	_, err := unitconfig.New(&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, client)
	if err != nil {
		t.Fatalf("Can't create unit config instance: %v", err)
	}

	client.nodeConfigStatusChannel <- unitconfig.NodeConfigStatus{
		NodeID:   "id1",
		NodeType: "type1",
	}

	select {
	case nodeConfig := <-client.nodeConfigSetCheckChannel:
		if !reflect.DeepEqual(nodeConfig, testNodeConfig{NodeID: "id1", NodeType: "type1", Version: "1.0.0"}) {
			t.Errorf("Wrong node config: %v", nodeConfig)
		}

	case <-time.After(1 * time.Second):
		t.Fatalf("Set node config timeout")
	}
}

func TestCurrentNodeConfigUpdate(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(node0TestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %v", err)
	}

	client := newTestClient()
	nodeInfoProvider := newTestInfoProvider("node0", "type1")

	instance, err := unitconfig.New(
		&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, nodeInfoProvider, client)
	if err != nil {
		t.Fatalf("Can't create unit config instance: %v", err)
	}

	client.nodeConfigStatusChannel <- unitconfig.NodeConfigStatus{
		NodeID:   "node0",
		NodeType: "type1",
	}

	node0 := "node0"

	for i := 0; i < 2; i++ {
		select {
		case nodeConfig := <-client.nodeConfigSetCheckChannel:
			if !reflect.DeepEqual(nodeConfig, testNodeConfig{NodeID: "node0", NodeType: "type1", Version: "1.0.0"}) {
				t.Errorf("Wrong node config: %v", nodeConfig)
			}

		case curNodeConfig := <-instance.CurrentNodeConfigChannel():
			if !reflect.DeepEqual(curNodeConfig, cloudprotocol.NodeConfig{NodeID: &node0, NodeType: "type1"}) {
				t.Errorf("Wrong node config: %v", curNodeConfig)
			}

		case <-time.After(1 * time.Second):
			t.Fatalf("Set node config timeout")
		}
	}
}

/***********************************************************************************************************************
 * testClient
 **********************************************************************************************************************/

func newTestClient() *testClient {
	return &testClient{
		nodeConfigStatusChannel:   make(chan unitconfig.NodeConfigStatus, 1),
		nodeConfigSetCheckChannel: make(chan testNodeConfig, 10),
	}
}

func (client *testClient) CheckNodeConfig(version string, nodeConfig cloudprotocol.NodeConfig) error {
	client.nodeConfigSetCheckChannel <- testNodeConfig{
		NodeID:   *nodeConfig.NodeID,
		NodeType: nodeConfig.NodeType,
		Version:  version,
	}

	return nil
}

func (client *testClient) SetNodeConfig(version string, nodeConfig cloudprotocol.NodeConfig) error {
	client.nodeConfigSetCheckChannel <- testNodeConfig{
		NodeID:   *nodeConfig.NodeID,
		NodeType: nodeConfig.NodeType,
		Version:  version,
	}

	return nil
}

func (client *testClient) GetNodeConfigStatuses() ([]unitconfig.NodeConfigStatus, error) {
	return client.nodeConfigStatuses, nil
}

func (client *testClient) NodeConfigStatusChannel() <-chan unitconfig.NodeConfigStatus {
	return client.nodeConfigStatusChannel
}

/***********************************************************************************************************************
 * testNodeInfoProvider
 **********************************************************************************************************************/

func newTestInfoProvider(nodeID, nodeType string) *testNodeInfoProvider {
	return &testNodeInfoProvider{nodeID: nodeID, nodeType: nodeType}
}

func (provider *testNodeInfoProvider) GetCurrentNodeInfo() (cloudprotocol.NodeInfo, error) {
	return cloudprotocol.NodeInfo{NodeID: provider.nodeID, NodeType: provider.nodeType}, nil
}
