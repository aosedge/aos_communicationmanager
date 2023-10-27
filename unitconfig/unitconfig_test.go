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
	"testing"

	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/unitconfig"

	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const validTestUnitConfig = `
 {
	 "formatVersion": 1,
	 "vendorVersion": "1.0.0",
	 "nodes": [
		{
			"nodeType" : "type1"
		}
	 ]
 }`

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testClient struct{}

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

	unitConfig, err := unitconfig.New(&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, &testClient{})
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

	if info.VendorVersion != "1.0.0" {
		t.Errorf("Wrong unit config version: %s", info.VendorVersion)
	}

	nodeUnitConfig := unitConfig.GetUnitConfiguration("type1")

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

	unitConfig, err := unitconfig.New(&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, &testClient{})
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

	unitConfig, err := unitconfig.New(&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create unit config instance: %s", err)
	}

	validUnitConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "2.0.0"
	}`

	vendorVersion, err := unitConfig.CheckUnitConfig(json.RawMessage(validUnitConfig))
	if err != nil {
		t.Errorf("Check unit config error: %s", err)
	}

	if vendorVersion != "2.0.0" {
		t.Errorf("Wrong unit config version: %s", vendorVersion)
	}

	invalidUnitConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "1.0.0"
	}`

	if vendorVersion, err = unitConfig.CheckUnitConfig(json.RawMessage(invalidUnitConfig)); err == nil {
		t.Error("Error expected")
	}

	if vendorVersion != "1.0.0" {
		t.Errorf("Wrong unit config version: %s", vendorVersion)
	}
}

func TestUpdateUnitConfig(t *testing.T) {
	if err := os.WriteFile(path.Join(tmpDir, "aos_unit.cfg"), []byte(validTestUnitConfig), 0o600); err != nil {
		t.Fatalf("Can't create unit config file: %s", err)
	}

	unitConfig, err := unitconfig.New(&config.Config{UnitConfigFile: path.Join(tmpDir, "aos_unit.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create unit config instance: %s", err)
	}

	newUnitConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "2.0.0"
	}`

	if err = unitConfig.UpdateUnitConfig(json.RawMessage(newUnitConfig)); err != nil {
		t.Fatalf("Can't update unit config: %s", err)
	}

	vendorVersion, err := unitConfig.GetUnitConfigVersion(json.RawMessage(newUnitConfig))
	if err != nil {
		t.Errorf("Get unit config version error: %s", err)
	}

	if vendorVersion != "2.0.0" {
		t.Errorf("Wrong unit config version: %s", vendorVersion)
	}

	readUnitConfig, err := os.ReadFile(path.Join(tmpDir, "aos_unit.cfg"))
	if err != nil {
		t.Fatalf("Can't read unit config file: %s", err)
	}

	if string(readUnitConfig) != newUnitConfig {
		t.Errorf("Read wrong unit config: %s", readUnitConfig)
	}
}

/***********************************************************************************************************************
 * testClient
 **********************************************************************************************************************/

func (client *testClient) CheckUnitConfig(unitConfig aostypes.UnitConfig) (err error) {
	return nil
}

func (client *testClient) SetUnitConfig(unitConfig aostypes.UnitConfig) (err error) {
	return nil
}
