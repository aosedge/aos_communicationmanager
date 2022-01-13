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

package boardconfig_test

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/aoscloud/aos_communicationmanager/boardconfig"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"

	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const validTestBoardConfig = `
 {
	 "formatVersion": 1,
	 "vendorVersion": "1.0.0"
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

	if tmpDir, err = ioutil.TempDir("", "cm_"); err != nil {
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
	if err := ioutil.WriteFile(path.Join(tmpDir, "aos_board.cfg"), []byte(validTestBoardConfig), 0o600); err != nil {
		t.Fatalf("Can't create board config file: %s", err)
	}

	boardConfig, err := boardconfig.New(&config.Config{BoardConfigFile: path.Join(tmpDir, "aos_board.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create board config instance: %s", err)
	}

	info, err := boardConfig.GetStatus()
	if err != nil {
		t.Fatalf("Can't get board config status: %s", err)
	}

	if info.Status != cloudprotocol.InstalledStatus {
		t.Errorf("Wrong board config status: %s", info.Status)
	}

	if info.VendorVersion != "1.0.0" {
		t.Errorf("Wrong board config version: %s", info.VendorVersion)
	}
}

func TestInvalidGetStatus(t *testing.T) {
	testBoardConfig := `
{
	"formatVersion": 1,
	"vendorVersion": "1.0.0",
	something not valid
}`

	if err := ioutil.WriteFile(path.Join(tmpDir, "aos_board.cfg"), []byte(testBoardConfig), 0o600); err != nil {
		t.Fatalf("Can't create board config file: %s", err)
	}

	boardConfig, err := boardconfig.New(&config.Config{BoardConfigFile: path.Join(tmpDir, "aos_board.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create board config instance: %s", err)
	}

	info, err := boardConfig.GetStatus()
	if err != nil {
		t.Fatalf("Can't get board config status: %s", err)
	}

	if info.Status != cloudprotocol.ErrorStatus {
		t.Errorf("Wrong board config status: %s", info.Status)
	}
}

func TestCheckBoardConfig(t *testing.T) {
	if err := ioutil.WriteFile(path.Join(tmpDir, "aos_board.cfg"), []byte(validTestBoardConfig), 0o600); err != nil {
		t.Fatalf("Can't create board config file: %s", err)
	}

	boardConfig, err := boardconfig.New(&config.Config{BoardConfigFile: path.Join(tmpDir, "aos_board.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create board config instance: %s", err)
	}

	validBoardConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "2.0.0"
	}`

	vendorVersion, err := boardConfig.CheckBoardConfig(json.RawMessage(validBoardConfig))
	if err != nil {
		t.Errorf("Check board config error: %s", err)
	}

	if vendorVersion != "2.0.0" {
		t.Errorf("Wrong board config version: %s", vendorVersion)
	}

	invalidBoardConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "1.0.0"
	}`

	if vendorVersion, err = boardConfig.CheckBoardConfig(json.RawMessage(invalidBoardConfig)); err == nil {
		t.Error("Error expected")
	}

	if vendorVersion != "1.0.0" {
		t.Errorf("Wrong board config version: %s", vendorVersion)
	}
}

func TestUpdateBoardConfig(t *testing.T) {
	if err := ioutil.WriteFile(path.Join(tmpDir, "aos_board.cfg"), []byte(validTestBoardConfig), 0o600); err != nil {
		t.Fatalf("Can't create board config file: %s", err)
	}

	boardConfig, err := boardconfig.New(&config.Config{BoardConfigFile: path.Join(tmpDir, "aos_board.cfg")}, &testClient{})
	if err != nil {
		t.Fatalf("Can't create board config instance: %s", err)
	}

	newBoardConfig := `
	{
		"formatVersion": 1,
		"vendorVersion": "2.0.0"
	}`

	if err = boardConfig.UpdateBoardConfig(json.RawMessage(newBoardConfig)); err != nil {
		t.Fatalf("Can't update board config: %s", err)
	}

	readBoardConfig, err := ioutil.ReadFile(path.Join(tmpDir, "aos_board.cfg"))
	if err != nil {
		t.Fatalf("Can't read board config file: %s", err)
	}

	if string(readBoardConfig) != newBoardConfig {
		t.Errorf("Read wrong board config: %s", readBoardConfig)
	}
}

/***********************************************************************************************************************
 * testClient
 **********************************************************************************************************************/

func (client *testClient) CheckBoardConfig(boardConfig boardconfig.BoardConfig) (err error) {
	return nil
}

func (client *testClient) SetBoardConfig(boardConfig boardconfig.BoardConfig) (err error) {
	return nil
}
