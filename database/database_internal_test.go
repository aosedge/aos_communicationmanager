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

package database

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Variables
 **********************************************************************************************************************/

var (
	tmpDir string
	db     *Database
)

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

	tmpDir, err = ioutil.TempDir("", "sm_")
	if err != nil {
		log.Fatalf("Error create temporary dir: %s", err)
	}

	db, err = New(&config.Config{
		WorkingDir: tmpDir,
		Migration: config.Migration{
			MigrationPath:       tmpDir,
			MergedMigrationPath: tmpDir,
		},
	})
	if err != nil {
		log.Fatalf("Can't create database: %s", err)
	}

	ret := m.Run()

	if err = os.RemoveAll(tmpDir); err != nil {
		log.Fatalf("Error cleaning up: %s", err)
	}

	db.Close()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestCursor(t *testing.T) {
	setCursor := "cursor123"

	if err := db.SetJournalCursor(setCursor); err != nil {
		t.Fatalf("Can't set logging cursor: %s", err)
	}

	getCursor, err := db.GetJournalCursor()
	if err != nil {
		t.Fatalf("Can't get logger cursor: %s", err)
	}

	if getCursor != setCursor {
		t.Fatalf("Wrong cursor value: %s", getCursor)
	}
}

func TestComponentsUpdateInfo(t *testing.T) {
	testData := []umcontroller.SystemComponent{
		{
			ID: "component1", VendorVersion: "v1", AosVersion: 1,
			Annotations: "Some annotation", URL: "url12", Sha512: []byte{1, 3, 90, 42},
		},
		{ID: "component2", VendorVersion: "v1", AosVersion: 1, URL: "url12", Sha512: []byte{1, 3, 90, 42}},
	}

	if err := db.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err := db.GetComponentsUpdateInfo()
	if err != nil {
		t.Fatal("Can't get update manager's update info ", err)
	}

	if !reflect.DeepEqual(testData, getUpdateInfo) {
		t.Fatalf("Wrong update info value: %v", getUpdateInfo)
	}

	testData = []umcontroller.SystemComponent{}

	if err := db.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err = db.GetComponentsUpdateInfo()
	if err != nil {
		t.Fatal("Can't get update manager's update info ", err)
	}

	if len(getUpdateInfo) != 0 {
		t.Fatalf("Wrong count of update elements 0 != %d", len(getUpdateInfo))
	}
}

func TestSotaFotaState(t *testing.T) {
	fotaState := json.RawMessage("fotaState")
	sotaState := json.RawMessage("sotaState")

	if err := db.SetFirmwareUpdateState(fotaState); err != nil {
		t.Fatal("Can't set FOTA state ", err)
	}

	if err := db.SetSoftwareUpdateState(sotaState); err != nil {
		t.Fatal("Can't set SOTA state ", err)
	}

	retFota, err := db.GetFirmwareUpdateState()
	if err != nil {
		t.Fatal("Can't get FOTA state ", err)
	}

	if string(retFota) != string(fotaState) {
		t.Errorf("Incorrect FOTA state %s", string(retFota))
	}

	retSota, err := db.GetSoftwareUpdateState()
	if err != nil {
		t.Fatal("Can't get SOTA state ", err)
	}

	if string(retSota) != string(sotaState) {
		t.Errorf("Incorrect FOTA state %s", string(retSota))
	}
}

func TestMultiThread(t *testing.T) {
	const numIterations = 1000

	var wg sync.WaitGroup

	wg.Add(4)

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if err := db.SetJournalCursor(strconv.Itoa(i)); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := db.GetJournalCursor(); err != nil {
				t.Errorf("Can't get journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if err := db.SetComponentsUpdateInfo([]umcontroller.SystemComponent{{AosVersion: uint64(i)}}); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := db.GetComponentsUpdateInfo(); err != nil {
				t.Errorf("Can't get journal cursor: %s", err)
			}
		}
	}()

	wg.Wait()
}
