// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

package storagestate_test

import (
	"bytes"
	"encoding/hex"
	"os"
	"path"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/storagestate"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/sha3"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const waitChannelTimeout = 100 * time.Millisecond

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testStorageInterface struct {
	data              map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo
	errorSave         bool
	errorSaveCheckSum bool
	errorGet          bool
	errorAdd          bool
}

type testMessageSender struct {
	chanNewState     chan cloudprotocol.NewState
	chanStateRequest chan cloudprotocol.StateRequest
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	tmpDir                 string
	storageDir             string
	stateDir               string
	stateStorageQuotaLimit uint64
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
	if err := setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	cleanup()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestSetupClean(t *testing.T) {
	type testSetupCleanInfo struct {
		storagestate.SetupParams
		expectedStorageQuotaLimit uint64
		expectedStateQuotaLimit   uint64
		instancePath              string
		expectedEmptyCheckSum     bool
		expectReceiveStateChanged bool
		errorSaveStorage          bool
		errorGet                  bool
		errorAdd                  bool
		cleanState                bool
		state                     string
	}

	testsData := []testSetupCleanInfo{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			expectedStorageQuotaLimit: 3000,
			expectedStateQuotaLimit:   3000,
			expectReceiveStateChanged: true,
			state:                     "new state",
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			expectedStorageQuotaLimit: 3000,
			expectedStateQuotaLimit:   3000,
			expectReceiveStateChanged: false,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   0,
				StorageQuota: 0,
			},
			expectedStorageQuotaLimit: 0,
			expectedStateQuotaLimit:   0,
			expectReceiveStateChanged: false,
			expectedEmptyCheckSum:     true,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   1000,
				StorageQuota: 1000,
			},
			expectedStorageQuotaLimit: 2000,
			expectedStateQuotaLimit:   2000,
			expectReceiveStateChanged: true,
			state:                     "new state",
		},
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, 1),
		chanStateRequest: make(chan cloudprotocol.StateRequest, 1),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	for _, testData := range testsData {
		storagePath, statePath, err := instance.Setup(testData.SetupParams)
		if err != nil {
			t.Fatalf("Can't setup instance: %v", err)
		}

		if storagePath != "" && !isPathExist(path.Join(storageDir, storagePath)) {
			t.Error("Storage dir doesn't exist")
		}

		if statePath != "" && !isPathExist(path.Join(stateDir, statePath)) {
			t.Error("State dir doesn't exist")
		}

		if stateStorageQuotaLimit != testData.expectedStorageQuotaLimit {
			t.Error("Unexpected storage quota limit")
		}

		if stateStorageQuotaLimit != testData.expectedStateQuotaLimit {
			t.Error("Unexpected state quota limit")
		}

		select {
		case stateRequest := <-messageSender.chanStateRequest:
			if !testData.expectReceiveStateChanged {
				t.Error("Should not receive a state request")
			}

			if testData.SetupParams.InstanceIdent != stateRequest.InstanceIdent {
				t.Error("Incorrect state changed instance ID")
			}

			calcSum := sha3.Sum224([]byte(testData.state))

			if err = instance.UpdateState(cloudprotocol.UpdateState{
				InstanceIdent: testData.SetupParams.InstanceIdent,
				Checksum:      hex.EncodeToString(calcSum[:]),
				State:         testData.state,
			}); err != nil {
				t.Fatalf("Can't send update state: %v", err)
			}

			expectedCheckSum := instance.GetInstanceCheckSum(testData.SetupParams.InstanceIdent)
			if testData.expectedEmptyCheckSum && expectedCheckSum != "" {
				t.Errorf("Expected empty checksum")
			}

			if !bytes.Equal([]byte(expectedCheckSum), calcSum[:]) {
				t.Error("Incorrect checksum")
			}

		case <-time.After(waitChannelTimeout):
			if testData.expectReceiveStateChanged {
				t.Error("Expected state request to be received")
			}
		}
	}

	testsData = []testSetupCleanInfo{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
			},
			instancePath: "1_subject1_service1",
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
			},
			instancePath: "2_subject1_service1",
		},
	}

	for _, testData := range testsData {
		if err = instance.Cleanup(testData.SetupParams.InstanceIdent); err != nil {
			t.Fatalf("Can't cleanup storage state: %v", err)
		}

		if err = instance.RemoveServiceInstance(testData.SetupParams.InstanceIdent); err != nil {
			t.Fatalf("Can't remove storage state: %v", err)
		}

		if _, ok := storage.data[testData.SetupParams.InstanceIdent]; ok {
			t.Fatal("Storage state info should not be exist in storage")
		}
	}

	testsData = []testSetupCleanInfo{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   500,
				StorageQuota: 500,
			},
			expectedStorageQuotaLimit: 1000,
			expectedStateQuotaLimit:   1000,
			errorGet:                  true,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   500,
				StorageQuota: 500,
			},
			expectedStorageQuotaLimit: 1000,
			expectedStateQuotaLimit:   1000,
			errorSaveStorage:          true,
			cleanState:                true,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   500,
				StorageQuota: 500,
			},
			expectedStorageQuotaLimit: 1000,
			expectedStateQuotaLimit:   1000,
			errorAdd:                  true,
		},
	}

	for _, testData := range testsData {
		storage.errorAdd = testData.errorAdd
		storage.errorGet = testData.errorGet
		storage.errorSave = testData.errorSaveStorage

		_, _, err := instance.Setup(testData.SetupParams)
		if err == nil {
			t.Fatalf("Should not be a setup instance")
		}

		if testData.cleanState {
			if err = instance.Cleanup(testData.SetupParams.InstanceIdent); err != nil {
				t.Fatalf("Can't cleanup storage state: %v", err)
			}

			if err = instance.RemoveServiceInstance(testData.SetupParams.InstanceIdent); err != nil {
				t.Fatalf("Can't remove storage state: %v", err)
			}
		}
	}
}

func TestUpdateState(t *testing.T) {
	testsData := []struct {
		storagestate.SetupParams
		expectReceiveStateRequest bool
		stateData                 string
	}{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			expectReceiveStateRequest: true,
			stateData:                 "state1",
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			expectReceiveStateRequest: false,
			stateData:                 "state2",
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   1000,
				StorageQuota: 1000,
			},
			expectReceiveStateRequest: true,
			stateData:                 "state3",
		},
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, 1),
		chanStateRequest: make(chan cloudprotocol.StateRequest, 1),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	for _, testData := range testsData {
		_, _, err := instance.Setup(testData.SetupParams)
		if err != nil {
			t.Fatalf("Can't setup instance: %v", err)
		}

		select {
		case stateRequest := <-messageSender.chanStateRequest:
			if !testData.expectReceiveStateRequest {
				t.Error("Should not receive a state request")
			}

			if testData.InstanceIdent != stateRequest.InstanceIdent {
				t.Error("Incorrect state changed instance ID")
			}

			calcSum := sha3.Sum224([]byte(testData.stateData))

			if err = instance.UpdateState(cloudprotocol.UpdateState{
				InstanceIdent: testData.InstanceIdent,
				State:         testData.stateData,
				Checksum:      hex.EncodeToString(calcSum[:]),
			}); err != nil {
				t.Fatalf("Can't send update state: %v", err)
			}

			expectedCheckSum := instance.GetInstanceCheckSum(testData.SetupParams.InstanceIdent)
			if expectedCheckSum == "" {
				t.Error("Unexpected checksum")
			}

			if !bytes.Equal([]byte(expectedCheckSum), calcSum[:]) {
				t.Error("Incorrect checksum")
			}

			stateStorageInfo, ok := storage.data[testData.SetupParams.InstanceIdent]
			if !ok {
				t.Error("Can't found state storage info by instance ident")
			}

			checkSumFromFile, err := getStateFileChecksum(path.Join(
				stateDir, stateStorageInfo.InstanceID+"_state.dat"))
			if err != nil {
				t.Fatalf("Can't get checksum from state file: %v", err)
			}

			if !bytes.Equal(checkSumFromFile, calcSum[:]) {
				t.Error("Incorrect checksum")
			}

			select {
			case <-messageSender.chanNewState:
				t.Fatal("Request on new state should not be send")

			case <-time.After(storagestate.StateChangeTimeout * 2):
			}

		case <-time.After(waitChannelTimeout):
			if testData.expectReceiveStateRequest {
				t.Error("Expected state change to be received")
			}
		}
	}
}

func TestUpdateStateFailed(t *testing.T) {
	sumByte := sha3.Sum224([]byte("state"))

	testsData := []struct {
		storagestate.SetupParams
		updateState       cloudprotocol.UpdateState
		errorSaveCheckSum bool
	}{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			updateState: cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service2",
					SubjectID: "subject2",
					Instance:  1,
				},
			},
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   7,
				StorageQuota: 1000,
			},
			updateState: cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				State: "new state",
			},
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   7,
				StorageQuota: 1000,
			},
			updateState: cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				State:    "state",
				Checksum: "decodingError",
			},
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   7,
				StorageQuota: 1000,
			},
			updateState: cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				State:    "state",
				Checksum: hex.EncodeToString([]byte("bad checksum")),
			},
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   7,
				StorageQuota: 1000,
			},
			errorSaveCheckSum: true,
			updateState: cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				State:    "state",
				Checksum: hex.EncodeToString(sumByte[:]),
			},
		},
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, len(testsData)),
		chanStateRequest: make(chan cloudprotocol.StateRequest, len(testsData)),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	for _, testData := range testsData {
		storage.errorSaveCheckSum = testData.errorSaveCheckSum

		_, _, err := instance.Setup(testData.SetupParams)
		if err != nil {
			t.Fatalf("Can't setup instance: %v", err)
		}

		if err = instance.UpdateState(testData.updateState); err == nil {
			t.Fatal("State should not be updated")
		}
	}
}

func TestStateAcceptanceFailed(t *testing.T) {
	setupParams := storagestate.SetupParams{
		InstanceIdent: aostypes.InstanceIdent{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  1,
		},
		UID:          1003,
		GID:          1003,
		StateQuota:   2000,
		StorageQuota: 1000,
	}

	testsData := []struct {
		storagestate.SetupParams
		stateAcceptance   cloudprotocol.StateAcceptance
		errorSaveCheckSum bool
	}{
		{
			SetupParams: setupParams,
			stateAcceptance: cloudprotocol.StateAcceptance{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service2",
					SubjectID: "subject2",
					Instance:  1,
				},
			},
		},
		{
			SetupParams: setupParams,
			stateAcceptance: cloudprotocol.StateAcceptance{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				Checksum: "decodingError",
			},
		},
		{
			SetupParams: setupParams,
			stateAcceptance: cloudprotocol.StateAcceptance{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				Checksum: hex.EncodeToString([]byte("bad checksum")),
			},
		},
		{
			SetupParams:       setupParams,
			errorSaveCheckSum: true,
			stateAcceptance: cloudprotocol.StateAcceptance{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				Result: "accepted",
			},
		},
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, len(testsData)),
		chanStateRequest: make(chan cloudprotocol.StateRequest, len(testsData)),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	for _, testData := range testsData {
		storage.errorSaveCheckSum = testData.errorSaveCheckSum

		_, _, err := instance.Setup(testData.SetupParams)
		if err != nil {
			t.Fatalf("Can't setup instance: %v", err)
		}

		if testData.errorSaveCheckSum {
			checksum := instance.GetInstanceCheckSum(testData.SetupParams.InstanceIdent)
			if checksum != "" {
				t.Error("Expected empty checksum")
			}

			testData.stateAcceptance.Checksum = hex.EncodeToString([]byte(checksum))
		}

		if err = instance.StateAcceptance(testData.stateAcceptance); err == nil {
			t.Fatalf("State should not be acceptance")
		}
	}
}

func TestNewStateCancel(t *testing.T) {
	setupParams := storagestate.SetupParams{
		InstanceIdent: aostypes.InstanceIdent{
			ServiceID: "service1",
			SubjectID: "subject1",
			Instance:  1,
		},
		UID:          1003,
		GID:          1003,
		StateQuota:   2000,
		StorageQuota: 1000,
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, 1),
		chanStateRequest: make(chan cloudprotocol.StateRequest, 1),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	if _, _, err = instance.Setup(setupParams); err != nil {
		t.Fatalf("Can't setup instance: %v", err)
	}

	if err := os.WriteFile(
		path.Join(stateDir, "1_subject1_service1_state.dat"), []byte("New state"), 0o600); err != nil {
		t.Fatalf("Can't write state file: %v", err)
	}

	if err = instance.Cleanup(setupParams.InstanceIdent); err != nil {
		t.Fatalf("Can't cleanup storage state: %v", err)
	}

	if err = instance.RemoveServiceInstance(setupParams.InstanceIdent); err != nil {
		t.Fatalf("Can't remove storage state: %v", err)
	}

	select {
	case <-messageSender.chanNewState:
		t.Fatal("New state request should not be send")

	case <-time.After(storagestate.StateChangeTimeout * 2):
	}
}

func TestStateAcceptance(t *testing.T) {
	testsData := []struct {
		storagestate.SetupParams
		stateData                 string
		result                    string
		expectReceiveStateRequest bool
	}{
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			stateData:                 "state1",
			result:                    "accepted",
			expectReceiveStateRequest: true,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				UID:          1003,
				GID:          1003,
				StateQuota:   2000,
				StorageQuota: 1000,
			},
			stateData:                 "state2",
			result:                    "rejected",
			expectReceiveStateRequest: false,
		},
		{
			SetupParams: storagestate.SetupParams{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  2,
				},
				UID:          1004,
				GID:          1004,
				StateQuota:   1000,
				StorageQuota: 1000,
			},
			stateData:                 "state3",
			result:                    "accepted",
			expectReceiveStateRequest: true,
		},
	}

	storage := testStorageInterface{
		data: make(map[aostypes.InstanceIdent]storagestate.StorageStateInstanceInfo),
	}

	messageSender := &testMessageSender{
		chanNewState:     make(chan cloudprotocol.NewState, len(testsData)),
		chanStateRequest: make(chan cloudprotocol.StateRequest, len(testsData)),
	}

	instance, err := storagestate.New(&config.Config{
		StorageDir: storageDir,
		StateDir:   stateDir,
	}, messageSender, &storage)
	if err != nil {
		t.Fatalf("Can't create storagestate instance: %v", err)
	}
	defer instance.Close()

	for _, testData := range testsData {
		_, _, err := instance.Setup(testData.SetupParams)
		if err != nil {
			t.Fatalf("Can't setup instance: %v", err)
		}

		select {
		case stateRequest := <-messageSender.chanStateRequest:
			if !testData.expectReceiveStateRequest {
				t.Error("Should not receive a state request")
			}

			if stateRequest.InstanceIdent != testData.InstanceIdent {
				t.Error("Incorrect instance ident")
			}

		case <-time.After(waitChannelTimeout):
			if testData.expectReceiveStateRequest {
				t.Fatal("Expected state request to be received")
			}
		}

		stateStorageInfo, ok := storage.data[testData.SetupParams.InstanceIdent]
		if !ok {
			t.Error("Can't found state storage info by instance ident")
		}

		pathToStateFile := path.Join(stateDir, stateStorageInfo.InstanceID+"_state.dat")

		if err := os.WriteFile(pathToStateFile, []byte(testData.stateData), 0o600); err != nil {
			t.Fatalf("Can't write state file: %v", err)
		}

		select {
		case newState := <-messageSender.chanNewState:
			if testData.InstanceIdent != newState.InstanceIdent {
				t.Error("Incorrect state changed instance ID")
			}

			if testData.stateData != newState.State {
				t.Error("Incorrect state data")
			}

			sumBytes, err := hex.DecodeString(newState.Checksum)
			if err != nil {
				t.Errorf("Problem to decode new state checksum: %v", err)
			}

			calcSum := sha3.Sum224([]byte(testData.stateData))

			if !bytes.Equal(calcSum[:], sumBytes) {
				t.Error("Incorrect new state checksum")
			}

			if err = instance.StateAcceptance(
				cloudprotocol.StateAcceptance{
					InstanceIdent: testData.InstanceIdent,
					Checksum:      newState.Checksum,
					Result:        testData.result,
					Reason:        "test",
				}); err != nil {
				t.Fatalf("Can't send accepted state: %v", err)
			}

			if testData.result == "accepted" {
				continue
			}

			select {
			case stateRequest := <-messageSender.chanStateRequest:
				if testData.result == "accepted" {
					t.Fatal("Unexpected state request")
				}

				if stateRequest.InstanceIdent != testData.InstanceIdent {
					t.Error("Incorrect instance id")
				}

				stateData := "new state"
				calcSum := sha3.Sum224([]byte(stateData))

				if err = instance.UpdateState(cloudprotocol.UpdateState{
					InstanceIdent: testData.InstanceIdent,
					State:         stateData,
					Checksum:      hex.EncodeToString(calcSum[:]),
				}); err != nil {
					t.Fatalf("Can't send update state: %v", err)
				}

				expectedCheckSum := instance.GetInstanceCheckSum(testData.SetupParams.InstanceIdent)
				if expectedCheckSum == "" {
					t.Error("Unexpected checksum")
				}

				if !bytes.Equal([]byte(expectedCheckSum), calcSum[:]) {
					t.Error("Incorrect checksum")
				}

			case <-time.After(waitChannelTimeout):
				t.Fatal("Timeout to wait requests")
			}

		case <-time.After(storagestate.StateChangeTimeout * 2):
			t.Fatal("Timeout to wait new state request")
		}
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (messageSender *testMessageSender) SendInstanceNewState(newState cloudprotocol.NewState) error {
	messageSender.chanNewState <- newState

	return nil
}

func (messageSender *testMessageSender) SendInstanceStateRequest(request cloudprotocol.StateRequest) error {
	messageSender.chanStateRequest <- request

	return nil
}

func (storage *testStorageInterface) GetStorageStateInfo(
	instanceIdent aostypes.InstanceIdent,
) (storageStateInfo storagestate.StorageStateInstanceInfo, err error) {
	if storage.errorGet {
		return storageStateInfo, aoserrors.New("can't get storagestate info")
	}

	data, ok := storage.data[instanceIdent]
	if !ok {
		return storageStateInfo, storagestate.ErrNotExist
	}

	return data, nil
}

func (storage *testStorageInterface) SetStorageStateQuotas(
	instanceIdent aostypes.InstanceIdent, storageQuota, stateQuota uint64,
) error {
	if storage.errorSave {
		return aoserrors.New("can't save storage state info")
	}

	data := storage.data[instanceIdent]

	data.StateQuota = stateQuota
	data.StorageQuota = storageQuota

	storage.data[instanceIdent] = data

	return nil
}

func (storage *testStorageInterface) AddStorageStateInfo(
	storageStateInfo storagestate.StorageStateInstanceInfo,
) error {
	if storage.errorAdd {
		return aoserrors.New("can't add statestorage entry")
	}

	storage.data[storageStateInfo.InstanceIdent] = storageStateInfo

	return nil
}

func (storage *testStorageInterface) SetStateChecksum(instanceIdent aostypes.InstanceIdent, checksum []byte) error {
	if storage.errorSaveCheckSum {
		return aoserrors.New("can't save checksum")
	}

	storageStateInfo, ok := storage.data[instanceIdent]
	if !ok {
		return aoserrors.New("instance not found")
	}

	storageStateInfo.StateChecksum = checksum

	storage.data[instanceIdent] = storageStateInfo

	return nil
}

func (storage *testStorageInterface) RemoveStorageStateInfo(instanceIdent aostypes.InstanceIdent) error {
	if _, ok := storage.data[instanceIdent]; !ok {
		return aoserrors.New("instance not found")
	}

	delete(storage.data, instanceIdent)

	return nil
}

func (storage *testStorageInterface) GetAllStorageStateInfo() (
	infos []storagestate.StorageStateInstanceInfo, err error,
) {
	for _, storageStateInfo := range storage.data {
		infos = append(infos, storageStateInfo)
	}

	return infos, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func setup() (err error) {
	if tmpDir, err = os.MkdirTemp("", "aos_"); err != nil {
		return aoserrors.Wrap(err)
	}

	storagestate.SetUserFSQuota = setUserFSQuota

	storagestate.StateChangeTimeout = 100 * time.Millisecond

	storageDir = path.Join(tmpDir, "storage")
	stateDir = path.Join(tmpDir, "state")

	return nil
}

func cleanup() {
	if err := os.RemoveAll(tmpDir); err != nil {
		log.Errorf("Can't remove tmp folder: %s", err)
	}
}

func setUserFSQuota(path string, limit uint64, uid, gid uint32) (err error) {
	stateStorageQuotaLimit = limit

	return nil
}

func getStateFileChecksum(fileName string) (checksum []byte, err error) {
	if _, err = os.Stat(fileName); err != nil {
		if !os.IsNotExist(err) {
			return nil, aoserrors.Wrap(err)
		}

		return nil, nil
	}

	data, err := os.ReadFile(fileName)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	calcSum := sha3.Sum224(data)

	return calcSum[:], nil
}

func isPathExist(dir string) bool {
	if _, err := os.Stat(dir); err != nil {
		return false
	}

	return true
}
