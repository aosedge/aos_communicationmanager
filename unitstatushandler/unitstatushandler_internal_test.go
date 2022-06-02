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

package unitstatushandler

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cmserver"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	downloadSuccess = iota
	downloadCanceled
	downloadError
)

const waitStatusTimeout = 5 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type TestSender struct {
	statusChannel chan cloudprotocol.UnitStatus
}

type TestBoardConfigUpdater struct {
	BoardConfigStatus cloudprotocol.BoardConfigStatus
	UpdateVersion     string
	UpdateError       error
}

type TestFirmwareUpdater struct {
	UpdateTime           time.Duration
	InitComponentsInfo   []cloudprotocol.ComponentStatus
	UpdateComponentsInfo []cloudprotocol.ComponentStatus
	UpdateError          error
}

type TestSoftwareUpdater struct {
	AllServices     []cloudprotocol.ServiceStatus
	AllLayers       []cloudprotocol.LayerStatus
	UpdateError     error
	runInstanceChan chan []cloudprotocol.InstanceInfo
}

type TestDownloader struct {
	DownloadTime   time.Duration
	DownloadedURLs []string

	errorURL    string
	downloadErr error
}

type TestResult struct {
	ctx          context.Context // nolint:containedctx
	downloadTime time.Duration
	fileName     string
	err          error
}

type testStatusHandler struct {
	downloadTime time.Duration
	result       map[string]*downloadResult
}

type TestStorage struct {
	sotaState json.RawMessage
	fotaState json.RawMessage
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

	tmpDir, err = ioutil.TempDir("", "cm_")
	if err != nil {
		log.Fatalf("Can't create tmp dir: %s", tmpDir)
	}

	ret := m.Run()

	if err = os.RemoveAll(tmpDir); err != nil {
		log.Fatalf("Can't remove tmp dir: %s", tmpDir)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestDownload(t *testing.T) {
	testDownloader := NewTestDownloader()

	statusHandler, err := New(&config.Config{},
		NewTestBoardConfigUpdater(cloudprotocol.BoardConfigStatus{}), NewTestFirmwareUpdater(nil),
		NewTestSoftwareUpdater(nil, nil), testDownloader, NewTestStorage(), NewTestSender())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	type testData struct {
		request          map[string]cloudprotocol.DecryptDataStruct
		errorURL         string
		downloadError    error
		continueOnError  bool
		downloadTime     time.Duration
		cancelDownloadIn time.Duration
		check            map[string]int
	}

	data := []testData{
		{
			request:         map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {}, "2": {}},
			continueOnError: false,
			check:           map[string]int{"0": downloadSuccess, "1": downloadSuccess, "2": downloadSuccess},
			downloadTime:    1 * time.Second,
		},
		{
			request:         map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {URLs: []string{"1"}}, "2": {}},
			errorURL:        "1",
			downloadError:   aoserrors.New("download error"),
			continueOnError: false,
			check:           map[string]int{"0": downloadCanceled, "1": downloadError, "2": downloadCanceled},
			downloadTime:    1 * time.Second,
		},
		{
			request:         map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {URLs: []string{"1"}}, "2": {}},
			errorURL:        "1",
			downloadError:   aoserrors.New("download error"),
			continueOnError: true,
			check:           map[string]int{"0": downloadSuccess, "1": downloadError, "2": downloadSuccess},
			downloadTime:    1 * time.Second,
		},
		{
			request:          map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {}, "2": {}},
			continueOnError:  false,
			check:            map[string]int{"0": downloadCanceled, "1": downloadCanceled, "2": downloadCanceled},
			downloadTime:     5 * time.Second,
			cancelDownloadIn: 2 * time.Second,
		},
		{
			request:          map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {}, "2": {}},
			continueOnError:  true,
			check:            map[string]int{"0": downloadCanceled, "1": downloadCanceled, "2": downloadCanceled},
			downloadTime:     5 * time.Second,
			cancelDownloadIn: 2 * time.Second,
		},
	}

	for i, item := range data {
		t.Logf("Item: %d", i)

		testDownloader.SetError(item.errorURL, item.downloadError)
		testDownloader.DownloadTime = item.downloadTime

		ctx, cancel := context.WithCancel(context.Background())

		if item.cancelDownloadIn != 0 {
			time.Sleep(item.cancelDownloadIn)
			cancel()
		}

		result := statusHandler.download(ctx, item.request, item.continueOnError,
			func(id string, status string, componentErr string) {
				log.WithFields(log.Fields{
					"id": id, "status": status, "error": componentErr,
				}).Debug("Component download status")
			}, nil, nil)

		if err = checkDownloadResult(result, item.check); err != nil {
			t.Errorf("Check result failed: %s", err)
		}

		cancel()
	}
}

func TestFirmwareManager(t *testing.T) {
	type testData struct {
		testID                  string
		initState               *firmwareManager
		initStatus              *cmserver.UpdateStatus
		initComponentStatuses   []cloudprotocol.ComponentStatus
		desiredStatus           *cloudprotocol.DecodedDesiredStatus
		downloadTime            time.Duration
		downloadResult          map[string]*downloadResult
		updateTime              time.Duration
		updateComponentStatuses []cloudprotocol.ComponentStatus
		boardConfigError        error
		triggerUpdate           bool
		updateWaitStatuses      []cmserver.UpdateStatus
	}

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ID:                "comp1",
			VersionInfo:       cloudprotocol.VersionInfo{VendorVersion: "1.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{1}},
		},
		{
			ID:                "comp2",
			VersionInfo:       cloudprotocol.VersionInfo{VendorVersion: "2.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{2}},
		},
	}

	otherUpdateComponents := []cloudprotocol.ComponentInfo{
		{
			ID:                "comp3",
			VersionInfo:       cloudprotocol.VersionInfo{VendorVersion: "3.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
		},
		{
			ID:                "comp4",
			VersionInfo:       cloudprotocol.VersionInfo{VendorVersion: "4.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{4}},
		},
	}

	updateTimeSlots := []cloudprotocol.TimeSlot{{
		Start:  aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
		Finish: aostypes.Time{Time: time.Date(0, 1, 1, 23, 59, 59, 999999, time.Local)},
	}}

	updateTimetable := []cloudprotocol.TimetableEntry{
		{DayOfWeek: 1, TimeSlots: updateTimeSlots},
		{DayOfWeek: 2, TimeSlots: updateTimeSlots},
		{DayOfWeek: 3, TimeSlots: updateTimeSlots},
		{DayOfWeek: 4, TimeSlots: updateTimeSlots},
		{DayOfWeek: 5, TimeSlots: updateTimeSlots},
		{DayOfWeek: 6, TimeSlots: updateTimeSlots},
		{DayOfWeek: 7, TimeSlots: updateTimeSlots},
	}

	data := []testData{
		{
			testID:     "success update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "download error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {Error: "download error"},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.NoUpdate, Error: "download error"},
			},
		},
		{
			testID:     "update error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{
					ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.ErrorStatus,
					ErrorInfo: &cloudprotocol.ErrorInfo{Message: "update error"},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate, Error: "update error"},
			},
		},
		{
			testID: "continue download on startup",
			initState: &firmwareManager{
				CurrentState:  stateDownloading,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Downloading},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate}, {State: cmserver.Updating}, {State: cmserver.NoUpdate},
			},
		},
		{
			testID: "continue update on ready to update state",
			initState: &firmwareManager{
				CurrentState:  stateReadyToUpdate,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					updateComponents[0].ID: {
						ID:            updateComponents[0].ID,
						VendorVersion: updateComponents[0].VendorVersion,
					},
					updateComponents[1].ID: {
						ID:            updateComponents[1].ID,
						VendorVersion: updateComponents[1].VendorVersion,
					},
				},
			},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{{State: cmserver.Updating}, {State: cmserver.NoUpdate}},
		},
		{
			testID: "continue update on updating state",
			initState: &firmwareManager{
				CurrentState:  stateUpdating,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					updateComponents[0].ID: {
						ID:            updateComponents[0].ID,
						VendorVersion: updateComponents[0].VendorVersion,
					},
					updateComponents[1].ID: {
						ID:            updateComponents[1].ID,
						VendorVersion: updateComponents[1].VendorVersion,
					},
				},
			},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{{State: cmserver.NoUpdate}},
		},
		{
			testID: "same update on ready to update state",
			initState: &firmwareManager{
				CurrentState: stateReadyToUpdate,
				CurrentUpdate: &firmwareUpdate{
					Schedule:   cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
					Components: updateComponents,
				},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					updateComponents[0].ID: {
						ID:            updateComponents[0].ID,
						VendorVersion: updateComponents[0].VendorVersion,
					},
					updateComponents[1].ID: {
						ID:            updateComponents[1].ID,
						VendorVersion: updateComponents[1].VendorVersion,
					},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.ReadyToUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Components:   updateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			triggerUpdate:      true,
			updateWaitStatuses: []cmserver.UpdateStatus{{State: cmserver.Updating}, {State: cmserver.NoUpdate}},
		},
		{
			testID: "new update on downloading state",
			initState: &firmwareManager{
				CurrentState:  stateDownloading,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Downloading},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp3", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID:      {},
				updateComponents[1].ID:      {},
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: context.Canceled.Error()},
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID: "new update on ready to update state",
			initState: &firmwareManager{
				CurrentState: stateReadyToUpdate,
				CurrentUpdate: &firmwareUpdate{
					Schedule:   cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
					Components: updateComponents,
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.ReadyToUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp3", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Components:   otherUpdateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			downloadResult: map[string]*downloadResult{
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: context.Canceled.Error()},
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
			},
		},
		{
			testID: "new update on updating state",
			initState: &firmwareManager{
				CurrentState: stateUpdating,
				CurrentUpdate: &firmwareUpdate{
					Components: updateComponents,
				},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					updateComponents[0].ID: {
						ID:            updateComponents[0].ID,
						VendorVersion: updateComponents[0].VendorVersion,
						Status:        cloudprotocol.InstallingStatus,
					},
					updateComponents[1].ID: {
						ID:            updateComponents[1].ID,
						VendorVersion: updateComponents[1].VendorVersion,
						Status:        cloudprotocol.InstallingStatus,
					},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Updating},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp3", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateTime:   1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate},
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:        "update board config",
			initStatus:    &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:           "error board config",
			initStatus:       &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus:    &cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")},
			boardConfigError: aoserrors.New("board config error"),
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.NoUpdate, Error: "board config error"},
			},
		},
		{
			testID:     "timetable update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				FOTASchedule: cloudprotocol.ScheduleRule{
					Type:      cloudprotocol.TimetableUpdate,
					Timetable: updateTimetable,
				},
				Components: updateComponents,
			},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "update TTL",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ID: "comp1", VendorVersion: "0.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				FOTASchedule: cloudprotocol.ScheduleRule{
					Type: cloudprotocol.TriggerUpdate,
					TTL:  3,
				},
				Components: updateComponents,
			},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.NoUpdate, Error: "update timeout"},
			},
		},
	}

	firmwareUpdater := NewTestFirmwareUpdater(nil)
	boardConfigUpdater := NewTestBoardConfigUpdater(cloudprotocol.BoardConfigStatus{})
	statusHandler := newTestStatusHandler()
	testStorage := NewTestStorage()

	for _, item := range data {
		t.Logf("Test item: %s", item.testID)

		statusHandler.result = item.downloadResult
		statusHandler.downloadTime = item.downloadTime
		firmwareUpdater.InitComponentsInfo = item.initComponentStatuses
		firmwareUpdater.UpdateComponentsInfo = item.updateComponentStatuses
		firmwareUpdater.UpdateTime = item.updateTime
		boardConfigUpdater.UpdateError = item.boardConfigError

		if err := testStorage.saveFirmwareState(item.initState); err != nil {
			t.Errorf("Can't save init state: %s", err)
			continue
		}

		// Create firmware manager

		firmwareManager, err := newFirmwareManager(statusHandler, firmwareUpdater, boardConfigUpdater,
			testStorage, 30*time.Second)
		if err != nil {
			t.Errorf("Can't create firmware manager: %s", err)
			continue
		}

		// Check init status

		if item.initStatus != nil {
			if err = compareStatuses(*item.initStatus, firmwareManager.getCurrentStatus().UpdateStatus); err != nil {
				t.Errorf("Wrong init status: %s", err)
			}
		}

		// Process desired status

		if item.desiredStatus != nil {
			if err = firmwareManager.processDesiredStatus(*item.desiredStatus); err != nil {
				t.Errorf("Process desired status failed: %s", err)
				goto closeFM
			}
		}

		// Trigger update

		if item.triggerUpdate {
			if err = firmwareManager.startUpdate(); err != nil {
				t.Errorf("Start update failed: %s", err)
			}
		}

		for _, expectedStatus := range item.updateWaitStatuses {
			if err = waitForFOTAUpdateStatus(firmwareManager.statusChannel, expectedStatus); err != nil {
				t.Errorf("Wait for update status error: %s", err)

				if strings.Contains(err.Error(), "status timeout") {
					goto closeFM
				}
			}
		}

	closeFM:
		// Close firmware manager

		if err = firmwareManager.close(); err != nil {
			t.Errorf("Error closing firmware manager: %s", err)
		}
	}
}

func TestSoftwareManager(t *testing.T) {
	type testData struct {
		testID             string
		initState          *softwareManager
		initStatus         *cmserver.UpdateStatus
		desiredStatus      *cloudprotocol.DecodedDesiredStatus
		downloadTime       time.Duration
		downloadResult     map[string]*downloadResult
		triggerUpdate      bool
		updateError        error
		updateWaitStatuses []cmserver.UpdateStatus
	}

	updateLayers := []cloudprotocol.LayerInfo{
		{
			ID:          "layer1",
			Digest:      "digest1",
			VersionInfo: cloudprotocol.VersionInfo{AosVersion: 1},
		},
		{
			ID:          "layer2",
			Digest:      "digest2",
			VersionInfo: cloudprotocol.VersionInfo{AosVersion: 2},
		},
	}

	updateServices := []cloudprotocol.ServiceInfo{
		{
			ID:          "service1",
			VersionInfo: cloudprotocol.VersionInfo{AosVersion: 1},
		},
		{
			ID:          "service2",
			VersionInfo: cloudprotocol.VersionInfo{AosVersion: 2},
		},
	}

	updateTimeSlots := []cloudprotocol.TimeSlot{{
		Start:  aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
		Finish: aostypes.Time{Time: time.Date(0, 1, 1, 23, 59, 59, 999999, time.Local)},
	}}

	updateTimetable := []cloudprotocol.TimetableEntry{
		{DayOfWeek: 1, TimeSlots: updateTimeSlots},
		{DayOfWeek: 2, TimeSlots: updateTimeSlots},
		{DayOfWeek: 3, TimeSlots: updateTimeSlots},
		{DayOfWeek: 4, TimeSlots: updateTimeSlots},
		{DayOfWeek: 5, TimeSlots: updateTimeSlots},
		{DayOfWeek: 6, TimeSlots: updateTimeSlots},
		{DayOfWeek: 7, TimeSlots: updateTimeSlots},
	}

	data := []testData{
		{
			testID:     "success update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "one item download error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {Error: "download error"},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate, Error: "download error"},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "all items download error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {Error: "download error"}, updateLayers[1].Digest: {Error: "download error"},
				updateServices[0].ID: {Error: "download error"}, updateServices[1].ID: {Error: "download error"},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.NoUpdate, Error: "download error"},
			},
		},
		{
			testID:     "update error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateError: aoserrors.New("update error"),
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate, Error: "update error"},
			},
		},
		{
			testID: "continue download on startup",
			initState: &softwareManager{
				CurrentState: stateDownloading,
				CurrentUpdate: &softwareUpdate{
					InstallLayers: updateLayers, InstallServices: updateServices,
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Downloading},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate}, {State: cmserver.Updating}, {State: cmserver.NoUpdate},
			},
		},
		{
			testID: "continue update on ready to update state",
			initState: &softwareManager{
				CurrentState: stateReadyToUpdate,
				CurrentUpdate: &softwareUpdate{
					InstallLayers: updateLayers, InstallServices: updateServices,
				},
				DownloadResult: map[string]*downloadResult{
					updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
					updateServices[0].ID: {}, updateServices[1].ID: {},
				},
				LayerStatuses: map[string]*cloudprotocol.LayerStatus{
					updateLayers[0].Digest: {
						ID:         updateLayers[0].ID,
						Digest:     updateLayers[0].Digest,
						AosVersion: updateLayers[0].AosVersion,
						Status:     cloudprotocol.InstallingStatus,
					},
					updateLayers[1].Digest: {
						ID:         updateLayers[1].ID,
						Digest:     updateLayers[1].Digest,
						AosVersion: updateLayers[1].AosVersion,
						Status:     cloudprotocol.InstallingStatus,
					},
				},
				ServiceStatuses: map[string]*cloudprotocol.ServiceStatus{
					updateServices[0].ID: {
						ID:         updateServices[0].ID,
						AosVersion: updateServices[0].AosVersion,
						Status:     cloudprotocol.InstallingStatus,
					},
					updateServices[1].ID: {
						ID:         updateServices[1].ID,
						AosVersion: updateServices[1].AosVersion,
						Status:     cloudprotocol.InstallingStatus,
					},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Updating}, {State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "timetable update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				SOTASchedule: cloudprotocol.ScheduleRule{
					Type:      cloudprotocol.TimetableUpdate,
					Timetable: updateTimetable,
				},
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "update TTL",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				SOTASchedule: cloudprotocol.ScheduleRule{
					TTL:  3,
					Type: cloudprotocol.TriggerUpdate,
				},
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ID: {}, updateServices[1].ID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.NoUpdate, Error: "update timeout"},
			},
		},
	}

	softwareUpdater := NewTestSoftwareUpdater(nil, nil)
	statusHandler := newTestStatusHandler()
	testStorage := NewTestStorage()

	for _, item := range data {
		t.Logf("Test item: %s", item.testID)

		statusHandler.result = item.downloadResult
		statusHandler.downloadTime = item.downloadTime
		softwareUpdater.UpdateError = item.updateError

		if err := testStorage.saveSoftwareState(item.initState); err != nil {
			t.Errorf("Can't save init state: %s", err)
			continue
		}

		// Create software manager

		softwareManager, err := newSoftwareManager(statusHandler, softwareUpdater, testStorage, 30*time.Second)
		if err != nil {
			t.Errorf("Can't create software manager: %s", err)
			continue
		}

		// Check init status

		if item.initStatus != nil {
			if err = compareStatuses(*item.initStatus, softwareManager.getCurrentStatus().UpdateStatus); err != nil {
				t.Errorf("Wrong init status: %s", err)
			}
		}

		// Process desired status

		if item.desiredStatus != nil {
			if err = softwareManager.processDesiredStatus(*item.desiredStatus); err != nil {
				t.Errorf("Process desired status failed: %s", err)
				goto closeSM
			}
		}

		// Trigger update

		if item.triggerUpdate {
			if err = softwareManager.startUpdate(); err != nil {
				t.Errorf("Start update failed: %s", err)
			}
		}

		for _, expectedStatus := range item.updateWaitStatuses {
			if expectedStatus.State == cmserver.Updating {
				if _, err := softwareUpdater.WaitForRunInstance(time.Second); err != nil {
					t.Errorf("Wait run instances error: %v", err)
				}
			}

			if err = waitForSOTAUpdateStatus(softwareManager.statusChannel, expectedStatus); err != nil {
				t.Errorf("Wait for update status error: %s", err)

				if strings.Contains(err.Error(), "status timeout") {
					goto closeSM
				}
			}
		}

	closeSM:
		// Close software manager

		if err = softwareManager.close(); err != nil {
			t.Errorf("Error closing firmware manager: %s", err)
		}
	}
}

func TestTimeTable(t *testing.T) {
	type testData struct {
		fromDate  time.Time
		timetable []cloudprotocol.TimetableEntry
		result    time.Duration
		err       string
	}

	data := []testData{
		{
			timetable: []cloudprotocol.TimetableEntry{},
			err:       "timetable is empty",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{{DayOfWeek: 0}},
			err:       "invalid day of week value",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{{DayOfWeek: 1}},
			err:       "no time slots",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 2, 0, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			err: "start value should contain only time",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 2, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			err: "finish value should contain only time",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 1, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			err: "start value should be before finish value",
		},
		{
			fromDate: time.Date(1, 1, 1, 0, 0, 0, 0, time.Local),
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			result: 0,
		},
		{
			fromDate: time.Date(1, 1, 1, 0, 0, 0, 0, time.Local),
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 2, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
						},
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 14, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 3, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 16, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 18, 0, 0, 0, time.Local)},
						},
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 20, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 22, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			result: 10 * time.Hour,
		},
		{
			fromDate: time.Date(1, 1, 5, 10, 0, 0, 0, time.Local),
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
						},
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 14, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 2, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 16, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 18, 0, 0, 0, time.Local)},
						},
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 20, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 22, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 3, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 4, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 5, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start:  aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							Finish: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			result: 70 * time.Hour,
		},
	}

	for i, item := range data {
		t.Logf("Item: %d", i)

		availableTime, err := getAvailableTimetableTime(item.fromDate, item.timetable)
		if err != nil {
			if item.err == "" {
				t.Errorf("Can't get available timetable time: %s", err)
				continue
			}

			if !strings.Contains(err.Error(), item.err) {
				t.Errorf("Wrong error: %s", err)
			}

			continue
		}

		if item.err != "" {
			t.Errorf("Error expected")
			continue
		}

		if availableTime != item.result {
			t.Errorf("Wrong available time: %v", availableTime)
		}
	}
}

func TestSyncExecutor(t *testing.T) {
	const (
		numExecuteTasks  = 10
		numCanceledTasks = 10
	)

	resultChannel := make(chan int, 1)

	cancelCtx, cancelFunc := context.WithCancel(context.Background())

	for i := 0; i < numExecuteTasks+numCanceledTasks; i++ {
		ctx := cancelCtx

		if i < numExecuteTasks {
			ctx = context.Background()
		}

		value := i

		updateSynchronizer.execute(ctx, func() {
			time.Sleep(1 * time.Second)
			resultChannel <- value
		})
	}

	cancelFunc()

	index := 0

	for {
		select {
		case result := <-resultChannel:
			log.Debugf("Receive result: %d, index: %d", result, index)

			if result != index {
				t.Errorf("Wrong result received: %d, index: %d", result, index)
			}

			if index > numExecuteTasks {
				t.Errorf("Unexpected result received: %d, index: %d", result, index)
			}

			index++

		case <-time.After(5 * time.Second):
			if index < numExecuteTasks {
				t.Error("Wait execution timeout")
			}

			return
		}
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * TestSender
 **********************************************************************************************************************/

func NewTestSender() (sender *TestSender) {
	return &TestSender{statusChannel: make(chan cloudprotocol.UnitStatus, 1)}
}

func (sender *TestSender) SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error) {
	sender.statusChannel <- unitStatus

	return nil
}

func (sender *TestSender) WaitForStatus(timeout time.Duration) (status cloudprotocol.UnitStatus, err error) {
	select {
	case receivedUnitStatus := <-sender.statusChannel:
		return receivedUnitStatus, nil

	case <-time.After(timeout):
		return status, aoserrors.New("receive status timeout")
	}
}

/***********************************************************************************************************************
 * TestBoardConfigUpdater
 **********************************************************************************************************************/

func NewTestBoardConfigUpdater(boardConfigInfo cloudprotocol.BoardConfigStatus) (updater *TestBoardConfigUpdater) {
	return &TestBoardConfigUpdater{BoardConfigStatus: boardConfigInfo}
}

func (updater *TestBoardConfigUpdater) GetStatus() (info cloudprotocol.BoardConfigStatus, err error) {
	return updater.BoardConfigStatus, nil
}

func (updater *TestBoardConfigUpdater) GetBoardConfigVersion(configJSON json.RawMessage) (version string, err error) {
	return updater.UpdateVersion, updater.UpdateError
}

func (updater *TestBoardConfigUpdater) CheckBoardConfig(configJSON json.RawMessage) (version string, err error) {
	return updater.UpdateVersion, updater.UpdateError
}

func (updater *TestBoardConfigUpdater) UpdateBoardConfig(configJSON json.RawMessage) (err error) {
	return updater.UpdateError
}

/***********************************************************************************************************************
 * TestFirmwareUpdater
 **********************************************************************************************************************/

func NewTestFirmwareUpdater(componentsInfo []cloudprotocol.ComponentStatus) (updater *TestFirmwareUpdater) {
	return &TestFirmwareUpdater{InitComponentsInfo: componentsInfo}
}

func (updater *TestFirmwareUpdater) GetStatus() (info []cloudprotocol.ComponentStatus, err error) {
	return updater.InitComponentsInfo, nil
}

func (updater *TestFirmwareUpdater) UpdateComponents(components []cloudprotocol.ComponentInfo) (
	componentsInfo []cloudprotocol.ComponentStatus, err error,
) {
	time.Sleep(updater.UpdateTime)
	return updater.UpdateComponentsInfo, updater.UpdateError
}

/***********************************************************************************************************************
 * TestSoftwareUpdater
 **********************************************************************************************************************/

func NewTestSoftwareUpdater(
	services []cloudprotocol.ServiceStatus, layers []cloudprotocol.LayerStatus,
) *TestSoftwareUpdater {
	return &TestSoftwareUpdater{
		AllServices: services, AllLayers: layers, runInstanceChan: make(chan []cloudprotocol.InstanceInfo, 1),
	}
}

func (updater *TestSoftwareUpdater) GetServicesStatus() ([]cloudprotocol.ServiceStatus, error) {
	return updater.AllServices, nil
}

func (updater *TestSoftwareUpdater) GetLayersStatus() ([]cloudprotocol.LayerStatus, error) {
	return updater.AllLayers, nil
}

func (updater *TestSoftwareUpdater) InstallService(serviceInfo cloudprotocol.ServiceInfo) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RemoveService(serviceID string) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) InstallLayer(layerInfo cloudprotocol.LayerInfo) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RunInstances(instances []cloudprotocol.InstanceInfo) error {
	updater.runInstanceChan <- instances

	return nil
}

func (updater *TestSoftwareUpdater) WaitForRunInstance(timeout time.Duration) ([]cloudprotocol.InstanceInfo, error) {
	select {
	case receivedRunInstances := <-updater.runInstanceChan:
		return receivedRunInstances, nil

	case <-time.After(timeout):
		return nil, aoserrors.New("receive run instances timeout")
	}
}

/***********************************************************************************************************************
 * TestDownloader
 **********************************************************************************************************************/

func NewTestDownloader() (testDownloader *TestDownloader) {
	return &TestDownloader{DownloadTime: 1 * time.Second}
}

func (testDownloader *TestDownloader) SetError(url string, err error) {
	testDownloader.errorURL = url
	testDownloader.downloadErr = err
}

func (testDownloader *TestDownloader) DownloadAndDecrypt(
	ctx context.Context, packageInfo cloudprotocol.DecryptDataStruct,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result downloader.Result, err error,
) {
	file, err := ioutil.TempFile(tmpDir, "*.dec")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	defer file.Close()

	var downloadErr error

	if len(packageInfo.URLs) != 0 {
		if testDownloader.errorURL == packageInfo.URLs[0] {
			downloadErr = testDownloader.downloadErr
		}

		if downloadErr == nil {
			testDownloader.DownloadedURLs = append(testDownloader.DownloadedURLs, packageInfo.URLs[0])
		}
	}

	return &TestResult{
		ctx:          ctx,
		downloadTime: testDownloader.DownloadTime,
		fileName:     file.Name(),
		err:          downloadErr,
	}, nil
}

func (result *TestResult) GetFileName() (fileName string) { return result.fileName }

func (result *TestResult) Wait() (err error) {
	select {
	case <-result.ctx.Done():
		return aoserrors.Wrap(result.ctx.Err())

	case <-time.After(result.downloadTime):
		return aoserrors.Wrap(result.err)
	}
}

func (result *TestResult) Release() {
	os.RemoveAll(result.fileName)
}

/***********************************************************************************************************************
 * testFirmwareStatusHandler
 **********************************************************************************************************************/

func newTestStatusHandler() (statusHandler *testStatusHandler) {
	return &testStatusHandler{}
}

func (statusHandler *testStatusHandler) download(
	ctx context.Context, request map[string]cloudprotocol.DecryptDataStruct,
	continueOnError bool, updateStatus statusNotifier,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) (result map[string]*downloadResult) {
	for id := range request {
		updateStatus(id, cloudprotocol.DownloadingStatus, "")
	}

	select {
	case <-time.After(statusHandler.downloadTime):
		if getDownloadError(statusHandler.result) != "" && !continueOnError {
			for id := range request {
				if statusHandler.result[id].Error == "" {
					statusHandler.result[id].Error = aoserrors.Wrap(context.Canceled).Error()
				}
			}
		}

		for id := range request {
			if statusHandler.result[id].Error != "" {
				updateStatus(id, cloudprotocol.ErrorStatus, statusHandler.result[id].Error)
			} else {
				updateStatus(id, cloudprotocol.DownloadedStatus, "")
			}
		}

		return statusHandler.result

	case <-ctx.Done():
		for id := range request {
			statusHandler.result[id].Error = aoserrors.Wrap(context.Canceled).Error()
			updateStatus(id, cloudprotocol.ErrorStatus, statusHandler.result[id].Error)
		}

		return result
	}
}

func (statusHandler *testStatusHandler) updateComponentStatus(componentInfo cloudprotocol.ComponentStatus) {
	log.WithFields(log.Fields{
		"id":      componentInfo.ID,
		"version": componentInfo.VendorVersion,
		"status":  componentInfo.Status,
		"error":   componentInfo.ErrorInfo,
	}).Debug("Update component status")
}

func (statusHandler *testStatusHandler) updateBoardConfigStatus(boardConfigInfo cloudprotocol.BoardConfigStatus) {
	log.WithFields(log.Fields{
		"version": boardConfigInfo.VendorVersion,
		"status":  boardConfigInfo.Status,
		"error":   boardConfigInfo.ErrorInfo,
	}).Debug("Update board config status")
}

func (statusHandler *testStatusHandler) updateLayerStatus(layerInfo cloudprotocol.LayerStatus) {
	log.WithFields(log.Fields{
		"id":      layerInfo.ID,
		"digest":  layerInfo.Digest,
		"version": layerInfo.AosVersion,
		"status":  layerInfo.Status,
		"error":   layerInfo.ErrorInfo,
	}).Debug("Update layer status")
}

func (statusHandler *testStatusHandler) updateServiceStatus(serviceInfo cloudprotocol.ServiceStatus) {
	log.WithFields(log.Fields{
		"id":      serviceInfo.ID,
		"version": serviceInfo.AosVersion,
		"status":  serviceInfo.Status,
		"error":   serviceInfo.ErrorInfo,
	}).Debug("Update service status")
}

/***********************************************************************************************************************
 * testStorage
 **********************************************************************************************************************/

func NewTestStorage() (storage *TestStorage) {
	return &TestStorage{}
}

func (storage *TestStorage) SetFirmwareUpdateState(state json.RawMessage) (err error) {
	storage.fotaState = state
	return nil
}

func (storage *TestStorage) GetFirmwareUpdateState() (state json.RawMessage, err error) {
	return storage.fotaState, nil
}

func (storage *TestStorage) SetSoftwareUpdateState(state json.RawMessage) (err error) {
	storage.sotaState = state
	return nil
}

func (storage *TestStorage) GetSoftwareUpdateState() (state json.RawMessage, err error) {
	return storage.sotaState, nil
}

func (storage *TestStorage) saveFirmwareState(state *firmwareManager) (err error) {
	if state == nil {
		storage.fotaState = nil

		return nil
	}

	if storage.fotaState, err = json.Marshal(state); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storage *TestStorage) saveSoftwareState(state *softwareManager) (err error) {
	if state == nil {
		storage.sotaState = nil

		return nil
	}

	if storage.sotaState, err = json.Marshal(state); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func checkDownloadResult(result map[string]*downloadResult, check map[string]int) (err error) {
	if len(result) != len(check) {
		return aoserrors.New("wrong result item count")
	}

	for id, item := range result {
		wrongItem := false

		switch check[id] {
		case downloadSuccess:
			if item.Error != "" {
				wrongItem = true
			}

		case downloadCanceled:
			if !isCancelError(item.Error) {
				wrongItem = true
			}

		case downloadError:
			if item.Error == "" || isCancelError(item.Error) {
				wrongItem = true
			}
		}

		if wrongItem {
			return aoserrors.Errorf("wrong item %s error: %s", id, item.Error)
		}
	}

	return nil
}

func compareStatuses(expectedStatus, comparedStatus cmserver.UpdateStatus) (err error) {
	if expectedStatus.State != comparedStatus.State {
		return aoserrors.Errorf("wrong state: %s", comparedStatus.State)
	}

	if comparedStatus.Error == "" && expectedStatus.Error != "" ||
		comparedStatus.Error != "" && expectedStatus.Error == "" {
		return aoserrors.Errorf("wrong error: %s", comparedStatus.Error)
	}

	if !strings.Contains(comparedStatus.Error, expectedStatus.Error) {
		return aoserrors.Errorf("wrong error: %s", comparedStatus.Error)
	}

	return nil
}

func waitForFOTAUpdateStatus(
	statusChannel <-chan cmserver.UpdateFOTAStatus, expectedStatus cmserver.UpdateStatus,
) (err error) {
	select {
	case status := <-statusChannel:
		if err = compareStatuses(expectedStatus, status.UpdateStatus); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil

	case <-time.After(waitStatusTimeout):
		return aoserrors.Errorf("wait for FOTA %s status timeout", expectedStatus.State)
	}
}

func waitForSOTAUpdateStatus(
	statusChannel <-chan cmserver.UpdateSOTAStatus, expectedStatus cmserver.UpdateStatus,
) (err error) {
	select {
	case status := <-statusChannel:
		if err = compareStatuses(expectedStatus, status.UpdateStatus); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil

	case <-time.After(waitStatusTimeout):
		return aoserrors.Errorf("wait for SOTA %s status timeout", expectedStatus.State)
	}
}
