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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/downloader"
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

type TestNodeManager struct {
	nodesInfo       map[string]*cloudprotocol.NodeInfo
	nodeInfoChannel chan cloudprotocol.NodeInfo
}

type TestSender struct {
	Consumer      amqphandler.ConnectionEventsConsumer
	statusChannel chan cloudprotocol.UnitStatus
}

type TestUnitConfigUpdater struct {
	UnitConfigStatus cloudprotocol.UnitConfigStatus
	UpdateError      error
}

type TestFirmwareUpdater struct {
	UpdateTime           time.Duration
	InitComponentsInfo   []cloudprotocol.ComponentStatus
	UpdateComponentsInfo []cloudprotocol.ComponentStatus
	UpdateError          error

	newComponentsChannel chan []cloudprotocol.ComponentStatus
}

type TestSoftwareUpdater struct {
	AllServices      []ServiceStatus
	AllLayers        []LayerStatus
	RevertedServices []string
	UpdateError      error
}

type TestInstanceRunner struct {
	runInstanceChan chan []cloudprotocol.InstanceInfo
}

type TestSystemQuotaAlertProvider struct {
	alertsChannel chan cloudprotocol.SystemQuotaAlert
}

type TestDownloader struct {
	DownloadTime   time.Duration
	DownloadedURLs []string

	errorURL    string
	downloadErr error
}

type TestResult struct {
	ctx          context.Context //nolint:containedctx
	downloadTime time.Duration
	fileName     string
	err          error
}

type testGroupDownloader struct {
	downloadTime time.Duration
	result       map[string]*downloadResult
	sotaReleased bool
	fotaReleased bool
}

type testStatusHandler struct{}

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

	tmpDir, err = os.MkdirTemp("", "cm_")
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

func TestGroupDownloader(t *testing.T) {
	testDownloader := NewTestDownloader()

	testGroupDownloader := newGroupDownloader(testDownloader)

	type testData struct {
		request          map[string]downloader.PackageInfo
		errorURL         string
		downloadError    error
		continueOnError  bool
		downloadTime     time.Duration
		cancelDownloadIn time.Duration
		check            map[string]int
	}

	data := []testData{
		{
			request:         map[string]downloader.PackageInfo{"0": {}, "1": {}, "2": {}},
			continueOnError: false,
			check:           map[string]int{"0": downloadSuccess, "1": downloadSuccess, "2": downloadSuccess},
			downloadTime:    1 * time.Second,
		},
		{
			request:         map[string]downloader.PackageInfo{"0": {}, "1": {URLs: []string{"1"}}, "2": {}},
			errorURL:        "1",
			downloadError:   aoserrors.New("download error"),
			continueOnError: false,
			check:           map[string]int{"0": downloadCanceled, "1": downloadError, "2": downloadCanceled},
			downloadTime:    1 * time.Second,
		},
		{
			request:         map[string]downloader.PackageInfo{"0": {}, "1": {URLs: []string{"1"}}, "2": {}},
			errorURL:        "1",
			downloadError:   aoserrors.New("download error"),
			continueOnError: true,
			check:           map[string]int{"0": downloadSuccess, "1": downloadError, "2": downloadSuccess},
			downloadTime:    1 * time.Second,
		},
		{
			request:          map[string]downloader.PackageInfo{"0": {}, "1": {}, "2": {}},
			continueOnError:  false,
			check:            map[string]int{"0": downloadCanceled, "1": downloadCanceled, "2": downloadCanceled},
			downloadTime:     5 * time.Second,
			cancelDownloadIn: 2 * time.Second,
		},
		{
			request:          map[string]downloader.PackageInfo{"0": {}, "1": {}, "2": {}},
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

		result := testGroupDownloader.download(ctx, item.request, item.continueOnError,
			func(id string, status string, componentErr *cloudprotocol.ErrorInfo) {
				log.WithFields(log.Fields{
					"id": id, "status": status, "error": componentErr,
				}).Debug("Component download status")
			})

		if err := checkDownloadResult(result, item.check); err != nil {
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
		desiredStatus           *cloudprotocol.DesiredStatus
		downloadTime            time.Duration
		downloadResult          map[string]*downloadResult
		updateTime              time.Duration
		updateComponentStatuses []cloudprotocol.ComponentStatus
		triggerUpdate           bool
		updateWaitStatuses      []cmserver.UpdateStatus
	}

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID:   convertToComponentID("comp1"),
			ComponentType: "rootfs",
			Version:       "1.0.0",
			DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{1}},
		},
		{
			ComponentID:   convertToComponentID("comp2"),
			ComponentType: "rootfs",
			Version:       "2.0.0",
			DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
		},
	}

	otherUpdateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID:   convertToComponentID("comp3"),
			ComponentType: "rootfs",
			Version:       "3.0.0",
			DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{3}},
		},
		{
			ComponentID:   convertToComponentID("comp4"),
			ComponentType: "rootfs",
			Version:       "4.0.0",
			DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{4}},
		},
	}

	updateTimeSlots := []cloudprotocol.TimeSlot{{
		Start: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
		End:   aostypes.Time{Time: time.Date(0, 1, 1, 23, 59, 59, 999999, time.Local)},
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
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {},
				convertToDownloadID(updateComponents[1]): {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "success update components with no ID",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "bios", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: []cloudprotocol.ComponentInfo{
				{
					ComponentID:   convertToComponentID("comp1"),
					ComponentType: "rootfs",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{1}},
				},
				{
					ComponentID:   nil,
					ComponentType: "bios",
					Version:       "0.0.2",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
				{
					ComponentID:   nil,
					ComponentType: "rootfs",
					Version:       "broken-version",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
				{
					ComponentID:   nil,
					ComponentType: "rootfs",
					Version:       "0.0.2",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
				{
					ComponentID:   nil,
					ComponentType: "rootfs",
					Version:       "0.0.3",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
			}},
			downloadResult: map[string]*downloadResult{
				"rootfs:0.0.1": {},
				"bios:0.0.2":   {},
				"rootfs:0.0.3": {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.1", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "bios", Version: "0.0.2", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "0.0.3", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "success update with different component types",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "bios", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: []cloudprotocol.ComponentInfo{
				{
					ComponentID:   convertToComponentID("comp1"),
					ComponentType: "rootfs",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{1}},
				},
				{
					ComponentID:   convertToComponentID("comp2"),
					ComponentType: "rootfs",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
				{
					ComponentID:   convertToComponentID("comp3"),
					ComponentType: "bios",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
			}},
			downloadResult: map[string]*downloadResult{
				"rootfs:0.0.1": {},
				"bios:0.0.1":   {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.1", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "0.0.1", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "bios", Version: "0.0.1", Status: cloudprotocol.InstalledStatus},
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
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {Error: "download error"},
				convertToDownloadID(updateComponents[1]): {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "download error"}},
			},
		},
		{
			testID:     "download error on same component type",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: []cloudprotocol.ComponentInfo{
				{
					ComponentID:   convertToComponentID("comp1"),
					ComponentType: "rootfs",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{1}},
				},
				{
					ComponentID:   convertToComponentID("comp2"),
					ComponentType: "rootfs",
					Version:       "0.0.1",
					DownloadInfo:  cloudprotocol.DownloadInfo{Sha256: []byte{2}},
				},
			}},
			downloadResult: map[string]*downloadResult{
				"rootfs:0.0.1": {Error: "download error"},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", Version: "0.0.0", ComponentType: "rootfs", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", Version: "0.0.0", ComponentType: "rootfs", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "download error"}},
			},
		},
		{
			testID:     "update error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {},
				convertToDownloadID(updateComponents[1]): {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{
					ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.ErrorStatus,
					ErrorInfo: &cloudprotocol.ErrorInfo{Message: "update error"},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "update error"}},
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
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {},
				convertToDownloadID(updateComponents[1]): {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID: "continue update on ready to update state",
			initState: &firmwareManager{
				CurrentState:  stateReadyToUpdate,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					convertToDownloadID(updateComponents[0]): {},
					convertToDownloadID(updateComponents[1]): {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					*updateComponents[0].ComponentID: {
						ComponentID:   *updateComponents[0].ComponentID,
						ComponentType: updateComponents[0].ComponentType,
						Version:       updateComponents[0].Version,
					},
					*updateComponents[1].ComponentID: {
						ComponentID:   *updateComponents[1].ComponentID,
						ComponentType: updateComponents[1].ComponentType,
						Version:       updateComponents[1].Version,
					},
				},
			},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID: "continue update on updating state",
			initState: &firmwareManager{
				CurrentState:  stateUpdating,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					convertToDownloadID(updateComponents[0]): {},
					convertToDownloadID(updateComponents[1]): {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					*updateComponents[0].ComponentID: {
						ComponentID:   *updateComponents[0].ComponentID,
						ComponentType: updateComponents[0].ComponentType,
						Version:       updateComponents[0].Version,
					},
					*updateComponents[1].ComponentID: {
						ComponentID:   *updateComponents[1].ComponentID,
						ComponentType: updateComponents[1].ComponentType,
						Version:       updateComponents[1].Version,
					},
				},
			},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate},
			},
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
					convertToDownloadID(updateComponents[0]): {},
					convertToDownloadID(updateComponents[1]): {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					*updateComponents[0].ComponentID: {
						ComponentID:   *updateComponents[0].ComponentID,
						ComponentType: updateComponents[0].ComponentType,
						Version:       updateComponents[0].Version,
					},
					*updateComponents[1].ComponentID: {
						ComponentID:   *updateComponents[1].ComponentID,
						ComponentType: updateComponents[1].ComponentType,
						Version:       updateComponents[1].Version,
					},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.ReadyToUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Components:   updateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			},
			triggerUpdate: true,
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID: "new update on downloading state",
			initState: &firmwareManager{
				CurrentState:  stateDownloading,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					convertToDownloadID(updateComponents[0]): {},
					convertToDownloadID(updateComponents[1]): {},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Downloading},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]):      {},
				convertToDownloadID(updateComponents[1]):      {},
				convertToDownloadID(otherUpdateComponents[0]): {},
				convertToDownloadID(otherUpdateComponents[1]): {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "4.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: context.Canceled.Error()}},
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
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Components:   otherUpdateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(otherUpdateComponents[0]): {},
				convertToDownloadID(otherUpdateComponents[1]): {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "4.0.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: context.Canceled.Error()}},
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
					convertToDownloadID(updateComponents[0]): {},
					convertToDownloadID(updateComponents[1]): {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentStatus{
					*updateComponents[0].ComponentID: {
						ComponentID:   *updateComponents[0].ComponentID,
						ComponentType: updateComponents[0].ComponentType,
						Version:       updateComponents[0].Version,
						Status:        cloudprotocol.InstallingStatus,
					},
					*updateComponents[1].ComponentID: {
						ComponentID:   *updateComponents[1].ComponentID,
						ComponentType: updateComponents[1].ComponentType,
						Version:       updateComponents[1].Version,
						Status:        cloudprotocol.InstallingStatus,
					},
				},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Updating},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(otherUpdateComponents[0]): {},
				convertToDownloadID(otherUpdateComponents[1]): {},
			},
			downloadTime: 1 * time.Second,
			updateTime:   1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp3", ComponentType: "rootfs", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp4", ComponentType: "rootfs", Version: "4.0.0", Status: cloudprotocol.InstalledStatus},
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
			testID:     "timetable update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{
				FOTASchedule: cloudprotocol.ScheduleRule{
					Type:      cloudprotocol.TimetableUpdate,
					Timetable: updateTimetable,
				},
				Components: updateComponents,
			},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {},
				convertToDownloadID(updateComponents[1]): {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentStatus{
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
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
				{ComponentID: "comp1", ComponentType: "rootfs", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
				{ComponentID: "comp2", ComponentType: "rootfs", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{
				FOTASchedule: cloudprotocol.ScheduleRule{
					Type: cloudprotocol.TriggerUpdate,
					TTL:  3,
				},
				Components: updateComponents,
			},
			downloadResult: map[string]*downloadResult{
				convertToDownloadID(updateComponents[0]): {},
				convertToDownloadID(updateComponents[1]): {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{
					Message: "update timeout",
				}},
			},
		},
	}

	firmwareUpdater := NewTestFirmwareUpdater(nil)
	firmwareDownloader := newTestGroupDownloader()
	testStorage := NewTestStorage()

	for _, item := range data {
		t.Logf("Test item: %s", item.testID)

		firmwareDownloader.fotaReleased = false
		firmwareDownloader.result = item.downloadResult
		firmwareDownloader.downloadTime = item.downloadTime
		firmwareUpdater.InitComponentsInfo = item.initComponentStatuses
		firmwareUpdater.UpdateComponentsInfo = item.updateComponentStatuses
		firmwareUpdater.UpdateTime = item.updateTime

		if err := testStorage.saveFirmwareState(item.initState); err != nil {
			t.Errorf("Can't save init state: %s", err)
			continue
		}

		// Create firmware manager

		firmwareManager, err := newFirmwareManager(newTestStatusHandler(), firmwareDownloader, firmwareUpdater,
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

		if !firmwareDownloader.fotaReleased {
			t.Error("FOTA downloads should be released")
		}
	}
}

func TestSoftwareManager(t *testing.T) {
	type testData struct {
		testID             string
		initState          *softwareManager
		initStatus         *cmserver.UpdateStatus
		initServices       []ServiceStatus
		initLayers         []LayerStatus
		desiredStatus      *cloudprotocol.DesiredStatus
		downloadTime       time.Duration
		downloadResult     map[string]*downloadResult
		triggerUpdate      bool
		updateError        error
		updateWaitStatuses []cmserver.UpdateStatus
		requestRebalancing bool
	}

	updateLayers := []cloudprotocol.LayerInfo{
		{
			LayerID: "layer1",
			Digest:  "digest1",
			Version: "1.0.0",
		},
		{
			LayerID: "layer2",
			Digest:  "digest2",
			Version: "2.0.0",
		},
	}

	updateServices := []cloudprotocol.ServiceInfo{
		{
			ServiceID: "service1",
			Version:   "1.0.0",
		},
		{
			ServiceID: "service2",
			Version:   "2.0.0",
		},
	}

	updateTimeSlots := []cloudprotocol.TimeSlot{{
		Start: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
		End:   aostypes.Time{Time: time.Date(0, 1, 1, 23, 59, 59, 999999, time.Local)},
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
			desiredStatus: &cloudprotocol.DesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "new services",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			initServices: []ServiceStatus{
				{
					ServiceStatus: cloudprotocol.ServiceStatus{
						ServiceID: updateServices[0].ServiceID,
						Version:   updateServices[0].Version,
						Status:    cloudprotocol.InstalledStatus,
					},
				},
				{
					ServiceStatus: cloudprotocol.ServiceStatus{
						ServiceID: updateServices[1].ServiceID,
						Version:   updateServices[1].Version,
						Status:    cloudprotocol.InstalledStatus,
					},
				},
			},
			initLayers: []LayerStatus{
				{
					LayerStatus: cloudprotocol.LayerStatus{
						LayerID: updateLayers[0].LayerID,
						Version: updateLayers[0].Version,
						Digest:  updateLayers[0].Digest,
						Status:  cloudprotocol.InstalledStatus,
					},
				},
				{
					LayerStatus: cloudprotocol.LayerStatus{
						LayerID: updateLayers[1].LayerID,
						Version: updateLayers[1].Version,
						Digest:  updateLayers[1].Digest,
						Status:  cloudprotocol.InstalledStatus,
					},
				},
			},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Layers: updateLayers,
				Services: append(updateServices, cloudprotocol.ServiceInfo{
					ServiceID: "service3",
					Version:   "1.0",
				}, cloudprotocol.ServiceInfo{
					ServiceID: "service4",
					Version:   "1.0",
				}),
			},
			downloadResult: map[string]*downloadResult{
				"service3": {}, "service4": {},
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
			desiredStatus: &cloudprotocol.DesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {Error: "download error"},
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate, Error: &cloudprotocol.ErrorInfo{Message: "download error"}},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "all items download error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest:      {Error: "download error"},
				updateLayers[1].Digest:      {Error: "download error"},
				updateServices[0].ServiceID: {Error: "download error"},
				updateServices[1].ServiceID: {Error: "download error"},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {
					State: cmserver.NoUpdate,
					Error: &cloudprotocol.ErrorInfo{Message: "download error"},
				},
			},
		},
		{
			testID:     "update error",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
			},
			updateError: aoserrors.New("update error"),
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "update error"}},
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
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
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
					updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
				},
				LayerStatuses: map[string]*cloudprotocol.LayerStatus{
					updateLayers[0].Digest: {
						LayerID: updateLayers[0].LayerID,
						Digest:  updateLayers[0].Digest,
						Version: updateLayers[0].Version,
						Status:  cloudprotocol.InstallingStatus,
					},
					updateLayers[1].Digest: {
						LayerID: updateLayers[1].LayerID,
						Digest:  updateLayers[1].Digest,
						Version: updateLayers[1].Version,
						Status:  cloudprotocol.InstallingStatus,
					},
				},
				ServiceStatuses: map[string]*cloudprotocol.ServiceStatus{
					updateServices[0].ServiceID: {
						ServiceID: updateServices[0].ServiceID,
						Version:   updateServices[0].Version,
						Status:    cloudprotocol.InstallingStatus,
					},
					updateServices[1].ServiceID: {
						ServiceID: updateServices[1].ServiceID,
						Version:   updateServices[1].Version,
						Status:    cloudprotocol.InstallingStatus,
					},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "update unit config",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{UnitConfig: &cloudprotocol.UnitConfig{
				Version: "1.0.0",
			}},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID:     "error unit config",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{UnitConfig: &cloudprotocol.UnitConfig{
				Version: "1.0.0",
			}},
			updateError: aoserrors.New("unit config error"),
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "unit config error"}},
			},
		},
		{
			testID:     "timetable update",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{
				SOTASchedule: cloudprotocol.ScheduleRule{
					Type:      cloudprotocol.TimetableUpdate,
					Timetable: updateTimetable,
				},
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
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
			desiredStatus: &cloudprotocol.DesiredStatus{
				SOTASchedule: cloudprotocol.ScheduleRule{
					TTL:  3,
					Type: cloudprotocol.TriggerUpdate,
				},
				Layers: updateLayers, Services: updateServices,
			},
			downloadResult: map[string]*downloadResult{
				updateLayers[0].Digest: {}, updateLayers[1].Digest: {},
				updateServices[0].ServiceID: {}, updateServices[1].ServiceID: {},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading},
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.NoUpdate, Error: &cloudprotocol.ErrorInfo{Message: "update timeout"}},
			},
		},
		{
			testID:     "update node status",
			initStatus: &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DesiredStatus{
				Nodes: []cloudprotocol.NodeStatus{
					{NodeID: "node1", Status: cloudprotocol.NodeStatusPaused},
					{NodeID: "node2", Status: cloudprotocol.NodeStatusPaused},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
		},
		{
			testID: "rebalancing request",
			initState: &softwareManager{
				CurrentState: stateNoUpdate,
				CurrentUpdate: &softwareUpdate{
					RunInstances: []cloudprotocol.InstanceInfo{
						{ServiceID: "service1", SubjectID: "subject1", NumInstances: 1},
						{ServiceID: "service2", SubjectID: "subject2", NumInstances: 1},
					},
				},
				ServiceStatuses: map[string]*cloudprotocol.ServiceStatus{
					"service1": {ServiceID: "service1", Version: "1.0.0"},
					"service2": {ServiceID: "service2", Version: "2.0.0"},
				},
			},
			initServices: []ServiceStatus{
				{
					ServiceStatus: cloudprotocol.ServiceStatus{
						ServiceID: updateServices[0].ServiceID,
						Version:   updateServices[0].Version,
						Status:    cloudprotocol.InstalledStatus,
					},
				},
				{
					ServiceStatus: cloudprotocol.ServiceStatus{
						ServiceID: updateServices[1].ServiceID,
						Version:   updateServices[1].Version,
						Status:    cloudprotocol.InstalledStatus,
					},
				},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating},
				{State: cmserver.NoUpdate},
			},
			requestRebalancing: true,
		},
	}

	nodeManager := NewTestNodeManager([]cloudprotocol.NodeInfo{
		{NodeID: "node1", NodeType: "type1", Status: cloudprotocol.NodeStatusProvisioned},
		{NodeID: "node2", NodeType: "type2", Status: cloudprotocol.NodeStatusProvisioned},
	})
	unitConfigUpdater := NewTestUnitConfigUpdater(cloudprotocol.UnitConfigStatus{})
	softwareUpdater := NewTestSoftwareUpdater(nil, nil)
	instanceRunner := NewTestInstanceRunner()
	softwareDownloader := newTestGroupDownloader()
	testStorage := NewTestStorage()

	for _, item := range data {
		t.Logf("Test item: %s", item.testID)

		softwareDownloader.result = item.downloadResult
		softwareDownloader.sotaReleased = false
		softwareDownloader.downloadTime = item.downloadTime
		softwareUpdater.AllServices = item.initServices
		softwareUpdater.AllLayers = item.initLayers
		softwareUpdater.UpdateError = item.updateError
		unitConfigUpdater.UpdateError = item.updateError

		if err := testStorage.saveSoftwareState(item.initState); err != nil {
			t.Errorf("Can't save init state: %s", err)
			continue
		}

		// Create software manager

		softwareManager, err := newSoftwareManager(newTestStatusHandler(), softwareDownloader, nodeManager,
			unitConfigUpdater, softwareUpdater, instanceRunner, testStorage, 30*time.Second)
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

		if item.requestRebalancing {
			if err = softwareManager.requestRebalancing(); err != nil {
				t.Errorf("Rebalancing failed: %s", err)
			}
		}

		for _, expectedStatus := range item.updateWaitStatuses {
			if expectedStatus.State == cmserver.Updating {
				if _, err := instanceRunner.WaitForRunInstance(time.Second); err != nil {
					t.Errorf("Wait run instances error: %v", err)
				}

				softwareManager.processRunStatus(nil)
			}

			if err = waitForSOTAUpdateStatus(softwareManager.statusChannel, expectedStatus); err != nil {
				t.Errorf("Wait for update status error: %s", err)

				if strings.Contains(err.Error(), "status timeout") {
					goto closeSM
				}
			}
		}

		if item.desiredStatus != nil && item.desiredStatus.Nodes != nil {
			for _, nodeStatus := range item.desiredStatus.Nodes {
				nodeInfo, err := nodeManager.GetNodeInfo(nodeStatus.NodeID)
				if err != nil {
					t.Errorf("Get node info error: %v", err)
				}

				if nodeInfo.Status != nodeStatus.Status {
					t.Errorf("Wrong node status: %v", nodeInfo.Status)
				}
			}
		}

	closeSM:
		// Close software manager

		if err = softwareManager.close(); err != nil {
			t.Errorf("Error closing firmware manager: %s", err)
		}

		if !softwareDownloader.sotaReleased {
			t.Error("SOTA downloads should be released")
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
							Start: aostypes.Time{Time: time.Date(0, 1, 2, 0, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
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
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 2, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			err: "end value should contain only time",
		},
		{
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 1, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
						},
					},
				},
			},
			err: "start value should be before end value",
		},
		{
			fromDate: time.Date(1, 1, 1, 0, 0, 0, 0, time.Local),
			timetable: []cloudprotocol.TimetableEntry{
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 0, 0, 0, 0, time.Local)},
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
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
						},
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 14, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 3, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 16, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 18, 0, 0, 0, time.Local)},
						},
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 20, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 22, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 1, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
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
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
						},
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 14, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 2, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 16, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 18, 0, 0, 0, time.Local)},
						},
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 20, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 22, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 3, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 4, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 12, 0, 0, 0, time.Local)},
						},
					},
				},
				{
					DayOfWeek: 5, TimeSlots: []cloudprotocol.TimeSlot{
						{
							Start: aostypes.Time{Time: time.Date(0, 1, 1, 8, 0, 0, 0, time.Local)},
							End:   aostypes.Time{Time: time.Date(0, 1, 1, 10, 0, 0, 0, time.Local)},
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
 * TestNodeManager
 **********************************************************************************************************************/

func NewTestNodeManager(nodesInfo []cloudprotocol.NodeInfo) *TestNodeManager {
	nodesInfoMap := make(map[string]*cloudprotocol.NodeInfo)

	for _, nodeInfo := range nodesInfo {
		nodesInfoMap[nodeInfo.NodeID] = &cloudprotocol.NodeInfo{}
		*nodesInfoMap[nodeInfo.NodeID] = nodeInfo
	}

	return &TestNodeManager{
		nodesInfo: nodesInfoMap,
	}
}

func (manager *TestNodeManager) GetAllNodeIDs() ([]string, error) {
	nodeIDs := make([]string, 0, len(manager.nodesInfo))

	for nodeID := range manager.nodesInfo {
		nodeIDs = append(nodeIDs, nodeID)
	}

	return nodeIDs, nil
}

func (manager *TestNodeManager) GetNodeInfo(nodeID string) (cloudprotocol.NodeInfo, error) {
	nodeInfo, ok := manager.nodesInfo[nodeID]
	if !ok {
		return cloudprotocol.NodeInfo{}, aoserrors.New("node not found")
	}

	return *nodeInfo, nil
}

func (manager *TestNodeManager) SubscribeNodeInfoChange() <-chan cloudprotocol.NodeInfo {
	manager.nodeInfoChannel = make(chan cloudprotocol.NodeInfo, 1)

	return manager.nodeInfoChannel
}

func (manager *TestNodeManager) NodeInfoChanged(nodeInfo cloudprotocol.NodeInfo) {
	if _, ok := manager.nodesInfo[nodeInfo.NodeID]; !ok {
		manager.nodesInfo[nodeInfo.NodeID] = &cloudprotocol.NodeInfo{}
	}

	*manager.nodesInfo[nodeInfo.NodeID] = nodeInfo

	if manager.nodeInfoChannel != nil {
		manager.nodeInfoChannel <- nodeInfo
	}
}

func (manager *TestNodeManager) GetAllNodesInfo() []cloudprotocol.NodeInfo {
	nodesInfo := make([]cloudprotocol.NodeInfo, 0, len(manager.nodesInfo))

	for _, nodeInfo := range manager.nodesInfo {
		nodesInfo = append(nodesInfo, *nodeInfo)
	}

	return nodesInfo
}

func (manager *TestNodeManager) PauseNode(nodeID string) error {
	if _, ok := manager.nodesInfo[nodeID]; !ok {
		return aoserrors.New("node not found")
	}

	manager.nodesInfo[nodeID].Status = cloudprotocol.NodeStatusPaused

	return nil
}

func (manager *TestNodeManager) ResumeNode(nodeID string) error {
	if _, ok := manager.nodesInfo[nodeID]; !ok {
		return aoserrors.New("node not found")
	}

	manager.nodesInfo[nodeID].Status = cloudprotocol.NodeStatusProvisioned

	return nil
}

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

func (sender *TestSender) SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
	sender.Consumer = consumer

	return nil
}

/***********************************************************************************************************************
 * TestUnitConfigUpdater
 **********************************************************************************************************************/

func NewTestUnitConfigUpdater(unitConfigsStatus cloudprotocol.UnitConfigStatus) *TestUnitConfigUpdater {
	return &TestUnitConfigUpdater{UnitConfigStatus: unitConfigsStatus}
}

func (updater *TestUnitConfigUpdater) GetStatus() (cloudprotocol.UnitConfigStatus, error) {
	return updater.UnitConfigStatus, nil
}

func (updater *TestUnitConfigUpdater) CheckUnitConfig(unitConfig cloudprotocol.UnitConfig) error {
	return updater.UpdateError
}

func (updater *TestUnitConfigUpdater) UpdateUnitConfig(unitConfig cloudprotocol.UnitConfig) (err error) {
	return updater.UpdateError
}

/***********************************************************************************************************************
 * TestFirmwareUpdater
 **********************************************************************************************************************/

func NewTestFirmwareUpdater(componentsInfo []cloudprotocol.ComponentStatus) (updater *TestFirmwareUpdater) {
	return &TestFirmwareUpdater{
		InitComponentsInfo:   componentsInfo,
		newComponentsChannel: make(chan []cloudprotocol.ComponentStatus, 1),
	}
}

func (updater *TestFirmwareUpdater) GetStatus() (info []cloudprotocol.ComponentStatus, err error) {
	return updater.InitComponentsInfo, nil
}

func (updater *TestFirmwareUpdater) UpdateComponents(
	components []cloudprotocol.ComponentInfo, chains []cloudprotocol.CertificateChain,
	certs []cloudprotocol.Certificate,
) (componentsInfo []cloudprotocol.ComponentStatus, err error) {
	time.Sleep(updater.UpdateTime)
	return updater.UpdateComponentsInfo, updater.UpdateError
}

func (updater *TestFirmwareUpdater) NewComponentsChannel() <-chan []cloudprotocol.ComponentStatus {
	return updater.newComponentsChannel
}

func (updater *TestFirmwareUpdater) SetNewComponents(newComponents []cloudprotocol.ComponentStatus) {
	updater.newComponentsChannel <- newComponents
}

/***********************************************************************************************************************
 * TestSoftwareUpdater
 **********************************************************************************************************************/

func NewTestSoftwareUpdater(services []ServiceStatus, layers []LayerStatus) *TestSoftwareUpdater {
	return &TestSoftwareUpdater{AllServices: services, AllLayers: layers}
}

func (updater *TestSoftwareUpdater) GetServicesStatus() ([]ServiceStatus, error) {
	return updater.AllServices, nil
}

func (updater *TestSoftwareUpdater) GetLayersStatus() ([]LayerStatus, error) {
	return updater.AllLayers, nil
}

func (updater *TestSoftwareUpdater) InstallService(serviceInfo cloudprotocol.ServiceInfo,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RestoreService(serviceID string) error {
	return nil
}

func (updater *TestSoftwareUpdater) RemoveService(serviceID string) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RevertService(serviceID string) error {
	updater.RevertedServices = append(updater.RevertedServices, serviceID)

	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) InstallLayer(layerInfo cloudprotocol.LayerInfo,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) error {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RemoveLayer(digest string) error {
	return nil
}

func (updater *TestSoftwareUpdater) RestoreLayer(digest string) error {
	return nil
}

/***********************************************************************************************************************
 * TestInstanceRunner
 **********************************************************************************************************************/

func NewTestInstanceRunner() *TestInstanceRunner {
	return &TestInstanceRunner{runInstanceChan: make(chan []cloudprotocol.InstanceInfo, 1)}
}

func (runner *TestInstanceRunner) RunInstances(instances []cloudprotocol.InstanceInfo, rebalancing bool) error {
	runner.runInstanceChan <- instances

	return nil
}

func (runner *TestInstanceRunner) WaitForRunInstance(timeout time.Duration) ([]cloudprotocol.InstanceInfo, error) {
	select {
	case receivedRunInstances := <-runner.runInstanceChan:
		return receivedRunInstances, nil

	case <-time.After(timeout):
		return nil, aoserrors.New("receive run instances timeout")
	}
}

/***********************************************************************************************************************
 * TestSystemQuotaAlertProvider
 **********************************************************************************************************************/

func NewTestSystemQuotaAlertProvider() *TestSystemQuotaAlertProvider {
	return &TestSystemQuotaAlertProvider{alertsChannel: make(chan cloudprotocol.SystemQuotaAlert, 1)}
}

func (provider *TestSystemQuotaAlertProvider) GetSystemQuoteAlertChannel() <-chan cloudprotocol.SystemQuotaAlert {
	return provider.alertsChannel
}

func (provider *TestSystemQuotaAlertProvider) SendSystemQuotaAlert(alert cloudprotocol.SystemQuotaAlert) {
	provider.alertsChannel <- alert
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

func (testDownloader *TestDownloader) Download(
	ctx context.Context, packageInfo downloader.PackageInfo,
) (result downloader.Result, err error) {
	file, err := os.CreateTemp(tmpDir, "*.dec")
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

func (testDownloader *TestDownloader) Release(filePath string) error {
	if err := os.RemoveAll(filePath); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (testDownloader *TestDownloader) ReleaseByType(targetType string) error {
	return nil
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

/***********************************************************************************************************************
 * testGroupDownloader
 **********************************************************************************************************************/

func newTestGroupDownloader() *testGroupDownloader {
	return &testGroupDownloader{}
}

func (downloader *testGroupDownloader) download(
	ctx context.Context, request map[string]downloader.PackageInfo, continueOnError bool, updateStatus statusNotifier,
) (result map[string]*downloadResult) {
	for id := range request {
		updateStatus(id, cloudprotocol.DownloadingStatus, nil)
	}

	select {
	case <-time.After(downloader.downloadTime):
		if getDownloadError(downloader.result) != nil && !continueOnError {
			for id := range request {
				if result, ok := downloader.result[id]; ok {
					if result.Error == "" {
						result.Error = aoserrors.Wrap(context.Canceled).Error()
					}

					continue
				}

				log.Errorf("Download result for id: %s not found", id)
			}
		}

		for id := range request {
			if _, ok := downloader.result[id]; !ok {
				log.Fatalf("Download result for id: %s not found", id)
			}

			if downloader.result[id].Error != "" {
				updateStatus(id, cloudprotocol.ErrorStatus,
					&cloudprotocol.ErrorInfo{Message: downloader.result[id].Error})
			} else {
				updateStatus(id, cloudprotocol.DownloadedStatus, nil)
			}
		}

		return downloader.result

	case <-ctx.Done():
		for id := range request {
			downloader.result[id].Error = aoserrors.Wrap(context.Canceled).Error()
			updateStatus(id, cloudprotocol.ErrorStatus, &cloudprotocol.ErrorInfo{Message: downloader.result[id].Error})
		}

		return result
	}
}

func (downloader *testGroupDownloader) releaseDownloadedFirmware() error {
	downloader.fotaReleased = true

	return nil
}

func (downloader *testGroupDownloader) releaseDownloadedSoftware() error {
	downloader.sotaReleased = true

	return nil
}

/***********************************************************************************************************************
 * testStatusHandler
 **********************************************************************************************************************/

func newTestStatusHandler() *testStatusHandler {
	return &testStatusHandler{}
}

func (statusHandler *testStatusHandler) updateComponentStatus(componentInfo cloudprotocol.ComponentStatus) {
	log.WithFields(log.Fields{
		"id":      componentInfo.ComponentID,
		"version": componentInfo.Version,
		"status":  componentInfo.Status,
		"error":   componentInfo.ErrorInfo,
	}).Debug("Update component status")
}

func (statusHandler *testStatusHandler) updateUnitConfigStatus(unitConfigInfo cloudprotocol.UnitConfigStatus) {
	log.WithFields(log.Fields{
		"version": unitConfigInfo.Version,
		"status":  unitConfigInfo.Status,
		"error":   unitConfigInfo.ErrorInfo,
	}).Debug("Update unit config status")
}

func (statusHandler *testStatusHandler) updateLayerStatus(layerInfo cloudprotocol.LayerStatus) {
	log.WithFields(log.Fields{
		"id":      layerInfo.LayerID,
		"digest":  layerInfo.Digest,
		"version": layerInfo.Version,
		"status":  layerInfo.Status,
		"error":   layerInfo.ErrorInfo,
	}).Debug("Update layer status")
}

func (statusHandler *testStatusHandler) updateServiceStatus(serviceInfo cloudprotocol.ServiceStatus) {
	log.WithFields(log.Fields{
		"id":      serviceInfo.ServiceID,
		"version": serviceInfo.Version,
		"status":  serviceInfo.Status,
		"error":   serviceInfo.ErrorInfo,
	}).Debug("Update service status")
}

func (statusHandler *testStatusHandler) setInstancesStatus(statuses []cloudprotocol.InstanceStatus) {
	for _, instanceStatus := range statuses {
		log.WithFields(log.Fields{
			"serviceID":  instanceStatus.ServiceID,
			"subjectID":  instanceStatus.SubjectID,
			"instanceID": instanceStatus.Instance,
			"version":    instanceStatus.ServiceVersion,
			"error":      instanceStatus.ErrorInfo,
			"nodeID":     instanceStatus.NodeID,
		}).Debug("Update instance status")
	}
}

func (statusHandler *testStatusHandler) getNodesStatus() ([]cloudprotocol.NodeStatus, error) {
	return []cloudprotocol.NodeStatus{}, nil
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

	if expectedStatus.Error == nil && comparedStatus.Error == nil {
		return nil
	}

	if comparedStatus.Error == nil && expectedStatus.Error != nil ||
		comparedStatus.Error != nil && expectedStatus.Error == nil {
		return aoserrors.Errorf("wrong error: %s", comparedStatus.Error.Message)
	}

	if !strings.Contains(comparedStatus.Error.Message, expectedStatus.Error.Message) {
		return aoserrors.Errorf("wrong error: %s", comparedStatus.Error.Message)
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

func convertToComponentID(id string) *string {
	return &id
}

func convertToDownloadID(component cloudprotocol.ComponentInfo) string {
	return component.ComponentType + ":" + component.Version
}
