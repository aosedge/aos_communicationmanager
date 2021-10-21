// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/cmserver"
	"aos_communicationmanager/config"
	"aos_communicationmanager/downloader"
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
	BoardConfigInfo cloudprotocol.BoardConfigInfo
	UpdateVersion   string
	UpdateError     error
}

type TestFirmwareUpdater struct {
	UpdateTime           time.Duration
	InitComponentsInfo   []cloudprotocol.ComponentInfo
	UpdateComponentsInfo []cloudprotocol.ComponentInfo
	UpdateError          error
}

type TestSoftwareUpdater struct {
	LayersInfo   []cloudprotocol.LayerInfo
	ServicesInfo []cloudprotocol.ServiceInfo
	UpdateError  error
}

type TestDownloader struct {
	errorURL     string
	downloadErr  error
	DownloadTime time.Duration
}

type TestResult struct {
	ctx          context.Context
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
		FullTimestamp:    true})
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
		NewTestBoardConfigUpdater(cloudprotocol.BoardConfigInfo{}), NewTestFirmwareUpdater(nil),
		NewTestSoftwareUpdater(nil, nil), testDownloader, NewTestSender())
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
			downloadError:   errors.New("download error"),
			continueOnError: false,
			check:           map[string]int{"0": downloadCanceled, "1": downloadError, "2": downloadCanceled},
			downloadTime:    1 * time.Second},
		{
			request:         map[string]cloudprotocol.DecryptDataStruct{"0": {}, "1": {URLs: []string{"1"}}, "2": {}},
			errorURL:        "1",
			downloadError:   errors.New("download error"),
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
					"id": id, "status": status, "error": componentErr}).Debug("Component download status")
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
		desiredStatus           *cloudprotocol.DecodedDesiredStatus
		downloadTime            time.Duration
		downloadResult          map[string]*downloadResult
		updateTime              time.Duration
		updateComponentStatuses []cloudprotocol.ComponentInfo
		boardConfigError        error
		triggerUpdate           bool
		updateWaitStatuses      []cmserver.UpdateStatus
	}

	updateComponents := []cloudprotocol.ComponentInfoFromCloud{
		{
			ID:                "comp1",
			VersionFromCloud:  cloudprotocol.VersionFromCloud{VendorVersion: "1.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{1}},
		},
		{
			ID:                "comp2",
			VersionFromCloud:  cloudprotocol.VersionFromCloud{VendorVersion: "2.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{2}},
		},
	}

	otherUpdateComponents := []cloudprotocol.ComponentInfoFromCloud{
		{
			ID:                "comp3",
			VersionFromCloud:  cloudprotocol.VersionFromCloud{VendorVersion: "3.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
		},
		{
			ID:                "comp4",
			VersionFromCloud:  cloudprotocol.VersionFromCloud{VendorVersion: "4.0"},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{4}},
		},
	}

	data := []testData{
		{
			testID:        "success update",
			initStatus:    &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating}, {State: cmserver.NoUpdate}},
		},
		{
			testID:        "download error",
			initStatus:    &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {Error: "download error"},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.NoUpdate, Error: "download error"}},
		},
		{
			testID:        "update error",
			initStatus:    &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: updateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.ErrorStatus, Error: "update error"},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating}, {State: cmserver.NoUpdate, Error: "update error"}},
		},
		{
			testID: "continue download on startup",
			initState: &firmwareManager{
				CurrentState:  stateDownloading,
				CurrentUpdate: &firmwareUpdate{Components: updateComponents},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.Downloading},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID: {},
				updateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.ReadyToUpdate}, {State: cmserver.Updating}, {State: cmserver.NoUpdate}},
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
				ComponentStatuses: map[string]*cloudprotocol.ComponentInfo{
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
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
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
				ComponentStatuses: map[string]*cloudprotocol.ComponentInfo{
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
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
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
					Components: updateComponents},
				DownloadResult: map[string]*downloadResult{
					updateComponents[0].ID: {},
					updateComponents[1].ID: {},
				},
				ComponentStatuses: map[string]*cloudprotocol.ComponentInfo{
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
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Components:   updateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
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
			initStatus:    &cmserver.UpdateStatus{State: cmserver.Downloading},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				updateComponents[0].ID:      {},
				updateComponents[1].ID:      {},
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: context.Canceled.Error()},
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating}, {State: cmserver.NoUpdate},
			},
		},
		{
			testID: "new update on ready to update state",
			initState: &firmwareManager{
				CurrentState: stateReadyToUpdate,
				CurrentUpdate: &firmwareUpdate{
					Schedule:   cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
					Components: updateComponents},
			},
			initStatus: &cmserver.UpdateStatus{State: cmserver.ReadyToUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{
				Components:   otherUpdateComponents,
				FOTASchedule: cloudprotocol.ScheduleRule{Type: cloudprotocol.TriggerUpdate},
			},
			downloadResult: map[string]*downloadResult{
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate, Error: context.Canceled.Error()},
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
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
				ComponentStatuses: map[string]*cloudprotocol.ComponentInfo{
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
			initStatus:    &cmserver.UpdateStatus{State: cmserver.Updating},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{Components: otherUpdateComponents},
			downloadResult: map[string]*downloadResult{
				otherUpdateComponents[0].ID: {},
				otherUpdateComponents[1].ID: {},
			},
			downloadTime: 1 * time.Second,
			updateTime:   1 * time.Second,
			updateComponentStatuses: []cloudprotocol.ComponentInfo{
				{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp3", VendorVersion: "3.0", Status: cloudprotocol.InstalledStatus},
				{ID: "comp4", VendorVersion: "4.0", Status: cloudprotocol.InstalledStatus},
			},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.NoUpdate},
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating}, {State: cmserver.NoUpdate},
			},
		},
		{
			testID:        "update board config",
			initStatus:    &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus: &cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")},
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.ReadyToUpdate},
				{State: cmserver.Updating}, {State: cmserver.NoUpdate}},
		},
		{
			testID:           "error board config",
			initStatus:       &cmserver.UpdateStatus{State: cmserver.NoUpdate},
			desiredStatus:    &cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")},
			boardConfigError: errors.New("board config error"),
			updateWaitStatuses: []cmserver.UpdateStatus{
				{State: cmserver.Downloading}, {State: cmserver.NoUpdate, Error: "board config error"}},
		},
	}

	firmwareUpdater := NewTestFirmwareUpdater(nil)
	boardConfigUpdater := NewTestBoardConfigUpdater(cloudprotocol.BoardConfigInfo{})
	statusHandler := newTestStatusHandler()
	testStorage := NewTestStorage()

	for _, item := range data {
		t.Logf("Test item: %s", item.testID)

		statusHandler.result = item.downloadResult
		statusHandler.downloadTime = item.downloadTime
		firmwareUpdater.UpdateComponentsInfo = item.updateComponentStatuses
		firmwareUpdater.UpdateTime = item.updateTime
		boardConfigUpdater.UpdateError = item.boardConfigError

		if err := testStorage.saveFirmwareState(item.initState); err != nil {
			t.Errorf("Can't save init state: %s", err)
			continue
		}

		// Create firmware manager

		firmwareManager, err := newFirmwareManager(statusHandler, firmwareUpdater, boardConfigUpdater, testStorage)
		if err != nil {
			t.Errorf("Can't create firmware manager: %s", err)
			continue
		}

		// Check init status

		if item.initStatus != nil {
			if err = compareStatuses(*item.initStatus, firmwareManager.getCurrentStatus()); err != nil {
				t.Errorf("Wrong init status: %s", err)
			}
		}

		// Process desired status

		if item.desiredStatus != nil {
			if err = firmwareManager.processDesiredStatus(*item.desiredStatus); err != nil {
				t.Errorf("Process desired status failed: %s", err)
				goto close
			}
		}

		// Trigger update

		if item.triggerUpdate {
			if err = firmwareManager.startUpdate(); err != nil {
				t.Errorf("Start update failed: %s", err)
			}
		}

		for _, expectedStatus := range item.updateWaitStatuses {
			if err = waitForUpdateStatus(firmwareManager.statusChannel, expectedStatus); err != nil {
				t.Errorf("Wait for update status error: %s", err)

				if strings.Contains(err.Error(), "status timeout") {
					goto close
				}
			}
		}

	close:
		// Close firmware manager

		if err = firmwareManager.close(); err != nil {
			t.Errorf("Error closing firmware manager: %s", err)
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

func NewTestBoardConfigUpdater(boardConfigInfo cloudprotocol.BoardConfigInfo) (updater *TestBoardConfigUpdater) {
	return &TestBoardConfigUpdater{BoardConfigInfo: boardConfigInfo}
}

func (updater *TestBoardConfigUpdater) GetStatus() (info cloudprotocol.BoardConfigInfo, err error) {
	return updater.BoardConfigInfo, nil
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

func NewTestFirmwareUpdater(componentsInfo []cloudprotocol.ComponentInfo) (updater *TestFirmwareUpdater) {
	return &TestFirmwareUpdater{InitComponentsInfo: componentsInfo}
}

func (updater *TestFirmwareUpdater) GetStatus() (info []cloudprotocol.ComponentInfo, err error) {
	return updater.InitComponentsInfo, nil
}

func (updater *TestFirmwareUpdater) UpdateComponents(components []cloudprotocol.ComponentInfoFromCloud) (
	componentsInfo []cloudprotocol.ComponentInfo, err error) {
	time.Sleep(updater.UpdateTime)
	return updater.UpdateComponentsInfo, updater.UpdateError
}

/***********************************************************************************************************************
 * TestSoftwareUpdater
 **********************************************************************************************************************/

func NewTestSoftwareUpdater(
	layersInfo []cloudprotocol.LayerInfo,
	servicesInfo []cloudprotocol.ServiceInfo) (updater *TestSoftwareUpdater) {
	return &TestSoftwareUpdater{LayersInfo: layersInfo, ServicesInfo: servicesInfo}
}

func (updater *TestSoftwareUpdater) SetUsers(users []string) (err error) {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) GetStatus() (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	return updater.ServicesInfo, updater.LayersInfo, nil
}

func (updater *TestSoftwareUpdater) InstallLayer(layerInfo cloudprotocol.LayerInfoFromCloud) (err error) {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) RemoveLayer(layerInfo cloudprotocol.LayerInfo) (err error) {
	return updater.UpdateError
}

func (updater *TestSoftwareUpdater) InstallService(
	serviceInfo cloudprotocol.ServiceInfoFromCloud) (stateChecksum string, err error) {
	return "", updater.UpdateError
}

func (updater *TestSoftwareUpdater) RemoveService(serviceInfo cloudprotocol.ServiceInfo) (err error) {
	return updater.UpdateError
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
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result downloader.Result, err error) {

	file, err := ioutil.TempFile(tmpDir, "*.dec")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var downloadErr error

	if len(packageInfo.URLs) != 0 {
		if testDownloader.errorURL == packageInfo.URLs[0] {
			downloadErr = testDownloader.downloadErr
		}
	}

	return &TestResult{
		ctx:          ctx,
		downloadTime: testDownloader.DownloadTime,
		fileName:     file.Name(),
		err:          downloadErr}, nil
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
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result map[string]*downloadResult) {
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

func (statusHandler *testStatusHandler) updateComponentStatus(componentInfo cloudprotocol.ComponentInfo) {
	log.WithFields(log.Fields{
		"id":      componentInfo.ID,
		"version": componentInfo.VendorVersion,
		"status":  componentInfo.Status,
		"error":   componentInfo.Error,
	}).Debug("Update component status")
}

func (statusHandler *testStatusHandler) updateBoardConfigStatus(boardConfigInfo cloudprotocol.BoardConfigInfo) {
	log.WithFields(log.Fields{
		"version": boardConfigInfo.VendorVersion,
		"status":  boardConfigInfo.Status,
		"error":   boardConfigInfo.Error,
	}).Debug("Update board config status")
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

func waitForUpdateStatus(statusChannel <-chan cmserver.UpdateStatus, expectedStatus cmserver.UpdateStatus) (err error) {
	select {
	case status := <-statusChannel:
		if err = compareStatuses(expectedStatus, status); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil

	case <-time.After(waitStatusTimeout):
		return aoserrors.Errorf("wait for %s status timeout", expectedStatus.State)
	}
}
