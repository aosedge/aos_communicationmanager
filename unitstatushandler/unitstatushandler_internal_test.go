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
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"

	"aos_communicationmanager/cloudprotocol"
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
	ComponentsInfo []cloudprotocol.ComponentInfo
	UpdateError    error
	statusChannel  chan cloudprotocol.ComponentInfo
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
	return &TestFirmwareUpdater{ComponentsInfo: componentsInfo, statusChannel: make(chan cloudprotocol.ComponentInfo)}
}

func (updater *TestFirmwareUpdater) GetStatus() (info []cloudprotocol.ComponentInfo, err error) {
	return updater.ComponentsInfo, nil
}

func (updater *TestFirmwareUpdater) UpdateComponents(components []cloudprotocol.ComponentInfoFromCloud) (err error) {
	for _, component := range components {
		componentInfo := cloudprotocol.ComponentInfo{
			ID:            component.ID,
			AosVersion:    component.AosVersion,
			VendorVersion: component.VendorVersion,
			Status:        cloudprotocol.InstalledStatus,
		}

		if updater.UpdateError != nil {
			componentInfo.Status = cloudprotocol.ErrorStatus
			componentInfo.Error = updater.UpdateError.Error()
		}

		updater.statusChannel <- componentInfo
	}

	return nil
}

func (updater *TestFirmwareUpdater) StatusChannel() (statusChannel <-chan cloudprotocol.ComponentInfo) {
	return updater.statusChannel
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
