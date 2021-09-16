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

package unitstatushandler_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
	"aos_communicationmanager/downloader"
	"aos_communicationmanager/unitstatushandler"
)

/*******************************************************************************
 * Consts
 ******************************************************************************/

const waitStatusTimeout = 5 * time.Second

/*******************************************************************************
 * Types
 ******************************************************************************/

type testSender struct {
	statusChannel chan cloudprotocol.UnitStatus
}

type testBoardConfigUpdater struct {
	boardConfigInfo cloudprotocol.BoardConfigInfo
	updateVersion   string
	updateError     error
}

type testFirmwareUpdater struct {
	componentsInfo []cloudprotocol.ComponentInfo
	statusChannel  chan cloudprotocol.ComponentInfo
	updateError    error
}

type testSoftwareUpdater struct {
	servicesInfo []cloudprotocol.ServiceInfo
	layersInfo   []cloudprotocol.LayerInfo
	updateError  error
}

type testDownloader struct {
}

type testResult struct {
	fileName string
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var cfg = &config.Config{UnitStatusSendTimeout: config.Duration{Duration: 3 * time.Second}}
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

func TestSendInitialStatus(t *testing.T) {
	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{
			{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
		},
		Components: []cloudprotocol.ComponentInfo{
			{ID: "comp0", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "1.1", Status: cloudprotocol.InstalledStatus},
			{ID: "comp2", VendorVersion: "1.2", Status: cloudprotocol.InstalledStatus},
		},
		Layers: []cloudprotocol.LayerInfo{
			{ID: "layer0", Digest: "digest0", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "layer1", Digest: "digest1", AosVersion: 2, Status: cloudprotocol.InstalledStatus},
			{ID: "layer2", Digest: "digest2", AosVersion: 3, Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceInfo{
			{ID: "service0", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service1", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service2", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
		},
	}

	boardConfigUpdater := newTestBoardConfigUpdater(expectedUnitStatus.BoardConfig[0])
	fotaUpdater := newTestFirmwareUpdater(expectedUnitStatus.Components)
	sotaUpdater := newTestSoftwareUpdater(expectedUnitStatus.Services, expectedUnitStatus.Layers)
	sender := newTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, fotaUpdater, sotaUpdater, &testDownloader{}, sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	receivedUnitStatus, err := sender.waitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateBoardConfig(t *testing.T) {
	boardConfigUpdater := newTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	fotaUpdater := newTestFirmwareUpdater(nil)
	sotaUpdater := newTestSoftwareUpdater(nil, nil)
	sender := newTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, fotaUpdater, sotaUpdater, &testDownloader{}, sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	boardConfigUpdater.boardConfigInfo = cloudprotocol.BoardConfigInfo{VendorVersion: "1.1", Status: cloudprotocol.InstalledStatus}
	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers:      []cloudprotocol.LayerInfo{},
		Services:    []cloudprotocol.ServiceInfo{},
	}

	boardConfigUpdater.updateVersion = "1.1"

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")})

	receivedUnitStatus, err := sender.waitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	boardConfigUpdater.updateVersion = "1.2"
	boardConfigUpdater.updateError = aoserrors.New("some error occurs")

	boardConfigUpdater.boardConfigInfo = cloudprotocol.BoardConfigInfo{
		VendorVersion: "1.2", Status: cloudprotocol.ErrorStatus, Error: boardConfigUpdater.updateError.Error()}
	expectedUnitStatus.BoardConfig = append(expectedUnitStatus.BoardConfig, boardConfigUpdater.boardConfigInfo)

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")})

	if receivedUnitStatus, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateComponents(t *testing.T) {
	boardConfigUpdater := newTestBoardConfigUpdater(cloudprotocol.BoardConfigInfo{
		VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := newTestFirmwareUpdater([]cloudprotocol.ComponentInfo{
		{ID: "comp0", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
		{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
		{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
	})
	softwareUpdater := newTestSoftwareUpdater(nil, nil)
	sender := newTestSender()

	statusHandler, err := unitstatushandler.New(cfg,
		boardConfigUpdater, firmwareUpdater, softwareUpdater, &testDownloader{}, sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components: []cloudprotocol.ComponentInfo{
			{ID: "comp0", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers:   []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Components: []cloudprotocol.ComponentInfoFromCloud{
			{ID: "comp0", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
			{ID: "comp2", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
		},
	})

	receivedUnitStatus, err := sender.waitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	firmwareUpdater.updateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components: []cloudprotocol.ComponentInfo{
			{ID: "comp0", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "2.0", Status: cloudprotocol.ErrorStatus,
				Error: firmwareUpdater.updateError.Error()},
			{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers:   []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Components: []cloudprotocol.ComponentInfoFromCloud{
			{ID: "comp1", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
		}})

	if receivedUnitStatus, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateLayers(t *testing.T) {
	boardConfigUpdater := newTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := newTestFirmwareUpdater(nil)
	softwareUpdater := newTestSoftwareUpdater(nil, []cloudprotocol.LayerInfo{
		{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	})
	sender := newTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, firmwareUpdater, softwareUpdater, &testDownloader{}, sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers: []cloudprotocol.LayerInfo{
			{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer3", Digest: "digest3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "layer4", Digest: "digest4", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceInfo{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Layers: []cloudprotocol.LayerInfoFromCloud{
			{ID: "layer1", Digest: "digest1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0}},
			{ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
			{ID: "layer4", Digest: "digest4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
		}})

	receivedUnitStatus, err := sender.waitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	softwareUpdater.updateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers: []cloudprotocol.LayerInfo{
			{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.ErrorStatus, Error: softwareUpdater.updateError.Error()},
			{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer3", Digest: "digest3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "layer4", Digest: "digest4", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "layer5", Digest: "digest5", AosVersion: 1, Status: cloudprotocol.ErrorStatus, Error: softwareUpdater.updateError.Error()},
		},
		Services: []cloudprotocol.ServiceInfo{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Layers: []cloudprotocol.LayerInfoFromCloud{
			{ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
			{ID: "layer4", Digest: "digest4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
			{ID: "layer5", Digest: "digest5", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
		}})

	if receivedUnitStatus, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateServices(t *testing.T) {
	boardConfigUpdater := newTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := newTestFirmwareUpdater(nil)
	softwareUpdater := newTestSoftwareUpdater([]cloudprotocol.ServiceInfo{
		{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	}, nil)
	sender := newTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, firmwareUpdater, softwareUpdater, &testDownloader{}, sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.waitForStatus(5 * time.Second); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers:      []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{
			{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "service1", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service2", Status: cloudprotocol.RemovedStatus},
			{ID: "service3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Services: []cloudprotocol.ServiceInfoFromCloud{
			{ID: "service0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0}},
			{ID: "service1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
			{ID: "service3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
		}})

	receivedUnitStatus, err := sender.waitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	softwareUpdater.updateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.boardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers:      []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{
			{ID: "service0", AosVersion: 0, Status: cloudprotocol.ErrorStatus,
				Error: softwareUpdater.updateError.Error()},
			{ID: "service1", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service2", Status: cloudprotocol.RemovedStatus},
			{ID: "service3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service3", AosVersion: 2, Status: cloudprotocol.ErrorStatus, Error: softwareUpdater.updateError.Error()},
			{ID: "service4", AosVersion: 2, Status: cloudprotocol.ErrorStatus, Error: softwareUpdater.updateError.Error()},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Services: []cloudprotocol.ServiceInfoFromCloud{
			{ID: "service1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1}},
			{ID: "service3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2}},
			{ID: "service4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2}},
		}})

	if receivedUnitStatus, err = sender.waitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func compareStatus(len1, len2 int, compare func(index1, index2 int) bool) (err error) {
	if len1 != len2 {
		return aoserrors.New("data mismatch")
	}

	for index1 := 0; index1 < len1; index1++ {
		found := false

		for index2 := 0; index2 < len2; index2++ {
			if compare(index1, index2) {
				found = true
				break
			}
		}

		if !found {
			return aoserrors.New("data mismatch")
		}
	}

	for index2 := 0; index2 < len2; index2++ {
		found := false

		for index1 := 0; index1 < len1; index1++ {
			if compare(index1, index2) {
				found = true
				break
			}
		}

		if !found {
			return aoserrors.New("data mismatch")
		}
	}

	return nil
}

func compareUnitStatus(status1, status2 cloudprotocol.UnitStatus) (err error) {
	if err = compareStatus(len(status1.BoardConfig), len(status2.BoardConfig),
		func(index1, index2 int) (result bool) {
			return status1.BoardConfig[index1] == status2.BoardConfig[index2]
		}); err != nil {
		return err
	}

	if err = compareStatus(len(status1.Components), len(status2.Components),
		func(index1, index2 int) (result bool) {
			return status1.Components[index1] == status2.Components[index2]
		}); err != nil {
		return err
	}

	if err = compareStatus(len(status1.Layers), len(status2.Layers),
		func(index1, index2 int) (result bool) {
			return status1.Layers[index1] == status2.Layers[index2]
		}); err != nil {
		return err
	}

	if err = compareStatus(len(status1.Services), len(status2.Services),
		func(index1, index2 int) (result bool) {
			return status1.Services[index1] == status2.Services[index2]
		}); err != nil {
		return err
	}

	return nil
}

/*******************************************************************************
 * testSender
 ******************************************************************************/

func newTestSender() (sender *testSender) {
	return &testSender{statusChannel: make(chan cloudprotocol.UnitStatus, 1)}
}

func (sender *testSender) SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error) {
	sender.statusChannel <- unitStatus

	return nil
}

func (sender *testSender) waitForStatus(timeout time.Duration) (status cloudprotocol.UnitStatus, err error) {
	select {
	case receivedUnitStatus := <-sender.statusChannel:
		return receivedUnitStatus, nil

	case <-time.After(timeout):
		return status, aoserrors.New("receive status timeout")
	}
}

/*******************************************************************************
 * testBoardConfigUpdater
 ******************************************************************************/

func newTestBoardConfigUpdater(boardConfigInfo cloudprotocol.BoardConfigInfo) (updater *testBoardConfigUpdater) {
	return &testBoardConfigUpdater{boardConfigInfo: boardConfigInfo}
}

func (updater *testBoardConfigUpdater) GetStatus() (info cloudprotocol.BoardConfigInfo, err error) {
	return updater.boardConfigInfo, nil
}

func (updater *testBoardConfigUpdater) CheckBoardConfig(configJSON json.RawMessage) (version string, err error) {
	return updater.updateVersion, updater.updateError
}

func (updater *testBoardConfigUpdater) UpdateBoardConfig(configJSON json.RawMessage) (err error) {
	return updater.updateError
}

/*******************************************************************************
 * testFirmwareUpdater
 ******************************************************************************/

func newTestFirmwareUpdater(componentsInfo []cloudprotocol.ComponentInfo) (updater *testFirmwareUpdater) {
	return &testFirmwareUpdater{componentsInfo: componentsInfo, statusChannel: make(chan cloudprotocol.ComponentInfo)}
}

func (updater *testFirmwareUpdater) GetStatus() (info []cloudprotocol.ComponentInfo, err error) {
	return updater.componentsInfo, updater.updateError
}

func (updater *testFirmwareUpdater) UpdateComponents(components []cloudprotocol.ComponentInfoFromCloud) (err error) {
	for _, component := range components {
		componentInfo := cloudprotocol.ComponentInfo{
			ID:            component.ID,
			AosVersion:    component.AosVersion,
			VendorVersion: component.VendorVersion,
			Status:        cloudprotocol.InstalledStatus,
		}

		if updater.updateError != nil {
			componentInfo.Status = cloudprotocol.ErrorStatus
			componentInfo.Error = updater.updateError.Error()
		}

		updater.statusChannel <- componentInfo
	}

	return nil
}

func (updater *testFirmwareUpdater) StatusChannel() (statusChannel <-chan cloudprotocol.ComponentInfo) {
	return updater.statusChannel
}

/*******************************************************************************
 * testSoftwareUpdater
 ******************************************************************************/

func newTestSoftwareUpdater(servicesInfo []cloudprotocol.ServiceInfo,
	layersInfo []cloudprotocol.LayerInfo) (updater *testSoftwareUpdater) {
	return &testSoftwareUpdater{servicesInfo: servicesInfo, layersInfo: layersInfo}
}

func (updater *testSoftwareUpdater) SetUsers(users []string) (err error) {
	return updater.updateError
}

func (updater *testSoftwareUpdater) GetStatus() (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	return updater.servicesInfo, updater.layersInfo, updater.updateError
}

func (updater *testSoftwareUpdater) InstallService(
	serviceInfo cloudprotocol.ServiceInfoFromCloud) (stateChecksum string, err error) {
	return "", updater.updateError
}

func (updater *testSoftwareUpdater) RemoveService(serviceInfo cloudprotocol.ServiceInfo) (err error) {
	return updater.updateError
}

func (updater *testSoftwareUpdater) InstallLayer(layerInfo cloudprotocol.LayerInfoFromCloud) (err error) {
	return updater.updateError
}

func (updater *testSoftwareUpdater) RemoveLayer(layerInfo cloudprotocol.LayerInfo) (err error) {
	return updater.updateError
}

/*******************************************************************************
 * testSoftwareUpdater
 ******************************************************************************/

func (testDownloader *testDownloader) DownloadAndDecrypt(
	ctx context.Context, packageInfo cloudprotocol.DecryptDataStruct,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result downloader.Result, err error) {

	file, err := ioutil.TempFile(tmpDir, "*.dec")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	return &testResult{fileName: file.Name()}, nil
}

func (result *testResult) GetFileName() (fileName string) { return result.fileName }

func (result *testResult) Wait() (err error) { return nil }

func (result *testResult) Release() {
	os.RemoveAll(result.fileName)
}
