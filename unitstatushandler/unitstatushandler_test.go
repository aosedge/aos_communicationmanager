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

package unitstatushandler_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const waitStatusTimeout = 5 * time.Second

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var cfg = &config.Config{UnitStatusSendTimeout: config.Duration{Duration: 3 * time.Second}}

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

	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(expectedUnitStatus.BoardConfig[0])
	fotaUpdater := unitstatushandler.NewTestFirmwareUpdater(expectedUnitStatus.Components)
	sotaUpdater := unitstatushandler.NewTestSoftwareUpdater(expectedUnitStatus.Services, expectedUnitStatus.Layers)
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, fotaUpdater, sotaUpdater, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateBoardConfig(t *testing.T) {
	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	fotaUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	sotaUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, fotaUpdater, sotaUpdater, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	go handleUpdateStatus(statusHandler)

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	boardConfigUpdater.BoardConfigInfo = cloudprotocol.BoardConfigInfo{VendorVersion: "1.1", Status: cloudprotocol.InstalledStatus}
	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers:      []cloudprotocol.LayerInfo{},
		Services:    []cloudprotocol.ServiceInfo{},
	}

	boardConfigUpdater.UpdateVersion = "1.1"

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	boardConfigUpdater.UpdateVersion = "1.2"
	boardConfigUpdater.UpdateError = aoserrors.New("some error occurs")

	boardConfigUpdater.BoardConfigInfo = cloudprotocol.BoardConfigInfo{
		VendorVersion: "1.2", Status: cloudprotocol.ErrorStatus, Error: boardConfigUpdater.UpdateError.Error(),
	}
	expectedUnitStatus.BoardConfig = append(expectedUnitStatus.BoardConfig, boardConfigUpdater.BoardConfigInfo)

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{BoardConfig: json.RawMessage("{}")})

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateComponents(t *testing.T) {
	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(cloudprotocol.BoardConfigInfo{
		VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus,
	})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater([]cloudprotocol.ComponentInfo{
		{ID: "comp0", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
		{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
		{ID: "comp2", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
	})
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(cfg,
		boardConfigUpdater, firmwareUpdater, softwareUpdater, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	go handleUpdateStatus(statusHandler)

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components: []cloudprotocol.ComponentInfo{
			{ID: "comp0", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers:   []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{},
	}

	firmwareUpdater.UpdateComponentsInfo = expectedUnitStatus.Components

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Components: []cloudprotocol.ComponentInfoFromCloud{
			{ID: "comp0", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
			{ID: "comp2", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
		},
	})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	firmwareUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components: []cloudprotocol.ComponentInfo{
			{ID: "comp0", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
			{ID: "comp1", VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus},
			{
				ID: "comp1", VendorVersion: "2.0", Status: cloudprotocol.ErrorStatus,
				Error: firmwareUpdater.UpdateError.Error(),
			},
			{ID: "comp2", VendorVersion: "2.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers:   []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{},
	}

	firmwareUpdater.UpdateComponentsInfo = expectedUnitStatus.Components

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Components: []cloudprotocol.ComponentInfoFromCloud{
			{ID: "comp1", VersionFromCloud: cloudprotocol.VersionFromCloud{VendorVersion: "2.0"}},
		},
	})

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateLayers(t *testing.T) {
	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, []cloudprotocol.LayerInfo{
		{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	})
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, firmwareUpdater, softwareUpdater, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	go handleUpdateStatus(statusHandler)

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
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
			{
				ID: "layer1", Digest: "digest1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{1}},
			},
			{
				ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
			},
			{
				ID: "layer4", Digest: "digest4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{4}},
			},
		},
	})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	softwareUpdater.UsersLayers = expectedUnitStatus.Layers

	// failed update

	softwareUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers: []cloudprotocol.LayerInfo{
			{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.RemovedStatus},
			{ID: "layer3", Digest: "digest3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "layer4", Digest: "digest4", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{
				ID: "layer5", Digest: "digest5", AosVersion: 1, Status: cloudprotocol.ErrorStatus,
				Error: softwareUpdater.UpdateError.Error(),
			},
		},
		Services: []cloudprotocol.ServiceInfo{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Layers: []cloudprotocol.LayerInfoFromCloud{
			{
				ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
			},
			{
				ID: "layer4", Digest: "digest4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{4}},
			},
			{
				ID: "layer5", Digest: "digest5", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{5}},
			},
		},
	})

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateServices(t *testing.T) {
	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater([]cloudprotocol.ServiceInfo{
		{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	}, nil)
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, firmwareUpdater, softwareUpdater, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	go handleUpdateStatus(statusHandler)

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.WaitForStatus(5 * time.Second); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
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
			{
				ID: "service0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{0}},
			},
			{
				ID: "service1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{1}},
			},
			{
				ID: "service3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
			},
		},
	})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	softwareUpdater.UsersServices = expectedUnitStatus.Services
	softwareUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers:      []cloudprotocol.LayerInfo{},
		Services: []cloudprotocol.ServiceInfo{
			{
				ID: "service0", AosVersion: 0, Status: cloudprotocol.ErrorStatus,
				Error: softwareUpdater.UpdateError.Error(),
			},
			{ID: "service1", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{ID: "service2", Status: cloudprotocol.RemovedStatus},
			{ID: "service3", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
			{
				ID: "service3", AosVersion: 2, Status: cloudprotocol.ErrorStatus,
				Error: softwareUpdater.UpdateError.Error(),
			},
			{
				ID: "service4", AosVersion: 2, Status: cloudprotocol.ErrorStatus,
				Error: softwareUpdater.UpdateError.Error(),
			},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Services: []cloudprotocol.ServiceInfoFromCloud{
			{
				ID: "service1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{1}},
			},
			{
				ID: "service3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{3}},
			},
			{
				ID: "service4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{Sha256: []byte{4}},
			},
		},
	})

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateCachedSOTA(t *testing.T) {
	boardConfigUpdater := unitstatushandler.NewTestBoardConfigUpdater(
		cloudprotocol.BoardConfigInfo{VendorVersion: "1.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater([]cloudprotocol.ServiceInfo{
		{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	}, []cloudprotocol.LayerInfo{
		{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	})
	softwareUpdater.AllServices = []cloudprotocol.ServiceInfo{
		{ID: "service1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	}
	softwareUpdater.AllLayers = []cloudprotocol.LayerInfo{
		{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
	}
	sender := unitstatushandler.NewTestSender()
	downloader := unitstatushandler.NewTestDownloader()

	statusHandler, err := unitstatushandler.New(
		cfg, boardConfigUpdater, firmwareUpdater, softwareUpdater, downloader,
		unitstatushandler.NewTestStorage(), sender)
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	go handleUpdateStatus(statusHandler)

	if err = statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't set users: %s", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		BoardConfig: []cloudprotocol.BoardConfigInfo{boardConfigUpdater.BoardConfigInfo},
		Components:  []cloudprotocol.ComponentInfo{},
		Layers: []cloudprotocol.LayerInfo{
			{ID: "layer0", Digest: "digest0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "layer1", Digest: "digest1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "layer2", Digest: "digest2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "layer3", Digest: "digest3", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceInfo{
			{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "service1", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "service2", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
			{ID: "service3", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DecodedDesiredStatus{
		Services: []cloudprotocol.ServiceInfoFromCloud{
			{
				ID: "service0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"service0"}, Sha256: []byte{0}},
			},
			{
				ID: "service1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"service1"}, Sha256: []byte{1}},
			},
			{
				ID: "service2", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"service2"}, Sha256: []byte{2}},
			},
			{
				ID: "service3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"service3"}, Sha256: []byte{3}},
			},
		},
		Layers: []cloudprotocol.LayerInfoFromCloud{
			{
				ID: "layer0", Digest: "digest0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"layer0"}, Sha256: []byte{0}},
			},
			{
				ID: "layer1", Digest: "digest1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"layer1"}, Sha256: []byte{1}},
			},
			{
				ID: "layer2", Digest: "digest2", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"layer2"}, Sha256: []byte{2}},
			},
			{
				ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
				DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"layer3"}, Sha256: []byte{3}},
			},
		},
	})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	for _, url := range downloader.DownloadedURLs {
		if url == "service1" || url == "service2" || url == "layer1" || url == "layer2" {
			t.Errorf("Unexpected download URL: %s", url)
		}

		if url != "service3" && url != "layer3" {
			t.Errorf("Unexpected download URL: %s", url)
		}
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

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

func handleUpdateStatus(handler *unitstatushandler.Instance) {
	for {
		select {
		case _, ok := <-handler.GetFOTAStatusChannel():
			if !ok {
				return
			}

		case _, ok := <-handler.GetSOTAStatusChannel():
			if !ok {
				return
			}

		}
	}
}
