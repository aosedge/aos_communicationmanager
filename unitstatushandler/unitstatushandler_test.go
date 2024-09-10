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
	"reflect"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	waitStatusTimeout      = 5 * time.Second
	waitRunInstanceTimeout = 5 * time.Second
)

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var cfg = &config.Config{UnitStatusSendTimeout: aostypes.Duration{Duration: 3 * time.Second}}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestSendInitialStatus(t *testing.T) {
	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitSubjects: []string{"subject1"},
		UnitConfig: []cloudprotocol.UnitConfigStatus{
			{Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		},
		Components: []cloudprotocol.ComponentStatus{
			{ComponentID: "comp0", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{ComponentID: "comp1", Version: "1.1.0", Status: cloudprotocol.InstalledStatus},
			{ComponentID: "comp2", Version: "1.2.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers: []cloudprotocol.LayerStatus{
			{LayerID: "layer0", Digest: "digest0", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer1", Digest: "digest1", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer2", Digest: "digest2", Version: "3.0.0", Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceStatus{
			{ServiceID: "service0", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service2", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		},
	}

	initialServices := []unitstatushandler.ServiceStatus{
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service0", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},

		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service2", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{
			ServiceStatus: cloudprotocol.ServiceStatus{
				ServiceID: "service3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
			},
			Cached: true,
		},
	}

	initialLayers := []unitstatushandler.LayerStatus{
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer0", Digest: "digest0", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer1", Digest: "digest1", Version: "2.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer2", Digest: "digest2", Version: "3.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(expectedUnitStatus.UnitConfig[0])
	fotaUpdater := unitstatushandler.NewTestFirmwareUpdater(expectedUnitStatus.Components)
	sotaUpdater := unitstatushandler.NewTestSoftwareUpdater(initialServices, initialLayers)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, fotaUpdater, sotaUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	if err := statusHandler.SendUnitStatus(); err != nil {
		t.Fatalf("Can't send unit status: %v", err)
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	sender.Consumer.CloudDisconnected()

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err := sender.WaitForStatus(time.Second); err == nil {
		t.Fatal("Should be receive status timeout")
	}
}

func TestUpdateUnitConfig(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	fotaUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	sotaUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, fotaUpdater, sotaUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	unitConfigUpdater.UnitConfigStatus = cloudprotocol.UnitConfigStatus{
		Version: "1.1.0", Status: cloudprotocol.InstalledStatus,
	}
	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers:     []cloudprotocol.LayerStatus{},
		Services:   []cloudprotocol.ServiceStatus{},
	}

	statusHandler.ProcessDesiredStatus(
		cloudprotocol.DesiredStatus{UnitConfig: &cloudprotocol.UnitConfig{Version: "1.1.0"}})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	unitConfigUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus.UnitConfig = append(expectedUnitStatus.UnitConfig, cloudprotocol.UnitConfigStatus{
		Version: "1.2.0", Status: cloudprotocol.ErrorStatus,
		ErrorInfo: &cloudprotocol.ErrorInfo{Message: unitConfigUpdater.UpdateError.Error()},
	})

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{UnitConfig: &cloudprotocol.UnitConfig{
		Version: "1.2.0",
	}})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateComponents(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(cloudprotocol.UnitConfigStatus{
		Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
	})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater([]cloudprotocol.ComponentStatus{
		{ComponentID: "comp0", ComponentType: "type-1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		{ComponentID: "comp1", ComponentType: "type-1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		{ComponentID: "comp2", ComponentType: "type-1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
	})
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(cfg, unitstatushandler.NewTestNodeManager(nil),
		unitConfigUpdater, firmwareUpdater, softwareUpdater, instanceRunner, unitstatushandler.NewTestDownloader(),
		unitstatushandler.NewTestStorage(), sender, unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		Components: []cloudprotocol.ComponentStatus{
			{ComponentID: "comp0", ComponentType: "type-1", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
			{ComponentID: "comp2", ComponentType: "type-1", Version: "2.0.0", Status: cloudprotocol.InstalledStatus},
		},
		Layers:      []cloudprotocol.LayerStatus{},
		Services:    []cloudprotocol.ServiceStatus{},
		IsDeltaInfo: true,
	}

	firmwareUpdater.UpdateComponentsInfo = expectedUnitStatus.Components

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Components: []cloudprotocol.ComponentInfo{
			{ComponentID: convertToComponentID("comp0"), ComponentType: "type-1", Version: "2.0.0"},
			{ComponentID: convertToComponentID("comp2"), ComponentType: "type-1", Version: "2.0.0"},
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
		Components: []cloudprotocol.ComponentStatus{
			{
				ComponentID: "comp1", ComponentType: "type-1", Version: "2.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: firmwareUpdater.UpdateError.Error()},
			},
		},
		Layers:      []cloudprotocol.LayerStatus{},
		Services:    []cloudprotocol.ServiceStatus{},
		IsDeltaInfo: true,
	}

	firmwareUpdater.UpdateComponentsInfo = expectedUnitStatus.Components

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Components: []cloudprotocol.ComponentInfo{
			{ComponentID: convertToComponentID("comp1"), ComponentType: "type-1", Version: "2.0.0"},
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
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	softwareUpdater.AllLayers = []unitstatushandler.LayerStatus{
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer0", Digest: "digest0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer2", Digest: "digest2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers: []cloudprotocol.LayerStatus{
			{LayerID: "layer0", Digest: "digest0", Version: "0.0.0", Status: cloudprotocol.RemovedStatus},
			{LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer2", Digest: "digest2", Version: "0.0.0", Status: cloudprotocol.RemovedStatus},
			{LayerID: "layer3", Digest: "digest3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer4", Digest: "digest4", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceStatus{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Layers: []cloudprotocol.LayerInfo{
			{
				LayerID: "layer1", Digest: "digest1", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{1}},
			},
			{
				LayerID: "layer3", Digest: "digest3", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{3}},
			},
			{
				LayerID: "layer4", Digest: "digest4", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{4}},
			},
		},
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	softwareUpdater.AllLayers = []unitstatushandler.LayerStatus{
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer3", Digest: "digest3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer4", Digest: "digest4", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	softwareUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers: []cloudprotocol.LayerStatus{
			{LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.RemovedStatus},
			{LayerID: "layer3", Digest: "digest3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer4", Digest: "digest4", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{
				LayerID: "layer5", Digest: "digest5", Version: "1.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: softwareUpdater.UpdateError.Error()},
			},
		},
		Services: []cloudprotocol.ServiceStatus{},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Layers: []cloudprotocol.LayerInfo{
			{
				LayerID: "layer3", Digest: "digest3", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{3}},
			},
			{
				LayerID: "layer4", Digest: "digest4", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{4}},
			},
			{
				LayerID: "layer5", Digest: "digest5", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{5}},
			},
		},
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	softwareUpdater.AllLayers = []unitstatushandler.LayerStatus{
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer3", Digest: "digest3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer4", Digest: "digest4", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateServices(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	softwareUpdater.AllServices = []unitstatushandler.ServiceStatus{
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(5 * time.Second); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	// success update

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers:     []cloudprotocol.LayerStatus{},
		Services: []cloudprotocol.ServiceStatus{
			{ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service2", Version: "0.0.0", Status: cloudprotocol.RemovedStatus},
			{ServiceID: "service3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Services: []cloudprotocol.ServiceInfo{
			{
				ServiceID: "service0", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{0}},
			},
			{
				ServiceID: "service1", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{1}},
			},
			{
				ServiceID: "service3", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{3}},
			},
		},
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	softwareUpdater.AllServices = []unitstatushandler.ServiceStatus{
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus,
		}},
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// failed update

	softwareUpdater.UpdateError = aoserrors.New("some error occurs")

	expectedUnitStatus = cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers:     []cloudprotocol.LayerStatus{},
		Services: []cloudprotocol.ServiceStatus{
			{
				ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: softwareUpdater.UpdateError.Error()},
			},
			{ServiceID: "service1", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service3", Version: "1.0.0", Status: cloudprotocol.InstalledStatus},
			{
				ServiceID: "service3", Version: "2.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: softwareUpdater.UpdateError.Error()},
			},
			{
				ServiceID: "service4", Version: "2.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: softwareUpdater.UpdateError.Error()},
			},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Services: []cloudprotocol.ServiceInfo{
			{
				ServiceID: "service1", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{1}},
			},
			{
				ServiceID: "service3", Version: "2.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{3}},
			},
			{
				ServiceID: "service4", Version: "2.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{4}},
			},
		},
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestRunInstances(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %v", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	initialInstancesStatus := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 0},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 1},
			ServiceVersion: "1.0.0",
		},
	}

	if err := statusHandler.ProcessRunStatus(initialInstancesStatus); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Instances:  initialInstancesStatus,
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// success run

	expectedRunInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: "Serv1", SubjectID: "Subj1", NumInstances: 3},
		{ServiceID: "Serv1", SubjectID: "Subj2", NumInstances: 1},
		{ServiceID: "Serv2", SubjectID: "Subj1", NumInstances: 1},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Instances: expectedRunInstances,
	})

	receivedRunInstances, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout)
	if err != nil {
		t.Fatalf("Can't receive run instances: %v", err)
	}

	if !reflect.DeepEqual(receivedRunInstances, expectedRunInstances) {
		t.Error("Incorrect run instances")
	}

	updatedInstancesStatus := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 0},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 1},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 2},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj2", Instance: 0},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv2", SubjectID: "Subj1", Instance: 0},
			ServiceVersion: "1.0.0",
		},
	}

	if err := statusHandler.ProcessRunStatus(updatedInstancesStatus); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus = cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Instances:  updatedInstancesStatus,
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// send the same run instances
	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Instances: expectedRunInstances,
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err == nil {
		t.Error("Should be no run instances request")
	}
}

func TestRevertServices(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %v", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	// success run

	expectedRunInstances := []cloudprotocol.InstanceInfo{
		{ServiceID: "service0", SubjectID: "subject0", NumInstances: 3},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Instances: expectedRunInstances,
		Services: []cloudprotocol.ServiceInfo{
			{
				ServiceID: "service0", Version: "1.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{Sha256: []byte{0}},
			},
		},
	})

	instancesStatus := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subject0", Instance: 0},
			ServiceVersion: "1.0.0",
			Status:         cloudprotocol.InstanceStateFailed,
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subject0", Instance: 1},
			ServiceVersion: "1.0.0",
			Status:         cloudprotocol.InstanceStateFailed,
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subject0", Instance: 2},
			ServiceVersion: "1.0.0",
			Status:         cloudprotocol.InstanceStateFailed,
		},
	}

	receivedRunInstances, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout)
	if err != nil {
		t.Fatalf("Can't receive run instances: %v", err)
	}

	if !reflect.DeepEqual(receivedRunInstances, expectedRunInstances) {
		t.Error("Incorrect run instances")
	}

	if err := statusHandler.ProcessRunStatus(instancesStatus); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedRunInstances, err = instanceRunner.WaitForRunInstance(waitRunInstanceTimeout)
	if err != nil {
		t.Fatalf("Can't receive run instances: %v", err)
	}

	if !reflect.DeepEqual(receivedRunInstances, expectedRunInstances) {
		t.Error("Incorrect run instances")
	}

	if err := statusHandler.ProcessRunStatus(instancesStatus); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Services: []cloudprotocol.ServiceStatus{
			{
				ServiceID: "service0", Version: "1.0.0", Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{Message: "can't run any instances of service"},
			},
		},
		Instances: instancesStatus,
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	if !reflect.DeepEqual(softwareUpdater.RevertedServices, []string{"service0"}) {
		t.Errorf("Incorrect reverted services: %v", softwareUpdater.RevertedServices)
	}
}

func TestUpdateInstancesStatus(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %v", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus([]cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 0},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 1},
			ServiceVersion: "1.0.0",
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv2", SubjectID: "Subj2", Instance: 1},
			ServiceVersion: "1.0.0",
		},
	}); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err := sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		Instances: []cloudprotocol.InstanceStatus{
			{
				InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 0},
				ServiceVersion: "1.0.0",
				Status:         "fail", ErrorInfo: &cloudprotocol.ErrorInfo{Message: "someError"},
			},
			{
				InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv2", SubjectID: "Subj2", Instance: 1},
				ServiceVersion: "1.0.0",
				StateChecksum:  "newState",
			},
		},
		IsDeltaInfo: true,
	}

	statusHandler.ProcessUpdateInstanceStatus([]cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv1", SubjectID: "Subj1", Instance: 0},
			ServiceVersion: "1.0.0",
			Status:         "fail", ErrorInfo: &cloudprotocol.ErrorInfo{Message: "someError"},
		},
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "Serv2", SubjectID: "Subj2", Instance: 1},
			ServiceVersion: "1.0.0",
			StateChecksum:  "newState",
		},
	})

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestUpdateCachedSOTA(t *testing.T) {
	serviceStatuses := []unitstatushandler.ServiceStatus{
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{ServiceStatus: cloudprotocol.ServiceStatus{
			ServiceID: "service4", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}, Cached: true},
	}
	layerStatuses := []unitstatushandler.LayerStatus{
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer0", Digest: "digest0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer2", Digest: "digest2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer4", Digest: "digest4", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}, Cached: true},
		{LayerStatus: cloudprotocol.LayerStatus{
			LayerID: "layer5", Digest: "digest5", Version: "0.0.0", Status: cloudprotocol.InstalledStatus,
		}, Cached: true},
	}
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(serviceStatuses, layerStatuses)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()
	downloader := unitstatushandler.NewTestDownloader()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, downloader, unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %s", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	if _, err = sender.WaitForStatus(waitStatusTimeout); err != nil {
		t.Fatalf("Can't receive unit status: %s", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Components: []cloudprotocol.ComponentStatus{},
		Layers: []cloudprotocol.LayerStatus{
			{LayerID: "layer0", Digest: "digest0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer1", Digest: "digest1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer2", Digest: "digest2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer3", Digest: "digest3", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{LayerID: "layer5", Digest: "digest5", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
		},
		Services: []cloudprotocol.ServiceStatus{
			{ServiceID: "service0", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service1", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service2", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service3", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
			{ServiceID: "service4", Version: "0.0.0", Status: cloudprotocol.InstalledStatus},
		},
	}

	statusHandler.ProcessDesiredStatus(cloudprotocol.DesiredStatus{
		Services: []cloudprotocol.ServiceInfo{
			{
				ServiceID: "service0", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"service0"}, Sha256: []byte{0}},
			},
			{
				ServiceID: "service1", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"service1"}, Sha256: []byte{1}},
			},
			{
				ServiceID: "service2", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"service2"}, Sha256: []byte{2}},
			},
			{
				ServiceID: "service3", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"service3"}, Sha256: []byte{3}},
			},
			{
				ServiceID: "service4", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"service3"}, Sha256: []byte{3}},
			},
		},
		Layers: []cloudprotocol.LayerInfo{
			{
				LayerID: "layer0", Digest: "digest0", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"layer0"}, Sha256: []byte{0}},
			},
			{
				LayerID: "layer1", Digest: "digest1", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"layer1"}, Sha256: []byte{1}},
			},
			{
				LayerID: "layer2", Digest: "digest2", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"layer2"}, Sha256: []byte{2}},
			},
			{
				LayerID: "layer3", Digest: "digest3", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"layer3"}, Sha256: []byte{3}},
			},
			{
				LayerID: "layer5", Digest: "digest5", Version: "0.0.0",
				DownloadInfo: cloudprotocol.DownloadInfo{URLs: []string{"layer5"}, Sha256: []byte{3}},
			},
		},
	})

	if _, err := instanceRunner.WaitForRunInstance(waitRunInstanceTimeout); err != nil {
		t.Errorf("Wait run instances error: %v", err)
	}

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

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

func TestNewComponents(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()

	statusHandler, err := unitstatushandler.New(
		cfg, unitstatushandler.NewTestNodeManager(nil), unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %v", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// New components

	newComponents := []cloudprotocol.ComponentStatus{
		{
			ComponentID: "comp1", ComponentType: "type1", Version: "1.0.0",
			Status: cloudprotocol.InstalledStatus, NodeID: "node1",
		},
		{
			ComponentID: "comp2", ComponentType: "type2", Version: "1.0.0",
			Status: cloudprotocol.InstalledStatus, NodeID: "node2",
		},
		{
			ComponentID: "comp3", ComponentType: "type3", Version: "1.0.0",
			Status: cloudprotocol.InstalledStatus, NodeID: "node3",
		},
	}

	firmwareUpdater.SetNewComponents(newComponents)

	receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus = cloudprotocol.UnitStatus{
		Components:  newComponents,
		IsDeltaInfo: true,
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}
}

func TestNodeInfoChanged(t *testing.T) {
	unitConfigUpdater := unitstatushandler.NewTestUnitConfigUpdater(
		cloudprotocol.UnitConfigStatus{Version: "1.0.0", Status: cloudprotocol.InstalledStatus})
	firmwareUpdater := unitstatushandler.NewTestFirmwareUpdater(nil)
	softwareUpdater := unitstatushandler.NewTestSoftwareUpdater(nil, nil)
	instanceRunner := unitstatushandler.NewTestInstanceRunner()
	sender := unitstatushandler.NewTestSender()
	nodeInfoProvider := unitstatushandler.NewTestNodeManager([]cloudprotocol.NodeInfo{
		{NodeID: "node1", NodeType: "type1", Status: cloudprotocol.NodeStatusProvisioned},
		{NodeID: "node2", NodeType: "type2", Status: cloudprotocol.NodeStatusProvisioned},
	})

	statusHandler, err := unitstatushandler.New(
		cfg, nodeInfoProvider, unitConfigUpdater, firmwareUpdater, softwareUpdater,
		instanceRunner, unitstatushandler.NewTestDownloader(), unitstatushandler.NewTestStorage(), sender,
		unitstatushandler.NewTestSystemQuotaAlertProvider())
	if err != nil {
		t.Fatalf("Can't create unit status handler: %v", err)
	}
	defer statusHandler.Close()

	sender.Consumer.CloudConnected()

	go handleUpdateStatus(statusHandler)

	if err := statusHandler.ProcessRunStatus(nil); err != nil {
		t.Fatalf("Can't process run status: %v", err)
	}

	receivedUnitStatus, err := sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus := cloudprotocol.UnitStatus{
		UnitConfig: []cloudprotocol.UnitConfigStatus{unitConfigUpdater.UnitConfigStatus},
		Nodes:      nodeInfoProvider.GetAllNodesInfo(),
	}

	if err = compareUnitStatus(receivedUnitStatus, expectedUnitStatus); err != nil {
		t.Errorf("Wrong unit status received: %v, expected: %v", receivedUnitStatus, expectedUnitStatus)
	}

	// New nodes info

	newNodesInfo := []cloudprotocol.NodeInfo{
		{NodeID: "node1", NodeType: "type1", Status: cloudprotocol.NodeStatusPaused},
		{NodeID: "node2", NodeType: "type2", Status: cloudprotocol.NodeStatusProvisioned},
		{NodeID: "node3", NodeType: "type3", Status: cloudprotocol.NodeStatusProvisioned},
	}

	for _, newNodeInfo := range newNodesInfo {
		nodeInfoProvider.NodeInfoChanged(newNodeInfo)
	}

	receivedUnitStatus, err = sender.WaitForStatus(waitStatusTimeout)
	if err != nil {
		t.Fatalf("Can't receive unit status: %v", err)
	}

	expectedUnitStatus = cloudprotocol.UnitStatus{
		Nodes:       newNodesInfo,
		IsDeltaInfo: true,
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
	if err = compareStatus(len(status1.UnitConfig), len(status2.UnitConfig),
		func(index1, index2 int) (result bool) {
			return reflect.DeepEqual(status1.UnitConfig[index1], status2.UnitConfig[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = compareStatus(len(status1.Components), len(status2.Components),
		func(index1, index2 int) (result bool) {
			return reflect.DeepEqual(status1.Components[index1], status2.Components[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = compareStatus(len(status1.Layers), len(status2.Layers),
		func(index1, index2 int) (result bool) {
			return reflect.DeepEqual(status1.Layers[index1], status2.Layers[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = compareStatus(len(status1.Services), len(status2.Services),
		func(index1, index2 int) (result bool) {
			status1.Services[index1].ErrorInfo = nil
			status2.Services[index2].ErrorInfo = nil

			return reflect.DeepEqual(status1.Services[index1], status2.Services[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = compareStatus(len(status1.Instances), len(status2.Instances),
		func(index1, index2 int) (result bool) {
			return reflect.DeepEqual(status1.Instances[index1], status2.Instances[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = compareStatus(len(status1.Nodes), len(status2.Nodes),
		func(index1, index2 int) (result bool) {
			return reflect.DeepEqual(status1.Nodes[index1], status2.Nodes[index2])
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	if status1.IsDeltaInfo != status2.IsDeltaInfo {
		return aoserrors.New("IsDeltaInfo mismatch")
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

func convertToComponentID(id string) *string {
	return &id
}
