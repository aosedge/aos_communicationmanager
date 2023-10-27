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

package config_test

import (
	"encoding/json"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const testConfigContent = `{
	"fcrypt" : {
		"CACert" : "CACert",
		"tpmDevice": "/dev/tpmrm0",
		"pkcs11Library": "/path/to/pkcs11/library"
	},
	"certStorage": "/var/aos/crypt/cm/",
	"storageDir" : "/var/aos/storage",
	"stateDir" : "/var/aos/state",
	"serviceDiscoveryUrl" : "www.aos.com",
	"iamProtectedServerUrl" : "localhost:8089",
	"iamPublicServerUrl" : "localhost:8090",
	"cmServerUrl":"localhost:8094",
	"workingDir" : "workingDir",
	"imageStoreDir": "imagestoreDir",
	"componentsDir": "componentDir",
	"serviceTtlDays": 30,
	"layerTtlDays": 40,
	"unitConfigFile" : "/var/aos/aos_unit.cfg",
	"downloader": {
		"downloadDir": "/path/to/download",
		"maxConcurrentDownloads": 10,
		"retryDelay": "10s",
		"maxRetryDelay": "30s",
		"downloadPartLimit": 57
	},
	"monitoring": {
		"monitorConfig": {
			"sendPeriod": "5m",
			"pollPeriod": "1s",
			"ram": {
				"minTimeout": "10s",
				"minThreshold": 10,
				"maxThreshold": 150
			},
			"outTraffic": {
				"minTimeout": "20s",
				"minThreshold": 10,
				"maxThreshold": 150
			}
		},
		"maxOfflineMessages": 25
	},
	"alerts": {		
		"sendPeriod": "20s",
		"maxMessageSize": 1024,
		"maxOfflineMessages": 32,
		"journalAlerts": {
			"filter": ["(test)", "(regexp)"]
		}
	},
	"migration": {
		"migrationPath" : "/usr/share/aos_communicationmanager/migration",
		"mergedMigrationPath" : "/var/aos/communicationmanager/migration"
	},
	"smController": {
		"fileServerUrl":"localhost:8094",
		"cmServerUrl": "localhost:8093",
		"nodeIds": [ "sm1", "sm2"],	
		"nodesConnectionTimeout": "100s",
		"updateTTL": "30h"
	},
	"umController": {
		"fileServerUrl":"localhost:8092",
		"cmServerUrl": "localhost:8091",
		"umClients": [{
			"umId": "um",
			"priority": 0,
			"isLocal": true
		}],
		"updateTTL": "100h"
	}
}`

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	tmpDir  string
	testCfg *config.Config
)

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Error creating service images: %s", err)
	}

	ret := m.Run()

	if err := cleanup(); err != nil {
		log.Fatalf("Error cleaning up: %s", err)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestGetCrypt(t *testing.T) {
	if testCfg.Crypt.TpmDevice != "/dev/tpmrm0" {
		t.Errorf("Wrong TPMEngine Interface value: %s", testCfg.Crypt.TpmDevice)
	}

	if testCfg.Crypt.CACert != "CACert" {
		t.Errorf("Wrong CACert value: %s", testCfg.Crypt.CACert)
	}

	if testCfg.Crypt.Pkcs11Library != "/path/to/pkcs11/library" {
		t.Errorf("Wrong PKCS11 library value: %s", testCfg.Crypt.Pkcs11Library)
	}
}

func TestGetServiceDiscoveryURL(t *testing.T) {
	if testCfg.ServiceDiscoveryURL != "www.aos.com" {
		t.Errorf("Wrong server URL value: %s", testCfg.ServiceDiscoveryURL)
	}
}

func TestGetImageStoreDir(t *testing.T) {
	if testCfg.ImageStoreDir != "imagestoreDir" {
		t.Errorf("Wrong image store directory value: %s", testCfg.ImageStoreDir)
	}
}

func TestGetStorageDir(t *testing.T) {
	if testCfg.StorageDir != "/var/aos/storage" {
		t.Errorf("Wrong storageDir value: %s", testCfg.StorageDir)
	}
}

func TestGetStateDir(t *testing.T) {
	if testCfg.StateDir != "/var/aos/state" {
		t.Errorf("Wrong stateDir value: %s", testCfg.StateDir)
	}
}

func TestGetWorkingDir(t *testing.T) {
	if testCfg.WorkingDir != "workingDir" {
		t.Errorf("Wrong working directory value: %s", testCfg.WorkingDir)
	}
}

func TestGetUnitConfigFile(t *testing.T) {
	if testCfg.UnitConfigFile != "/var/aos/aos_unit.cfg" {
		t.Errorf("Wrong unit config file value: %s", testCfg.UnitConfigFile)
	}
}

func TestGetIAMProtectedServerURL(t *testing.T) {
	if testCfg.IAMProtectedServerURL != "localhost:8089" {
		t.Errorf("Wrong IAM server value: %s", testCfg.IAMProtectedServerURL)
	}
}

func TestGetIAMPublicServerURL(t *testing.T) {
	if testCfg.IAMPublicServerURL != "localhost:8090" {
		t.Errorf("wrong IAM public server value: %s", testCfg.IAMPublicServerURL)
	}
}

func TestDurationMarshal(t *testing.T) {
	d := aostypes.Duration{Duration: 32 * time.Second}

	result, err := json.Marshal(d)
	if err != nil {
		t.Errorf("Can't marshal: %s", err)
	}

	if string(result) != `"32s"` {
		t.Errorf("Wrong value: %s", result)
	}
}

func TestGetMonitoringConfig(t *testing.T) {
	if testCfg.Monitoring.MonitorConfig.SendPeriod.Duration != 5*time.Minute {
		t.Errorf("Wrong send period value: %s", testCfg.Monitoring.MonitorConfig.SendPeriod)
	}

	if testCfg.Monitoring.MonitorConfig.PollPeriod.Duration != 1*time.Second {
		t.Errorf("Wrong poll period value: %s", testCfg.Monitoring.MonitorConfig.PollPeriod)
	}

	if testCfg.Monitoring.MonitorConfig.RAM.MinTimeout.Duration != 10*time.Second {
		t.Errorf("Wrong value: %s", testCfg.Monitoring.MonitorConfig.RAM.MinTimeout)
	}

	if testCfg.Monitoring.MonitorConfig.OutTraffic.MinTimeout.Duration != 20*time.Second {
		t.Errorf("Wrong value: %s", testCfg.Monitoring.MonitorConfig.RAM.MinTimeout)
	}
}

func TestGetAlertsConfig(t *testing.T) {
	if testCfg.Alerts.SendPeriod.Duration != 20*time.Second {
		t.Errorf("Wrong poll period value: %s", testCfg.Alerts.SendPeriod)
	}

	if testCfg.Alerts.MaxMessageSize != 1024 {
		t.Errorf("Wrong max message size value: %d", testCfg.Alerts.MaxMessageSize)
	}

	if testCfg.Alerts.MaxOfflineMessages != 32 {
		t.Errorf("Wrong max offline message value: %d", testCfg.Alerts.MaxOfflineMessages)
	}

	filter := []string{"(test)", "(regexp)"}

	if !reflect.DeepEqual(testCfg.Alerts.JournalAlerts.Filter, filter) {
		t.Errorf("Wrong filter value: %v", testCfg.Alerts.JournalAlerts.Filter)
	}
}

func TestUMControllerConfig(t *testing.T) {
	umClient := config.UMClientConfig{UMID: "um", Priority: 0, IsLocal: true}

	originalConfig := config.UMController{
		FileServerURL: "localhost:8092",
		CMServerURL:   "localhost:8091",
		UMClients:     []config.UMClientConfig{umClient},
		UpdateTTL:     aostypes.Duration{Duration: 100 * time.Hour},
	}

	if !reflect.DeepEqual(originalConfig, testCfg.UMController) {
		t.Errorf("Wrong UM controller value: %v", testCfg.UMController)
	}
}

func TestDownloaderConfig(t *testing.T) {
	originalConfig := config.Downloader{
		DownloadDir:            "/path/to/download",
		MaxConcurrentDownloads: 10,
		RetryDelay:             aostypes.Duration{Duration: 10 * time.Second},
		MaxRetryDelay:          aostypes.Duration{Duration: 30 * time.Second},
		DownloadPartLimit:      57,
	}

	if !reflect.DeepEqual(originalConfig, testCfg.Downloader) {
		t.Errorf("Wrong downloader config value: %v", testCfg.Downloader)
	}
}

func TestSMControllerConfig(t *testing.T) {
	originalConfig := config.SMController{
		FileServerURL:          "localhost:8094",
		CMServerURL:            "localhost:8093",
		NodeIDs:                []string{"sm1", "sm2"},
		NodesConnectionTimeout: aostypes.Duration{Duration: 100 * time.Second},
		UpdateTTL:              aostypes.Duration{Duration: 30 * time.Hour},
	}

	if !reflect.DeepEqual(originalConfig, testCfg.SMController) {
		t.Errorf("Wrong SM controller value: %v", testCfg.SMController)
	}
}

func TestDatabaseMigration(t *testing.T) {
	if testCfg.Migration.MigrationPath != "/usr/share/aos_communicationmanager/migration" {
		t.Errorf("Wrong migration path value: %s", testCfg.Migration.MigrationPath)
	}

	if testCfg.Migration.MergedMigrationPath != "/var/aos/communicationmanager/migration" {
		t.Errorf("Wrong merged migration path value: %s", testCfg.Migration.MergedMigrationPath)
	}
}

func TestCertStorage(t *testing.T) {
	if testCfg.CertStorage != "/var/aos/crypt/cm/" {
		t.Errorf("Wrong certificate storage value: %s", testCfg.CertStorage)
	}
}

func TestCMServer(t *testing.T) {
	if testCfg.CMServerURL != "localhost:8094" {
		t.Errorf("Wrong cm server URL value: %s", testCfg.CMServerURL)
	}
}

func TestGetLayerTTLDays(t *testing.T) {
	if testCfg.LayerTTLDays != 40 {
		t.Errorf("Wrong LayerTTLDays value: %d", testCfg.LayerTTLDays)
	}
}

func TestGetServiceTTLDays(t *testing.T) {
	if testCfg.ServiceTTLDays != 30 {
		t.Errorf("Wrong ServiceTTLDays value: %d", testCfg.ServiceTTLDays)
	}
}

func TestComponentStoreDir(t *testing.T) {
	if testCfg.ComponentsDir != "componentDir" {
		t.Errorf("Wrong components directory value: %s", testCfg.ComponentsDir)
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createConfigFile(fileName string) (err error) {
	if err := os.WriteFile(fileName, []byte(testConfigContent), 0o600); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func setup() (err error) {
	if tmpDir, err = os.MkdirTemp("", "aos_"); err != nil {
		return aoserrors.Wrap(err)
	}

	fileName := path.Join(tmpDir, "aos_communicationmanager.cfg")

	if err = createConfigFile(fileName); err != nil {
		return aoserrors.Wrap(err)
	}

	if testCfg, err = config.New(fileName); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func cleanup() (err error) {
	if err := os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
