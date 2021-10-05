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

package config_test

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"testing"
	"time"

	"aos_communicationmanager/config"
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
	"serviceDiscoveryUrl" : "www.aos.com",
	"iamServerUrl" : "localhost:8090",
	"fileServerUrl":"localhost:8092",
	"cmServerUrl":"localhost:8094",
	"workingDir" : "workingDir",
	"boardConfigFile" : "/var/aos/aos_board.cfg",
	"downloader": {
		"downloadDir": "/path/to/download",
		"decryptDir": "/path/to/decrypt",
		"maxConcurrentDownloads": 10,
		"retryCount": 5,
		"retryDelay": "10s"
	},
	"monitoring": {
		"sendPeriod": "00:05:00",
		"pollPeriod": "00:00:01",
		"maxOfflineMessages": 25,
		"ram": {
			"minTimeout": "00:00:10",
			"minThreshold": 10,
			"maxThreshold": 150
		},
		"outTraffic": {
			"minTimeout": "00:00:20",
			"minThreshold": 10,
			"maxThreshold": 150
		}
	},
	"alerts": {
		"enableSystemAlerts": true,
		"sendPeriod": "00:00:20",
		"maxMessageSize": 1024,
		"maxOfflineMessages": 32,
		"filter": ["(test)", "(regexp)"]
	},
	"migration": {
		"migrationPath" : "/usr/share/aos_communicationmanager/migration",
		"mergedMigrationPath" : "/var/aos/communicationmanager/migration"
	},
	"smController": {
		"smList": [
			{
				"smId": "sm0",
				"serverUrl": "localhost:8888",
				"isLocal": true
			},
			{
				"smId": "sm1",
				"serverUrl": "remotehost:8888"
			}
		],
		"updateTTL": "30h"
	},
	"umController": {
		"serverUrl": "localhost:8091",
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

var tmpDir string
var testCfg *config.Config

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

func TestGetWorkingDir(t *testing.T) {
	if testCfg.WorkingDir != "workingDir" {
		t.Errorf("Wrong working directory value: %s", testCfg.WorkingDir)
	}
}

func TestGetBoardConfigFile(t *testing.T) {
	if testCfg.BoardConfigFile != "/var/aos/aos_board.cfg" {
		t.Errorf("Wrong board config file value: %s", testCfg.BoardConfigFile)
	}
}

func TestGetIAMServerURL(t *testing.T) {
	if testCfg.IAMServerURL != "localhost:8090" {
		t.Errorf("Wrong IAM server value: %s", testCfg.IAMServerURL)
	}
}

func TestDurationMarshal(t *testing.T) {
	d := config.Duration{Duration: 32 * time.Second}

	result, err := json.Marshal(d)
	if err != nil {
		t.Errorf("Can't marshal: %s", err)
	}

	if string(result) != `"00:00:32"` {
		t.Errorf("Wrong value: %s", result)
	}
}

func TestGetMonitoringConfig(t *testing.T) {
	if testCfg.Monitoring.SendPeriod.Duration != 5*time.Minute {
		t.Errorf("Wrong send period value: %s", testCfg.Monitoring.SendPeriod)
	}

	if testCfg.Monitoring.PollPeriod.Duration != 1*time.Second {
		t.Errorf("Wrong poll period value: %s", testCfg.Monitoring.PollPeriod)
	}

	if testCfg.Monitoring.RAM.MinTimeout.Duration != 10*time.Second {
		t.Errorf("Wrong value: %s", testCfg.Monitoring.RAM.MinTimeout)
	}

	if testCfg.Monitoring.OutTraffic.MinTimeout.Duration != 20*time.Second {
		t.Errorf("Wrong value: %s", testCfg.Monitoring.RAM.MinTimeout)
	}
}

func TestGetAlertsConfig(t *testing.T) {
	if !testCfg.Alerts.EnableSystemAlerts {
		t.Errorf("Wrong enable system alerts value: %v", testCfg.Alerts.EnableSystemAlerts)
	}

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

	if !reflect.DeepEqual(testCfg.Alerts.Filter, filter) {
		t.Errorf("Wrong filter value: %v", testCfg.Alerts.Filter)
	}
}

func TestUMControllerConfig(t *testing.T) {
	umClient := config.UMClientConfig{UMID: "um", Priority: 0, IsLocal: true}

	originalConfig := config.UMController{
		ServerURL: "localhost:8091",
		UMClients: []config.UMClientConfig{umClient},
		UpdateTTL: config.Duration{100 * time.Hour},
	}

	if !reflect.DeepEqual(originalConfig, testCfg.UMController) {
		t.Errorf("Wrong UM controller value: %v", testCfg.UMController)
	}
}

func TestDownloaderConfig(t *testing.T) {
	originalConfig := config.Downloader{
		DownloadDir:            "/path/to/download",
		DecryptDir:             "/path/to/decrypt",
		MaxConcurrentDownloads: 10,
		RetryCount:             5,
		RetryDelay:             config.Duration{10 * time.Second},
	}

	if !reflect.DeepEqual(originalConfig, testCfg.Downloader) {
		t.Errorf("Wrong downloader config value: %v", testCfg.Downloader)
	}
}

func TestSMControllerConfig(t *testing.T) {
	originalConfig := config.SMController{
		SMList: []config.SMConfig{
			{SMID: "sm0", ServerURL: "localhost:8888", IsLocal: true},
			{SMID: "sm1", ServerURL: "remotehost:8888"},
		},
		UpdateTTL: config.Duration{30 * time.Hour},
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

func TestFileServer(t *testing.T) {
	if testCfg.FileServerURL != "localhost:8092" {
		t.Errorf("Wrong file server URL value: %s", testCfg.FileServerURL)
	}
}

func TestCMServer(t *testing.T) {
	if testCfg.CMServerURL != "localhost:8094" {
		t.Errorf("Wrong cm server URL value: %s", testCfg.CMServerURL)
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createConfigFile(fileName string) (err error) {
	if err := ioutil.WriteFile(fileName, []byte(testConfigContent), 0644); err != nil {
		return err
	}

	return nil
}

func setup() (err error) {
	if tmpDir, err = ioutil.TempDir("", "aos_"); err != nil {
		return err
	}

	fileName := path.Join(tmpDir, "aos_communicationmanager.cfg")

	if err = createConfigFile(fileName); err != nil {
		return err
	}

	if testCfg, err = config.New(fileName); err != nil {
		return err
	}

	return nil
}

func cleanup() (err error) {
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}

	return nil
}
