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

// Package config provides set of API to provide aos configuration
package config

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Crypt configuration structure with crypto attributes.
type Crypt struct {
	CACert        string `json:"caCert"`
	TpmDevice     string `json:"tpmDevice,omitempty"`
	Pkcs11Library string `json:"pkcs11Library,omitempty"`
}

// UMController configuration for update controller.
type UMController struct {
	ServerURL string            `json:"serverUrl"`
	UMClients []UMClientConfig  `json:"umClients"`
	UpdateTTL aostypes.Duration `json:"updateTtl"`
}

// UMClientConfig update manager config.
type UMClientConfig struct {
	UMID     string `json:"umId"`
	Priority uint32 `json:"priority"`
	IsLocal  bool   `json:"isLocal,omitempty"`
}

// AlertRule describes alert rule.
type AlertRule struct {
	MinTimeout   aostypes.Duration `json:"minTimeout"`
	MinThreshold uint64            `json:"minThreshold"`
	MaxThreshold uint64            `json:"maxThreshold"`
}

// Alerts configuration for alerts.
type Alerts struct {
	EnableSystemAlerts bool              `json:"enableSystemAlerts"`
	SendPeriod         aostypes.Duration `json:"sendPeriod"`
	MaxMessageSize     int               `json:"maxMessagesize"`
	MaxOfflineMessages int               `json:"maxOfflineMessages"`
	Filter             []string          `json:"filter"`
}

// Migration struct represents path for db migration.
type Migration struct {
	MigrationPath       string `json:"migrationPath"`
	MergedMigrationPath string `json:"mergedMigrationPath"`
}

// Downloader downloader configuration.
type Downloader struct {
	DownloadDir            string            `json:"downloadDir"`
	DecryptDir             string            `json:"decryptDir"`
	MaxConcurrentDownloads int               `json:"maxConcurrentDownloads"`
	RetryDelay             aostypes.Duration `json:"retryDelay"`
	MaxRetryDelay          aostypes.Duration `json:"maxRetryDelay"`
	DownloadPartLimit      int               `json:"downloadPartLimit"`
}

// SMConfig SM configuration.
type SMConfig struct {
	SMID      string `json:"smId"`
	ServerURL string `json:"serverUrl"`
	IsLocal   bool   `json:"isLocal,omitempty"`
}

// SMController SM controller configuration.
type SMController struct {
	SMList    []SMConfig        `json:"smList"`
	UpdateTTL aostypes.Duration `json:"updateTtl"`
}

// Config instance.
type Config struct {
	Crypt                 Crypt               `json:"fcrypt"`
	CertStorage           string              `json:"certStorage"`
	ServiceDiscoveryURL   string              `json:"serviceDiscoveryUrl"`
	IAMServerURL          string              `json:"iamServerUrl"`
	IAMPublicServerURL    string              `json:"iamPublicServerUrl"`
	FileServerURL         string              `json:"fileServerUrl"`
	CMServerURL           string              `json:"cmServerUrl"`
	Downloader            Downloader          `json:"downloader"`
	WorkingDir            string              `json:"workingDir"`
	BoardConfigFile       string              `json:"boardConfigFile"`
	UnitStatusSendTimeout aostypes.Duration   `json:"unitStatusSendTimeout"`
	Monitoring            aostypes.Monitoring `json:"monitoring"`
	Alerts                Alerts              `json:"alerts"`
	Migration             Migration           `json:"migration"`
	SMController          SMController        `json:"smController"`
	UMController          UMController        `json:"umController"`
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new config object.
func New(fileName string) (config *Config, err error) {
	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, aoserrors.Wrap(err)
	}

	config = &Config{
		UnitStatusSendTimeout: aostypes.Duration{Duration: 30 * time.Second},
		Monitoring: aostypes.Monitoring{
			SendPeriod:         aostypes.Duration{Duration: 1 * time.Minute},
			PollPeriod:         aostypes.Duration{Duration: 10 * time.Second},
			MaxOfflineMessages: 25,
		},
		Alerts: Alerts{
			SendPeriod:         aostypes.Duration{Duration: 10 * time.Second},
			MaxMessageSize:     65536,
			MaxOfflineMessages: 25,
		},
		Downloader: Downloader{
			MaxConcurrentDownloads: 4,
			RetryDelay:             aostypes.Duration{Duration: 1 * time.Minute},
			MaxRetryDelay:          aostypes.Duration{Duration: 30 * time.Minute},
			DownloadPartLimit:      100,
		},
		SMController: SMController{UpdateTTL: aostypes.Duration{Duration: 30 * 24 * time.Hour}},
		UMController: UMController{UpdateTTL: aostypes.Duration{Duration: 30 * 24 * time.Hour}},
	}

	if err = json.Unmarshal(raw, &config); err != nil {
		return config, aoserrors.Wrap(err)
	}

	if config.CertStorage == "" {
		config.CertStorage = "/var/aos/crypt/cm/"
	}

	if config.Downloader.DownloadDir == "" {
		config.Downloader.DownloadDir = path.Join(config.WorkingDir, "download")
	}

	if config.Downloader.DecryptDir == "" {
		config.Downloader.DecryptDir = path.Join(config.WorkingDir, "decrypt")
	}

	if config.BoardConfigFile == "" {
		config.BoardConfigFile = path.Join(config.WorkingDir, "aos_board.cfg")
	}

	if config.Migration.MigrationPath == "" {
		config.Migration.MigrationPath = "/usr/share/aos/communicationmanager/migration"
	}

	if config.Migration.MergedMigrationPath == "" {
		config.Migration.MergedMigrationPath = path.Join(config.WorkingDir, "migration")
	}

	return config, nil
}
