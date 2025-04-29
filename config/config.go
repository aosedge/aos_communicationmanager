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
	"os"
	"path"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/journalalerts"
	"github.com/aosedge/aos_common/resourcemonitor"
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
	FileServerURL string            `json:"fileServerUrl"`
	CMServerURL   string            `json:"cmServerUrl"`
	UpdateTTL     aostypes.Duration `json:"updateTtl"`
}

// Monitoring configuration for system monitoring.
type Monitoring struct {
	MonitorConfig      *resourcemonitor.Config `json:"monitorConfig"`
	MaxOfflineMessages int                     `json:"maxOfflineMessages"`
	SendPeriod         aostypes.Duration       `json:"sendPeriod"`
	MaxMessageSize     int                     `json:"maxMessageSize"`
}

// Alerts configuration for alerts.
type Alerts struct {
	JournalAlerts      *journalalerts.Config `json:"journalAlerts,omitempty"`
	SendPeriod         aostypes.Duration     `json:"sendPeriod"`
	MaxMessageSize     int                   `json:"maxMessageSize"`
	MaxOfflineMessages int                   `json:"maxOfflineMessages"`
}

// Migration struct represents path for db migration.
type Migration struct {
	MigrationPath       string `json:"migrationPath"`
	MergedMigrationPath string `json:"mergedMigrationPath"`
}

// Downloader downloader configuration.
type Downloader struct {
	DownloadDir            string            `json:"downloadDir"`
	MaxConcurrentDownloads int               `json:"maxConcurrentDownloads"`
	RetryDelay             aostypes.Duration `json:"retryDelay"`
	MaxRetryDelay          aostypes.Duration `json:"maxRetryDelay"`
	DownloadPartLimit      int               `json:"downloadPartLimit"`
}

// SMController SM controller configuration.
type SMController struct {
	FileServerURL          string            `json:"fileServerUrl"`
	CMServerURL            string            `json:"cmServerUrl"`
	NodesConnectionTimeout aostypes.Duration `json:"nodesConnectionTimeout"`
	UpdateTTL              aostypes.Duration `json:"updateTtl"`
}

// Config instance.
type Config struct {
	Crypt                 Crypt             `json:"fcrypt"`
	CertStorage           string            `json:"certStorage"`
	ServiceDiscoveryURL   string            `json:"serviceDiscoveryUrl"`
	IAMProtectedServerURL string            `json:"iamProtectedServerUrl"`
	IAMPublicServerURL    string            `json:"iamPublicServerUrl"`
	CMServerURL           string            `json:"cmServerUrl"`
	Downloader            Downloader        `json:"downloader"`
	StorageDir            string            `json:"storageDir"`
	StateDir              string            `json:"stateDir"`
	WorkingDir            string            `json:"workingDir"`
	ImageStoreDir         string            `json:"imageStoreDir"`
	ComponentsDir         string            `json:"componentsDir"`
	UnitConfigFile        string            `json:"unitConfigFile"`
	ServiceTTL            aostypes.Duration `json:"serviceTtlDays"`
	LayerTTL              aostypes.Duration `json:"layerTtlDays"`
	UnitStatusSendTimeout aostypes.Duration `json:"unitStatusSendTimeout"`
	Monitoring            Monitoring        `json:"monitoring"`
	Alerts                Alerts            `json:"alerts"`
	Migration             Migration         `json:"migration"`
	SMController          SMController      `json:"smController"`
	UMController          UMController      `json:"umController"`
	DNSIP                 string            `json:"dnsIp"`
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new config object.
func New(fileName string) (config *Config, err error) {
	raw, err := os.ReadFile(fileName)
	if err != nil {
		return config, aoserrors.Wrap(err)
	}

	config = &Config{
		ServiceTTL:            aostypes.Duration{Duration: 30 * 24 * time.Hour},
		LayerTTL:              aostypes.Duration{Duration: 30 * 24 * time.Hour},
		UnitStatusSendTimeout: aostypes.Duration{Duration: 30 * time.Second},
		Alerts: Alerts{
			SendPeriod:         aostypes.Duration{Duration: 10 * time.Second},
			MaxMessageSize:     65536,
			MaxOfflineMessages: 25,
		},
		Monitoring: Monitoring{
			MaxOfflineMessages: 16,
			SendPeriod:         aostypes.Duration{Duration: 1 * time.Minute},
			MaxMessageSize:     65536,
		},
		Downloader: Downloader{
			MaxConcurrentDownloads: 4,
			RetryDelay:             aostypes.Duration{Duration: 1 * time.Minute},
			MaxRetryDelay:          aostypes.Duration{Duration: 30 * time.Minute},
			DownloadPartLimit:      100,
		},
		SMController: SMController{
			NodesConnectionTimeout: aostypes.Duration{Duration: 10 * time.Minute},
			UpdateTTL:              aostypes.Duration{Duration: 30 * 24 * time.Hour},
		},
		UMController: UMController{UpdateTTL: aostypes.Duration{Duration: 30 * 24 * time.Hour}},
	}

	if err = json.Unmarshal(raw, &config); err != nil {
		return config, aoserrors.Wrap(err)
	}

	if config.CertStorage == "" {
		config.CertStorage = "/var/aos/crypt/cm/"
	}

	if config.StorageDir == "" {
		config.StorageDir = path.Join(config.WorkingDir, "storages")
	}

	if config.StateDir == "" {
		config.StateDir = path.Join(config.WorkingDir, "states")
	}

	if config.Downloader.DownloadDir == "" {
		config.Downloader.DownloadDir = path.Join(config.WorkingDir, "download")
	}

	if config.ImageStoreDir == "" {
		config.ImageStoreDir = path.Join(config.WorkingDir, "imagestore")
	}

	if config.ComponentsDir == "" {
		config.ComponentsDir = path.Join(config.WorkingDir, "components")
	}

	if config.UnitConfigFile == "" {
		config.UnitConfigFile = path.Join(config.WorkingDir, "aos_unit.cfg")
	}

	if config.Migration.MigrationPath == "" {
		config.Migration.MigrationPath = "/usr/share/aos/communicationmanager/migration"
	}

	if config.Migration.MergedMigrationPath == "" {
		config.Migration.MergedMigrationPath = path.Join(config.WorkingDir, "migration")
	}

	return config, nil
}
