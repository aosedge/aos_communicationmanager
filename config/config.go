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

// Package config provides set of API to provide aos configuration
package config

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"time"

	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Crypt configuration structure with crypto attributes
type Crypt struct {
	CACert        string `json:"CACert"`
	TpmDevice     string `json:"tpmDevice,omitempty"`
	Pkcs11Library string `json:"pkcs11Library,omitempty"`
}

// UMController configuration for update controller
type UMController struct {
	ServerURL     string           `json:"serverUrl"`
	FileServerURL string           `json:"fileServerUrl,omitempty"`
	UMClients     []UMClientConfig `json:"umClients"`
	UpdateDir     string           `json:"updateDir"`
}

// UMClientConfig update manager config
type UMClientConfig struct {
	UMID     string `json:"umId"`
	Priority uint32 `json:"priority"`
	IsLocal  bool   `json:"isLocal,omitempty"`
}

// Duration represents duration in format "00:00:00"
type Duration struct {
	time.Duration
}

// AlertRule describes alert rule
type AlertRule struct {
	MinTimeout   Duration `json:"minTimeout"`
	MinThreshold uint64   `json:"minThreshold"`
	MaxThreshold uint64   `json:"maxThreshold"`
}

// Monitoring configuration for system monitoring
type Monitoring struct {
	EnableSystemMonitoring bool       `json:"enableSystemMonitoring"`
	MaxOfflineMessages     int        `json:"maxOfflineMessages"`
	SendPeriod             Duration   `json:"sendPeriod"`
	PollPeriod             Duration   `json:"pollPeriod"`
	RAM                    *AlertRule `json:"ram"`
	CPU                    *AlertRule `json:"cpu"`
	UsedDisk               *AlertRule `json:"usedDisk"`
	InTraffic              *AlertRule `json:"inTraffic"`
	OutTraffic             *AlertRule `json:"outTraffic"`
}

// Logging configuration for system and service logging
type Logging struct {
	MaxPartSize  uint64 `json:"maxPartSize"`
	MaxPartCount uint64 `json:"maxPartCount"`
}

// Alerts configuration for alerts
type Alerts struct {
	EnableSystemAlerts bool     `json:"enableSystemAlerts"`
	SendPeriod         Duration `json:"sendPeriod"`
	MaxMessageSize     int      `json:"maxMessagesize"`
	MaxOfflineMessages int      `json:"maxOfflineMessages"`
	Filter             []string `json:"filter"`
}

// Migration struct represents path for db migration
type Migration struct {
	MigrationPath       string `json:"migrationPath"`
	MergedMigrationPath string `json:"mergedMigrationPath"`
}

// Downloader downloader configuration
type Downloader struct {
	DownloadDir            string   `json:"downloadDir"`
	DecryptDir             string   `json:"decryptDir"`
	MaxConcurrentDownloads int      `json:"maxConcurrentDownloads"`
	RetryCount             int      `json:"retryCount"`
	RetryDelay             Duration `json:"retryDelay"`
}

// Config instance
type Config struct {
	Crypt                 Crypt        `json:"fcrypt"`
	CertStorage           string       `json:"certStorage"`
	ServiceDiscoveryURL   string       `json:"serviceDiscoveryUrl"`
	IAMServerURL          string       `json:"iamServerUrl"`
	FileServerURL         string       `json:"fileServerUrl"`
	Downloader            Downloader   `json:"downloader"`
	WorkingDir            string       `json:"workingDir"`
	BoardConfigFile       string       `json:"boardConfigFile"`
	UnitStatusSendTimeout Duration     `json:"unitStatusSendTimeout"`
	Monitoring            Monitoring   `json:"monitoring"`
	Logging               Logging      `json:"logging"`
	Alerts                Alerts       `json:"alerts"`
	Migration             Migration    `json:"migration"`
	UMController          UMController `json:"umController"`
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new config object
func New(fileName string) (config *Config, err error) {
	raw, err := ioutil.ReadFile(fileName)
	if err != nil {
		return config, err
	}

	config = &Config{
		UnitStatusSendTimeout: Duration{30 * time.Second},
		Monitoring: Monitoring{
			SendPeriod:         Duration{1 * time.Minute},
			PollPeriod:         Duration{10 * time.Second},
			MaxOfflineMessages: 25},
		Logging: Logging{
			MaxPartSize:  524288,
			MaxPartCount: 20},
		Alerts: Alerts{
			SendPeriod:         Duration{10 * time.Second},
			MaxMessageSize:     65536,
			MaxOfflineMessages: 25},
		Downloader: Downloader{
			MaxConcurrentDownloads: 4,
			RetryCount:             3,
			RetryDelay:             Duration{1 * time.Minute},
		},
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

	if config.UMController.UpdateDir == "" {
		config.UMController.UpdateDir = path.Join(config.WorkingDir, "update")
	}

	return config, nil
}

// MarshalJSON marshals JSON Duration type
func (d Duration) MarshalJSON() (b []byte, err error) {
	t, err := time.Parse("15:04:05", "00:00:00")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}
	t.Add(d.Duration)

	return json.Marshal(t.Add(d.Duration).Format("15:04:05"))
}

// UnmarshalJSON unmarshals JSON Duration type
func (d *Duration) UnmarshalJSON(b []byte) (err error) {
	var v interface{}

	if err := json.Unmarshal(b, &v); err != nil {
		return aoserrors.Wrap(err)
	}

	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value) * time.Second
		return nil

	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			t1, err := time.Parse("15:04:05", value)
			if err != nil {
				return aoserrors.Wrap(err)
			}
			t2, err := time.Parse("15:04:05", "00:00:00")
			if err != nil {
				return aoserrors.Wrap(err)
			}

			tmp = t1.Sub(t2)
		}

		d.Duration = tmp

		return nil

	default:
		return aoserrors.Errorf("invalid duration value: %v", value)
	}
}
