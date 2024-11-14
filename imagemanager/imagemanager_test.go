// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

// Package launcher provides set of API to controls services lifecycle

package imagemanager_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/image"
	"github.com/aosedge/aos_common/spaceallocator"
	"github.com/aosedge/aos_common/utils/fs"
	"github.com/opencontainers/go-digest"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/fcrypt"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	megabyte = uint64(1 << 20)

	blobsFolder = "blobs"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testCryptoContext struct{}

type testStorageProvider struct {
	layers   map[string]imagemanager.LayerInfo
	services map[string][]imagemanager.ServiceInfo
}

type testAllocator struct {
	sync.Mutex

	totalSize     uint64
	allocatedSize uint64
	remover       spaceallocator.ItemRemover
	outdatedItems []testOutdatedItem
}

type testOutdatedItem struct {
	id   string
	size uint64
}

type testSpace struct {
	allocator *testAllocator
	size      uint64
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	tmpDir           string
	layersDir        string
	servicesDir      string
	layerAllocator   = &testAllocator{}
	serviceAllocator = &testAllocator{}
)

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	var err error

	if err = setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	if err = cleanup(); err != nil {
		log.Errorf("Error cleaning up: %s", err)
	}

	os.Exit(ret)
}

func TestInstallService(t *testing.T) {
	storage := &testStorageProvider{
		services: make(map[string][]imagemanager.ServiceInfo),
	}

	serviceAllocator = &testAllocator{
		totalSize: 3 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	cases := []struct {
		serviceID            string
		version              string
		size                 uint64
		expectedCountService int
		installErr           error
		serviceConfig        aostypes.ServiceConfig
	}{
		{
			serviceID:            "service1",
			version:              "1.0",
			size:                 1 * megabyte,
			expectedCountService: 1,
			installErr:           nil,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(1000),
					DownloadSpeed: allocateUint64(2000),
				},
				Resources: []string{"resource1", "resource2"},
			},
		},
		{
			serviceID:            "service2",
			version:              "1.0",
			size:                 1 * megabyte,
			expectedCountService: 2,
			installErr:           nil,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service2"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
				Resources: []string{"resource1"},
			},
		},
		{
			serviceID:            "service3",
			version:              "1.0",
			size:                 2 * megabyte,
			expectedCountService: 2,
			installErr:           spaceallocator.ErrNoSpace,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service3"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
			},
		},
	}

	defer func() {
		if err = clearServicesDir(); err != nil {
			t.Errorf("Can't clear services dir: %v", err)
		}
	}()

	for _, tCase := range cases {
		configJSON, err := json.Marshal(tCase.serviceConfig)
		if err != nil {
			t.Errorf("Can't generate config json: %v", err)
		}

		servicePath, layerDigests, err := prepareService(tCase.size, configJSON)
		if err != nil {
			t.Errorf("Can't prepare service file: %v", err)
		}

		serviceInfo, err := prepareServiceInfo(servicePath, tCase.serviceID, tCase.version)
		if err != nil {
			t.Errorf("Can't prepare service info: %v", err)
		}

		if err := imagemanagerInstance.InstallService(serviceInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install service: %v", err)
		}

		if tCase.installErr == nil {
			service, err := imagemanagerInstance.GetServiceInfo(tCase.serviceID)
			if err != nil {
				t.Errorf("Can't get service: %v", err)
			}

			if service.ServiceID != tCase.serviceID || service.Version != tCase.version {
				t.Error("Unexpected service info")
			}

			if !reflect.DeepEqual(layerDigests, service.Layers) {
				t.Error("Unexpected layer digest")
			}

			if !reflect.DeepEqual(service.Config, tCase.serviceConfig) {
				t.Error("Unexpected service config")
			}
		}

		services, err := imagemanagerInstance.GetServicesStatus()
		if err != nil {
			t.Errorf("Can't get services status: %v", err)
		}

		if len(services) != tCase.expectedCountService {
			t.Error("Unexpected count services status")
		}

		for _, service := range services {
			if service.Status != cloudprotocol.InstalledStatus {
				t.Error("Unexpected service status")
			}
		}
	}
}

func TestRevertService(t *testing.T) {
	storage := &testStorageProvider{
		services: make(map[string][]imagemanager.ServiceInfo),
	}

	serviceAllocator = &testAllocator{
		totalSize: 5 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	cases := []struct {
		serviceID     string
		version       string
		size          uint64
		serviceConfig aostypes.ServiceConfig
		cacheService  bool
	}{
		{
			serviceID: "service1",
			version:   "1.0.0",
			size:      1 * megabyte,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(1000),
					DownloadSpeed: allocateUint64(2000),
				},
				Resources: []string{"resource1", "resource2"},
			},
			cacheService: true,
		},
		{
			serviceID: "service1",
			version:   "1.0.1",
			size:      1 * megabyte,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(1000),
					DownloadSpeed: allocateUint64(2000),
				},
				Resources: []string{"resource1", "resource2"},
			},
			cacheService: true,
		},
		{
			serviceID: "service1",
			version:   "2.0.0",
			size:      1 * megabyte,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
				Resources: []string{"resource1"},
			},
			cacheService: false,
		},
		{
			serviceID: "service1",
			version:   "3.0.0",
			size:      1 * megabyte,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
				Resources: []string{"resource1"},
			},
			cacheService: true,
		},
	}

	for _, tCase := range cases {
		configJSON, err := json.Marshal(tCase.serviceConfig)
		if err != nil {
			t.Errorf("Can't generate config json: %v", err)
		}

		servicePath, _, err := prepareService(tCase.size, configJSON)
		if err != nil {
			t.Errorf("Can't prepare service file: %v", err)
		}

		serviceInfo, err := prepareServiceInfo(servicePath, tCase.serviceID, tCase.version)
		if err != nil {
			t.Errorf("Can't prepare service info: %v", err)
		}

		if err := imagemanagerInstance.InstallService(serviceInfo, nil, nil); err != nil {
			t.Errorf("Can't install service: %v", err)
		}

		if tCase.cacheService {
			if err := storage.SetServiceCached(tCase.serviceID, true); err != nil {
				t.Errorf("Can't set service cached: %v", err)
			}
		}
	}

	casesRevert := []struct {
		revertServiceID string
		expectedVersion string
		err             error
	}{
		{
			revertServiceID: "service1",
			expectedVersion: "1.0.1",
			err:             nil,
		},
		{
			revertServiceID: "service1",
			expectedVersion: "1.0.0",
			err:             nil,
		},
		{
			revertServiceID: "service1",
			err:             imagemanager.ErrNotExist,
		},
	}

	for _, tCase := range casesRevert {
		if err := imagemanagerInstance.RevertService(tCase.revertServiceID); err != nil {
			t.Errorf("Can't revert service to previous version: %v", err)
		}

		service, err := imagemanagerInstance.GetServiceInfo(tCase.revertServiceID)
		if !errors.Is(err, tCase.err) {
			t.Errorf("Can't get service: %v", err)
		}

		if service.Version != tCase.expectedVersion {
			t.Error("Unexpected service version")
		}
	}
}

func TestRemoveService(t *testing.T) {
	storage := &testStorageProvider{
		services: make(map[string][]imagemanager.ServiceInfo),
	}

	serviceAllocator = &testAllocator{
		totalSize: 2 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}

	go func() {
		for serviceID := range imagemanagerInstance.GetRemoveServiceChannel() {
			if serviceID != "service1" {
				t.Errorf("Expected service1, got: %s", serviceID)
			}
		}
	}()

	defer imagemanagerInstance.Close()

	cases := []struct {
		serviceID            string
		removeServiceID      string
		version              string
		size                 uint64
		expectedCountService int
		installErr           error
		serviceConfig        aostypes.ServiceConfig
	}{
		{
			serviceID:            "service1",
			version:              "1.0",
			size:                 1 * megabyte,
			expectedCountService: 1,
			installErr:           nil,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(1000),
					DownloadSpeed: allocateUint64(2000),
				},
				Resources: []string{"resource1", "resource2"},
			},
		},
		{
			serviceID:            "service2",
			removeServiceID:      "service1",
			version:              "1.0",
			size:                 1 * megabyte,
			expectedCountService: 1,
			installErr:           spaceallocator.ErrNoSpace,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service2"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
				Resources: []string{"resource1"},
			},
		},
	}

	for _, tCase := range cases {
		configJSON, err := json.Marshal(tCase.serviceConfig)
		if err != nil {
			t.Errorf("Can't generate config json: %v", err)
		}

		servicePath, _, err := prepareService(tCase.size, configJSON)
		if err != nil {
			t.Errorf("Can't prepare service file: %v", err)
		}

		serviceInfo, err := prepareServiceInfo(servicePath, tCase.serviceID, tCase.version)
		if err != nil {
			t.Errorf("Can't prepare service info: %v", err)
		}

		if err := imagemanagerInstance.InstallService(serviceInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install service: %v", err)
		}

		if tCase.removeServiceID != "" {
			if err := imagemanagerInstance.RemoveService(tCase.removeServiceID); err != nil {
				t.Errorf("Can't remove service: %v", err)
			}

			service, err := imagemanagerInstance.GetServiceInfo(tCase.removeServiceID)
			if err != nil {
				t.Errorf("Can't get service info: %v", err)
			}

			if !service.Cached {
				t.Error("Service should be cached")
			}

			if err := imagemanagerInstance.InstallService(serviceInfo, nil, nil); err != nil {
				t.Errorf("Can't install service: %v", err)
			}

			if _, err = imagemanagerInstance.GetServiceInfo(tCase.removeServiceID); err == nil {
				t.Error("Expect error service not found")
			}

			if _, err = imagemanagerInstance.GetServiceInfo(tCase.serviceID); err != nil {
				t.Errorf("Can't get service info: %v", err)
			}
		}

		services, err := imagemanagerInstance.GetServicesStatus()
		if err != nil {
			t.Errorf("Can't get services status: %v", err)
		}

		if len(services) != tCase.expectedCountService {
			t.Error("Unexpected count services status")
		}

		for _, service := range services {
			if service.Status != cloudprotocol.InstalledStatus {
				t.Error("Unexpected service status")
			}
		}
	}
}

func TestRestoreService(t *testing.T) {
	storage := &testStorageProvider{
		services: make(map[string][]imagemanager.ServiceInfo),
	}

	serviceAllocator = &testAllocator{
		totalSize: 2 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}

	go func() {
		for serviceID := range imagemanagerInstance.GetRemoveServiceChannel() {
			if serviceID != "service1" {
				t.Errorf("Expected service1, got: %s", serviceID)
			}
		}
	}()

	defer imagemanagerInstance.Close()

	cases := []struct {
		serviceID       string
		removeServiceID string
		version         string
		size            uint64
		installErr      error
		serviceConfig   aostypes.ServiceConfig
	}{
		{
			serviceID:  "service1",
			version:    "1.0",
			size:       1 * megabyte,
			installErr: nil,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service1"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(1000),
					DownloadSpeed: allocateUint64(2000),
				},
				Resources: []string{"resource1", "resource2"},
			},
		},
		{
			serviceID:       "service2",
			removeServiceID: "service1",
			version:         "1.0",
			size:            1 * megabyte,
			installErr:      spaceallocator.ErrNoSpace,
			serviceConfig: aostypes.ServiceConfig{
				Hostname: allocateString("service2"),
				Quotas: aostypes.ServiceQuotas{
					UploadSpeed:   allocateUint64(500),
					DownloadSpeed: allocateUint64(1000),
				},
				Resources: []string{"resource1"},
			},
		},
	}

	for _, tCase := range cases {
		configJSON, err := json.Marshal(tCase.serviceConfig)
		if err != nil {
			t.Errorf("Can't generate config json: %v", err)
		}

		servicePath, _, err := prepareService(tCase.size, configJSON)
		if err != nil {
			t.Errorf("Can't prepare service file: %v", err)
		}

		serviceInfo, err := prepareServiceInfo(servicePath, tCase.serviceID, tCase.version)
		if err != nil {
			t.Errorf("Can't prepare service info: %v", err)
		}

		if err := imagemanagerInstance.InstallService(serviceInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install service: %v", err)
		}

		if tCase.installErr != nil {
			if err := imagemanagerInstance.RemoveService(tCase.removeServiceID); err != nil {
				t.Errorf("Can't remove service: %v", err)
			}

			service, err := imagemanagerInstance.GetServiceInfo(tCase.removeServiceID)
			if err != nil {
				t.Errorf("Can't get service info: %v", err)
			}

			if !service.Cached {
				t.Error("Service should be cached")
			}

			if err := imagemanagerInstance.RestoreService(tCase.removeServiceID); err != nil {
				t.Errorf("Can't restore service: %v", err)
			}

			if service, err = imagemanagerInstance.GetServiceInfo(tCase.removeServiceID); err != nil {
				t.Errorf("Can't get service info: %v", err)
			}

			if service.Cached {
				t.Error("Service should not be cached")
			}

			if err := imagemanagerInstance.RemoveService(tCase.removeServiceID); err != nil {
				t.Errorf("Can't remove service: %v", err)
			}

			if err = imagemanagerInstance.InstallService(serviceInfo, nil, nil); err != nil {
				t.Errorf("Can't install service: %v", err)
			}

			if err := imagemanagerInstance.RestoreService(tCase.removeServiceID); err == nil {
				t.Error("Expected error service not found")
			}
		}
	}
}

func TestRestoreLayer(t *testing.T) {
	storage := &testStorageProvider{
		layers: make(map[string]imagemanager.LayerInfo),
	}

	layerAllocator = &testAllocator{
		totalSize: 2 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	cases := []struct {
		digest             string
		removedDigest      string
		size               uint64
		expectedCountLayer int
		installErr         error
	}{
		{
			digest:             "digest1",
			size:               1 * megabyte,
			expectedCountLayer: 1,
			installErr:         nil,
		},
		{
			digest:             "digest2",
			size:               1 * megabyte,
			expectedCountLayer: 1,
			installErr:         nil,
		},
		{
			digest:             "digest3",
			size:               1 * megabyte,
			removedDigest:      "digest1",
			expectedCountLayer: 1,
			installErr:         spaceallocator.ErrNoSpace,
		},
	}

	for index, tCase := range cases {
		fileName := path.Join(tmpDir, fmt.Sprintf("layer_%d", index))

		if err = generateFile(fileName, tCase.size); err != nil {
			t.Errorf("Can't generate file: %v", err)
		}
		defer os.RemoveAll(fileName)

		layerInfo, err := prepareLayerInfo(fileName, tCase.digest, index)
		if err != nil {
			t.Errorf("Can't prepare layer info data: %v", err)
		}

		if err = imagemanagerInstance.InstallLayer(layerInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install layer: %v", err)
		}

		if tCase.installErr != nil {
			if err := imagemanagerInstance.RemoveLayer(tCase.removedDigest); err != nil {
				t.Errorf("Can't remove layer: %v", err)
			}

			layer, err := imagemanagerInstance.GetLayerInfo(tCase.removedDigest)
			if err != nil {
				t.Errorf("Can't get layer info: %v", err)
			}

			if !layer.Cached {
				t.Error("Layer should be cached")
			}

			if err := imagemanagerInstance.RestoreLayer(tCase.removedDigest); err != nil {
				t.Errorf("Can't restore layer: %v", err)
			}

			if layer, err = imagemanagerInstance.GetLayerInfo(tCase.removedDigest); err != nil {
				t.Errorf("Can't get layer info: %v", err)
			}

			if layer.Cached {
				t.Error("Layer should not be cached")
			}

			if err := imagemanagerInstance.RemoveLayer(tCase.removedDigest); err != nil {
				t.Errorf("Can't remove layer: %v", err)
			}

			if err = imagemanagerInstance.InstallLayer(layerInfo, nil, nil); err != nil {
				t.Errorf("Can't install layer: %v", err)
			}

			if err := imagemanagerInstance.RestoreLayer(tCase.removedDigest); err == nil {
				t.Error("Expected error layer not found")
			}
		}
	}
}

func TestRemoveLayer(t *testing.T) {
	storage := &testStorageProvider{
		layers: make(map[string]imagemanager.LayerInfo),
	}

	layerAllocator = &testAllocator{
		totalSize: 2 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	cases := []struct {
		digest             string
		removeDigest       string
		size               uint64
		expectedCountLayer int
		installErr         error
	}{
		{
			digest:             "digest1",
			size:               1 * megabyte,
			expectedCountLayer: 1,
			installErr:         nil,
		},
		{
			digest:             "digest2",
			removeDigest:       "digest1",
			size:               2 * megabyte,
			expectedCountLayer: 1,
			installErr:         spaceallocator.ErrNoSpace,
		},
	}

	for index, tCase := range cases {
		fileName := path.Join(tmpDir, fmt.Sprintf("layer_%d", index))

		if err = generateFile(fileName, tCase.size); err != nil {
			t.Errorf("Can't generate file: %v", err)
		}
		defer os.RemoveAll(fileName)

		layerInfo, err := prepareLayerInfo(fileName, tCase.digest, index)
		if err != nil {
			t.Errorf("Can't prepare layer info data: %v", err)
		}

		if err = imagemanagerInstance.InstallLayer(layerInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install layer: %v", err)
		}

		if tCase.removeDigest != "" {
			if err := imagemanagerInstance.RemoveLayer(tCase.removeDigest); err != nil {
				t.Errorf("Can't remove layer: %v", err)
			}

			layer, err := imagemanagerInstance.GetLayerInfo(tCase.removeDigest)
			if err != nil {
				t.Errorf("Can't get layer info: %v", err)
			}

			if !layer.Cached {
				t.Error("Layer should be cached")
			}

			if err := imagemanagerInstance.InstallLayer(layerInfo, nil, nil); err != nil {
				t.Errorf("Can't install layer: %v", err)
			}

			if _, err = imagemanagerInstance.GetLayerInfo(tCase.removeDigest); err == nil {
				t.Error("Expect error layer not found")
			}

			if _, err = imagemanagerInstance.GetLayerInfo(tCase.digest); err != nil {
				t.Errorf("Can't get layer info: %v", err)
			}
		}

		layers, err := imagemanagerInstance.GetLayersStatus()
		if err != nil {
			t.Errorf("Can't get layer status: %v", err)
		}

		if len(layers) != tCase.expectedCountLayer {
			t.Error("Unexpected count services status")
		}

		for _, layer := range layers {
			if layer.Status != cloudprotocol.InstalledStatus {
				t.Error("Unexpected layer status")
			}
		}
	}
}

func TestInstallLayer(t *testing.T) {
	storage := &testStorageProvider{
		layers: make(map[string]imagemanager.LayerInfo),
	}

	layerAllocator = &testAllocator{
		totalSize: 2 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	cases := []struct {
		digest             string
		size               uint64
		expectedCountLayer int
		installErr         error
	}{
		{
			digest:             "digest1",
			size:               1 * megabyte,
			expectedCountLayer: 1,
			installErr:         nil,
		},
		{
			digest:             "digest2",
			size:               1 * megabyte,
			expectedCountLayer: 2,
			installErr:         nil,
		},
		{
			digest:             "digest1",
			size:               1 * megabyte,
			expectedCountLayer: 2,
			installErr:         nil,
		},
		{
			digest:             "digest3",
			size:               2 * megabyte,
			expectedCountLayer: 2,
			installErr:         spaceallocator.ErrNoSpace,
		},
	}

	defer func() {
		if err = clearLayersDir(); err != nil {
			t.Errorf("Can't clear layers dir: %v", err)
		}
	}()

	for index, tCase := range cases {
		fileName := path.Join(tmpDir, fmt.Sprintf("layer_%d", index))

		if err = generateFile(fileName, tCase.size); err != nil {
			t.Errorf("Can't generate file: %v", err)
		}
		defer os.RemoveAll(fileName)

		layerInfo, err := prepareLayerInfo(fileName, tCase.digest, index)
		if err != nil {
			t.Errorf("Can't prepare layer info data: %v", err)
		}

		if err = imagemanagerInstance.InstallLayer(layerInfo, nil, nil); !errors.Is(err, tCase.installErr) {
			t.Errorf("Can't install layer: %v", err)
		}

		if tCase.installErr == nil {
			layer, err := imagemanagerInstance.GetLayerInfo(tCase.digest)
			if err != nil {
				t.Errorf("Can't install layer: %v", err)
			}

			if layer.Digest != tCase.digest || layer.Size != tCase.size {
				t.Error("Unexpected layer info")
			}
		}

		layers, err := imagemanagerInstance.GetLayersStatus()
		if err != nil {
			t.Errorf("Can't get layers status: %v", err)
		}

		if tCase.expectedCountLayer != len(layers) {
			t.Error("Unexpected layers count")
		}

		for _, layer := range layers {
			if layer.Status != cloudprotocol.InstalledStatus {
				t.Error("Unexpected layer status")
			}
		}
	}
}

func TestFileServer(t *testing.T) {
	storage := &testStorageProvider{
		layers: make(map[string]imagemanager.LayerInfo),
	}

	layerAllocator = &testAllocator{
		totalSize: 5 * megabyte,
	}

	imagemanagerInstance, err := imagemanager.New(&config.Config{
		ImageStoreDir: tmpDir,
		WorkingDir:    tmpDir,
		SMController: config.SMController{
			FileServerURL: "localhost:8092",
		},
	}, storage, &testCryptoContext{})
	if err != nil {
		t.Fatalf("Can't create image manager instance: %v", err)
	}
	defer imagemanagerInstance.Close()

	fileName := path.Join(tmpDir, "layer")

	if err := os.WriteFile(fileName, []byte("Hello fileserver"), 0o600); err != nil {
		t.Fatalf("Can't create package file: %s", err)
	}

	layerInfo, err := prepareLayerInfo(fileName, "digest", 1)
	if err != nil {
		t.Fatalf("Can't prepare layer info data: %v", err)
	}

	if err = imagemanagerInstance.InstallLayer(layerInfo, nil, nil); err != nil {
		t.Fatalf("Can't install layer: %v", err)
	}

	layer, err := imagemanagerInstance.GetLayerInfo("digest")
	if err != nil {
		t.Fatalf("Can't install layer: %v", err)
	}

	decryptFilePath := path.Join("layers", base64.URLEncoding.EncodeToString(layerInfo.Sha256)+".dec")

	if layer.RemoteURL != "http://localhost:8092/"+decryptFilePath ||
		layer.URL != "file://"+path.Join(layersDir, base64.URLEncoding.EncodeToString(layerInfo.Sha256)+".dec") {
		t.Fatalf("Unexpected urls")
	}

	time.Sleep(1 * time.Second)

	resp, err := http.Get(layer.RemoteURL)
	if err != nil {
		t.Fatalf("Can't download file: %s", err)
	}
	defer resp.Body.Close()

	var buffer bytes.Buffer

	_, err = io.Copy(&buffer, resp.Body)
	if err != nil {
		t.Fatalf("Can't get data from response: %s", err)
	}

	if buffer.String() != "Hello fileserver" {
		t.Errorf("incorrect file content: %s", buffer.String())
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newSpaceAllocator(
	path string, partLimit uint, remover spaceallocator.ItemRemover,
) (spaceallocator.Allocator, error) {
	switch path {
	case layersDir:
		layerAllocator.remover = remover
		return layerAllocator, nil

	case servicesDir:
		serviceAllocator.remover = remover
		return serviceAllocator, nil

	default:
		return &testAllocator{remover: remover}, nil
	}
}

func (allocator *testAllocator) AllocateSpace(size uint64) (spaceallocator.Space, error) {
	allocator.Lock()
	defer allocator.Unlock()

	if allocator.totalSize != 0 && allocator.allocatedSize+size > allocator.totalSize {
		for allocator.allocatedSize+size > allocator.totalSize {
			if len(allocator.outdatedItems) == 0 {
				return nil, spaceallocator.ErrNoSpace
			}

			if err := allocator.remover(allocator.outdatedItems[0].id); err != nil {
				return nil, err
			}

			if allocator.outdatedItems[0].size < allocator.allocatedSize {
				allocator.allocatedSize -= allocator.outdatedItems[0].size
			} else {
				allocator.allocatedSize = 0
			}

			allocator.outdatedItems = allocator.outdatedItems[1:]
		}
	}

	allocator.allocatedSize += size

	return &testSpace{allocator: allocator, size: size}, nil
}

func (allocator *testAllocator) FreeSpace(size uint64) {
	allocator.Lock()
	defer allocator.Unlock()

	if size > allocator.allocatedSize {
		allocator.allocatedSize = 0
	} else {
		allocator.allocatedSize -= size
	}
}

func (allocator *testAllocator) AddOutdatedItem(id string, size uint64, timestamp time.Time) error {
	allocator.outdatedItems = append(allocator.outdatedItems, testOutdatedItem{id: id, size: size})

	return nil
}

func (allocator *testAllocator) RestoreOutdatedItem(id string) {
	for i, item := range allocator.outdatedItems {
		if item.id == id {
			allocator.outdatedItems = append(allocator.outdatedItems[:i], allocator.outdatedItems[i+1:]...)

			break
		}
	}
}

func (allocator *testAllocator) Close() error {
	return nil
}

func (space *testSpace) Accept() error {
	return nil
}

func (space *testSpace) Release() error {
	space.allocator.FreeSpace(space.size)

	return nil
}

func (context *testCryptoContext) DecryptAndValidate(
	encryptedFile, decryptedFile string, params fcrypt.DecryptParams,
) error {
	srcFile, err := os.Open(encryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(decryptedFile, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (storage *testStorageProvider) GetServiceVersions(
	serviceID string,
) (services []imagemanager.ServiceInfo, err error) {
	services, ok := storage.services[serviceID]
	if !ok {
		return nil, imagemanager.ErrNotExist
	}

	return services, nil
}

func (storage *testStorageProvider) GetServicesInfo() (services []imagemanager.ServiceInfo, err error) {
	for _, servicesStore := range storage.services {
		services = append(services, servicesStore[len(servicesStore)-1])
	}

	return services, nil
}

func (storage *testStorageProvider) GetLayersInfo() (layers []imagemanager.LayerInfo, err error) {
	for _, layer := range storage.layers {
		layers = append(layers, layer)
	}

	return layers, nil
}

func (storage *testStorageProvider) GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error) {
	services, ok := storage.services[serviceID]
	if !ok {
		return imagemanager.ServiceInfo{}, imagemanager.ErrNotExist
	}

	return services[len(services)-1], nil
}

func (storage *testStorageProvider) GetLayerInfo(digest string) (imagemanager.LayerInfo, error) {
	layer, ok := storage.layers[digest]
	if !ok {
		return imagemanager.LayerInfo{}, imagemanager.ErrNotExist
	}

	return layer, nil
}

func (storage *testStorageProvider) AddLayer(layer imagemanager.LayerInfo) error {
	_, ok := storage.layers[layer.Digest]
	if ok {
		return aoserrors.New("layer already exist")
	}

	storage.layers[layer.Digest] = layer

	return nil
}

func (storage *testStorageProvider) AddService(service imagemanager.ServiceInfo) error {
	services := storage.services[service.ServiceID]

	services = append(services, service)

	storage.services[service.ServiceID] = services

	return nil
}

func (storage *testStorageProvider) SetLayerCached(digest string, cached bool) error {
	layer, ok := storage.layers[digest]
	if !ok {
		return aoserrors.New("layer not found")
	}

	layer.Cached = cached
	storage.layers[digest] = layer

	return nil
}

func (storage *testStorageProvider) SetServiceCached(serviceID string, cached bool) error {
	services, ok := storage.services[serviceID]
	if !ok {
		return aoserrors.New("service not found")
	}

	for i := 0; i < len(services); i++ {
		services[i].Cached = cached
	}

	return nil
}

func (storage *testStorageProvider) RemoveService(serviceID string, version string) error {
	services, ok := storage.services[serviceID]
	if !ok {
		return nil
	}

	if len(services) == 1 {
		delete(storage.services, serviceID)

		return nil
	}

	for i := 0; i < len(services); i++ {
		if services[i].Version == version {
			services = append(services[:i], services[i+1:]...)
		}
	}

	storage.services[serviceID] = services

	return nil
}

func (storage *testStorageProvider) RemoveLayer(digest string) error {
	if _, ok := storage.layers[digest]; !ok {
		return nil
	}

	delete(storage.layers, digest)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func prepareLayerInfo(filePath, digest string, index int) (layerInfo cloudprotocol.LayerInfo, err error) {
	imageFileInfo, err := image.CreateFileInfo(context.Background(), filePath)
	if err != nil {
		return cloudprotocol.LayerInfo{}, nil
	}

	url := url.URL{
		Scheme: "file",
		Path:   filePath,
	}

	recInfo := struct {
		Serial string `json:"serial"`
		Issuer []byte `json:"issuer"`
	}{
		Serial: "string",
		Issuer: []byte("issuer"),
	}

	installRequest := cloudprotocol.LayerInfo{
		Version: "v" + strconv.Itoa(index),
		LayerID: "testLayer" + strconv.Itoa(index),
		Digest:  digest,
		DownloadInfo: cloudprotocol.DownloadInfo{
			URLs:   []string{url.String()},
			Sha256: imageFileInfo.Sha256,
			Size:   imageFileInfo.Size,
		},
		DecryptionInfo: cloudprotocol.DecryptionInfo{
			BlockAlg:     "AES256/CBC/pkcs7",
			BlockIv:      []byte{},
			BlockKey:     []byte{},
			AsymAlg:      "RSA/PKCS1v1_5",
			ReceiverInfo: &recInfo,
		},
	}

	return installRequest, nil
}

func prepareServiceInfo(
	filePath, serviceID string, version string,
) (layerInfo cloudprotocol.ServiceInfo, err error) {
	imageFileInfo, err := image.CreateFileInfo(context.Background(), filePath)
	if err != nil {
		return cloudprotocol.ServiceInfo{}, nil
	}

	url := url.URL{
		Scheme: "file",
		Path:   filePath,
	}

	recInfo := struct {
		Serial string `json:"serial"`
		Issuer []byte `json:"issuer"`
	}{
		Serial: "string",
		Issuer: []byte("issuer"),
	}

	installRequest := cloudprotocol.ServiceInfo{
		Version:    version,
		ServiceID:  serviceID,
		ProviderID: serviceID,
		DownloadInfo: cloudprotocol.DownloadInfo{
			URLs:   []string{url.String()},
			Sha256: imageFileInfo.Sha256,
			Size:   imageFileInfo.Size,
		},
		DecryptionInfo: cloudprotocol.DecryptionInfo{
			BlockAlg:     "AES256/CBC/pkcs7",
			BlockIv:      []byte{},
			BlockKey:     []byte{},
			AsymAlg:      "RSA/PKCS1v1_5",
			ReceiverInfo: &recInfo,
		},
	}

	return installRequest, nil
}

func prepareService(servicelayerSize uint64, srvConfig []byte,
) (outputURL string, layersDigest []string, err error) {
	imageDir, err := os.MkdirTemp("", "aos_")
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	defer os.RemoveAll(imageDir)

	if err := os.MkdirAll(filepath.Join(imageDir, "rootfs", "home"), 0o755); err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	if err := generateFile(filepath.Join(imageDir, "rootfs", "home", "service.py"), servicelayerSize); err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	rootFsPath := filepath.Join(imageDir, "rootfs")

	serviceSize, err := fs.GetDirSize(rootFsPath)
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	fsDigest, err := generateFsLayer(imageDir, rootFsPath)
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	aosSrvConfigDigest, err := generateAndSaveDigest(filepath.Join(imageDir, blobsFolder), srvConfig)
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	imgSpecDigestDigest, err := generateAndSaveDigest(filepath.Join(imageDir, blobsFolder), []byte("{}"))
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	imgAosLayerDigest, err := generateAndSaveDigest(imageDir, []byte("{}"))
	if err != nil {
		return "", nil, aoserrors.Wrap(err)
	}

	layersDigest = append(layersDigest, string(imgAosLayerDigest))

	if err := genarateImageManfest(
		imageDir, &imgSpecDigestDigest, &aosSrvConfigDigest, &fsDigest,
		serviceSize, []digest.Digest{imgAosLayerDigest}); err != nil {
		return "", layersDigest, aoserrors.Wrap(err)
	}

	imageFile, err := os.CreateTemp("", "aos_")
	if err != nil {
		return "", layersDigest, aoserrors.Wrap(err)
	}

	outputURL = imageFile.Name()
	imageFile.Close()

	if err = packImage(imageDir, outputURL); err != nil {
		return "", layersDigest, aoserrors.Wrap(err)
	}

	return outputURL, layersDigest, nil
}

func packImage(source, name string) (err error) {
	log.WithFields(log.Fields{"source": source, "name": name}).Debug("Pack image")

	if output, err := exec.Command("tar", "-C", source, "-cf", name, "./").CombinedOutput(); err != nil {
		return aoserrors.Errorf("tar error: %s, code: %s", string(output), err)
	}

	return nil
}

func genarateImageManfest(folderPath string, imgConfig, aosSrvConfig, rootfsLayer *digest.Digest,
	rootfsLayerSize int64, srvLayers []digest.Digest,
) (err error) {
	var manifest aostypes.ServiceManifest

	manifest.SchemaVersion = 2

	manifest.Config = imagespec.Descriptor{
		MediaType: "application/vnd.oci.image.config.v1+json",
		Digest:    *imgConfig,
	}

	if aosSrvConfig != nil {
		manifest.AosService = &imagespec.Descriptor{
			MediaType: "application/vnd.aos.service.config.v1+json",
			Digest:    *aosSrvConfig,
		}
	}

	layerDescriptor := imagespec.Descriptor{
		MediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
		Digest:    *rootfsLayer,
		Size:      rootfsLayerSize,
	}

	manifest.Layers = append(manifest.Layers, layerDescriptor)

	for _, layerDigest := range srvLayers {
		layerDescriptor := imagespec.Descriptor{
			MediaType: "application/vnd.aos.image.layer.v1.tar",
			Digest:    layerDigest,
		}

		manifest.Layers = append(manifest.Layers, layerDescriptor)
	}

	data, err := json.Marshal(manifest)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	jsonFile, err := os.Create(filepath.Join(folderPath, "manifest.json"))
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err := jsonFile.Write(data); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func generateFsLayer(imgFolder, rootfs string) (digest digest.Digest, err error) {
	blobsDir := filepath.Join(imgFolder, blobsFolder)
	if err := os.MkdirAll(blobsDir, 0o755); err != nil {
		return digest, aoserrors.Wrap(err)
	}

	tarFile := filepath.Join(blobsDir, "_temp.tar.gz")

	if output, err := exec.Command("tar", "-C", rootfs, "-czf", tarFile, "./").CombinedOutput(); err != nil {
		return digest, aoserrors.Errorf("error: %s, code: %s", string(output), err)
	}
	defer os.Remove(tarFile)

	file, err := os.Open(tarFile)
	if err != nil {
		return digest, aoserrors.Wrap(err)
	}
	defer file.Close()

	byteValue, err := io.ReadAll(file)
	if err != nil {
		return digest, aoserrors.Wrap(err)
	}

	digest, err = generateAndSaveDigest(blobsDir, byteValue)
	if err != nil {
		return digest, aoserrors.Wrap(err)
	}

	os.RemoveAll(rootfs)

	return digest, nil
}

func generateAndSaveDigest(folder string, data []byte) (retDigest digest.Digest, err error) {
	fullPath := filepath.Join(folder, "sha256")
	if err := os.MkdirAll(fullPath, 0o755); err != nil {
		return retDigest, aoserrors.Wrap(err)
	}

	h := sha256.New()
	h.Write(data)
	retDigest = digest.NewDigest("sha256", h)

	file, err := os.Create(filepath.Join(fullPath, retDigest.Hex()))
	if err != nil {
		return retDigest, aoserrors.Wrap(err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return retDigest, aoserrors.Wrap(err)
	}

	return retDigest, nil
}

func setup() (err error) {
	tmpDir, err = os.MkdirTemp("", "cm_")
	if err != nil {
		return aoserrors.Wrap(err)
	}

	layersDir = path.Join(tmpDir, "layers")
	servicesDir = path.Join(tmpDir, "services")

	imagemanager.NewSpaceAllocator = newSpaceAllocator

	return nil
}

func cleanup() (err error) {
	if err = os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func generateFile(fileName string, size uint64) (err error) {
	if output, err := exec.Command("dd", "if=/dev/urandom", "of="+fileName, "bs=1",
		"count="+strconv.FormatUint(size, 10)).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

func clearLayersDir() error {
	if err := os.RemoveAll(layersDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func clearServicesDir() error {
	if err := os.RemoveAll(servicesDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func allocateString(value string) *string {
	return &value
}

func allocateUint64(value uint64) *uint64 {
	return &value
}
