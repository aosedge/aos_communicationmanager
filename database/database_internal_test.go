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

package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/migration"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/downloader"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
	"github.com/aosedge/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Variables
 **********************************************************************************************************************/

var (
	tmpDir string
	testDB *Database
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	servicePrefix = "serv"
	subjectPrefix = "subj"
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

	tmpDir, err = os.MkdirTemp("", "cm_")
	if err != nil {
		log.Fatalf("Error create temporary dir: %s", err)
	}

	testDB, err = New(&config.Config{
		WorkingDir: tmpDir,
		Migration: config.Migration{
			MigrationPath:       tmpDir,
			MergedMigrationPath: tmpDir,
		},
	})
	if err != nil {
		log.Fatalf("Can't create database: %s", err)
	}

	ret := m.Run()

	if err = os.RemoveAll(tmpDir); err != nil {
		log.Fatalf("Error cleaning up: %s", err)
	}

	testDB.Close()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestCursor(t *testing.T) {
	setCursor := "cursor123"

	if err := testDB.SetJournalCursor(setCursor); err != nil {
		t.Fatalf("Can't set logging cursor: %s", err)
	}

	getCursor, err := testDB.GetJournalCursor()
	if err != nil {
		t.Fatalf("Can't get logger cursor: %s", err)
	}

	if getCursor != setCursor {
		t.Fatalf("Wrong cursor value: %s", getCursor)
	}
}

func TestComponentsUpdateInfo(t *testing.T) {
	testData := []umcontroller.ComponentStatus{
		{
			ID: "component1", Version: "v1",
			Annotations: "Some annotation", URL: "url12", Sha256: []byte{1, 3, 90, 42},
		},
		{ID: "component2", Version: "v1", URL: "url12", Sha256: []byte{1, 3, 90, 42}},
	}

	if err := testDB.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err := testDB.GetComponentsUpdateInfo()
	if err != nil {
		t.Fatal("Can't get update manager's update info ", err)
	}

	if !reflect.DeepEqual(testData, getUpdateInfo) {
		t.Fatalf("Wrong update info value: %v", getUpdateInfo)
	}

	testData = []umcontroller.ComponentStatus{}

	if err := testDB.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err = testDB.GetComponentsUpdateInfo()
	if err != nil {
		t.Fatal("Can't get update manager's update info ", err)
	}

	if len(getUpdateInfo) != 0 {
		t.Fatalf("Wrong count of update elements 0 != %d", len(getUpdateInfo))
	}
}

func TestSotaFotaInstancesFields(t *testing.T) {
	fotaState := json.RawMessage("fotaState")
	sotaState := json.RawMessage("sotaState")
	desiredInstances := json.RawMessage("desiredInstances")

	if err := testDB.SetFirmwareUpdateState(fotaState); err != nil {
		t.Fatalf("Can't set FOTA state: %v", err)
	}

	if err := testDB.SetSoftwareUpdateState(sotaState); err != nil {
		t.Fatalf("Can't set SOTA state: %v", err)
	}

	if err := testDB.SetDesiredInstances(desiredInstances); err != nil {
		t.Fatalf("Can't set desired instances: %v", err)
	}

	retFota, err := testDB.GetFirmwareUpdateState()
	if err != nil {
		t.Fatalf("Can't get FOTA state: %v", err)
	}

	if string(retFota) != string(fotaState) {
		t.Errorf("Incorrect FOTA state: %s", string(retFota))
	}

	retSota, err := testDB.GetSoftwareUpdateState()
	if err != nil {
		t.Fatalf("Can't get SOTA state: %v", err)
	}

	if string(retSota) != string(sotaState) {
		t.Errorf("Incorrect SOTA state: %s", string(retSota))
	}

	retInstances, err := testDB.GetDesiredInstances()
	if err != nil {
		t.Fatalf("Can't get desired instances state %v", err)
	}

	if string(retInstances) != string(desiredInstances) {
		t.Errorf("Incorrect desired instances: %s", string(retInstances))
	}
}

func TestMultiThread(t *testing.T) {
	const numIterations = 1000

	var wg sync.WaitGroup

	wg.Add(4)

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if err := testDB.SetJournalCursor(strconv.Itoa(i)); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := testDB.GetJournalCursor(); err != nil {
				t.Errorf("Can't get journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if err := testDB.SetComponentsUpdateInfo([]umcontroller.ComponentStatus{{Version: strconv.Itoa(i)}}); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := testDB.GetComponentsUpdateInfo(); err != nil {
				t.Errorf("Can't get journal cursor: %s", err)
			}
		}
	}()

	wg.Wait()
}

func TestServiceStore(t *testing.T) {
	cases := []struct {
		service                      imagemanager.ServiceInfo
		expectedServiceVersionsCount int
		expectedServiceCount         int
		serviceErrorAfterRemove      error
	}{
		{
			service: imagemanager.ServiceInfo{
				ServiceInfo: aostypes.ServiceInfo{
					ID: "service1",
					VersionInfo: aostypes.VersionInfo{
						AosVersion:    1,
						VendorVersion: "1",
					},
					URL:  "file:///path/service1",
					Size: 30,
					GID:  1000,
				},
				RemoteURL: "http://path/service1",
				Path:      "/path/service1", Timestamp: time.Now().UTC(), Cached: false, Config: aostypes.ServiceConfig{
					Hostname: allocateString("service1"),
					Author:   "test",
					Quotas: aostypes.ServiceQuotas{
						UploadSpeed:   allocateUint64(1000),
						DownloadSpeed: allocateUint64(1000),
					},
				},
			},
			expectedServiceVersionsCount: 1,
			expectedServiceCount:         1,
			serviceErrorAfterRemove:      imagemanager.ErrNotExist,
		},
		{
			service: imagemanager.ServiceInfo{
				ServiceInfo: aostypes.ServiceInfo{
					ID: "service2",
					VersionInfo: aostypes.VersionInfo{
						AosVersion:    1,
						VendorVersion: "1",
					},
					URL:  "file:///path/service2",
					Size: 60,
					GID:  2000,
				},
				RemoteURL: "http://path/service2",
				Path:      "/path/service2", Timestamp: time.Now().UTC(), Cached: true, Config: aostypes.ServiceConfig{
					Hostname: allocateString("service2"),
					Author:   "test1",
					Quotas: aostypes.ServiceQuotas{
						UploadSpeed:   allocateUint64(500),
						DownloadSpeed: allocateUint64(500),
					},
					Resources: []string{"resource1", "resource2"},
				},
			},
			expectedServiceVersionsCount: 1,
			expectedServiceCount:         2,
			serviceErrorAfterRemove:      nil,
		},
		{
			service: imagemanager.ServiceInfo{
				ServiceInfo: aostypes.ServiceInfo{
					ID: "service2",
					VersionInfo: aostypes.VersionInfo{
						AosVersion:    2,
						VendorVersion: "1",
					},
					URL:  "file:///path/service2/new",
					Size: 20,
					GID:  1000,
				},
				RemoteURL: "http://path/service2/new",
				Path:      "/path/service2/new", Timestamp: time.Now().UTC(),
			},
			expectedServiceVersionsCount: 2,
			expectedServiceCount:         2,
			serviceErrorAfterRemove:      imagemanager.ErrNotExist,
		},
	}

	for _, tCase := range cases {
		if err := testDB.AddService(tCase.service); err != nil {
			t.Errorf("Can't add service: %v", err)
		}

		service, err := testDB.GetServiceInfo(tCase.service.ID)
		if err != nil {
			t.Errorf("Can't get service: %v", err)
		}

		if !reflect.DeepEqual(service, tCase.service) {
			t.Errorf("service %s doesn't match stored one", tCase.service.ID)
		}

		serviceVersions, err := testDB.GetServiceVersions(tCase.service.ID)
		if err != nil {
			t.Errorf("Can't get service versions: %v", err)
		}

		if len(serviceVersions) != tCase.expectedServiceVersionsCount {
			t.Errorf("Incorrect count of service versions: %v", len(serviceVersions))
		}

		services, err := testDB.GetServicesInfo()
		if err != nil {
			t.Errorf("Can't get services: %v", err)
		}

		if len(services) != tCase.expectedServiceCount {
			t.Errorf("Incorrect count of services: %v", len(services))
		}

		if err := testDB.SetServiceCached(tCase.service.ID, !tCase.service.Cached); err != nil {
			t.Errorf("Can't set service cached: %v", err)
		}

		if service, err = testDB.GetServiceInfo(tCase.service.ID); err != nil {
			t.Errorf("Can't get service: %v", err)
		}

		if service.Cached != !tCase.service.Cached {
			t.Error("Unexpected service cached status")
		}
	}

	for _, tCase := range cases {
		if err := testDB.RemoveService(tCase.service.ID, tCase.service.AosVersion); err != nil {
			t.Errorf("Can't remove service: %v", err)
		}

		if _, err := testDB.GetServiceInfo(tCase.service.ID); !errors.Is(err, tCase.serviceErrorAfterRemove) {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestLayerStore(t *testing.T) {
	cases := []struct {
		layer              imagemanager.LayerInfo
		expectedLayerCount int
	}{
		{
			layer: imagemanager.LayerInfo{
				LayerInfo: aostypes.LayerInfo{
					Version: "1.0",
					LayerID: "layer1",
					Digest:  "digest1",
					URL:     "file:///path/layer1",
					Size:    30,
				},
				RemoteURL: "http://path/layer1", Path: "/path/layer1",
				Timestamp: time.Now().UTC(), Cached: false,
			},
			expectedLayerCount: 1,
		},
		{
			layer: imagemanager.LayerInfo{
				LayerInfo: aostypes.LayerInfo{
					Version: "1.0",
					LayerID: "layer2",
					Digest:  "digest2",
					URL:     "file:///path/layer2",
					Size:    60,
				},
				RemoteURL: "http://path/layer2",
				Path:      "/path/layer2", Timestamp: time.Now().UTC(), Cached: true,
			},
			expectedLayerCount: 2,
		},
	}

	for _, tCase := range cases {
		if err := testDB.AddLayer(tCase.layer); err != nil {
			t.Errorf("Can't add layer: %v", err)
		}

		layer, err := testDB.GetLayerInfo(tCase.layer.Digest)
		if err != nil {
			t.Errorf("Can't get layer: %v", err)
		}

		if !reflect.DeepEqual(layer, tCase.layer) {
			t.Errorf("layer %s doesn't match stored one", tCase.layer.ID)
		}

		layers, err := testDB.GetLayersInfo()
		if err != nil {
			t.Errorf("Can't get layers: %v", err)
		}

		if len(layers) != tCase.expectedLayerCount {
			t.Errorf("Incorrect count of layers: %v", len(layers))
		}

		if err := testDB.SetLayerCached(tCase.layer.Digest, !tCase.layer.Cached); err != nil {
			t.Errorf("Can't set layer cached: %v", err)
		}

		if layer, err = testDB.GetLayerInfo(tCase.layer.Digest); err != nil {
			t.Errorf("Can't get layer: %v", err)
		}

		if layer.Cached != !tCase.layer.Cached {
			t.Error("Unexpected layer cached status")
		}
	}

	for _, tCase := range cases {
		if err := testDB.RemoveLayer(tCase.layer.Digest); err != nil {
			t.Errorf("Can't remove service: %v", err)
		}

		if _, err := testDB.GetServiceInfo(tCase.layer.Digest); !errors.Is(err, imagemanager.ErrNotExist) {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestNetworkInstanceConfiguration(t *testing.T) {
	casesAdd := []struct {
		networkInfo networkmanager.InstanceNetworkInfo
	}{
		{
			networkInfo: networkmanager.InstanceNetworkInfo{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject1",
					Instance:  1,
				},
				NetworkInfo: networkmanager.NetworkInfo{
					NetworkID: "network1",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.17.0.0/16",
						IP:     "172.17.0.1",
						VlanID: 1,
						FirewallRules: []aostypes.FirewallRule{
							{
								Proto:   "tcp",
								DstIP:   "172.18.0.1",
								SrcIP:   "172.17.0.1",
								DstPort: "80",
							},
						},
					},
				},
			},
		},
		{
			networkInfo: networkmanager.InstanceNetworkInfo{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject2",
					Instance:  1,
				},
				NetworkInfo: networkmanager.NetworkInfo{
					NetworkID: "network2",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.18.0.0/16",
						IP:     "172.18.0.1",
						VlanID: 1,
					},
				},
			},
		},
		{
			networkInfo: networkmanager.InstanceNetworkInfo{
				InstanceIdent: aostypes.InstanceIdent{
					ServiceID: "service1",
					SubjectID: "subject2",
					Instance:  2,
				},
				NetworkInfo: networkmanager.NetworkInfo{
					NetworkID: "network2",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.18.0.0/16",
						IP:     "172.18.0.2",
						VlanID: 1,
					},
				},
			},
		},
	}

	for _, tCase := range casesAdd {
		if err := testDB.AddNetworkInstanceInfo(tCase.networkInfo); err != nil {
			t.Errorf("Can't add network info: %v", err)
		}
	}

	casesRemove := []struct {
		expectedNetworkInfo []networkmanager.InstanceNetworkInfo
		removeInstance      aostypes.InstanceIdent
	}{
		{
			removeInstance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject1",
				Instance:  1,
			},
			expectedNetworkInfo: []networkmanager.InstanceNetworkInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{
						ServiceID: "service1",
						SubjectID: "subject2",
						Instance:  1,
					},
					NetworkInfo: networkmanager.NetworkInfo{
						NetworkID: "network2",
						NetworkParameters: aostypes.NetworkParameters{
							Subnet: "172.18.0.0/16",
							IP:     "172.18.0.1",
							VlanID: 1,
						},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{
						ServiceID: "service1",
						SubjectID: "subject2",
						Instance:  2,
					},
					NetworkInfo: networkmanager.NetworkInfo{
						NetworkID: "network2",
						NetworkParameters: aostypes.NetworkParameters{
							Subnet: "172.18.0.0/16",
							IP:     "172.18.0.2",
							VlanID: 1,
						},
					},
				},
			},
		},
		{
			removeInstance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject2",
				Instance:  1,
			},
			expectedNetworkInfo: []networkmanager.InstanceNetworkInfo{
				{
					InstanceIdent: aostypes.InstanceIdent{
						ServiceID: "service1",
						SubjectID: "subject2",
						Instance:  2,
					},
					NetworkInfo: networkmanager.NetworkInfo{
						NetworkID: "network2",
						NetworkParameters: aostypes.NetworkParameters{
							Subnet: "172.18.0.0/16",
							IP:     "172.18.0.2",
							VlanID: 1,
						},
					},
				},
			},
		},
		{
			removeInstance: aostypes.InstanceIdent{
				ServiceID: "service1",
				SubjectID: "subject2",
				Instance:  2,
			},
			expectedNetworkInfo: nil,
		},
	}

	for _, tCase := range casesRemove {
		if err := testDB.RemoveNetworkInstanceInfo(tCase.removeInstance); err != nil {
			t.Errorf("Can't remove network info: %v", err)
		}

		networkInfo, err := testDB.GetNetworkInstancesInfo()
		if err != nil {
			t.Errorf("Can't get network info: %v", err)
		}

		if !reflect.DeepEqual(networkInfo, tCase.expectedNetworkInfo) {
			t.Error("Unexpected network info")
		}
	}
}

func TestNetworkConfiguration(t *testing.T) {
	casesAdd := []struct {
		networkInfo networkmanager.NetworkInfo
	}{
		{
			networkInfo: networkmanager.NetworkInfo{
				NetworkID: "network1",
				NetworkParameters: aostypes.NetworkParameters{
					Subnet: "172.17.0.0/16",
					IP:     "172.17.0.1",
					VlanID: 1,
				},
			},
		},
		{
			networkInfo: networkmanager.NetworkInfo{
				NetworkID: "network2",
				NetworkParameters: aostypes.NetworkParameters{
					Subnet: "172.18.0.0/16",
					IP:     "172.18.0.1",
					VlanID: 1,
				},
			},
		},
		{
			networkInfo: networkmanager.NetworkInfo{
				NetworkID: "network3",
				NetworkParameters: aostypes.NetworkParameters{
					Subnet: "172.19.0.0/16",
					IP:     "172.19.0.2",
					VlanID: 1,
				},
			},
		},
	}

	for _, tCase := range casesAdd {
		if err := testDB.AddNetworkInfo(tCase.networkInfo); err != nil {
			t.Errorf("Can't add network info: %v", err)
		}
	}

	casesRemove := []struct {
		expectedNetworkInfo []networkmanager.NetworkInfo
		removeNetwork       string
	}{
		{
			removeNetwork: "network1",
			expectedNetworkInfo: []networkmanager.NetworkInfo{
				{
					NetworkID: "network2",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.18.0.0/16",
						IP:     "172.18.0.1",
						VlanID: 1,
					},
				},
				{
					NetworkID: "network3",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.19.0.0/16",
						IP:     "172.19.0.2",
						VlanID: 1,
					},
				},
			},
		},
		{
			removeNetwork: "network2",
			expectedNetworkInfo: []networkmanager.NetworkInfo{
				{
					NetworkID: "network3",
					NetworkParameters: aostypes.NetworkParameters{
						Subnet: "172.19.0.0/16",
						IP:     "172.19.0.2",
						VlanID: 1,
					},
				},
			},
		},
		{
			removeNetwork:       "network3",
			expectedNetworkInfo: nil,
		},
	}

	for _, tCase := range casesRemove {
		if err := testDB.RemoveNetworkInfo(tCase.removeNetwork); err != nil {
			t.Errorf("Can't remove network info: %v", err)
		}

		networkInfo, err := testDB.GetNetworksInfo()
		if err != nil {
			t.Errorf("Can't get network info: %v", err)
		}

		if !reflect.DeepEqual(networkInfo, tCase.expectedNetworkInfo) {
			t.Error("Unexpected network info")
		}
	}
}

func allocateString(value string) *string {
	return &value
}

func allocateUint64(value uint64) *uint64 {
	return &value
}

func TestDownloadInfo(t *testing.T) {
	cases := []struct {
		downloadInfo      downloader.DownloadInfo
		downloadInfoCount int
	}{
		{
			downloadInfo: downloader.DownloadInfo{
				Path:       "home",
				TargetType: cloudprotocol.DownloadTargetComponent,
			},
			downloadInfoCount: 1,
		},
		{
			downloadInfo: downloader.DownloadInfo{
				Path:            "/path/file",
				TargetType:      cloudprotocol.DownloadTargetLayer,
				InterruptReason: "error",
				Downloaded:      true,
			},
			downloadInfoCount: 2,
		},
	}

	for _, tCase := range cases {
		if err := testDB.SetDownloadInfo(tCase.downloadInfo); err != nil {
			t.Errorf("Can't set download info: %v", err)
		}

		downloadInfo, err := testDB.GetDownloadInfo(tCase.downloadInfo.Path)
		if err != nil {
			t.Errorf("Can't get download info: %v", err)
		}

		if !reflect.DeepEqual(downloadInfo, tCase.downloadInfo) {
			t.Error("Unexpected download info")
		}

		downloadInfos, err := testDB.GetDownloadInfos()
		if err != nil {
			t.Errorf("Can't get download info list: %v", err)
		}

		if len(downloadInfos) != tCase.downloadInfoCount {
			t.Error("Unexpected download info count")
		}
	}

	casesRemove := []struct {
		path string
	}{
		{
			path: "home",
		},
		{
			path: "/path/file",
		},
	}

	for _, tCase := range casesRemove {
		if err := testDB.RemoveDownloadInfo(tCase.path); err != nil {
			t.Errorf("Can't remove download info: %v", err)
		}

		if _, err := testDB.GetDownloadInfo(tCase.path); err == nil {
			t.Error("Download info should be removed")
		}
	}
}

func TestInstance(t *testing.T) {
	var expectedInstances []launcher.InstanceInfo

	instances, err := testDB.GetInstances()
	if err != nil {
		t.Errorf("Can't get all instances: %v", err)
	}

	if len(instances) != 0 {
		t.Error("Incorrect empty instances")
	}

	if _, err := testDB.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: "notexist", SubjectID: "notexist", Instance: 0,
	}); !errors.Is(err, launcher.ErrNotExist) {
		t.Errorf("Incorrect error: %v, should be %v", err, launcher.ErrNotExist)
	}

	for i := 100; i < 105; i++ {
		instanceInfo := launcher.InstanceInfo{
			InstanceIdent: aostypes.InstanceIdent{
				ServiceID: servicePrefix + strconv.Itoa(i), SubjectID: subjectPrefix + strconv.Itoa(i), Instance: 0,
			},
			UID: i,
		}

		if err := testDB.AddInstance(instanceInfo); err != nil {
			t.Errorf("Can't add instance: %v", err)
		}

		expectedInstances = append(expectedInstances, instanceInfo)
	}

	expectedUID := 103

	uid, err := testDB.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: servicePrefix + strconv.Itoa(expectedUID),
		SubjectID: subjectPrefix + strconv.Itoa(expectedUID), Instance: 0,
	})
	if err != nil {
		t.Errorf("Can't get instance uid: %v", err)
	}

	if uid != expectedUID {
		t.Error("Incorrect uid for instance")
	}

	instances, err = testDB.GetInstances()
	if err != nil {
		t.Errorf("Can't get all instances: %v", err)
	}

	if !reflect.DeepEqual(instances, expectedInstances) {
		t.Errorf("Incorrect result for get instances: %v, expected: %v", instances, expectedInstances)
	}

	expectedCachedInstanceIdent := aostypes.InstanceIdent{
		ServiceID: servicePrefix + strconv.Itoa(expectedUID),
		SubjectID: subjectPrefix + strconv.Itoa(expectedUID), Instance: 0,
	}

	if err := testDB.SetInstanceCached(expectedCachedInstanceIdent, true); err != nil {
		t.Errorf("Can't set instance cached: %v", err)
	}

	instances, err = testDB.GetInstances()
	if err != nil {
		t.Errorf("Can't get all instances: %v", err)
	}

	for _, instance := range instances {
		cached := instance.Cached
		if instance.InstanceIdent == expectedCachedInstanceIdent {
			if !cached {
				t.Error("Instance expected to be cached")
			}

			break
		}
	}

	if err := testDB.RemoveInstance(aostypes.InstanceIdent{
		ServiceID: servicePrefix + strconv.Itoa(expectedUID),
		SubjectID: subjectPrefix + strconv.Itoa(expectedUID), Instance: 0,
	}); err != nil {
		t.Errorf("Can't remove instance: %v", err)
	}

	if _, err := testDB.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: servicePrefix + strconv.Itoa(expectedUID),
		SubjectID: subjectPrefix + strconv.Itoa(expectedUID), Instance: 0,
	}); !errors.Is(err, launcher.ErrNotExist) {
		t.Errorf("Incorrect error: %v, should be %v", err, launcher.ErrNotExist)
	}
}

func TestStorageState(t *testing.T) {
	var (
		testInstanceID  = "test_instance_subjectID_serviceID"
		newCheckSum     = []byte("newCheckSum")
		newStorageQuota = uint64(88888)
		newStateQuota   = uint64(99999)
	)

	instanceIdent := aostypes.InstanceIdent{
		Instance:  1,
		ServiceID: "service1",
		SubjectID: "subject1",
	}

	if _, err := testDB.GetStorageStateInfo(instanceIdent); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be entry does not exist")
	}

	testStateStorageInfo := storagestate.StorageStateInstanceInfo{
		InstanceID:   testInstanceID,
		StorageQuota: 12345, StateQuota: 54321,
		StateChecksum: []byte("checksum1"),
		InstanceIdent: instanceIdent,
	}

	if err := testDB.AddStorageStateInfo(testStateStorageInfo); err != nil {
		t.Fatalf("Can't add state storage info: %v", err)
	}

	stateStorageInfos, err := testDB.GetAllStorageStateInfo()
	if err != nil {
		t.Fatalf("Can't get all state storage infos: %v", err)
	}

	if len(stateStorageInfos) != 1 {
		t.Errorf("Unexpected state storage info size")
	}

	if stateStorageInfos[0].InstanceID != testInstanceID {
		t.Errorf("Unexpected instance path")
	}

	if stateStorageInfos[0].InstanceIdent != testStateStorageInfo.InstanceIdent {
		t.Errorf("Unexpected instance ident")
	}

	info, err := testDB.GetStorageStateInfo(instanceIdent)
	if err != nil {
		t.Fatalf("Can't get state storage info: %v", err)
	}

	if !reflect.DeepEqual(info, testStateStorageInfo) {
		t.Error("State storage info from database doesn't match expected one")
	}

	notExistInstanceIdent := aostypes.InstanceIdent{
		ServiceID: "serviceID",
		SubjectID: "subjectID",
		Instance:  20,
	}

	if err := testDB.SetStateChecksum(notExistInstanceIdent, newCheckSum); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be entry does not exist")
	}

	if err := testDB.SetStateChecksum(instanceIdent, newCheckSum); err != nil {
		t.Fatalf("Can't update checksum: %v", err)
	}

	testStateStorageInfo.StateChecksum = newCheckSum

	info, err = testDB.GetStorageStateInfo(instanceIdent)
	if err != nil {
		t.Fatalf("Can't get state storage info: %v", err)
	}

	if !reflect.DeepEqual(info, testStateStorageInfo) {
		t.Error("Update state storage info from database doesn't match expected one")
	}

	if err := testDB.SetStorageStateQuotas(
		notExistInstanceIdent, newStorageQuota, newStateQuota); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be: entry does not exist")
	}

	if err := testDB.SetStorageStateQuotas(instanceIdent, newStorageQuota, newStateQuota); err != nil {
		t.Fatalf("Can't update state and storage quotas: %v", err)
	}

	testStateStorageInfo.StateQuota = newStateQuota
	testStateStorageInfo.StorageQuota = newStorageQuota

	info, err = testDB.GetStorageStateInfo(instanceIdent)
	if err != nil {
		t.Fatalf("Can't get state storage info: %v", err)
	}

	if !reflect.DeepEqual(info, testStateStorageInfo) {
		t.Error("Update state storage info from database doesn't match expected one")
	}

	if err := testDB.RemoveStorageStateInfo(instanceIdent); err != nil {
		t.Fatalf("Can't remove state storage info: %v", err)
	}

	if _, err := testDB.GetStorageStateInfo(instanceIdent); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be: entry does not exist")
	}
}

func TestNodeState(t *testing.T) {
	setNodeState := json.RawMessage("node state 1")

	if err := testDB.SetNodeState("nodeID", setNodeState); err != nil {
		t.Fatalf("Can't set node state: %v", err)
	}

	getNodeState, err := testDB.GetNodeState("nodeID")
	if err != nil {
		t.Errorf("Can't get node state: %v", err)
	}

	if string(setNodeState) != string(getNodeState) {
		t.Errorf("Wrong get node state: %s", string(getNodeState))
	}

	setNodeState = json.RawMessage("node state 2")

	if err := testDB.SetNodeState("nodeID", setNodeState); err != nil {
		t.Fatalf("Can't set node state: %v", err)
	}

	getNodeState, err = testDB.GetNodeState("nodeID")
	if err != nil {
		t.Errorf("Can't get node state: %v", err)
	}

	if string(setNodeState) != string(getNodeState) {
		t.Errorf("Wrong get node state: %s", string(getNodeState))
	}
}

func TestMigration(t *testing.T) {
	migrationDBName := filepath.Join(tmpDir, "test_migration.db")
	mergedMigrationDir := filepath.Join(tmpDir, "mergedMigration")
	migrationDir := "migration"

	if err := os.MkdirAll(mergedMigrationDir, 0o755); err != nil {
		t.Fatalf("Error creating merged migration dir: %v", err)
	}

	defer func() {
		if err := os.RemoveAll(mergedMigrationDir); err != nil {
			t.Fatalf("Error removing merged migration dir: %v", err)
		}

		if err := os.RemoveAll(migrationDBName); err != nil {
			t.Fatalf("Error removing migration db: %v", err)
		}
	}()

	if err := migration.MergeMigrationFiles(migrationDir, mergedMigrationDir); err != nil {
		t.Fatalf("Can't merge migration files: %v", err)
	}

	if err := createDatabaseV0(migrationDBName); err != nil {
		t.Fatalf("Can't create initial db: %v", err)
	}

	migrationDB, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=%d&_journal_mode=%s&_sync=%s",
		migrationDBName, busyTimeout, journalMode, syncMode))
	if err != nil {
		t.Fatalf("Can't create initial db: %v", err)
	}
	defer migrationDB.Close()

	// Migration upward

	if err = migration.DoMigrate(migrationDB, mergedMigrationDir, 1); err != nil {
		t.Fatalf("Can't perform migration: %v", err)
	}

	if err = checkDatabaseVer1(migrationDB); err != nil {
		t.Fatalf("Error checking db version: %v", err)
	}

	// Migration downward

	if err = migration.DoMigrate(migrationDB, mergedMigrationDir, 0); err != nil {
		t.Fatalf("Can't perform migration: %v", err)
	}

	if err = checkDatabaseVer0(migrationDB); err != nil {
		t.Fatalf("Error checking db version: %v", err)
	}
}

func createDatabaseV0(name string) error {
	sqlite, err := sql.Open("sqlite3", fmt.Sprintf("%s?_busy_timeout=%d&_journal_mode=%s&_sync=%s",
		name, busyTimeout, journalMode, syncMode))
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer sqlite.Close()

	// Create config table

	if _, err = sqlite.Exec(
		`CREATE TABLE config (
			cursor TEXT,
			componentsUpdateInfo BLOB,
			fotaUpdateState BLOB,
			sotaUpdateState BLOB,
			desiredInstances BLOB)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(
		`INSERT INTO config (
			cursor,
			componentsUpdateInfo,
			fotaUpdateState,
			sotaUpdateState,
			desiredInstances) values(?, ?, ?, ?, ?)`,
		"", "", json.RawMessage{}, json.RawMessage{}, json.RawMessage("[]")); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS download (path TEXT NOT NULL PRIMARY KEY,
		targetType TEXT NOT NULL,
		interruptReason TEXT,
		downloaded INTEGER)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS services (id TEXT NOT NULL ,
		aosVersion INTEGER,
		providerId TEXT,
		vendorVersion TEXT,
		description TEXT,
		localURL   TEXT,
		remoteURL  TEXT,
		path TEXT,
		size INTEGER,
		timestamp TIMESTAMP,
		cached INTEGER,
		config BLOB,
		layers BLOB,
		sha256 BLOB,
		sha512 BLOB,
		exposedPorts BLOB,
		gid INTEGER,
		PRIMARY KEY(id, aosVersion))`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS layers (digest TEXT NOT NULL PRIMARY KEY,
		layerId TEXT,
		aosVersion INTEGER,
		vendorVersion TEXT,
		description TEXT,
		localURL   TEXT,
		remoteURL  TEXT,
		Path       TEXT,
		Size       INTEGER,
		Timestamp  TIMESTAMP,
		sha256 BLOB,
		sha512 BLOB,
		cached INTEGER)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS instances (serviceId TEXT,
		subjectId TEXT,
		instance INTEGER,
		uid integer,
		PRIMARY KEY(serviceId, subjectId, instance))`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS instance_network (serviceId TEXT,
		subjectId TEXT,
		instance INTEGER,
		networkID TEXT,
		ip TEXT,
		subnet TEXT,
		vlanID INTEGER,
		port BLOB,
		PRIMARY KEY(serviceId, subjectId, instance))`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS network (networkID TEXT NOT NULL PRIMARY KEY,
		ip TEXT,
		subnet TEXT,
		vlanID INTEGER)`); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = sqlite.Exec(`CREATE TABLE IF NOT EXISTS storagestate (instanceID TEXT,
		serviceID TEXT,
		subjectID TEXT,
		instance INTEGER,
		storageQuota INTEGER,
		stateQuota INTEGER,
		stateChecksum BLOB,
		PRIMARY KEY(instance, subjectID, serviceID))`); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func checkDatabaseVer0(sqlite *sql.DB) error {
	exist, err := isTableExist(sqlite, "nodes")
	if err != nil {
		return err
	}

	if exist {
		return aoserrors.New("table nodes should not exist")
	}

	return nil
}

func checkDatabaseVer1(sqlite *sql.DB) error {
	exist, err := isTableExist(sqlite, "nodes")
	if err != nil {
		return err
	}

	if !exist {
		return errNotExist
	}

	return nil
}

func isTableExist(sqlite *sql.DB, tableName string) (bool, error) {
	rows, err := sqlite.Query("SELECT * FROM sqlite_master WHERE name = ? and type='table'", tableName)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}

	return false, aoserrors.Wrap(rows.Err())
}
