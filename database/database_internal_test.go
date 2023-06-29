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
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/networkmanager"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Variables
 **********************************************************************************************************************/

var (
	tmpDir string
	db     *Database
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

	tmpDir, err = ioutil.TempDir("", "sm_")
	if err != nil {
		log.Fatalf("Error create temporary dir: %s", err)
	}

	db, err = New(&config.Config{
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

	db.Close()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestCursor(t *testing.T) {
	setCursor := "cursor123"

	if err := db.SetJournalCursor(setCursor); err != nil {
		t.Fatalf("Can't set logging cursor: %s", err)
	}

	getCursor, err := db.GetJournalCursor()
	if err != nil {
		t.Fatalf("Can't get logger cursor: %s", err)
	}

	if getCursor != setCursor {
		t.Fatalf("Wrong cursor value: %s", getCursor)
	}
}

func TestComponentsUpdateInfo(t *testing.T) {
	testData := []umcontroller.SystemComponent{
		{
			ID: "component1", VendorVersion: "v1", AosVersion: 1,
			Annotations: "Some annotation", URL: "url12", Sha512: []byte{1, 3, 90, 42},
		},
		{ID: "component2", VendorVersion: "v1", AosVersion: 1, URL: "url12", Sha512: []byte{1, 3, 90, 42}},
	}

	if err := db.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err := db.GetComponentsUpdateInfo()
	if err != nil {
		t.Fatal("Can't get update manager's update info ", err)
	}

	if !reflect.DeepEqual(testData, getUpdateInfo) {
		t.Fatalf("Wrong update info value: %v", getUpdateInfo)
	}

	testData = []umcontroller.SystemComponent{}

	if err := db.SetComponentsUpdateInfo(testData); err != nil {
		t.Fatal("Can't set update manager's update info ", err)
	}

	getUpdateInfo, err = db.GetComponentsUpdateInfo()
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

	if err := db.SetFirmwareUpdateState(fotaState); err != nil {
		t.Fatalf("Can't set FOTA state: %v", err)
	}

	if err := db.SetSoftwareUpdateState(sotaState); err != nil {
		t.Fatalf("Can't set SOTA state: %v", err)
	}

	if err := db.SetDesiredInstances(desiredInstances); err != nil {
		t.Fatalf("Can't set desired instances: %v", err)
	}

	retFota, err := db.GetFirmwareUpdateState()
	if err != nil {
		t.Fatalf("Can't get FOTA state: %v", err)
	}

	if string(retFota) != string(fotaState) {
		t.Errorf("Incorrect FOTA state: %s", string(retFota))
	}

	retSota, err := db.GetSoftwareUpdateState()
	if err != nil {
		t.Fatalf("Can't get SOTA state: %v", err)
	}

	if string(retSota) != string(sotaState) {
		t.Errorf("Incorrect SOTA state: %s", string(retSota))
	}

	retInstances, err := db.GetDesiredInstances()
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
			if err := db.SetJournalCursor(strconv.Itoa(i)); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := db.GetJournalCursor(); err != nil {
				t.Errorf("Can't get journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if err := db.SetComponentsUpdateInfo([]umcontroller.SystemComponent{{AosVersion: uint64(i)}}); err != nil {
				t.Errorf("Can't set journal cursor: %s", err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		for i := 0; i < numIterations; i++ {
			if _, err := db.GetComponentsUpdateInfo(); err != nil {
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
		if err := db.AddService(tCase.service); err != nil {
			t.Errorf("Can't add service: %v", err)
		}

		service, err := db.GetServiceInfo(tCase.service.ID)
		if err != nil {
			t.Errorf("Can't get service: %v", err)
		}

		if !reflect.DeepEqual(service, tCase.service) {
			t.Errorf("service %s doesn't match stored one", tCase.service.ID)
		}

		serviceVersions, err := db.GetServiceVersions(tCase.service.ID)
		if err != nil {
			t.Errorf("Can't get service versions: %v", err)
		}

		if len(serviceVersions) != tCase.expectedServiceVersionsCount {
			t.Errorf("Incorrect count of service versions: %v", len(serviceVersions))
		}

		services, err := db.GetServicesInfo()
		if err != nil {
			t.Errorf("Can't get services: %v", err)
		}

		if len(services) != tCase.expectedServiceCount {
			t.Errorf("Incorrect count of services: %v", len(services))
		}

		if err := db.SetServiceCached(tCase.service.ID, !tCase.service.Cached); err != nil {
			t.Errorf("Can't set service cached: %v", err)
		}

		if service, err = db.GetServiceInfo(tCase.service.ID); err != nil {
			t.Errorf("Can't get service: %v", err)
		}

		if service.Cached != !tCase.service.Cached {
			t.Error("Unexpected service cached status")
		}
	}

	for _, tCase := range cases {
		if err := db.RemoveService(tCase.service.ID, tCase.service.AosVersion); err != nil {
			t.Errorf("Can't remove service: %v", err)
		}

		if _, err := db.GetServiceInfo(tCase.service.ID); !errors.Is(err, tCase.serviceErrorAfterRemove) {
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
					VersionInfo: aostypes.VersionInfo{
						AosVersion: 1,
					},
					ID:     "layer1",
					Digest: "digest1",
					URL:    "file:///path/layer1",
					Size:   30,
				},
				RemoteURL: "http://path/layer1", Path: "/path/layer1",
				Timestamp: time.Now().UTC(), Cached: false,
			},
			expectedLayerCount: 1,
		},
		{
			layer: imagemanager.LayerInfo{
				LayerInfo: aostypes.LayerInfo{
					VersionInfo: aostypes.VersionInfo{
						AosVersion: 1,
					},
					ID:     "layer2",
					Digest: "digest2",
					URL:    "file:///path/layer2",
					Size:   60,
				},
				RemoteURL: "http://path/layer2",
				Path:      "/path/layer2", Timestamp: time.Now().UTC(), Cached: true,
			},
			expectedLayerCount: 2,
		},
	}

	for _, tCase := range cases {
		if err := db.AddLayer(tCase.layer); err != nil {
			t.Errorf("Can't add layer: %v", err)
		}

		layer, err := db.GetLayerInfo(tCase.layer.Digest)
		if err != nil {
			t.Errorf("Can't get layer: %v", err)
		}

		if !reflect.DeepEqual(layer, tCase.layer) {
			t.Errorf("layer %s doesn't match stored one", tCase.layer.ID)
		}

		layers, err := db.GetLayersInfo()
		if err != nil {
			t.Errorf("Can't get layers: %v", err)
		}

		if len(layers) != tCase.expectedLayerCount {
			t.Errorf("Incorrect count of layers: %v", len(layers))
		}

		if err := db.SetLayerCached(tCase.layer.Digest, !tCase.layer.Cached); err != nil {
			t.Errorf("Can't set layer cached: %v", err)
		}

		if layer, err = db.GetLayerInfo(tCase.layer.Digest); err != nil {
			t.Errorf("Can't get layer: %v", err)
		}

		if layer.Cached != !tCase.layer.Cached {
			t.Error("Unexpected layer cached status")
		}
	}

	for _, tCase := range cases {
		if err := db.RemoveLayer(tCase.layer.Digest); err != nil {
			t.Errorf("Can't remove service: %v", err)
		}

		if _, err := db.GetServiceInfo(tCase.layer.Digest); !errors.Is(err, imagemanager.ErrNotExist) {
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
		if err := db.AddNetworkInstanceInfo(tCase.networkInfo); err != nil {
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
		if err := db.RemoveNetworkInstanceInfo(tCase.removeInstance); err != nil {
			t.Errorf("Can't remove network info: %v", err)
		}

		networkInfo, err := db.GetNetworkInstancesInfo()
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
		if err := db.AddNetworkInfo(tCase.networkInfo); err != nil {
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
		if err := db.RemoveNetworkInfo(tCase.removeNetwork); err != nil {
			t.Errorf("Can't remove network info: %v", err)
		}

		networkInfo, err := db.GetNetworksInfo()
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
		if err := db.SetDownloadInfo(tCase.downloadInfo); err != nil {
			t.Errorf("Can't set download info: %v", err)
		}

		downloadInfo, err := db.GetDownloadInfo(tCase.downloadInfo.Path)
		if err != nil {
			t.Errorf("Can't get download info: %v", err)
		}

		if !reflect.DeepEqual(downloadInfo, tCase.downloadInfo) {
			t.Error("Unexpected download info")
		}

		downloadInfos, err := db.GetDownloadInfos()
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
		if err := db.RemoveDownloadInfo(tCase.path); err != nil {
			t.Errorf("Can't remove download info: %v", err)
		}

		if _, err := db.GetDownloadInfo(tCase.path); err == nil {
			t.Error("Download info should be removed")
		}
	}
}

func TestInstance(t *testing.T) {
	var expectedInstances []launcher.InstanceInfo

	instances, err := db.GetInstances()
	if err != nil {
		t.Errorf("Can't get all instances: %v", err)
	}

	if len(instances) != 0 {
		t.Error("Incorrect empty instances")
	}

	if _, err := db.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: "notexist", SubjectID: "notexist", Instance: 0,
	}); !errors.Is(err, launcher.ErrNotExist) {
		t.Errorf("Incorrect error: %v, should be %v", err, launcher.ErrNotExist)
	}

	for i := 100; i < 105; i++ {
		instanceInfo := launcher.InstanceInfo{
			InstanceIdent: aostypes.InstanceIdent{
				ServiceID: "serv" + strconv.Itoa(i), SubjectID: "subj" + strconv.Itoa(i), Instance: 0,
			},
			UID: i,
		}

		if err := db.AddInstance(instanceInfo); err != nil {
			t.Errorf("Can't add instance: %v", err)
		}

		expectedInstances = append(expectedInstances, instanceInfo)
	}

	expectedUID := 103

	uid, err := db.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: "serv" + strconv.Itoa(expectedUID), SubjectID: "subj" + strconv.Itoa(expectedUID), Instance: 0,
	})
	if err != nil {
		t.Errorf("Can't get instance uid: %v", err)
	}

	if uid != expectedUID {
		t.Error("Incorrect uid for instance")
	}

	instances, err = db.GetInstances()
	if err != nil {
		t.Errorf("Can't get all instances: %v", err)
	}

	if !reflect.DeepEqual(instances, expectedInstances) {
		t.Errorf("Incorrect result for get instances: %v, expected: %v", instances, expectedInstances)
	}

	if err := db.RemoveInstance(aostypes.InstanceIdent{
		ServiceID: "serv" + strconv.Itoa(expectedUID), SubjectID: "subj" + strconv.Itoa(expectedUID), Instance: 0,
	}); err != nil {
		t.Errorf("Can't remove instance: %v", err)
	}

	if _, err := db.GetInstanceUID(aostypes.InstanceIdent{
		ServiceID: "serv" + strconv.Itoa(expectedUID), SubjectID: "subj" + strconv.Itoa(expectedUID), Instance: 0,
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

	if _, err := db.GetStorageStateInfo(instanceIdent); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be entry does not exist")
	}

	testStateStorageInfo := storagestate.StorageStateInstanceInfo{
		InstanceID:   testInstanceID,
		StorageQuota: 12345, StateQuota: 54321,
		StateChecksum: []byte("checksum1"),
		InstanceIdent: instanceIdent,
	}

	if err := db.AddStorageStateInfo(testStateStorageInfo); err != nil {
		t.Fatalf("Can't add state storage info: %v", err)
	}

	stateStorageInfos, err := db.GetAllStorageStateInfo()
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

	info, err := db.GetStorageStateInfo(instanceIdent)
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

	if err := db.SetStateChecksum(notExistInstanceIdent, newCheckSum); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be entry does not exist")
	}

	if err := db.SetStateChecksum(instanceIdent, newCheckSum); err != nil {
		t.Fatalf("Can't update checksum: %v", err)
	}

	testStateStorageInfo.StateChecksum = newCheckSum

	info, err = db.GetStorageStateInfo(instanceIdent)
	if err != nil {
		t.Fatalf("Can't get state storage info: %v", err)
	}

	if !reflect.DeepEqual(info, testStateStorageInfo) {
		t.Error("Update state storage info from database doesn't match expected one")
	}

	if err := db.SetStorageStateQuotas(
		notExistInstanceIdent, newStorageQuota, newStateQuota); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be: entry does not exist")
	}

	if err := db.SetStorageStateQuotas(instanceIdent, newStorageQuota, newStateQuota); err != nil {
		t.Fatalf("Can't update state and storage quotas: %v", err)
	}

	testStateStorageInfo.StateQuota = newStateQuota
	testStateStorageInfo.StorageQuota = newStorageQuota

	info, err = db.GetStorageStateInfo(instanceIdent)
	if err != nil {
		t.Fatalf("Can't get state storage info: %v", err)
	}

	if !reflect.DeepEqual(info, testStateStorageInfo) {
		t.Error("Update state storage info from database doesn't match expected one")
	}

	if err := db.RemoveStorageStateInfo(instanceIdent); err != nil {
		t.Fatalf("Can't remove state storage info: %v", err)
	}

	if _, err := db.GetStorageStateInfo(instanceIdent); !errors.Is(err, storagestate.ErrNotExist) {
		t.Errorf("Should be: entry does not exist")
	}
}
