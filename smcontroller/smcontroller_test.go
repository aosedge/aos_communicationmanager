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

package smcontroller_test

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	pb "gitpct.epam.com/epmd-aepr/aos_common/api/servicemanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"aos_communicationmanager/boardconfig"
	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
	"aos_communicationmanager/smcontroller"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const smURL = "localhost:8888"

const messageTimeout = 5 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testSM struct {
	sync.Mutex

	pb.UnimplementedSMServiceServer

	grpcServer    *grpc.Server
	users         []string
	usersServices []cloudprotocol.ServiceInfo
	usersLayers   []cloudprotocol.LayerInfo
	allServices   []cloudprotocol.ServiceInfo
	allLayers     []cloudprotocol.LayerInfo

	messageChannel chan interface{}

	boardConfigVersion string
	correlationId      string
	stateChecksum      string

	ctx            context.Context
	cancelFunction context.CancelFunc
}

type testURLTranslator struct {
}

type testMessageSender struct {
	messageChannel chan interface{}
}

type testAlertSender struct {
	messageChannel chan interface{}
}

type testMonitoringSender struct {
	messageChannel chan interface{}
}

type clientBoardConfig struct {
	FormatVersion uint64                       `json:"formatVersion"`
	VendorVersion string                       `json:"vendorVersion"`
	Devices       []boardconfig.DeviceResource `json:"devices"`
	Resources     []boardconfig.BoardResource  `json:"resources"`
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var tmpDir string

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestGetUsersStatus(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	sm.usersServices = []cloudprotocol.ServiceInfo{
		{ID: "id1", AosVersion: 1, Status: cloudprotocol.InstalledStatus, StateChecksum: "state1"},
		{ID: "id2", AosVersion: 2, Status: cloudprotocol.InstalledStatus, StateChecksum: "state2"},
	}

	sm.usersLayers = []cloudprotocol.LayerInfo{
		{ID: "id1", AosVersion: 1, Status: cloudprotocol.InstalledStatus, Digest: "digest1"},
		{ID: "id2", AosVersion: 2, Status: cloudprotocol.InstalledStatus, Digest: "digest2"},
	}

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	users := []string{"user1", "user2", "user3"}

	services, layers, err := controller.GetUsersStatus(users)
	if err != nil {
		t.Errorf("Error getting current info: %s", err)
	}

	if !reflect.DeepEqual(users, sm.users) {
		t.Errorf("Wrong users: %v", sm.users)
	}

	if !reflect.DeepEqual(services, sm.usersServices) {
		t.Errorf("Wrong services info: %v", services)
	}

	if !reflect.DeepEqual(layers, sm.usersLayers) {
		t.Errorf("Wrong layers info: %v", layers)
	}
}

func TestGetAllStatus(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	sm.allServices = []cloudprotocol.ServiceInfo{
		{ID: "id1", AosVersion: 1, Status: cloudprotocol.InstalledStatus, StateChecksum: "state1"},
		{ID: "id2", AosVersion: 2, Status: cloudprotocol.InstalledStatus, StateChecksum: "state2"},
		{ID: "id3", AosVersion: 3, Status: cloudprotocol.InstalledStatus, StateChecksum: "state3"},
	}

	sm.allLayers = []cloudprotocol.LayerInfo{
		{ID: "id1", AosVersion: 1, Status: cloudprotocol.InstalledStatus, Digest: "digest1"},
		{ID: "id2", AosVersion: 2, Status: cloudprotocol.InstalledStatus, Digest: "digest2"},
		{ID: "id3", AosVersion: 3, Status: cloudprotocol.InstalledStatus, Digest: "digest3"},
	}

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	services, layers, err := controller.GetAllStatus()
	if err != nil {
		t.Errorf("Error getting current info: %s", err)
	}

	if !reflect.DeepEqual(services, sm.allServices) {
		t.Errorf("Wrong services info: %v", services)
	}

	if !reflect.DeepEqual(layers, sm.allLayers) {
		t.Errorf("Wrong layers info: %v", layers)
	}
}

func TestCheckBoardConfig(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	boardConfig := boardconfig.BoardConfig{
		VendorVersion: "3.0",
	}

	if err = controller.CheckBoardConfig(boardConfig); err != nil {
		t.Errorf("Check board config error: %s", err)
	}

	if sm.boardConfigVersion != boardConfig.VendorVersion {
		t.Errorf("Wrong board config version: %s", sm.boardConfigVersion)
	}
}

func TestSetBoardConfig(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	sm.boardConfigVersion = "3.0"

	boardConfig := boardconfig.BoardConfig{
		VendorVersion: "4.0",
	}

	if err = controller.SetBoardConfig(boardConfig); err != nil {
		t.Errorf("Set board config error: %s", err)
	}

	if sm.boardConfigVersion != boardConfig.VendorVersion {
		t.Errorf("Wrong board config version: %s", sm.boardConfigVersion)
	}
}

func TestInstallServices(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	installServices := []cloudprotocol.ServiceInfoFromCloud{
		{ID: "service0", ProviderID: "provider0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url0"}}},
		{ID: "service1", ProviderID: "provider1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url1"}}},
		{ID: "service2", ProviderID: "provider2", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url2"}}},
		{ID: "service3", ProviderID: "provider3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 3},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url3"}}},
		{ID: "service4", ProviderID: "provider4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 4},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url4"}}},
		{ID: "service5", ProviderID: "provider5", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 5},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url5"}}},
		{ID: "service6", ProviderID: "provider6", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 6},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url6"}}},
		{ID: "service7", ProviderID: "provider7", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 7},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url7"}}},
		{ID: "service8", ProviderID: "provider8", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 8},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url8"}}},
		{ID: "service9", ProviderID: "provider9", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 9},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url9"}}},
	}

	expectedResult := []cloudprotocol.ServiceInfo{}

	for _, serviceInfo := range installServices {
		expectedResult = append(expectedResult, cloudprotocol.ServiceInfo{
			ID:         serviceInfo.ID,
			AosVersion: serviceInfo.AosVersion,
			Status:     cloudprotocol.InstalledStatus,
		})
	}

	users := []string{"user1", "user2", "user3"}

	var wg sync.WaitGroup

	for _, serviceInfo := range installServices {
		wg.Add(1)

		go func(serviceInfo cloudprotocol.ServiceInfoFromCloud) {
			defer wg.Done()

			if _, err = controller.InstallService(users, serviceInfo); err != nil {
				t.Errorf("Can't install service: %s", err)
			}
		}(serviceInfo)
	}

	wg.Wait()

	if !reflect.DeepEqual(users, sm.users) {
		t.Errorf("Wrong users: %v", sm.users)
	}

	sort.Slice(sm.usersServices, func(i, j int) (isLess bool) { return sm.usersServices[i].ID < sm.usersServices[j].ID })

	if !reflect.DeepEqual(sm.usersServices, expectedResult) {
		t.Errorf("Wrong services: %v", expectedResult)
	}
}

func TestRemoveServices(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	removeServices := []cloudprotocol.ServiceInfo{
		{ID: "service0", AosVersion: 0, Status: cloudprotocol.InstalledStatus},
		{ID: "service1", AosVersion: 1, Status: cloudprotocol.InstalledStatus},
		{ID: "service2", AosVersion: 2, Status: cloudprotocol.InstalledStatus},
		{ID: "service3", AosVersion: 3, Status: cloudprotocol.InstalledStatus},
		{ID: "service4", AosVersion: 4, Status: cloudprotocol.InstalledStatus},
		{ID: "service5", AosVersion: 5, Status: cloudprotocol.InstalledStatus},
		{ID: "service6", AosVersion: 6, Status: cloudprotocol.InstalledStatus},
		{ID: "service7", AosVersion: 7, Status: cloudprotocol.InstalledStatus},
		{ID: "service8", AosVersion: 8, Status: cloudprotocol.InstalledStatus},
		{ID: "service9", AosVersion: 9, Status: cloudprotocol.InstalledStatus},
	}

	sm.usersServices = make([]cloudprotocol.ServiceInfo, len(removeServices))

	copy(removeServices, sm.usersServices)

	users := []string{"user1", "user2", "user3"}

	var wg sync.WaitGroup

	for _, serviceInfo := range removeServices {
		wg.Add(1)

		go func(serviceInfo cloudprotocol.ServiceInfo) {
			defer wg.Done()

			if err = controller.RemoveService(users, serviceInfo); err != nil {
				t.Errorf("Can't remove service: %s", err)
			}
		}(serviceInfo)
	}

	wg.Wait()

	if !reflect.DeepEqual(users, sm.users) {
		t.Errorf("Wrong users: %v", sm.users)
	}

	if len(sm.usersServices) != 0 {
		t.Errorf("Wrong services: %v", sm.usersServices)
	}
}

func TestInstallLayers(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	installLayers := []cloudprotocol.LayerInfoFromCloud{
		{ID: "layer0", Digest: "digest0", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 0},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url0"}}},
		{ID: "layer1", Digest: "digest1", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 1},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url1"}}},
		{ID: "layer2", Digest: "digest2", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 2},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url2"}}},
		{ID: "layer3", Digest: "digest3", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 3},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url3"}}},
		{ID: "layer4", Digest: "digest4", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 4},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url4"}}},
		{ID: "layer5", Digest: "digest5", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 5},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url5"}}},
		{ID: "layer6", Digest: "digest6", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 6},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url6"}}},
		{ID: "layer7", Digest: "digest7", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 7},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url7"}}},
		{ID: "layer8", Digest: "digest8", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 8},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url8"}}},
		{ID: "layer9", Digest: "digest9", VersionFromCloud: cloudprotocol.VersionFromCloud{AosVersion: 9},
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{URLs: []string{"url9"}}},
	}

	expectedResult := []cloudprotocol.LayerInfo{}

	for _, layerInfo := range installLayers {
		expectedResult = append(expectedResult, cloudprotocol.LayerInfo{
			ID:         layerInfo.ID,
			Digest:     layerInfo.Digest,
			AosVersion: layerInfo.AosVersion,
			Status:     cloudprotocol.InstalledStatus,
		})
	}

	var wg sync.WaitGroup

	for _, layerInfo := range installLayers {
		wg.Add(1)

		go func(layerInfo cloudprotocol.LayerInfoFromCloud) {
			defer wg.Done()

			if err = controller.InstallLayer(layerInfo); err != nil {
				t.Errorf("Can't install layer: %s", err)
			}
		}(layerInfo)
	}

	wg.Wait()

	sort.Slice(sm.usersLayers, func(i, j int) (isLess bool) { return sm.usersLayers[i].ID < sm.usersLayers[j].ID })

	if !reflect.DeepEqual(sm.usersLayers, expectedResult) {
		t.Errorf("Wrong layers: %v", expectedResult)
	}
}

func TestServiceStateAcceptance(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	if err = controller.ServiceStateAcceptance("correlation0", cloudprotocol.StateAcceptance{Checksum: "checksum"}); err != nil {
		t.Errorf("Error sending service state acceptance: %s", err)
	}

	if sm.correlationId != "correlation0" || sm.stateChecksum != "checksum" {
		t.Error("Wrong service state acceptance values")
	}
}

func TestSetServiceState(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		&testMessageSender{}, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	users := []string{"user1", "user2", "user3"}

	if err = controller.SetServiceState(users, cloudprotocol.UpdateState{Checksum: "checksum"}); err != nil {
		t.Errorf("Error sending service set service state: %s", err)
	}

	if !reflect.DeepEqual(users, sm.users) {
		t.Errorf("Wrong users: %v", sm.users)
	}

	if sm.stateChecksum != "checksum" {
		t.Error("Wrong update service state values")
	}
}

func TestOverrideEnvVars(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	messageSender := newTestMessageSender()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		messageSender, &testAlertSender{}, &testMonitoringSender{}, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	envVars := []cloudprotocol.OverrideEnvsFromCloud{
		{ServiceID: "service0", SubjectID: "subject0", EnvVars: []cloudprotocol.EnvVarInfo{
			{ID: "id0", Variable: "var0"}}},
		{ServiceID: "service1", SubjectID: "subject1", EnvVars: []cloudprotocol.EnvVarInfo{
			{ID: "id1", Variable: "var1"}}},
		{ServiceID: "service2", SubjectID: "subject2", EnvVars: []cloudprotocol.EnvVarInfo{
			{ID: "id2", Variable: "var2"}}},
	}

	expectedStatus := []cloudprotocol.EnvVarInfoStatus{}

	for _, item := range envVars {
		status := cloudprotocol.EnvVarInfoStatus{
			ServiceID: item.ServiceID,
			SubjectID: item.SubjectID,
		}

		for _, envVar := range item.EnvVars {
			status.Statuses = append(status.Statuses, cloudprotocol.EnvVarStatus{ID: envVar.ID})
		}

		expectedStatus = append(expectedStatus, status)
	}

	if err = controller.OverrideEnvVars(cloudprotocol.DecodedOverrideEnvVars{OverrideEnvVars: envVars}); err != nil {
		t.Errorf("Error sending override env vars: %s", err)
	}

	message, err := waitMessage(messageSender.messageChannel, messageTimeout)
	if err != nil {
		t.Fatalf("Wait message error: %s", err)
	}

	if !reflect.DeepEqual(message.([]cloudprotocol.EnvVarInfoStatus), expectedStatus) {
		t.Errorf("Wrong env var status: %v", message.([]cloudprotocol.EnvVarInfoStatus))
	}
}

func TestSMNotifications(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}
	defer sm.close()

	messageSender := newTestMessageSender()
	alertSender := newTestAlertSender()
	monitoringSender := newTestMonitoringSender()

	controller, err := smcontroller.New(&config.Config{
		SMController: config.SMController{SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}}}},
		messageSender, alertSender, monitoringSender, &testURLTranslator{}, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	// Test messages

	testMessages := []interface{}{
		cloudprotocol.NewState{ServiceID: "service0", Checksum: "checksum0", State: "state0"},
		cloudprotocol.StateRequest{ServiceID: "service1", Default: true},
		cloudprotocol.PushLog{LogID: "log0", PartCount: 2, Part: 1, Data: []byte("this is log"), Error: "this is error"},
	}

	for _, sendMessage := range testMessages {
		sm.messageChannel <- sendMessage

		receiveMessage, err := waitMessage(messageSender.messageChannel, messageTimeout)
		if err != nil {
			t.Errorf("Wait message error: %s", err)
			continue
		}

		if !reflect.DeepEqual(receiveMessage, sendMessage) {
			t.Errorf("Wrong data received: %v", receiveMessage)
			continue
		}
	}

	// Test alerts

	testAlerts := []interface{}{
		cloudprotocol.AlertItem{Timestamp: time.Now().UTC(), Tag: "tag0", Source: "source0", AosVersion: 0,
			Payload: &cloudprotocol.SystemAlert{Message: "system alert"}},
		cloudprotocol.AlertItem{Timestamp: time.Now().UTC(), Tag: "tag1", Source: "source1", AosVersion: 1,
			Payload: &cloudprotocol.ResourceAlert{Parameter: "param0", Value: 123}},
		cloudprotocol.AlertItem{Timestamp: time.Now().UTC(), Tag: "tag2", Source: "source2", AosVersion: 1,
			Payload: &cloudprotocol.ResourceValidateAlert{Type: "type0", Message: []cloudprotocol.ResourceValidateError{
				{Name: "name0", Errors: []string{"error0", "error1", "error2"}},
				{Name: "name1", Errors: []string{"error3", "error4", "error5"}},
			}}},
	}

	for _, sendMessage := range testAlerts {
		sm.messageChannel <- sendMessage

		receiveMessage, err := waitMessage(alertSender.messageChannel, messageTimeout)
		if err != nil {
			t.Errorf("Wait message error: %s", err)
			continue
		}

		if !reflect.DeepEqual(receiveMessage, sendMessage) {
			t.Errorf("Wrong data received: %v %v", receiveMessage, sendMessage)
			continue
		}
	}

	// Test monitoring

	testMonitoringData := []interface{}{
		cloudprotocol.MonitoringData{Timestamp: time.Now().UTC(),
			Global: cloudprotocol.GlobalMonitoringData{
				RAM: 10, CPU: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50},
			ServicesData: []cloudprotocol.ServiceMonitoringData{
				{ServiceID: "service0", RAM: 60, CPU: 70, InTraffic: 80, OutTraffic: 90},
				{ServiceID: "service1", RAM: 61, CPU: 71, InTraffic: 81, OutTraffic: 91},
			},
		},
		cloudprotocol.MonitoringData{Timestamp: time.Now().UTC(),
			Global: cloudprotocol.GlobalMonitoringData{
				RAM: 11, CPU: 21, UsedDisk: 31, InTraffic: 41, OutTraffic: 51},
			ServicesData: []cloudprotocol.ServiceMonitoringData{
				{ServiceID: "service2", RAM: 62, CPU: 72, InTraffic: 83, OutTraffic: 92},
				{ServiceID: "service3", RAM: 63, CPU: 73, InTraffic: 83, OutTraffic: 93},
			},
		},
	}

	for _, sendMessage := range testMonitoringData {
		sm.messageChannel <- sendMessage

		receiveMessage, err := waitMessage(monitoringSender.messageChannel, messageTimeout)
		if err != nil {
			t.Errorf("Wait message error: %s", err)
			continue
		}

		if !reflect.DeepEqual(receiveMessage, sendMessage) {
			t.Errorf("Wrong data received: %v %v", receiveMessage, sendMessage)
			continue
		}
	}

}

/*******************************************************************************
 * Interfaces
 ******************************************************************************/

func (translator *testURLTranslator) TranslateURL(isLocal bool, inURL string) (outURL string, err error) {
	outURL = inURL

	return outURL, nil
}

func newTestAlertSender() (sender *testAlertSender) {
	return &testAlertSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testAlertSender) SendAlert(alert cloudprotocol.AlertItem) (err error) {
	sender.messageChannel <- alert

	return nil
}

func newTestMonitoringSender() (sender *testMonitoringSender) {
	return &testMonitoringSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testMonitoringSender) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error) {
	sender.messageChannel <- monitoringData

	return nil
}

func newTestMessageSender() (sender *testMessageSender) {
	return &testMessageSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testMessageSender) SendServiceNewState(correlationID, serviceID, state, checksum string) (err error) {
	sender.messageChannel <- cloudprotocol.NewState{
		ServiceID: serviceID,
		State:     state,
		Checksum:  checksum,
	}

	return nil
}

func (sender *testMessageSender) SendServiceStateRequest(serviceID string, defaultState bool) (err error) {
	sender.messageChannel <- cloudprotocol.StateRequest{
		ServiceID: serviceID,
		Default:   defaultState,
	}

	return nil
}

func (sender *testMessageSender) SendOverrideEnvVarsStatus(envStatus []cloudprotocol.EnvVarInfoStatus) (err error) {
	sender.messageChannel <- envStatus

	return nil
}

func (sender *testMessageSender) SendLog(serviceLog cloudprotocol.PushLog) (err error) {
	sender.messageChannel <- cloudprotocol.PushLog{
		LogID:     serviceLog.LogID,
		PartCount: serviceLog.PartCount,
		Part:      serviceLog.Part,
		Data:      serviceLog.Data,
		Error:     serviceLog.Error,
	}

	return nil
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func waitMessage(messageChannel <-chan interface{}, timeout time.Duration) (message interface{}, err error) {
	select {
	case <-time.After(timeout):
		return nil, aoserrors.New("wait message timeout")

	case message = <-messageChannel:
		return message, nil
	}
}

func newTestSM(url string) (sm *testSM, err error) {
	sm = &testSM{messageChannel: make(chan interface{}, 1)}

	sm.ctx, sm.cancelFunction = context.WithCancel(context.Background())

	sm.grpcServer = grpc.NewServer()

	pb.RegisterSMServiceServer(sm.grpcServer, sm)

	listener, err := net.Listen("tcp", url)
	if err != nil {
		return nil, err
	}

	go sm.grpcServer.Serve(listener)

	return sm, nil
}

func (sm *testSM) close() (err error) {
	if sm.grpcServer != nil {
		sm.grpcServer.Stop()
	}

	sm.cancelFunction()

	return nil
}

func (sm *testSM) SubscribeSMNotifications(
	request *empty.Empty, stream pb.SMService_SubscribeSMNotificationsServer) (err error) {
	for {
		select {
		case message := <-sm.messageChannel:
			switch data := message.(type) {
			case cloudprotocol.PushLog:
				if err = stream.Send(&pb.SMNotifications{SMNotification: &pb.SMNotifications_Log{
					Log: &pb.LogData{
						LogId:     data.LogID,
						PartCount: data.PartCount,
						Part:      data.Part,
						Data:      data.Data,
						Error:     data.Error,
					},
				}}); err != nil {
					return aoserrors.Wrap(err)
				}

			case cloudprotocol.NewState:
				if err = stream.Send(&pb.SMNotifications{SMNotification: &pb.SMNotifications_NewServiceState{
					NewServiceState: &pb.NewServiceState{
						CorrelationId: sm.correlationId,
						ServiceState: &pb.ServiceState{
							ServiceId:     data.ServiceID,
							StateChecksum: data.Checksum,
							State:         []byte(data.State),
						},
					},
				}}); err != nil {
					return aoserrors.Wrap(err)
				}

			case cloudprotocol.StateRequest:
				if err = stream.Send(&pb.SMNotifications{SMNotification: &pb.SMNotifications_ServiceStateRequest{
					ServiceStateRequest: &pb.ServiceStateRequest{
						ServiceId: data.ServiceID,
						Default:   data.Default,
					},
				}}); err != nil {
					return aoserrors.Wrap(err)
				}

			case cloudprotocol.MonitoringData:
				notification := &pb.SMNotifications_Monitoring{
					Monitoring: &pb.Monitoring{
						Timestamp: timestamppb.New(data.Timestamp),
						SystemMonitoring: &pb.SystemMonitoring{
							Ram:        data.Global.RAM,
							Cpu:        data.Global.CPU,
							UsedDisk:   data.Global.UsedDisk,
							InTraffic:  data.Global.InTraffic,
							OutTraffic: data.Global.OutTraffic,
						},
					},
				}

				for _, serviceData := range data.ServicesData {
					notification.Monitoring.ServiceMonitoring = append(notification.Monitoring.ServiceMonitoring,
						&pb.ServiceMonitoring{
							ServiceId:  serviceData.ServiceID,
							Ram:        serviceData.RAM,
							Cpu:        serviceData.CPU,
							UsedDisk:   serviceData.UsedDisk,
							InTraffic:  serviceData.InTraffic,
							OutTraffic: serviceData.OutTraffic})
				}

				if err = stream.Send(&pb.SMNotifications{SMNotification: notification}); err != nil {
					return aoserrors.Wrap(err)
				}

			case cloudprotocol.AlertItem:
				pbAlert := pb.Alert{
					Timestamp:  timestamppb.New(data.Timestamp),
					Tag:        data.Tag,
					Source:     data.Source,
					AosVersion: data.AosVersion,
				}

				switch payload := data.Payload.(type) {
				case *cloudprotocol.SystemAlert:
					pbAlert.Payload = &pb.Alert_SystemAlert{SystemAlert: &pb.SystemAlert{Message: payload.Message}}

				case *cloudprotocol.ResourceAlert:
					pbAlert.Payload = &pb.Alert_ResourceAlert{ResourceAlert: &pb.ResourceAlert{
						Parameter: payload.Parameter,
						Value:     payload.Value,
					}}

				case *cloudprotocol.ResourceValidateAlert:
					resourceValidateAlert := &pb.ResourceValidateAlert{Type: payload.Type}

					for _, resourceError := range payload.Message {
						resourceValidateAlert.Errors = append(resourceValidateAlert.Errors, &pb.ResourceValidateErrors{
							Name:     resourceError.Name,
							ErrorMsg: resourceError.Errors,
						})
					}

					pbAlert.Payload = &pb.Alert_ResourceValidateAlert{ResourceValidateAlert: resourceValidateAlert}
				}

				if err = stream.Send(&pb.SMNotifications{
					SMNotification: &pb.SMNotifications_Alert{Alert: &pbAlert}}); err != nil {
					return aoserrors.Wrap(err)
				}

			}

		case <-sm.ctx.Done():
			return nil
		}
	}
}

func (sm *testSM) GetUsersStatus(ctx context.Context, request *pb.Users) (response *pb.SMStatus, err error) {
	response = &pb.SMStatus{}

	sm.users = request.Users

	for _, service := range sm.usersServices {
		response.Services = append(response.Services, &pb.ServiceStatus{
			ServiceId: service.ID, AosVersion: service.AosVersion, StateChecksum: service.StateChecksum})
	}

	for _, layer := range sm.usersLayers {
		response.Layers = append(response.Layers, &pb.LayerStatus{
			LayerId: layer.ID, AosVersion: layer.AosVersion, Digest: layer.Digest})
	}

	return response, nil
}

func (sm *testSM) GetAllStatus(ctx context.Context, request *empty.Empty) (response *pb.SMStatus, err error) {
	response = &pb.SMStatus{}

	for _, service := range sm.allServices {
		response.Services = append(response.Services, &pb.ServiceStatus{
			ServiceId: service.ID, AosVersion: service.AosVersion, StateChecksum: service.StateChecksum})
	}

	for _, layer := range sm.allLayers {
		response.Layers = append(response.Layers, &pb.LayerStatus{
			LayerId: layer.ID, AosVersion: layer.AosVersion, Digest: layer.Digest})
	}

	return response, nil
}

func (sm *testSM) GetBoardConfigStatus(
	ctx context.Context, request *empty.Empty) (response *pb.BoardConfigStatus, err error) {
	response = &pb.BoardConfigStatus{VendorVersion: sm.boardConfigVersion}

	return response, nil
}

func (sm *testSM) SetBoardConfig(ctx context.Context, request *pb.BoardConfig) (response *empty.Empty, err error) {
	var boardconfig clientBoardConfig

	if err = json.Unmarshal([]byte(request.BoardConfig), &boardconfig); err != nil {
		return nil, err
	}

	sm.boardConfigVersion = boardconfig.VendorVersion

	return &empty.Empty{}, nil
}

func (sm *testSM) CheckBoardConfig(
	ctx context.Context, request *pb.BoardConfig) (response *pb.BoardConfigStatus, err error) {
	var boardconfig clientBoardConfig

	if err = json.Unmarshal([]byte(request.BoardConfig), &boardconfig); err != nil {
		return nil, err
	}

	sm.boardConfigVersion = boardconfig.VendorVersion

	return &pb.BoardConfigStatus{VendorVersion: boardconfig.VendorVersion}, nil
}

func (sm *testSM) InstallService(ctx context.Context,
	request *pb.InstallServiceRequest) (response *pb.ServiceStatus, err error) {
	sm.Lock()
	defer sm.Unlock()

	log.Debug("=== ", request.Users)

	sm.users = request.Users.Users

	serviceInfo := cloudprotocol.ServiceInfo{
		ID:         request.ServiceId,
		AosVersion: request.AosVersion,
		Status:     cloudprotocol.InstalledStatus,
	}

	response = &pb.ServiceStatus{
		ServiceId:     request.ServiceId,
		AosVersion:    request.AosVersion,
		VendorVersion: request.VendorVersion,
		StateChecksum: "",
	}

	for i, service := range sm.usersServices {
		if service.ID == request.ServiceId {
			sm.usersServices[i] = serviceInfo
		}
	}

	sm.usersServices = append(sm.usersServices, serviceInfo)

	return response, nil
}

func (sm *testSM) RemoveService(ctx context.Context, request *pb.RemoveServiceRequest) (response *empty.Empty, err error) {
	sm.Lock()
	defer sm.Unlock()

	sm.users = request.Users.Users

	for i, service := range sm.usersServices {
		if service.ID == request.ServiceId {
			sm.usersServices = append(sm.usersServices[:i], sm.usersServices[i+1:]...)
			return &empty.Empty{}, nil
		}
	}

	return &empty.Empty{}, aoserrors.New("no service found")
}

func (sm *testSM) InstallLayer(ctx context.Context, request *pb.InstallLayerRequest) (response *empty.Empty, err error) {
	sm.Lock()
	defer sm.Unlock()

	layerInfo := cloudprotocol.LayerInfo{
		ID:         request.LayerId,
		Digest:     request.Digest,
		AosVersion: request.AosVersion,
		Status:     cloudprotocol.InstalledStatus,
	}

	for i, layer := range sm.usersLayers {
		if layer.ID == request.LayerId {
			sm.usersLayers[i] = layerInfo
		}
	}

	sm.usersLayers = append(sm.usersLayers, layerInfo)

	return &empty.Empty{}, nil
}

func (sm *testSM) ServiceStateAcceptance(ctx context.Context, request *pb.StateAcceptance) (response *empty.Empty, err error) {
	sm.correlationId = request.CorrelationId
	sm.stateChecksum = request.StateChecksum

	return &empty.Empty{}, nil
}

func (sm *testSM) SetServiceState(ctx context.Context, request *pb.ServiceState) (response *empty.Empty, err error) {
	sm.users = request.Users.Users
	sm.stateChecksum = request.StateChecksum

	return &empty.Empty{}, nil
}

func (sm *testSM) OverrideEnvVars(ctx context.Context,
	request *pb.OverrideEnvVarsRequest) (response *pb.OverrideEnvVarStatus, err error) {
	response = &pb.OverrideEnvVarStatus{}

	for _, item := range request.EnvVars {
		envVarStatus := &pb.EnvVarStatus{
			ServiceId: item.ServiceId,
			SubjectId: item.SubjectId}

		for _, envVar := range item.Vars {
			envVarStatus.VarStatus = append(envVarStatus.VarStatus, &pb.VarStatus{
				VarId: envVar.VarId,
				Error: "",
			})
		}

		response.EnvVarStatus = append(response.EnvVarStatus, envVarStatus)
	}

	return response, nil
}
