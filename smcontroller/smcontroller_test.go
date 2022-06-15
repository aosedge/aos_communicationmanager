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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v2"
	"github.com/aoscloud/aos_common/utils/pbconvert"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/smcontroller"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
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
	grpcServer                   *grpc.Server
	messageChannel               chan *pb.SMNotifications
	currentServiceInstallRequest *pb.InstallServiceRequest
	currentServiceRestoreRequest *pb.RestoreServiceRequest
	currentServiceRemoveRequest  *pb.RemoveServiceRequest
	servicesStatus               pb.ServicesStatus
	currentLayerInstallRequest   *pb.InstallLayerRequest
	layersStatus                 pb.LayersStatus
	currentStateAcceptance       *pb.StateAcceptance
	currentSetState              *pb.InstanceState
	currentEnvVarRequest         *pb.OverrideEnvVarsRequest
	envVarStatus                 *pb.OverrideEnvVarStatus
	currentSysLogRequest         *pb.SystemLogRequest
	currentInstanceLogRequest    *pb.InstanceLogRequest
	currentRunRequest            *pb.RunInstancesRequest
	setBoardConfig               aostypes.BoardConfig
	checkBoardConfig             aostypes.BoardConfig
	boardConfigWasSet            bool
	ctx                          context.Context // nolint:containedctx
	cancelFunction               context.CancelFunc
}

type testURLTranslator struct{}

type testMessageSender struct {
	messageChannel chan interface{}
}

type testAlertSender struct {
	messageChannel chan interface{}
}

type testMonitoringSender struct {
	messageChannel chan interface{}
}

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
 * Tests
 **********************************************************************************************************************/

func TestBoardConfigMessages(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	config := config.Config{SMController: config.SMController{
		SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
	}}

	controller, err := smcontroller.New(&config, nil, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	boardConfig := aostypes.BoardConfig{
		VendorVersion: "3.0",
		Devices:       []aostypes.DeviceInfo{{Name: "dev1", SharedCount: 2, Groups: []string{"gid1"}}},
	}

	// test check board config
	if err = controller.CheckBoardConfig(boardConfig); err != nil {
		t.Fatalf("Check board config error: %v", err)
	}

	if !reflect.DeepEqual(boardConfig, sm.checkBoardConfig) {
		t.Error("Incorrect board config in check request")
	}

	// test set the same board config
	sm.setBoardConfig = boardConfig

	if err = controller.SetBoardConfig(boardConfig); err != nil {
		t.Fatalf("Can't send set board config: %v", err)
	}

	if sm.boardConfigWasSet {
		t.Error("New board config should not be set")
	}

	boardConfig.VendorVersion = "4.0"

	if err = controller.SetBoardConfig(boardConfig); err != nil {
		t.Fatalf("Can't send set board config: %v", err)
	}

	if !sm.boardConfigWasSet {
		t.Error("New board config should be set")
	}

	if !reflect.DeepEqual(boardConfig, sm.setBoardConfig) {
		t.Error("Incorrect board config")
	}
}

func TestServicesMessages(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	config := config.Config{SMController: config.SMController{
		SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
	}}

	controller, err := smcontroller.New(&config, nil, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	expectedStatus := make([]unitstatushandler.ServiceStatus, 5)
	sm.servicesStatus.Services = make([]*pb.ServiceStatus, 5)

	for i := 0; i < 5; i++ {
		installRequest := cloudprotocol.ServiceInfo{
			VersionInfo: cloudprotocol.VersionInfo{AosVersion: uint64(i), Description: "description" + strconv.Itoa(i)},
			ID:          "testService" + strconv.Itoa(i),
			ProviderID:  "provider" + strconv.Itoa(i),
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{
				URLs:   []string{"someurl" + strconv.Itoa(i)},
				Sha256: []byte{0, 0, 0, byte(i + 200)},
				Sha512: []byte{byte(i + 300), 0, 0, 0},
				Size:   uint64(i + 500),
			},
		}
		expectedInstallRequest := &pb.InstallServiceRequest{
			Url:         "someurl" + strconv.Itoa(i),
			ServiceId:   "testService" + strconv.Itoa(i),
			ProviderId:  "provider" + strconv.Itoa(i),
			AosVersion:  uint64(i),
			Description: "description" + strconv.Itoa(i),
			Sha256:      []byte{0, 0, 0, byte(i + 200)},
			Sha512:      []byte{byte(i + 300), 0, 0, 0},
			Size:        uint64(i + 500),
		}
		expectedStatus[i] = unitstatushandler.ServiceStatus{
			ServiceStatus: cloudprotocol.ServiceStatus{
				AosVersion: uint64(i),
				ID:         "testService" + strconv.Itoa(i),
				Status:     cloudprotocol.InstalledStatus,
			},
		}
		sm.servicesStatus.Services[i] = &pb.ServiceStatus{ServiceId: "testService" + strconv.Itoa(i), AosVersion: uint64(i)}

		if err := controller.InstallService(installRequest); err != nil {
			t.Fatalf("Can't install service: %v", err)
		}

		if !proto.Equal(expectedInstallRequest, sm.currentServiceInstallRequest) {
			t.Error("Incorrect install service request")
		}
	}

	servicesResult, err := controller.GetServicesStatus()
	if err != nil {
		t.Fatalf("Can't get all services status: %v", err)
	}

	if !reflect.DeepEqual(servicesResult, expectedStatus) {
		t.Error("Incorrect services status")
	}

	var (
		serviceID              = "someserviceID"
		expectedRemoveRequest  = &pb.RemoveServiceRequest{ServiceId: serviceID}
		expectedRestoreRequest = &pb.RestoreServiceRequest{ServiceId: serviceID}
	)

	if err := controller.RemoveService(serviceID); err != nil {
		t.Fatalf("Can't remove service: %v", err)
	}

	if !proto.Equal(expectedRemoveRequest, sm.currentServiceRemoveRequest) {
		t.Error("incorrect remove service request")
	}

	if err := controller.RestoreService(serviceID); err != nil {
		t.Fatalf("Can't restore service: %v", err)
	}

	if !proto.Equal(expectedRestoreRequest, sm.currentServiceRestoreRequest) {
		t.Error("incorrect restore service request")
	}
}

func TestLayerMessages(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	config := config.Config{SMController: config.SMController{
		SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
	}}

	controller, err := smcontroller.New(&config, nil, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	expectedLayersStatus := make([]unitstatushandler.LayerStatus, 5)
	sm.layersStatus.Layers = make([]*pb.LayerStatus, 5)

	for i := 0; i < 5; i++ {
		installLayerRequest := cloudprotocol.LayerInfo{
			VersionInfo: cloudprotocol.VersionInfo{
				AosVersion: uint64(i), Description: "description" + strconv.Itoa(i),
				VendorVersion: strconv.Itoa(i + 1),
			},
			ID:     "testLayer" + strconv.Itoa(i),
			Digest: "digest" + strconv.Itoa(i),
			DecryptDataStruct: cloudprotocol.DecryptDataStruct{
				URLs:   []string{"someurl" + strconv.Itoa(i)},
				Sha256: []byte{0, 0, 0, byte(i + 100)},
				Sha512: []byte{byte(i + 100), 0, 0, 0},
				Size:   uint64(i + 500),
			},
		}
		expectedInstallRequest := &pb.InstallLayerRequest{
			Url: "someurl" + strconv.Itoa(i), LayerId: "testLayer" + strconv.Itoa(i), AosVersion: uint64(i),
			VendorVersion: strconv.Itoa(i + 1), Digest: "digest" + strconv.Itoa(i),
			Description: "description" + strconv.Itoa(i),
			Sha256:      []byte{0, 0, 0, byte(i + 100)},
			Sha512:      []byte{byte(i + 100), 0, 0, 0},
			Size:        uint64(i + 500),
		}
		expectedLayersStatus[i] = unitstatushandler.LayerStatus{LayerStatus: cloudprotocol.LayerStatus{
			AosVersion: uint64(i),
			Status:     cloudprotocol.InstalledStatus,
			ID:         "testLayer" + strconv.Itoa(i),
			Digest:     "digest" + strconv.Itoa(i),
		}}
		sm.layersStatus.Layers[i] = &pb.LayerStatus{
			LayerId: "testLayer" + strconv.Itoa(i), AosVersion: uint64(i), VendorVersion: strconv.Itoa(i + 1),
			Digest: "digest" + strconv.Itoa(i),
		}

		if err := controller.InstallLayer(installLayerRequest); err != nil {
			t.Fatalf("Can't install layer: %v", err)
		}

		if !proto.Equal(expectedInstallRequest, sm.currentLayerInstallRequest) {
			t.Error("Incorrect layer install request")
		}
	}

	layersResult, err := controller.GetLayersStatus()
	if err != nil {
		t.Fatalf("Can't get all layers status: %v", err)
	}

	if !reflect.DeepEqual(layersResult, expectedLayersStatus) {
		t.Error("Incorrect layers status")
	}
}

func TestInstanceStateMessages(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	var (
		config = config.Config{SMController: config.SMController{
			SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
		}}
		messageSender = newTestMessageSender()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	expectedNewState := cloudprotocol.NewState{
		InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
		Checksum:      "someChecksum", State: "someState",
	}

	sm.messageChannel <- &pb.SMNotifications{SMNotification: &pb.SMNotifications_NewInstanceState{
		NewInstanceState: &pb.NewInstanceState{State: &pb.InstanceState{
			Instance:      pbconvert.InstanceIdentToPB(expectedNewState.InstanceIdent),
			StateChecksum: expectedNewState.Checksum,
			State:         []byte(expectedNewState.State),
		}},
	}}

	if err := waitMessage(messageSender.messageChannel, expectedNewState, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %s", err)
	}

	expectedStateRequest := cloudprotocol.StateRequest{
		InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
		Default:       true,
	}

	sm.messageChannel <- &pb.SMNotifications{SMNotification: &pb.SMNotifications_InstanceStateRequest{
		InstanceStateRequest: &pb.InstanceStateRequest{
			Instance: pbconvert.InstanceIdentToPB(expectedStateRequest.InstanceIdent),
			Default:  true,
		},
	}}

	if err := waitMessage(messageSender.messageChannel, expectedStateRequest, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %s", err)
	}

	if err = controller.InstanceStateAcceptance(cloudprotocol.StateAcceptance{
		InstanceIdent: expectedNewState.InstanceIdent,
		Checksum:      "someCheckSum", Result: "OK", Reason: "good",
	}); err != nil {
		t.Errorf("Error sending instance state acceptance: %v", err)
	}

	expectedPBStateAcceptance := &pb.StateAcceptance{
		Instance:      pbconvert.InstanceIdentToPB(expectedNewState.InstanceIdent),
		StateChecksum: "someCheckSum", Result: "OK", Reason: "good",
	}

	if !proto.Equal(expectedPBStateAcceptance, sm.currentStateAcceptance) {
		log.Error("Incorrect state acceptance")
	}

	if err := controller.SetInstanceState(cloudprotocol.UpdateState{
		InstanceIdent: expectedNewState.InstanceIdent, Checksum: "someCheckSum", State: "someState",
	}); err != nil {
		t.Fatalf("Can't send set instance state: %v", err)
	}

	expectedPBNewState := &pb.NewInstanceState{State: &pb.InstanceState{
		Instance:      pbconvert.InstanceIdentToPB(expectedNewState.InstanceIdent),
		StateChecksum: "someCheckSum", State: []byte("someState"),
	}}

	if !proto.Equal(expectedPBNewState, sm.currentSetState) {
		log.Error("Incorrect new instance state")
	}
}

func TestOverrideEnvVars(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	var (
		config = config.Config{SMController: config.SMController{
			SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
		}}
		messageSender = newTestMessageSender()
		currentTime   = time.Now()
		envVars       = cloudprotocol.DecodedOverrideEnvVars{
			OverrideEnvVars: []cloudprotocol.EnvVarsInstanceInfo{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					EnvVars: []cloudprotocol.EnvVarInfo{
						{ID: "id0", Variable: "var0", TTL: &currentTime},
					},
				},
			},
		}
		expectedPBEnvVarRequest = &pb.OverrideEnvVarsRequest{
			EnvVars: []*pb.OverrideInstanceEnvVar{{Instance: &pb.InstanceIdent{
				ServiceId: "service0",
				SubjectId: "subject0", Instance: -1,
			}, Vars: []*pb.EnvVarInfo{{VarId: "id0", Variable: "var0", Ttl: timestamppb.New(currentTime)}}}},
		}
		exepctedEnvVarStatus = cloudprotocol.OverrideEnvVarsStatus{
			OverrideEnvVarsStatus: []cloudprotocol.EnvVarsInstanceStatus{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					Statuses:       []cloudprotocol.EnvVarStatus{{ID: "id0", Error: "someError"}},
				},
			},
		}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	sm.envVarStatus = &pb.OverrideEnvVarStatus{EnvVarsStatus: []*pb.EnvVarInstanceStatus{
		{Instance: &pb.InstanceIdent{
			ServiceId: "service0",
			SubjectId: "subject0", Instance: -1,
		}, VarsStatus: []*pb.EnvVarStatus{{VarId: "id0", Error: "someError"}}},
	}}

	if err = controller.OverrideEnvVars(envVars); err != nil {
		t.Fatalf("Error sending override env vars: %v", err)
	}

	if !proto.Equal(sm.currentEnvVarRequest, expectedPBEnvVarRequest) {
		t.Error("Incorrect set env vars request")
	}

	if err := waitMessage(messageSender.messageChannel, exepctedEnvVarStatus, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}
}

func TestLogMessages(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	var (
		config = config.Config{SMController: config.SMController{
			SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
		}}
		messageSender = newTestMessageSender()
		currentTime   = time.Now().UTC()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	// Test get system log
	type testSystemLogRequest struct {
		sendLogRequest     cloudprotocol.RequestSystemLog
		expectedLogRequest *pb.SystemLogRequest
	}

	testRequests := []testSystemLogRequest{
		{
			sendLogRequest:     cloudprotocol.RequestSystemLog{LogID: "sysLogID1", From: &currentTime},
			expectedLogRequest: &pb.SystemLogRequest{LogId: "sysLogID1", From: timestamppb.New(currentTime)},
		},
		{
			sendLogRequest:     cloudprotocol.RequestSystemLog{LogID: "sysLogID2", Till: &currentTime},
			expectedLogRequest: &pb.SystemLogRequest{LogId: "sysLogID2", Till: timestamppb.New(currentTime)},
		},
	}

	for _, request := range testRequests {
		if err := controller.GetSystemLog(request.sendLogRequest); err != nil {
			t.Fatalf("Can't send get system log request: %v", err)
		}

		if !proto.Equal(request.expectedLogRequest, sm.currentSysLogRequest) {
			t.Error("Incorrect system log request")
		}
	}

	// Test get service log
	type testServiceLogRequest struct {
		sendLogRequest     cloudprotocol.RequestServiceLog
		expectedLogRequest *pb.InstanceLogRequest
	}

	instanceLogRequests := []testServiceLogRequest{
		{
			sendLogRequest: cloudprotocol.RequestServiceLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("ser1", "s1", -1),
				LogID:          "serviceLogID1", From: &currentTime,
			},
			expectedLogRequest: &pb.InstanceLogRequest{
				Instance: &pb.InstanceIdent{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
				LogId:    "serviceLogID1", From: timestamppb.New(currentTime),
			},
		},
		{
			sendLogRequest: cloudprotocol.RequestServiceLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("ser2", "", -1),
				LogID:          "serviceLogID1", Till: &currentTime,
			},
			expectedLogRequest: &pb.InstanceLogRequest{
				Instance: &pb.InstanceIdent{ServiceId: "ser2", SubjectId: "", Instance: -1},
				LogId:    "serviceLogID1", Till: timestamppb.New(currentTime),
			},
		},
	}

	for _, request := range instanceLogRequests {
		if err := controller.GetInstanceLog(request.sendLogRequest); err != nil {
			t.Fatalf("Can't send get service log request: %v", err)
		}

		if !proto.Equal(request.expectedLogRequest, sm.currentInstanceLogRequest) {
			t.Error("Incorrect service log request")
		}
	}

	// Test get service crash log
	type testCrashLogRequest struct {
		sendLogRequest     cloudprotocol.RequestServiceCrashLog
		expectedLogRequest *pb.InstanceLogRequest
	}

	crashLogRequests := []testCrashLogRequest{
		{
			sendLogRequest: cloudprotocol.RequestServiceCrashLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("ser1", "s1", -1),
				LogID:          "serviceLogID1", From: &currentTime,
			},
			expectedLogRequest: &pb.InstanceLogRequest{
				Instance: &pb.InstanceIdent{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
				LogId:    "serviceLogID1", From: timestamppb.New(currentTime),
			},
		},
		{
			sendLogRequest: cloudprotocol.RequestServiceCrashLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("ser2", "", -1),
				LogID:          "serviceLogID1", Till: &currentTime,
			},
			expectedLogRequest: &pb.InstanceLogRequest{
				Instance: &pb.InstanceIdent{ServiceId: "ser2", SubjectId: "", Instance: -1},
				LogId:    "serviceLogID1", Till: timestamppb.New(currentTime),
			},
		},
	}

	for _, request := range crashLogRequests {
		if err := controller.GetInstanceCrashLog(request.sendLogRequest); err != nil {
			t.Fatalf("Can't send get service log request: %v", err)
		}

		if !proto.Equal(request.expectedLogRequest, sm.currentInstanceLogRequest) {
			t.Error("Incorrect service log request")
		}
	}

	expectedLog := cloudprotocol.PushLog{
		LogID: "log0", PartCount: 2, Part: 1, Data: []byte("this is log"), Error: "this is error",
	}

	sm.messageChannel <- &pb.SMNotifications{
		SMNotification: &pb.SMNotifications_Log{
			Log: &pb.LogData{LogId: "log0", PartCount: 2, Part: 1, Data: []byte("this is log"), Error: "this is error"},
		},
	}

	if err := waitMessage(messageSender.messageChannel, expectedLog, messageTimeout); err != nil {
		t.Errorf("Incorrect log message: %v", err)
	}
}

func TestSMAlertNotifications(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	var (
		config = config.Config{SMController: config.SMController{
			SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
		}}
		alertSender = newTestAlertSender()
	)

	controller, err := smcontroller.New(&config, nil, alertSender, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	// Test alert notifications
	type testAlert struct {
		sendAlert     *pb.Alert
		expectedAlert cloudprotocol.AlertItem
	}

	testData := []testAlert{
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagSystemError,
				Payload: cloudprotocol.SystemAlert{Message: "SystemAlertMessage"},
			},
			sendAlert: &pb.Alert{
				Tag:     cloudprotocol.AlertTagSystemError,
				Payload: &pb.Alert_SystemAlert{SystemAlert: &pb.SystemAlert{Message: "SystemAlertMessage"}},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagAosCore,
				Payload: cloudprotocol.CoreAlert{CoreComponent: "SM", Message: "CoreAlertMessage"},
			},
			sendAlert: &pb.Alert{
				Tag:     cloudprotocol.AlertTagAosCore,
				Payload: &pb.Alert_CoreAlert{CoreAlert: &pb.CoreAlert{CoreComponent: "SM", Message: "CoreAlertMessage"}},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagResourceValidate,
				Payload: cloudprotocol.ResourceValidateAlert{
					ResourcesErrors: []cloudprotocol.ResourceValidateError{
						{Name: "someName1", Errors: []string{"error1", "error2"}},
						{Name: "someName2", Errors: []string{"error3", "error4"}},
					},
				},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagResourceValidate,
				Payload: &pb.Alert_ResourceValidateAlert{
					ResourceValidateAlert: &pb.ResourceValidateAlert{
						Errors: []*pb.ResourceValidateErrors{
							{Name: "someName1", ErrorMsg: []string{"error1", "error2"}},
							{Name: "someName2", ErrorMsg: []string{"error3", "error4"}},
						},
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagDeviceAllocate,
				Payload: cloudprotocol.DeviceAllocateAlert{
					InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Device:        "someDevice", Message: "someMessage",
				},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagDeviceAllocate,
				Payload: &pb.Alert_DeviceAllocateAlert{
					DeviceAllocateAlert: &pb.DeviceAllocateAlert{
						Instance: &pb.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Device:   "someDevice", Message: "someMessage",
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagSystemQuota,
				Payload: cloudprotocol.SystemQuotaAlert{Parameter: "param1", Value: 42},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: &pb.Alert_SystemQuotaAlert{
					SystemQuotaAlert: &pb.SystemQuotaAlert{Parameter: "param1", Value: 42},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagInstanceQuota,
				Payload: cloudprotocol.InstanceQuotaAlert{
					InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Parameter:     "param1", Value: 42,
				},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagInstanceQuota,
				Payload: &pb.Alert_InstanceQuotaAlert{
					InstanceQuotaAlert: &pb.InstanceQuotaAlert{
						Instance:  &pb.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Parameter: "param1", Value: 42,
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagServiceInstance,
				Payload: cloudprotocol.ServiceInstanceAlert{
					InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Message:       "ServiceInstanceAlert", AosVersion: 42,
				},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagServiceInstance,
				Payload: &pb.Alert_InstanceAlert{
					InstanceAlert: &pb.InstanceAlert{
						Instance: &pb.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Message:  "ServiceInstanceAlert", AosVersion: 42,
					},
				},
			},
		},
	}

	for _, testAlert := range testData {
		currentTime := time.Now().UTC()
		testAlert.expectedAlert.Timestamp = currentTime
		testAlert.sendAlert.Timestamp = timestamppb.New(currentTime)

		sm.messageChannel <- &pb.SMNotifications{SMNotification: &pb.SMNotifications_Alert{Alert: testAlert.sendAlert}}

		if err := waitMessage(alertSender.messageChannel, testAlert.expectedAlert, messageTimeout); err != nil {
			t.Errorf("Incorrect alert notification: %v", err)
		}
	}
}

func TestSMMonitoringNotifications(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	var (
		config = config.Config{SMController: config.SMController{
			SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
		}}
		monitoringSender = newTestMonitoringSender()
	)

	controller, err := smcontroller.New(&config, nil, nil, monitoringSender, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	type testMonitoringElement struct {
		expectedMonitoring cloudprotocol.MonitoringData
		sendMonitoring     *pb.Monitoring
	}

	testMonitoringData := []testMonitoringElement{
		{
			expectedMonitoring: cloudprotocol.MonitoringData{
				Global:           cloudprotocol.GlobalMonitoringData{RAM: 10, CPU: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50},
				ServiceInstances: []cloudprotocol.InstanceMonitoringData{},
			},
			sendMonitoring: &pb.Monitoring{
				SystemMonitoring: &pb.SystemMonitoring{Ram: 10, Cpu: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50},
			},
		},
		{
			expectedMonitoring: cloudprotocol.MonitoringData{
				Global: cloudprotocol.GlobalMonitoringData{RAM: 10, CPU: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50},
				ServiceInstances: []cloudprotocol.InstanceMonitoringData{
					{
						InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "service1", SubjectID: "s1", Instance: 1},
						RAM:           10, CPU: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 0,
					},
					{
						InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "service2", SubjectID: "s1", Instance: 1},
						RAM:           0, CPU: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50,
					},
				},
			},
			sendMonitoring: &pb.Monitoring{
				SystemMonitoring: &pb.SystemMonitoring{Ram: 10, Cpu: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50},
				InstanceMonitoring: []*pb.InstanceMonitoring{
					{
						Instance: &pb.InstanceIdent{ServiceId: "service1", SubjectId: "s1", Instance: 1},
						Ram:      10, Cpu: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 0,
					},
					{
						Instance: &pb.InstanceIdent{ServiceId: "service2", SubjectId: "s1", Instance: 1},
						Ram:      0, Cpu: 20, UsedDisk: 30, InTraffic: 40, OutTraffic: 50,
					},
				},
			},
		},
	}

	for _, testMonitoring := range testMonitoringData {
		currentTime := time.Now().UTC()
		testMonitoring.expectedMonitoring.Timestamp = currentTime
		testMonitoring.sendMonitoring.Timestamp = timestamppb.New(currentTime)

		sm.messageChannel <- &pb.SMNotifications{
			SMNotification: &pb.SMNotifications_Monitoring{Monitoring: testMonitoring.sendMonitoring},
		}

		if err := waitMessage(
			monitoringSender.messageChannel, testMonitoring.expectedMonitoring, messageTimeout); err != nil {
			t.Errorf("Incorrect monitoring notification: %v", err)
		}
	}
}

func TestSMInstancesStatusNotifications(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	config := config.Config{SMController: config.SMController{
		SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
	}}

	controller, err := smcontroller.New(&config, nil, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	sendRuntimeStatus := &pb.SMNotifications_RunInstancesStatus{
		RunInstancesStatus: &pb.RunInstancesStatus{
			UnitSubjects: []string{"unitsubject"},
			Instances: []*pb.InstanceStatus{
				{
					Instance:   &pb.InstanceIdent{ServiceId: "serv1", SubjectId: "subj1", Instance: 1},
					AosVersion: 1, StateChecksum: "superCheckSum", RunState: "running",
				},
			},
			ErrorServices: []*pb.ServiceError{
				{ServiceId: "serv2", AosVersion: 2},
				{ServiceId: "serv3", AosVersion: 2, ErrorInfo: &pb.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"}},
			},
		},
	}

	expectedRuntimeStatus := unitstatushandler.RunInstancesStatus{
		UnitSubjects: []string{"unitsubject"},
		Instances: []cloudprotocol.InstanceStatus{
			{
				InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
				AosVersion:    1, StateChecksum: "superCheckSum", RunState: "running",
			},
		},
		ErrorServices: []cloudprotocol.ServiceStatus{
			{ID: "serv2", AosVersion: 2, Status: cloudprotocol.ErrorStatus},
			{
				ID: "serv3", AosVersion: 2, Status: cloudprotocol.ErrorStatus,
				ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
			},
		},
	}

	sm.messageChannel <- &pb.SMNotifications{SMNotification: sendRuntimeStatus}

	if err := waitRunInstancesStatus(
		controller.GetRunInstancesStatusChannel(), expectedRuntimeStatus, messageTimeout); err != nil {
		t.Errorf("Incorrect runtime status notification: %v", err)
	}

	sendUpdateStatus := &pb.SMNotifications_UpdateInstancesStatus{
		UpdateInstancesStatus: &pb.UpdateInstancesStatus{
			Instances: []*pb.InstanceStatus{
				{
					Instance:   &pb.InstanceIdent{ServiceId: "serv1", SubjectId: "subj1", Instance: 1},
					AosVersion: 1, StateChecksum: "superCheckSum", RunState: "running",
				},
				{
					Instance:   &pb.InstanceIdent{ServiceId: "serv2", SubjectId: "subj2", Instance: 1},
					AosVersion: 1, StateChecksum: "superCheckSum2", RunState: "fail",
					ErrorInfo: &pb.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
				},
			},
		},
	}

	expextedUpdateState := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
			AosVersion:    1, StateChecksum: "superCheckSum", RunState: "running",
		},
		{
			InstanceIdent: cloudprotocol.InstanceIdent{ServiceID: "serv2", SubjectID: "subj2", Instance: 1},
			AosVersion:    1, StateChecksum: "superCheckSum2", RunState: "fail",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
		},
	}

	sm.messageChannel <- &pb.SMNotifications{SMNotification: sendUpdateStatus}

	if err := waitUpdateInstancesStatus(
		controller.GetUpdateInstancesStatusChannel(), expextedUpdateState, messageTimeout); err != nil {
		t.Errorf("Incorrect update status notification: %v", err)
	}
}

func TestRunInstances(t *testing.T) {
	sm, err := newTestSM(smURL)
	if err != nil {
		t.Fatalf("Can't create test SM: %s", err)
	}

	defer sm.close()

	config := config.Config{SMController: config.SMController{
		SMList: []config.SMConfig{{SMID: "testSM", ServerURL: smURL}},
	}}

	controller, err := smcontroller.New(&config, nil, nil, nil, &testURLTranslator{}, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM constoller: %s", err)
	}
	defer controller.Close()

	instances := []cloudprotocol.InstanceInfo{
		{ServiceID: "serv1", SubjectID: "subj1", NumInstances: 10},
		{ServiceID: "serv2", SubjectID: "subj1", NumInstances: 1},
	}
	expectedRequest := &pb.RunInstancesRequest{Instances: []*pb.RunInstanceRequest{
		{ServiceId: "serv1", SubjectId: "subj1", NumInstances: 10},
		{ServiceId: "serv2", SubjectId: "subj1", NumInstances: 1},
	}}

	if err := controller.RunInstances(instances); err != nil {
		t.Fatalf("Can't run instances: %v", err)
	}

	if !proto.Equal(expectedRequest, sm.currentRunRequest) {
		t.Error("Incorrect run instances request")
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (translator *testURLTranslator) TranslateURL(isLocal bool, inURL string) (outURL string, err error) {
	outURL = inURL

	return outURL, nil
}

func newTestAlertSender() *testAlertSender {
	return &testAlertSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testAlertSender) SendAlert(alert cloudprotocol.AlertItem) {
	sender.messageChannel <- alert
}

func newTestMonitoringSender() *testMonitoringSender {
	return &testMonitoringSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testMonitoringSender) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) {
	sender.messageChannel <- monitoringData
}

func newTestMessageSender() *testMessageSender {
	return &testMessageSender{messageChannel: make(chan interface{}, 1)}
}

func (sender *testMessageSender) SendInstanceNewState(newState cloudprotocol.NewState) error {
	sender.messageChannel <- newState

	return nil
}

func (sender *testMessageSender) SendInstanceStateRequest(request cloudprotocol.StateRequest) error {
	sender.messageChannel <- request

	return nil
}

func (sender *testMessageSender) SendOverrideEnvVarsStatus(envStatus cloudprotocol.OverrideEnvVarsStatus) error {
	sender.messageChannel <- envStatus

	return nil
}

func (sender *testMessageSender) SendLog(serviceLog cloudprotocol.PushLog) error {
	sender.messageChannel <- serviceLog

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func waitMessage(messageChannel <-chan interface{}, expectedMsg interface{}, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-messageChannel:
		if !reflect.DeepEqual(message, expectedMsg) {
			return aoserrors.New("Incorrect received message")
		}
	}

	return nil
}

func waitRunInstancesStatus(
	messageChannel <-chan unitstatushandler.RunInstancesStatus, expectedMsg unitstatushandler.RunInstancesStatus,
	timeout time.Duration,
) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-messageChannel:
		if !reflect.DeepEqual(message, expectedMsg) {
			return aoserrors.New("Incorrect received message")
		}
	}

	return nil
}

func waitUpdateInstancesStatus(
	messageChannel <-chan []cloudprotocol.InstanceStatus, expectedMsg []cloudprotocol.InstanceStatus,
	timeout time.Duration,
) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-messageChannel:
		if !reflect.DeepEqual(message, expectedMsg) {
			return aoserrors.New("Incorrect received message")
		}
	}

	return nil
}

func newTestSM(url string) (sm *testSM, err error) {
	sm = &testSM{messageChannel: make(chan *pb.SMNotifications, 1)}

	sm.ctx, sm.cancelFunction = context.WithCancel(context.Background())

	sm.grpcServer = grpc.NewServer()

	pb.RegisterSMServiceServer(sm.grpcServer, sm)

	listener, err := net.Listen("tcp", url)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go func() {
		if err := sm.grpcServer.Serve(listener); err != nil {
			log.Errorf("Can't serve gRPC server: %s", err)
		}
	}()

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
	request *empty.Empty, stream pb.SMService_SubscribeSMNotificationsServer,
) (err error) {
	for {
		select {
		case message := <-sm.messageChannel:
			if err = stream.Send(message); err != nil {
				return aoserrors.Wrap(err)
			}

		case <-sm.ctx.Done():
			return nil
		}
	}
}

func (sm *testSM) GetBoardConfigStatus(context.Context, *empty.Empty) (*pb.BoardConfigStatus, error) {
	return &pb.BoardConfigStatus{VendorVersion: sm.setBoardConfig.VendorVersion}, nil
}

func (sm *testSM) SetBoardConfig(ctx context.Context, request *pb.BoardConfig) (response *empty.Empty, err error) {
	if err = json.Unmarshal([]byte(request.BoardConfig), &sm.setBoardConfig); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	sm.boardConfigWasSet = true

	return &empty.Empty{}, nil
}

func (sm *testSM) CheckBoardConfig(ctx context.Context, request *pb.BoardConfig) (*empty.Empty, error) {
	if err := json.Unmarshal([]byte(request.BoardConfig), &sm.checkBoardConfig); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return &empty.Empty{}, nil
}

func (sm *testSM) InstallService(ctx context.Context, service *pb.InstallServiceRequest) (*empty.Empty, error) {
	sm.currentServiceInstallRequest = service

	return &empty.Empty{}, nil
}

func (sm *testSM) RestoreService(ctx context.Context, req *pb.RestoreServiceRequest) (*empty.Empty, error) {
	sm.currentServiceRestoreRequest = req

	return &empty.Empty{}, nil
}

func (sm *testSM) RemoveService(ctx context.Context, req *pb.RemoveServiceRequest) (*empty.Empty, error) {
	sm.currentServiceRemoveRequest = req

	return &empty.Empty{}, nil
}

func (sm *testSM) GetServicesStatus(context.Context, *empty.Empty) (*pb.ServicesStatus, error) {
	return &sm.servicesStatus, nil
}

func (sm *testSM) InstallLayer(ctx context.Context, layer *pb.InstallLayerRequest) (*empty.Empty, error) {
	sm.currentLayerInstallRequest = layer

	return &empty.Empty{}, nil
}

func (sm *testSM) GetLayersStatus(context.Context, *empty.Empty) (*pb.LayersStatus, error) {
	return &sm.layersStatus, nil
}

func (sm *testSM) InstanceStateAcceptance(ctx context.Context, accept *pb.StateAcceptance) (*empty.Empty, error) {
	sm.currentStateAcceptance = accept

	return &empty.Empty{}, nil
}

func (sm *testSM) SetInstanceState(ctx context.Context, state *pb.InstanceState) (*empty.Empty, error) {
	sm.currentSetState = state

	return &empty.Empty{}, nil
}

func (sm *testSM) OverrideEnvVars(
	ctx context.Context, envVars *pb.OverrideEnvVarsRequest,
) (*pb.OverrideEnvVarStatus, error) {
	sm.currentEnvVarRequest = envVars

	return sm.envVarStatus, nil
}

func (sm *testSM) GetSystemLog(ctx context.Context, req *pb.SystemLogRequest) (*empty.Empty, error) {
	sm.currentSysLogRequest = req

	return &emptypb.Empty{}, nil
}

func (sm *testSM) GetInstanceLog(ctx context.Context, req *pb.InstanceLogRequest) (*empty.Empty, error) {
	sm.currentInstanceLogRequest = req

	return &emptypb.Empty{}, nil
}

func (sm *testSM) GetInstanceCrashLog(ctx context.Context, req *pb.InstanceLogRequest) (*empty.Empty, error) {
	sm.currentInstanceLogRequest = req

	return &emptypb.Empty{}, nil
}

func (sm *testSM) RunInstances(ctx context.Context, runRequest *pb.RunInstancesRequest) (*empty.Empty, error) {
	sm.currentRunRequest = runRequest

	return &emptypb.Empty{}, nil
}
