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
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v3"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/launcher"
	"github.com/aoscloud/aos_communicationmanager/smcontroller"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const cmServerURL = "localhost:8093"

const messageTimeout = 5 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testSMClient struct {
	connection              *grpc.ClientConn
	pbClient                pb.SMServiceClient
	stream                  pb.SMService_RegisterSMClient
	sendMessageChannel      chan *pb.SMOutgoingMessages
	receivedMessagesChannel chan *pb.SMIncomingMessages
	cancelFunction          context.CancelFunc
}

type testMessageSender struct {
	messageChannel chan interface{}
}

type testAlertSender struct {
	messageChannel chan cloudprotocol.AlertItem
}

type testMonitoringSender struct {
	messageChannel chan cloudprotocol.NodeMonitoringData
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

func TestSMInstancesStatusNotifications(t *testing.T) {
	var (
		nodeID   = "mainSM"
		nodeType = "mainSMType"
		config   = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		nodeConfig = &pb.NodeConfiguration{
			NodeId: nodeID, NodeType: nodeType, RemoteNode: true,
			RunnerFeatures: []string{"runc"}, NumCpus: 1, TotalRam: 100,
			Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		expectedNodeConfiguration = launcher.NodeInfo{
			NodeInfo: cloudprotocol.NodeInfo{
				NodeID: nodeID, NodeType: nodeType,
				SystemInfo: cloudprotocol.SystemInfo{
					NumCPUs: 1, TotalRAM: 100,
					Partitions: []cloudprotocol.PartitionInfo{
						{Name: "services", Types: []string{"t1"}, TotalSize: 50},
					},
				},
			},
			RemoteNode:    true,
			RunnerFeature: []string{"runc"},
		}
		sendRuntimeStatus = &pb.RunInstancesStatus{
			Instances: []*pb.InstanceStatus{
				{
					Instance:   &pb.InstanceIdent{ServiceId: "serv1", SubjectId: "subj1", Instance: 1},
					AosVersion: 1, RunState: "running",
				},
				{
					Instance:   &pb.InstanceIdent{ServiceId: "serv2", SubjectId: "subj2", Instance: 1},
					AosVersion: 1, RunState: "fail",
					ErrorInfo: &pb.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
				},
			},
		}
		expectedRuntimeStatus = launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: nodeType,
			Instances: []cloudprotocol.InstanceStatus{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
					AosVersion:    1, RunState: "running", NodeID: nodeID,
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj2", Instance: 1},
					AosVersion:    1, RunState: "fail", NodeID: nodeID,
					ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
				},
			},
		}

		sendUpdateStatus = &pb.SMOutgoingMessages{
			SMOutgoingMessage: &pb.SMOutgoingMessages_UpdateInstancesStatus{
				UpdateInstancesStatus: &pb.UpdateInstancesStatus{
					Instances: []*pb.InstanceStatus{
						{
							Instance:   &pb.InstanceIdent{ServiceId: "serv1", SubjectId: "subj1", Instance: 1},
							AosVersion: 1, RunState: "running",
						},
						{
							Instance:   &pb.InstanceIdent{ServiceId: "serv2", SubjectId: "subj2", Instance: 1},
							AosVersion: 1, RunState: "fail",
							ErrorInfo: &pb.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
						},
					},
				},
			},
		}
		expectedUpdateState = []cloudprotocol.InstanceStatus{
			{
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
				AosVersion:    1, RunState: "running", NodeID: nodeID,
			},
			{
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj2", Instance: 1},
				AosVersion:    1, RunState: "fail", NodeID: nodeID,
				ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
			},
		}
	)

	controller, err := smcontroller.New(&config, nil, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, sendRuntimeStatus)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(
		controller.GetRunInstancesStatusChannel(), expectedRuntimeStatus, messageTimeout); err != nil {
		t.Errorf("Incorrect runtime status notification: %v", err)
	}

	cfg, err := controller.GetNodeConfiguration(nodeID)
	if err != nil {
		t.Errorf("Can't get node configuration: %v", err)
	}

	if !reflect.DeepEqual(cfg, expectedNodeConfiguration) {
		t.Error("Incorrect node configuration")
	}

	smClient.sendMessageChannel <- sendUpdateStatus

	if err := waitMessage(
		controller.GetUpdateInstancesStatusChannel(), expectedUpdateState, messageTimeout); err != nil {
		t.Error("Incorrect instance update status")
	}
}

func TestUnitConfigMessages(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainType"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, NodeType: nodeType, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		testWaitChan    = make(chan struct{})
		originalVersion = "version_1"
		configStatus    = &pb.SMOutgoingMessages{SMOutgoingMessage: &pb.SMOutgoingMessages_UnitConfigStatus{
			UnitConfigStatus: &pb.UnitConfigStatus{VendorVersion: originalVersion},
		}}
		newVersion = "version_2"
		unitConfig = fmt.Sprintf(`{"nodeType":"%s"}`, nodeType)
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, &pb.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, NodeType: nodeType, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	go func() {
		version, err := controller.GetUnitConfigStatus(nodeID)
		if err != nil {
			t.Errorf("Can't get unit config status: %v", err)
		}

		if version != originalVersion {
			t.Error("Incorrect unit config version")
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetUnitConfigStatus{}},
		messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.stream.Send(configStatus); err != nil {
		t.Errorf("Can't send unit config status: %v", err)
	}

	<-testWaitChan

	go func() {
		if err := controller.CheckUnitConfig(aostypes.UnitConfig{
			VendorVersion: newVersion,
			Nodes: []aostypes.NodeUnitConfig{
				{
					NodeType: nodeType,
				},
			},
		}); err != nil {
			t.Errorf("Error check unit config: %v", err)
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_CheckUnitConfig{
		CheckUnitConfig: &pb.CheckUnitConfig{UnitConfig: unitConfig, VendorVersion: newVersion},
	}}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	configStatus.GetUnitConfigStatus().VendorVersion = newVersion

	if err := smClient.stream.Send(configStatus); err != nil {
		t.Errorf("Can't send unit config status")
	}

	<-testWaitChan

	go func() {
		if err := controller.SetUnitConfig(aostypes.UnitConfig{
			VendorVersion: newVersion,
			Nodes: []aostypes.NodeUnitConfig{
				{
					NodeType: nodeType,
				},
			},
		}); err != nil {
			t.Errorf("Error check unit config: %v", err)
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SetUnitConfig{
		SetUnitConfig: &pb.SetUnitConfig{UnitConfig: unitConfig, VendorVersion: newVersion},
	}}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	configStatus.GetUnitConfigStatus().VendorVersion = newVersion

	if err := smClient.stream.Send(configStatus); err != nil {
		t.Errorf("Can't send unit config status")
	}

	<-testWaitChan
}

func TestSMAlertNotifications(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		alertSender = newTestAlertSender()
	)

	controller, err := smcontroller.New(&config, messageSender, alertSender, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	// Test alert notifications
	type testAlert struct {
		sendAlert     *pb.Alert
		expectedAlert cloudprotocol.AlertItem
	}

	testData := []testAlert{
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagSystemError,
				Payload: cloudprotocol.SystemAlert{Message: "SystemAlertMessage", NodeID: nodeID},
			},
			sendAlert: &pb.Alert{
				Tag:     cloudprotocol.AlertTagSystemError,
				Payload: &pb.Alert_SystemAlert{SystemAlert: &pb.SystemAlert{Message: "SystemAlertMessage"}},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagAosCore,
				Payload: cloudprotocol.CoreAlert{CoreComponent: "SM", Message: "CoreAlertMessage", NodeID: nodeID},
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
					NodeID: nodeID,
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
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Device:        "someDevice", Message: "someMessage",
					NodeID: nodeID,
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
				Payload: cloudprotocol.SystemQuotaAlert{Parameter: "cpu", Value: 42, NodeID: nodeID},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: &pb.Alert_SystemQuotaAlert{
					SystemQuotaAlert: &pb.SystemQuotaAlert{Parameter: "cpu", Value: 42},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagSystemQuota,
				Payload: cloudprotocol.SystemQuotaAlert{Parameter: "ram", Value: 99, NodeID: nodeID},
			},
			sendAlert: &pb.Alert{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: &pb.Alert_SystemQuotaAlert{
					SystemQuotaAlert: &pb.SystemQuotaAlert{Parameter: "ram", Value: 99},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagInstanceQuota,
				Payload: cloudprotocol.InstanceQuotaAlert{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
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
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
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

		smClient.sendMessageChannel <- &pb.SMOutgoingMessages{
			SMOutgoingMessage: &pb.SMOutgoingMessages_Alert{Alert: testAlert.sendAlert},
		}

		if err := waitMessage(alertSender.messageChannel, testAlert.expectedAlert, messageTimeout); err != nil {
			t.Errorf("Incorrect alert notification: %v", err)
		}
	}

	expectedSystemLimitAlert := []cloudprotocol.SystemQuotaAlert{
		{Parameter: "cpu", Value: 42, NodeID: nodeID},
		{Parameter: "ram", Value: 99, NodeID: nodeID},
	}

	for _, limitAlert := range expectedSystemLimitAlert {
		if err := waitMessage(controller.GetSystemLimitAlertChannel(), limitAlert, messageTimeout); err != nil {
			t.Errorf("Incorrect system limit alert: %v", err)
		}
	}
}

func TestSMMonitoringNotifications(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		monitoringSender = newTestMonitoringSender()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, monitoringSender, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	type testMonitoringElement struct {
		expectedMonitoring cloudprotocol.NodeMonitoringData
		sendMonitoring     *pb.NodeMonitoring
	}

	testMonitoringData := []testMonitoringElement{
		{
			expectedMonitoring: cloudprotocol.NodeMonitoringData{
				NodeID: nodeID,
				MonitoringData: cloudprotocol.MonitoringData{
					RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
					Disk: []cloudprotocol.PartitionUsage{{Name: "p1", UsedSize: 100}},
				},
				ServiceInstances: []cloudprotocol.InstanceMonitoringData{},
			},
			sendMonitoring: &pb.NodeMonitoring{
				MonitoringData: &pb.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk: []*pb.PartitionUsage{{Name: "p1", UsedSize: 100}},
				},
			},
		},
		{
			expectedMonitoring: cloudprotocol.NodeMonitoringData{
				NodeID: nodeID,
				MonitoringData: cloudprotocol.MonitoringData{
					RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
					Disk: []cloudprotocol.PartitionUsage{{Name: "p1", UsedSize: 100}},
				},
				ServiceInstances: []cloudprotocol.InstanceMonitoringData{
					{
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "s1", Instance: 1},
						MonitoringData: cloudprotocol.MonitoringData{
							RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 0,
							Disk: []cloudprotocol.PartitionUsage{{Name: "p1", UsedSize: 100}},
						},
					},
					{
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "s1", Instance: 1},
						MonitoringData: cloudprotocol.MonitoringData{
							RAM: 20, CPU: 30, InTraffic: 50, OutTraffic: 10,
							Disk: []cloudprotocol.PartitionUsage{{Name: "p2", UsedSize: 50}},
						},
					},
				},
			},
			sendMonitoring: &pb.NodeMonitoring{
				MonitoringData: &pb.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk: []*pb.PartitionUsage{{Name: "p1", UsedSize: 100}},
				},
				InstanceMonitoring: []*pb.InstanceMonitoring{
					{
						Instance: &pb.InstanceIdent{ServiceId: "service1", SubjectId: "s1", Instance: 1},
						MonitoringData: &pb.MonitoringData{
							Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 0,
							Disk: []*pb.PartitionUsage{{Name: "p1", UsedSize: 100}},
						},
					},
					{
						Instance: &pb.InstanceIdent{ServiceId: "service2", SubjectId: "s1", Instance: 1},
						MonitoringData: &pb.MonitoringData{
							Ram: 20, Cpu: 30, InTraffic: 50, OutTraffic: 10,
							Disk: []*pb.PartitionUsage{{Name: "p2", UsedSize: 50}},
						},
					},
				},
			},
		},
	}

	for _, testMonitoring := range testMonitoringData {
		currentTime := time.Now().UTC()
		testMonitoring.expectedMonitoring.Timestamp = currentTime
		testMonitoring.sendMonitoring.Timestamp = timestamppb.New(currentTime)

		smClient.sendMessageChannel <- &pb.SMOutgoingMessages{SMOutgoingMessage: &pb.SMOutgoingMessages_NodeMonitoring{
			NodeMonitoring: testMonitoring.sendMonitoring,
		}}

		if err := waitMessage(
			monitoringSender.messageChannel, testMonitoring.expectedMonitoring, messageTimeout); err != nil {
			t.Errorf("Incorrect monitoring notification: %v", err)
		}
	}
}

func TestLogMessages(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		currentTime = time.Now().UTC()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, &pb.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	type testLogRequest struct {
		sendLogRequest     cloudprotocol.RequestLog
		expectedLogRequest *pb.SMIncomingMessages
	}

	testRequests := []testLogRequest{
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogType: cloudprotocol.SystemLog,
				LogID:   "sysLogID1",
				Filter: cloudprotocol.LogFilter{
					From: &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SystemLogRequest{
				SystemLogRequest: &pb.SystemLogRequest{LogId: "sysLogID1", From: timestamppb.New(currentTime)},
			}},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogType: cloudprotocol.SystemLog,
				LogID:   "sysLogID2",
				Filter: cloudprotocol.LogFilter{
					Till: &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SystemLogRequest{
				SystemLogRequest: &pb.SystemLogRequest{LogId: "sysLogID2", Till: timestamppb.New(currentTime)},
			}},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID1",
				LogType: cloudprotocol.ServiceLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("ser1", "s1", -1),
					From:           &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceLogRequest{
				InstanceLogRequest: &pb.InstanceLogRequest{
					Instance: &pb.InstanceIdent{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
					LogId:    "serviceLogID1", From: timestamppb.New(currentTime),
				},
			}},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID1",
				LogType: cloudprotocol.ServiceLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("ser2", "", -1),
					Till:           &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceLogRequest{
				InstanceLogRequest: &pb.InstanceLogRequest{
					Instance: &pb.InstanceIdent{ServiceId: "ser2", SubjectId: "", Instance: -1},
					LogId:    "serviceLogID1", Till: timestamppb.New(currentTime),
				},
			}},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID1",
				LogType: cloudprotocol.CrashLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("ser1", "s1", -1),
					From:           &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceCrashLogRequest{
				InstanceCrashLogRequest: &pb.InstanceCrashLogRequest{
					Instance: &pb.InstanceIdent{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
					LogId:    "serviceLogID1", From: timestamppb.New(currentTime),
				},
			}},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID1",
				LogType: cloudprotocol.CrashLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("ser2", "", -1),
					Till:           &currentTime,
				},
			},
			expectedLogRequest: &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceCrashLogRequest{
				InstanceCrashLogRequest: &pb.InstanceCrashLogRequest{
					Instance: &pb.InstanceIdent{ServiceId: "ser2", SubjectId: "", Instance: -1},
					LogId:    "serviceLogID1", Till: timestamppb.New(currentTime),
				},
			}},
		},
	}

	for _, request := range testRequests {
		if err := controller.GetLog(request.sendLogRequest); err != nil {
			t.Fatalf("Can't send get system log request: %v", err)
		}

		if err := smClient.waitMessage(request.expectedLogRequest, messageTimeout); err != nil {
			t.Fatalf("Wait message error: %v", err)
		}
	}

	expectedLog := cloudprotocol.PushLog{
		NodeID: "mainSM", LogID: "log0", PartsCount: 2, Part: 1, Content: []byte("this is log"),
		ErrorInfo: &cloudprotocol.ErrorInfo{
			Message: "this is error",
		},
	}

	smClient.sendMessageChannel <- &pb.SMOutgoingMessages{
		SMOutgoingMessage: &pb.SMOutgoingMessages_Log{
			Log: &pb.LogData{LogId: "log0", PartCount: 2, Part: 1, Data: []byte("this is log"), Error: "this is error"},
		},
	}

	if err := waitMessage(messageSender.messageChannel, expectedLog, messageTimeout); err != nil {
		t.Errorf("Incorrect log message: %v", err)
	}
}

func TestOverrideEnvVars(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		currentTime = time.Now().UTC()
		envVars     = cloudprotocol.OverrideEnvVars{
			OverrideEnvVars: []cloudprotocol.EnvVarsInstanceInfo{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					EnvVars: []cloudprotocol.EnvVarInfo{
						{ID: "id0", Variable: "var0", TTL: &currentTime},
					},
				},
			},
		}
		expectedPbEnvVarRequest = &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_OverrideEnvVars{
			OverrideEnvVars: &pb.OverrideEnvVars{
				EnvVars: []*pb.OverrideInstanceEnvVar{{Instance: &pb.InstanceIdent{
					ServiceId: "service0",
					SubjectId: "subject0", Instance: -1,
				}, Vars: []*pb.EnvVarInfo{{VarId: "id0", Variable: "var0", Ttl: timestamppb.New(currentTime)}}}},
			},
		}}
		pbEnvVarStatus = &pb.SMOutgoingMessages{SMOutgoingMessage: &pb.SMOutgoingMessages_OverrideEnvVarStatus{
			OverrideEnvVarStatus: &pb.OverrideEnvVarStatus{EnvVarsStatus: []*pb.EnvVarInstanceStatus{
				{Instance: &pb.InstanceIdent{
					ServiceId: "service0",
					SubjectId: "subject0", Instance: -1,
				}, VarsStatus: []*pb.EnvVarStatus{{VarId: "id0", Error: "someError"}}},
			}},
		}}
		expectedEnvVarStatus = cloudprotocol.OverrideEnvVarsStatus{
			OverrideEnvVarsStatus: []cloudprotocol.EnvVarsInstanceStatus{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					Statuses:       []cloudprotocol.EnvVarStatus{{ID: "id0", Error: "someError"}},
				},
			},
		}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, &pb.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err = controller.OverrideEnvVars(nodeID, envVars); err != nil {
		t.Fatalf("Error sending override env vars: %v", err)
	}

	if err := smClient.waitMessage(expectedPbEnvVarRequest, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	smClient.sendMessageChannel <- pbEnvVarStatus

	if err := waitMessage(messageSender.messageChannel, expectedEnvVarStatus, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}
}

func TestRunInstances(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		expectedRunInstances = &pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_RunInstances{
			RunInstances: &pb.RunInstances{
				Services: []*pb.ServiceInfo{{
					VersionInfo: &pb.VesionInfo{AosVersion: 1, VendorVersion: "1", Description: "desc"},
					Url:         "url1", ServiceId: "s1", ProviderId: "p1", Gid: 600,
					Sha256: []byte{0, 0, 0, byte(100)}, Sha512: []byte{byte(200), 0, 0, 0}, Size: uint64(500),
				}},
				Layers: []*pb.LayerInfo{
					{
						VersionInfo: &pb.VesionInfo{AosVersion: 2, VendorVersion: "3", Description: "desc2"},
						Url:         "url2", LayerId: "l1", Digest: "digest1", Sha256: []byte{0, 0, 0, byte(100)},
						Sha512: []byte{byte(200), 0, 0, 0}, Size: uint64(500),
					},
				},
				Instances: []*pb.InstanceInfo{
					{
						Instance: &pb.InstanceIdent{ServiceId: "s1", SubjectId: "subj1", Instance: 1},
						Uid:      500, Priority: 1, StoragePath: "storage1", StatePath: "state1",
					},
				},
			},
		}}
		sendServices = []aostypes.ServiceInfo{{
			VersionInfo: aostypes.VersionInfo{AosVersion: 1, VendorVersion: "1", Description: "desc"},
			ID:          "s1", ProviderID: "p1", URL: "url1", GID: 600,
			Sha256: []byte{0, 0, 0, byte(100)}, Sha512: []byte{byte(200), 0, 0, 0}, Size: uint64(500),
		}}
		sendLayers = []aostypes.LayerInfo{{
			VersionInfo: aostypes.VersionInfo{AosVersion: 2, VendorVersion: "3", Description: "desc2"},
			URL:         "url2", ID: "l1", Digest: "digest1", Sha256: []byte{0, 0, 0, byte(100)},
			Sha512: []byte{byte(200), 0, 0, 0}, Size: uint64(500),
		}}
		sendInstances = []aostypes.InstanceInfo{{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "s1", SubjectID: "subj1", Instance: 1},
			UID:           500, Priority: 1, StoragePath: "storage1", StatePath: "state1",
		}}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, &pb.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := controller.RunInstances(nodeID, sendServices, sendLayers, sendInstances, false); err != nil {
		t.Fatalf("Can't send run instances: %v", err)
	}

	if err := smClient.waitMessage(expectedRunInstances, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}
}

func TestGetNodeMonitoringData(t *testing.T) {
	var (
		nodeID        = "mainSM"
		messageSender = newTestMessageSender()
		nodeConfig    = &pb.NodeConfiguration{
			NodeId: nodeID, RemoteNode: true, RunnerFeatures: []string{"runc"}, NumCpus: 1,
			TotalRam: 100, Partitions: []*pb.Partition{{Name: "services", Types: []string{"t1"}, TotalSize: 50}},
		}
		config = config.Config{
			SMController: config.SMController{
				CMServerURL: cmServerURL,
				NodeIDs:     []string{nodeID},
			},
		}
		testWaitChan       = make(chan struct{})
		currentTime        = time.Now().UTC()
		expectedMonitoring = cloudprotocol.NodeMonitoringData{
			NodeID:    nodeID,
			Timestamp: currentTime,
			MonitoringData: cloudprotocol.MonitoringData{
				RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
				Disk: []cloudprotocol.PartitionUsage{{Name: "p1", UsedSize: 100}},
			},
			ServiceInstances: []cloudprotocol.InstanceMonitoringData{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "s1", Instance: 1},
					MonitoringData: cloudprotocol.MonitoringData{
						RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 0,
						Disk: []cloudprotocol.PartitionUsage{{Name: "p1", UsedSize: 100}},
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "s1", Instance: 1},
					MonitoringData: cloudprotocol.MonitoringData{
						RAM: 20, CPU: 30, InTraffic: 50, OutTraffic: 10,
						Disk: []cloudprotocol.PartitionUsage{{Name: "p2", UsedSize: 50}},
					},
				},
			},
		}
		sendMonitoring = &pb.SMOutgoingMessages{SMOutgoingMessage: &pb.SMOutgoingMessages_NodeMonitoring{
			NodeMonitoring: &pb.NodeMonitoring{
				Timestamp: timestamppb.New(currentTime),
				MonitoringData: &pb.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk: []*pb.PartitionUsage{{Name: "p1", UsedSize: 100}},
				},
				InstanceMonitoring: []*pb.InstanceMonitoring{
					{
						Instance: &pb.InstanceIdent{ServiceId: "service1", SubjectId: "s1", Instance: 1},
						MonitoringData: &pb.MonitoringData{
							Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 0,
							Disk: []*pb.PartitionUsage{{Name: "p1", UsedSize: 100}},
						},
					},
					{
						Instance: &pb.InstanceIdent{ServiceId: "service2", SubjectId: "s1", Instance: 1},
						MonitoringData: &pb.MonitoringData{
							Ram: 20, Cpu: 30, InTraffic: 50, OutTraffic: 10,
							Disk: []*pb.PartitionUsage{{Name: "p2", UsedSize: 50}},
						},
					},
				},
			},
		}}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, nodeConfig, &pb.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	go func() {
		data, err := controller.GetNodeMonitoringData(nodeID)
		if err != nil {
			t.Errorf("Can't get node monitoring data: %v", err)
		}

		if !reflect.DeepEqual(data, expectedMonitoring) {
			log.Debug("Exp: ", expectedMonitoring)
			log.Debug("Rec: ", data)
			t.Errorf("Incorrect monitoring data")
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pb.SMIncomingMessages{
		SMIncomingMessage: &pb.SMIncomingMessages_GetNodeMonitoring{},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.stream.Send(sendMonitoring); err != nil {
		t.Errorf("Can't send unit config status")
	}

	<-testWaitChan
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newTestAlertSender() *testAlertSender {
	return &testAlertSender{messageChannel: make(chan cloudprotocol.AlertItem, 1)}
}

func (sender *testAlertSender) SendAlert(alert cloudprotocol.AlertItem) {
	sender.messageChannel <- alert
}

func newTestMonitoringSender() *testMonitoringSender {
	return &testMonitoringSender{messageChannel: make(chan cloudprotocol.NodeMonitoringData, 1)}
}

func (sender *testMonitoringSender) SendMonitoringData(monitoringData cloudprotocol.NodeMonitoringData) {
	sender.messageChannel <- monitoringData
}

func newTestMessageSender() *testMessageSender {
	return &testMessageSender{messageChannel: make(chan interface{}, 1)}
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

func waitMessage[T any](messageChannel <-chan T, expectedMsg interface{}, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-messageChannel:
		if !reflect.DeepEqual(message, expectedMsg) {
			return aoserrors.New("incorrect received message")
		}
	}

	return nil
}

func newTestSMClient(
	url string, config *pb.NodeConfiguration, runStatus *pb.RunInstancesStatus,
) (client *testSMClient, err error) {
	client = &testSMClient{
		sendMessageChannel:      make(chan *pb.SMOutgoingMessages, 10),
		receivedMessagesChannel: make(chan *pb.SMIncomingMessages, 10),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if client.connection, err = grpc.DialContext(
		ctx, url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	client.pbClient = pb.NewSMServiceClient(client.connection)

	if client.stream, err = pb.NewSMServiceClient(client.connection).RegisterSM(context.Background()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err := client.stream.Send(
		&pb.SMOutgoingMessages{
			SMOutgoingMessage: &pb.SMOutgoingMessages_NodeConfiguration{NodeConfiguration: config},
		}); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if runStatus != nil {
		if err := client.stream.Send(
			&pb.SMOutgoingMessages{
				SMOutgoingMessage: &pb.SMOutgoingMessages_RunInstancesStatus{RunInstancesStatus: runStatus},
			}); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	ctxMessages, cancelFunction := context.WithCancel(context.Background())

	client.cancelFunction = cancelFunction

	go client.processReceiveMessages()
	go client.processSendMessages(ctxMessages)

	return client, nil
}

func (client *testSMClient) close() {
	if client.connection != nil {
		client.connection.Close()
	}

	client.cancelFunction()
}

func (client *testSMClient) processReceiveMessages() {
	for {
		data, err := client.stream.Recv()
		if err != nil {
			if code, ok := status.FromError(err); ok {
				if code.Code() == codes.Canceled {
					log.Debug("SM client connection closed")
				}
			}

			return
		}

		client.receivedMessagesChannel <- data
	}
}

func (client *testSMClient) processSendMessages(ctx context.Context) {
	for {
		select {
		case message := <-client.sendMessageChannel:
			if err := client.stream.Send(message); err != nil {
				log.Error("Can't send msg")
			}

		case <-ctx.Done():
			return
		}
	}
}

func (client *testSMClient) waitMessage(expectedMsg *pb.SMIncomingMessages, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-client.receivedMessagesChannel:
		if !proto.Equal(message, expectedMsg) {
			return aoserrors.New("incorrect received client message")
		}
	}

	return nil
}
