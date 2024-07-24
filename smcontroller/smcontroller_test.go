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

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/resourcemonitor"
	"github.com/aosedge/aos_common/utils/pbconvert"

	pbcommon "github.com/aosedge/aos_common/api/common"
	pbsm "github.com/aosedge/aos_common/api/servicemanager"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/smcontroller"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
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
	pbClient                pbsm.SMServiceClient
	stream                  pbsm.SMService_RegisterSMClient
	sendMessageChannel      chan *pbsm.SMOutgoingMessages
	receivedMessagesChannel chan *pbsm.SMIncomingMessages
	cancelFunction          context.CancelFunc
}

type testMessageSender struct {
	messageChannel chan interface{}
}

type testAlertSender struct {
	messageChannel chan cloudprotocol.AlertItem
}

type testMonitoringSender struct {
	messageChannel chan aostypes.NodeMonitoring
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
		nodeID            = "mainSM"
		nodeType          = "mainSMType"
		config            = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		sendRuntimeStatus = &pbsm.RunInstancesStatus{
			Instances: []*pbsm.InstanceStatus{
				{
					Instance:       &pbcommon.InstanceIdent{ServiceId: "serv1", SubjectId: "subj1", Instance: 1},
					ServiceVersion: "1.0.0", Status: "running",
				},
				{
					Instance:       &pbcommon.InstanceIdent{ServiceId: "serv2", SubjectId: "subj2", Instance: 1},
					ServiceVersion: "1.0.0", Status: "fail",
					ErrorInfo: &pbcommon.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
				},
			},
		}
		expectedRuntimeStatus = launcher.NodeRunInstanceStatus{
			NodeID: nodeID, NodeType: nodeType,
			Instances: []cloudprotocol.InstanceStatus{
				{
					InstanceIdent:  aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
					ServiceVersion: "1.0.0", Status: "running", NodeID: nodeID,
				},
				{
					InstanceIdent:  aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj2", Instance: 1},
					ServiceVersion: "1.0.0", Status: "fail", NodeID: nodeID,
					ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
				},
			},
		}

		sendUpdateStatus = &pbsm.SMOutgoingMessages{
			SMOutgoingMessage: &pbsm.SMOutgoingMessages_UpdateInstancesStatus{
				UpdateInstancesStatus: &pbsm.UpdateInstancesStatus{
					Instances: []*pbsm.InstanceStatus{
						{
							Instance: &pbcommon.InstanceIdent{
								ServiceId: "serv1", SubjectId: "subj1", Instance: 1,
							},
							ServiceVersion: "1.0.0", Status: "running",
						},
						{
							Instance: &pbcommon.InstanceIdent{
								ServiceId: "serv2", SubjectId: "subj2", Instance: 1,
							},
							ServiceVersion: "1.0.0", Status: "fail",
							ErrorInfo: &pbcommon.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
						},
					},
				},
			},
		}
		expectedUpdateState = []cloudprotocol.InstanceStatus{
			{
				InstanceIdent:  aostypes.InstanceIdent{ServiceID: "serv1", SubjectID: "subj1", Instance: 1},
				ServiceVersion: "1.0.0", Status: "running", NodeID: nodeID,
			},
			{
				InstanceIdent:  aostypes.InstanceIdent{ServiceID: "serv2", SubjectID: "subj2", Instance: 1},
				ServiceVersion: "1.0.0", Status: "fail", NodeID: nodeID,
				ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 200, ExitCode: 300, Message: "superError"},
			},
		}
	)

	controller, err := smcontroller.New(&config, nil, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, sendRuntimeStatus)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(
		controller.GetRunInstancesStatusChannel(), expectedRuntimeStatus, messageTimeout); err != nil {
		t.Errorf("Incorrect runtime status notification: %v", err)
	}

	smClient.sendMessageChannel <- sendUpdateStatus

	if err := waitMessage(
		controller.GetUpdateInstancesStatusChannel(), expectedUpdateState, messageTimeout); err != nil {
		t.Error("Incorrect instance update status")
	}
}

func TestNodeConfigMessages(t *testing.T) {
	var (
		nodeID           = "mainSM"
		nodeType         = "mainType"
		messageSender    = newTestMessageSender()
		config           = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		testWaitChan     = make(chan struct{})
		originalVersion  = "1.0.0"
		nodeConfigStatus = &pbsm.SMOutgoingMessages{SMOutgoingMessage: &pbsm.SMOutgoingMessages_NodeConfigStatus{
			NodeConfigStatus: &pbsm.NodeConfigStatus{Version: originalVersion},
		}}
		newVersion = "2.0.0"
		nodeConfig = fmt.Sprintf(`{"nodeId":"%s","nodeType":"%s"}`, nodeID, nodeType)
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType, Version: originalVersion,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(
		controller.NodeConfigStatusChannel(), unitconfig.NodeConfigStatus{
			NodeID: nodeID, NodeType: nodeType, Version: originalVersion,
		}, messageTimeout); err != nil {
		t.Errorf("Incorrect runtime status notification: %v", err)
	}

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{ConnectionStatus: &pbsm.ConnectionStatus{}},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	go func() {
		nodeConfigStatus, err := controller.GetNodeConfigStatus(nodeID)
		if err != nil {
			t.Errorf("Can't get node config status: %v", err)
		}

		if nodeConfigStatus.Version != originalVersion {
			t.Errorf("Incorrect node config version: %s", nodeConfigStatus.Version)
		}

		testWaitChan <- struct{}{}
	}()

	<-testWaitChan

	go func() {
		if err := controller.CheckNodeConfig(newVersion,
			cloudprotocol.NodeConfig{NodeID: &nodeID, NodeType: nodeType}); err != nil {
			t.Errorf("Error check unit config: %v", err)
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_CheckNodeConfig{
			CheckNodeConfig: &pbsm.CheckNodeConfig{NodeConfig: nodeConfig, Version: newVersion},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	nodeConfigStatus.GetNodeConfigStatus().Version = newVersion

	if err := smClient.stream.Send(nodeConfigStatus); err != nil {
		t.Errorf("Can't send unit config status")
	}

	<-testWaitChan

	go func() {
		if err := controller.SetNodeConfig(newVersion,
			cloudprotocol.NodeConfig{NodeID: &nodeID, NodeType: nodeType}); err != nil {
			t.Errorf("Error check unit config: %v", err)
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_SetNodeConfig{
			SetNodeConfig: &pbsm.SetNodeConfig{NodeConfig: nodeConfig, Version: newVersion},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.stream.Send(nodeConfigStatus); err != nil {
		t.Errorf("Can't send node config status: %v", err)
	}

	if err := smClient.waitMessage(
		&pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_GetNodeConfigStatus{}},
		messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.stream.Send(nodeConfigStatus); err != nil {
		t.Errorf("Can't send node config status: %v", err)
	}

	nodeConfigStatus.GetNodeConfigStatus().Version = newVersion

	<-testWaitChan
}

func TestSMAlertNotifications(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		alertSender   = newTestAlertSender()
	)

	controller, err := smcontroller.New(&config, messageSender, alertSender, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	// Test alert notifications
	type testAlert struct {
		sendAlert     *pbsm.Alert
		expectedAlert cloudprotocol.AlertItem
	}

	testData := []testAlert{
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagSystemError,
				Payload: cloudprotocol.SystemAlert{Message: "SystemAlertMessage", NodeID: nodeID},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagSystemError,
				Payload: &pbsm.Alert_SystemAlert{
					SystemAlert: &pbsm.SystemAlert{Message: "SystemAlertMessage"},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag:     cloudprotocol.AlertTagAosCore,
				Payload: cloudprotocol.CoreAlert{CoreComponent: "SM", Message: "CoreAlertMessage", NodeID: nodeID},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagAosCore,
				Payload: &pbsm.Alert_CoreAlert{
					CoreAlert: &pbsm.CoreAlert{CoreComponent: "SM", Message: "CoreAlertMessage"},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagResourceValidate,
				Payload: cloudprotocol.ResourceValidateAlert{
					NodeID: nodeID,
					Name:   "someName",
					Errors: []cloudprotocol.ErrorInfo{
						{AosCode: 200, Message: "error1"},
						{AosCode: 300, Message: "error2"},
					},
				},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagResourceValidate,
				Payload: &pbsm.Alert_ResourceValidateAlert{
					ResourceValidateAlert: &pbsm.ResourceValidateAlert{
						Name: "someName",
						Errors: []*pbcommon.ErrorInfo{
							{AosCode: 200, Message: "error1"},
							{AosCode: 300, Message: "error2"},
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
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagDeviceAllocate,
				Payload: &pbsm.Alert_DeviceAllocateAlert{
					DeviceAllocateAlert: &pbsm.DeviceAllocateAlert{
						Instance: &pbcommon.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Device:   "someDevice", Message: "someMessage",
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: cloudprotocol.SystemQuotaAlert{
					Parameter: "cpu", Value: 42, NodeID: nodeID, Status: resourcemonitor.AlertStatusRaise,
				},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: &pbsm.Alert_SystemQuotaAlert{
					SystemQuotaAlert: &pbsm.SystemQuotaAlert{
						Parameter: "cpu", Value: 42, Status: resourcemonitor.AlertStatusRaise,
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: cloudprotocol.SystemQuotaAlert{
					Parameter: "ram", Value: 99, NodeID: nodeID, Status: resourcemonitor.AlertStatusRaise,
				},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagSystemQuota,
				Payload: &pbsm.Alert_SystemQuotaAlert{
					SystemQuotaAlert: &pbsm.SystemQuotaAlert{
						Parameter: "ram", Value: 99, Status: resourcemonitor.AlertStatusRaise,
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagInstanceQuota,
				Payload: cloudprotocol.InstanceQuotaAlert{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Parameter:     "param1", Value: 42, Status: resourcemonitor.AlertStatusRaise,
				},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagInstanceQuota,
				Payload: &pbsm.Alert_InstanceQuotaAlert{
					InstanceQuotaAlert: &pbsm.InstanceQuotaAlert{
						Instance:  &pbcommon.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Parameter: "param1", Value: 42, Status: resourcemonitor.AlertStatusRaise,
					},
				},
			},
		},
		{
			expectedAlert: cloudprotocol.AlertItem{
				Tag: cloudprotocol.AlertTagServiceInstance,
				Payload: cloudprotocol.ServiceInstanceAlert{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "id1", SubjectID: "s1", Instance: 1},
					Message:       "ServiceInstanceAlert", ServiceVersion: "42.0.0",
				},
			},
			sendAlert: &pbsm.Alert{
				Tag: cloudprotocol.AlertTagServiceInstance,
				Payload: &pbsm.Alert_InstanceAlert{
					InstanceAlert: &pbsm.InstanceAlert{
						Instance: &pbcommon.InstanceIdent{ServiceId: "id1", SubjectId: "s1", Instance: 1},
						Message:  "ServiceInstanceAlert", ServiceVersion: "42.0.0",
					},
				},
			},
		},
	}

	for _, testAlert := range testData {
		currentTime := time.Now().UTC()
		testAlert.expectedAlert.Timestamp = currentTime
		testAlert.sendAlert.Timestamp = timestamppb.New(currentTime)

		smClient.sendMessageChannel <- &pbsm.SMOutgoingMessages{
			SMOutgoingMessage: &pbsm.SMOutgoingMessages_Alert{Alert: testAlert.sendAlert},
		}

		if err := waitMessage(alertSender.messageChannel, testAlert.expectedAlert, messageTimeout); err != nil {
			t.Errorf("Incorrect alert notification: %v", err)
		}
	}

	expectedSystemLimitAlert := []cloudprotocol.SystemQuotaAlert{
		{Parameter: "cpu", Value: 42, NodeID: nodeID, Status: resourcemonitor.AlertStatusRaise},
		{Parameter: "ram", Value: 99, NodeID: nodeID, Status: resourcemonitor.AlertStatusRaise},
	}

	for _, limitAlert := range expectedSystemLimitAlert {
		if err := waitMessage(controller.GetSystemQuoteAlertChannel(), limitAlert, messageTimeout); err != nil {
			t.Errorf("Incorrect system limit alert: %v", err)
		}
	}
}

func TestSMMonitoringNotifications(t *testing.T) {
	var (
		nodeID           = "mainSM"
		nodeType         = "mainSMType"
		messageSender    = newTestMessageSender()
		config           = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		monitoringSender = newTestMonitoringSender()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, monitoringSender, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	type testMonitoringElement struct {
		expectedMonitoring aostypes.NodeMonitoring
		sendMonitoring     *pbsm.InstantMonitoring
	}

	now := time.Now().UTC()

	testMonitoringData := []testMonitoringElement{
		{
			expectedMonitoring: aostypes.NodeMonitoring{
				NodeID: nodeID,
				NodeData: aostypes.MonitoringData{
					RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
					Disk:      []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
					Timestamp: now,
				},
				InstancesData: []aostypes.InstanceMonitoring{},
			},
			sendMonitoring: &pbsm.InstantMonitoring{
				NodeMonitoring: &pbsm.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk:      []*pbsm.PartitionUsage{{Name: "p1", UsedSize: 100}},
					Timestamp: timestamppb.New(now),
				},
			},
		},
		{
			expectedMonitoring: aostypes.NodeMonitoring{
				NodeID: nodeID,
				NodeData: aostypes.MonitoringData{
					RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
					Disk:      []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
					Timestamp: now,
				},
				InstancesData: []aostypes.InstanceMonitoring{
					{
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "s1", Instance: 1},
						MonitoringData: aostypes.MonitoringData{
							RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 0,
							Disk:      []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
							Timestamp: now,
						},
					},
					{
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "s1", Instance: 1},
						MonitoringData: aostypes.MonitoringData{
							RAM: 20, CPU: 30, InTraffic: 50, OutTraffic: 10,
							Disk:      []aostypes.PartitionUsage{{Name: "p2", UsedSize: 50}},
							Timestamp: now,
						},
					},
				},
			},
			sendMonitoring: &pbsm.InstantMonitoring{
				NodeMonitoring: &pbsm.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk:      []*pbsm.PartitionUsage{{Name: "p1", UsedSize: 100}},
					Timestamp: timestamppb.New(now),
				},
				InstancesMonitoring: []*pbsm.InstanceMonitoring{
					{
						Instance: &pbcommon.InstanceIdent{ServiceId: "service1", SubjectId: "s1", Instance: 1},
						MonitoringData: &pbsm.MonitoringData{
							Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 0,
							Disk:      []*pbsm.PartitionUsage{{Name: "p1", UsedSize: 100}},
							Timestamp: timestamppb.New(now),
						},
					},
					{
						Instance: &pbcommon.InstanceIdent{ServiceId: "service2", SubjectId: "s1", Instance: 1},
						MonitoringData: &pbsm.MonitoringData{
							Ram: 20, Cpu: 30, InTraffic: 50, OutTraffic: 10,
							Disk:      []*pbsm.PartitionUsage{{Name: "p2", UsedSize: 50}},
							Timestamp: timestamppb.New(now),
						},
					},
				},
			},
		},
	}

	for _, testMonitoring := range testMonitoringData {
		smClient.sendMessageChannel <- &pbsm.SMOutgoingMessages{
			SMOutgoingMessage: &pbsm.SMOutgoingMessages_InstantMonitoring{
				InstantMonitoring: testMonitoring.sendMonitoring,
			},
		}

		if err := waitMessage(
			monitoringSender.messageChannel, testMonitoring.expectedMonitoring, messageTimeout); err != nil {
			t.Errorf("Incorrect monitoring notification: %v", err)
		}
	}
}

func TestLogMessages(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		currentTime   = time.Now().UTC()
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	type testLogRequest struct {
		sendLogRequest     cloudprotocol.RequestLog
		expectedLogRequest *pbsm.SMIncomingMessages
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
			expectedLogRequest: &pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_SystemLogRequest{
				SystemLogRequest: &pbsm.SystemLogRequest{LogId: "sysLogID1", From: timestamppb.New(currentTime)},
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
			expectedLogRequest: &pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_SystemLogRequest{
				SystemLogRequest: &pbsm.SystemLogRequest{LogId: "sysLogID2", Till: timestamppb.New(currentTime)},
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
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceLogRequest{
					InstanceLogRequest: &pbsm.InstanceLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
						LogId:          "serviceLogID1", From: timestamppb.New(currentTime),
					},
				},
			},
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
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceLogRequest{
					InstanceLogRequest: &pbsm.InstanceLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "ser2", SubjectId: "", Instance: -1},
						LogId:          "serviceLogID1", Till: timestamppb.New(currentTime),
					},
				},
			},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID2",
				LogType: cloudprotocol.ServiceLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("", "", -1),
					Till:           &currentTime,
				},
			},
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceLogRequest{
					InstanceLogRequest: &pbsm.InstanceLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "", SubjectId: "", Instance: -1},
						LogId:          "serviceLogID2", Till: timestamppb.New(currentTime),
					},
				},
			},
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
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceCrashLogRequest{
					InstanceCrashLogRequest: &pbsm.InstanceCrashLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "ser1", SubjectId: "s1", Instance: -1},
						LogId:          "serviceLogID1", From: timestamppb.New(currentTime),
					},
				},
			},
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
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceCrashLogRequest{
					InstanceCrashLogRequest: &pbsm.InstanceCrashLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "ser2", SubjectId: "", Instance: -1},
						LogId:          "serviceLogID1", Till: timestamppb.New(currentTime),
					},
				},
			},
		},
		{
			sendLogRequest: cloudprotocol.RequestLog{
				LogID:   "serviceLogID2",
				LogType: cloudprotocol.CrashLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("", "", -1),
					Till:           &currentTime,
				},
			},
			expectedLogRequest: &pbsm.SMIncomingMessages{
				SMIncomingMessage: &pbsm.SMIncomingMessages_InstanceCrashLogRequest{
					InstanceCrashLogRequest: &pbsm.InstanceCrashLogRequest{
						InstanceFilter: &pbsm.InstanceFilter{ServiceId: "", SubjectId: "", Instance: -1},
						LogId:          "serviceLogID2", Till: timestamppb.New(currentTime),
					},
				},
			},
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

	smClient.sendMessageChannel <- &pbsm.SMOutgoingMessages{
		SMOutgoingMessage: &pbsm.SMOutgoingMessages_Log{
			Log: &pbsm.LogData{
				LogId: "log0", PartCount: 2, Part: 1, Data: []byte("this is log"),
				Error: &pbcommon.ErrorInfo{Message: "this is error"},
			},
		},
	}

	if err := waitMessage(messageSender.messageChannel, expectedLog, messageTimeout); err != nil {
		t.Errorf("Incorrect log message: %v", err)
	}
}

func TestOverrideEnvVars(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		currentTime   = time.Now().UTC()
		envVars       = cloudprotocol.OverrideEnvVars{
			Items: []cloudprotocol.EnvVarsInstanceInfo{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					Variables: []cloudprotocol.EnvVarInfo{
						{Name: "var0", Value: "val0", TTL: &currentTime},
					},
				},
			},
		}
		expectedPbEnvVarRequest = &pbsm.SMIncomingMessages{
			SMIncomingMessage: &pbsm.SMIncomingMessages_OverrideEnvVars{
				OverrideEnvVars: &pbsm.OverrideEnvVars{
					EnvVars: []*pbsm.OverrideInstanceEnvVar{{InstanceFilter: &pbsm.InstanceFilter{
						ServiceId: "service0",
						SubjectId: "subject0", Instance: -1,
					}, Variables: []*pbsm.EnvVarInfo{{Name: "var0", Value: "val0", Ttl: timestamppb.New(currentTime)}}}},
				},
			},
		}
		pbEnvVarStatus = &pbsm.SMOutgoingMessages{SMOutgoingMessage: &pbsm.SMOutgoingMessages_OverrideEnvVarStatus{
			OverrideEnvVarStatus: &pbsm.OverrideEnvVarStatus{EnvVarsStatus: []*pbsm.EnvVarInstanceStatus{
				{InstanceFilter: &pbsm.InstanceFilter{
					ServiceId: "service0",
					SubjectId: "subject0", Instance: -1,
				}, Statuses: []*pbsm.EnvVarStatus{{Name: "var0", Error: &pbcommon.ErrorInfo{
					Message: "someError",
				}}}},
			}},
		}}
		expectedEnvVarStatus = cloudprotocol.OverrideEnvVarsStatus{
			Statuses: []cloudprotocol.EnvVarsInstanceStatus{
				{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
					Statuses: []cloudprotocol.EnvVarStatus{{
						Name:      "var0",
						ErrorInfo: &cloudprotocol.ErrorInfo{Message: "someError"},
					}},
				},
			},
		}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
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
		nodeID               = "mainSM"
		nodeType             = "mainSMType"
		messageSender        = newTestMessageSender()
		config               = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		expectedRunInstances = &pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_RunInstances{
			RunInstances: &pbsm.RunInstances{
				Services: []*pbsm.ServiceInfo{{
					Version: "1.1.0",
					Url:     "url1", ServiceId: "s1", ProviderId: "p1", Gid: 600,
					Sha256: []byte{0, 0, 0, byte(100)}, Size: uint64(500),
				}},
				Layers: []*pbsm.LayerInfo{
					{
						Version: "3.0.0",
						Url:     "url2", LayerId: "l1", Digest: "digest1", Sha256: []byte{0, 0, 0, byte(100)},
						Size: uint64(500),
					},
				},
				Instances: []*pbsm.InstanceInfo{
					{
						Instance: &pbcommon.InstanceIdent{
							ServiceId: "s1", SubjectId: "subj1", Instance: 1,
						},
						NetworkParameters: &pbsm.NetworkParameters{
							Ip: "172.17.0.3", Subnet: "172.17.0.0/16", VlanId: 1,
						},
						Uid: 500, Priority: 1, StoragePath: "storage1", StatePath: "state1",
					},
				},
			},
		}}
		sendServices = []aostypes.ServiceInfo{{
			Version:   "1.1.0",
			ServiceID: "s1", ProviderID: "p1", URL: "url1", GID: 600,
			Sha256: []byte{0, 0, 0, byte(100)}, Size: uint64(500),
		}}
		sendLayers = []aostypes.LayerInfo{{
			Version: "3.0.0",
			URL:     "url2", LayerID: "l1", Digest: "digest1", Sha256: []byte{0, 0, 0, byte(100)},
			Size: uint64(500),
		}}
		sendInstances = []aostypes.InstanceInfo{{
			InstanceIdent:     aostypes.InstanceIdent{ServiceID: "s1", SubjectID: "subj1", Instance: 1},
			NetworkParameters: aostypes.NetworkParameters{IP: "172.17.0.3", Subnet: "172.17.0.0/16", VlanID: 1},
			UID:               500, Priority: 1, StoragePath: "storage1", StatePath: "state1",
		}}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, &pbsm.RunInstancesStatus{})
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := waitMessage(controller.GetRunInstancesStatusChannel(), launcher.NodeRunInstanceStatus{
		NodeID: nodeID, NodeType: nodeType, Instances: make([]cloudprotocol.InstanceStatus, 0),
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
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

func TestUpdateNetwork(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
	)

	networkParameters := []aostypes.NetworkParameters{
		{
			Subnet:    "172.17.0.0/16",
			IP:        "172.17.0.1",
			VlanID:    1,
			NetworkID: "network1",
		},
		{
			Subnet:    "172.18.0.0/16",
			IP:        "172.18.0.1",
			VlanID:    2,
			NetworkID: "network2",
		},
	}

	expectedUpdatesNetwork := &pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_UpdateNetworks{
		UpdateNetworks: &pbsm.UpdateNetworks{
			Networks: []*pbsm.NetworkParameters{
				{
					Subnet:    "172.17.0.0/16",
					Ip:        "172.17.0.1",
					VlanId:    1,
					NetworkId: "network1",
				},
				{
					Subnet:    "172.18.0.0/16",
					Ip:        "172.18.0.1",
					VlanId:    2,
					NetworkId: "network2",
				},
			},
		},
	}}

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := controller.UpdateNetwork(nodeID, networkParameters); err != nil {
		t.Fatalf("Can't send run instances: %v", err)
	}

	if err := smClient.waitMessage(expectedUpdatesNetwork, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}
}

func TestSyncClock(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	smClient.sendMessageChannel <- &pbsm.SMOutgoingMessages{
		SMOutgoingMessage: &pbsm.SMOutgoingMessages_ClockSyncRequest{},
	}

	select {
	case <-time.After(messageTimeout):
		t.Fatalf("Wait message error: %v", err)

	case message := <-smClient.receivedMessagesChannel:
		clockSync, ok := message.GetSMIncomingMessage().(*pbsm.SMIncomingMessages_ClockSync)
		if !ok {
			t.Fatalf("Incorrect message type: %v", message)
		}

		if clockSync.ClockSync.GetCurrentTime().CheckValid() != nil {
			t.Fatalf("Incorrect time: %v", clockSync.ClockSync.GetCurrentTime())
		}
	}
}

func TestGetAverageMonitoring(t *testing.T) {
	var (
		nodeID             = "mainSM"
		nodeType           = "mainSMType"
		messageSender      = newTestMessageSender()
		config             = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
		testWaitChan       = make(chan struct{})
		currentTime        = time.Now().UTC()
		expectedMonitoring = aostypes.NodeMonitoring{
			NodeID: nodeID,
			NodeData: aostypes.MonitoringData{
				RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 50,
				Disk:      []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
				Timestamp: currentTime,
			},
			InstancesData: []aostypes.InstanceMonitoring{
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "s1", Instance: 1},
					MonitoringData: aostypes.MonitoringData{
						RAM: 10, CPU: 20, InTraffic: 40, OutTraffic: 0,
						Disk:      []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
						Timestamp: currentTime,
					},
				},
				{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "s1", Instance: 1},
					MonitoringData: aostypes.MonitoringData{
						RAM: 20, CPU: 30, InTraffic: 50, OutTraffic: 10,
						Disk:      []aostypes.PartitionUsage{{Name: "p2", UsedSize: 50}},
						Timestamp: currentTime,
					},
				},
			},
		}
		sendMonitoring = &pbsm.SMOutgoingMessages{SMOutgoingMessage: &pbsm.SMOutgoingMessages_AverageMonitoring{
			AverageMonitoring: &pbsm.AverageMonitoring{
				NodeMonitoring: &pbsm.MonitoringData{
					Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 50,
					Disk:      []*pbsm.PartitionUsage{{Name: "p1", UsedSize: 100}},
					Timestamp: timestamppb.New(currentTime),
				},
				InstancesMonitoring: []*pbsm.InstanceMonitoring{
					{
						Instance: &pbcommon.InstanceIdent{ServiceId: "service1", SubjectId: "s1", Instance: 1},
						MonitoringData: &pbsm.MonitoringData{
							Ram: 10, Cpu: 20, InTraffic: 40, OutTraffic: 0,
							Disk:      []*pbsm.PartitionUsage{{Name: "p1", UsedSize: 100}},
							Timestamp: timestamppb.New(currentTime),
						},
					},
					{
						Instance: &pbcommon.InstanceIdent{ServiceId: "service2", SubjectId: "s1", Instance: 1},
						MonitoringData: &pbsm.MonitoringData{
							Ram: 20, Cpu: 30, InTraffic: 50, OutTraffic: 10,
							Disk:      []*pbsm.PartitionUsage{{Name: "p2", UsedSize: 50}},
							Timestamp: timestamppb.New(currentTime),
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

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pbsm.ConnectionStatus{},
		},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	go func() {
		data, err := controller.GetAverageMonitoring(nodeID)
		if err != nil {
			t.Errorf("Can't get average node monitoring: %v", err)
		}

		if !reflect.DeepEqual(data, expectedMonitoring) {
			t.Errorf("Incorrect monitoring data")
		}

		testWaitChan <- struct{}{}
	}()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{
		SMIncomingMessage: &pbsm.SMIncomingMessages_GetAverageMonitoring{},
	}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	if err := smClient.stream.Send(sendMonitoring); err != nil {
		t.Errorf("Can't send unit config status")
	}

	<-testWaitChan
}

func TestConnectionStatus(t *testing.T) {
	var (
		nodeID        = "mainSM"
		nodeType      = "mainSMType"
		messageSender = newTestMessageSender()
		config        = config.Config{SMController: config.SMController{CMServerURL: cmServerURL}}
	)

	controller, err := smcontroller.New(&config, messageSender, nil, nil, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create SM controller: %v", err)
	}
	defer controller.Close()

	controller.CloudConnected()

	smClient, err := newTestSMClient(cmServerURL, unitconfig.NodeConfigStatus{
		NodeID: nodeID, NodeType: nodeType,
	}, nil)
	if err != nil {
		t.Fatalf("Can't create test SM: %v", err)
	}

	defer smClient.close()

	// check receive correct connection status on registration

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
		ConnectionStatus: &pbsm.ConnectionStatus{CloudStatus: pbsm.ConnectionEnum_CONNECTED},
	}}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	// check receive correct connection status when cloud disconnected

	controller.CloudDisconnected()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
		ConnectionStatus: &pbsm.ConnectionStatus{CloudStatus: pbsm.ConnectionEnum_DISCONNECTED},
	}}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}

	// check receive correct connection status when cloud connected

	controller.CloudConnected()

	if err := smClient.waitMessage(&pbsm.SMIncomingMessages{SMIncomingMessage: &pbsm.SMIncomingMessages_ConnectionStatus{
		ConnectionStatus: &pbsm.ConnectionStatus{CloudStatus: pbsm.ConnectionEnum_CONNECTED},
	}}, messageTimeout); err != nil {
		t.Fatalf("Wait message error: %v", err)
	}
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
	return &testMonitoringSender{messageChannel: make(chan aostypes.NodeMonitoring, 1)}
}

func (sender *testMonitoringSender) SendNodeMonitoring(monitoring aostypes.NodeMonitoring) {
	sender.messageChannel <- monitoring
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

func (sender *testMessageSender) SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
	return nil
}

func (sender *testMessageSender) UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
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
			log.Debugf("%v", message)
			log.Debugf("%v", expectedMsg)

			return aoserrors.New("incorrect received message")
		}
	}

	return nil
}

func newTestSMClient(
	url string, nodeConfigStatus unitconfig.NodeConfigStatus, runStatus *pbsm.RunInstancesStatus,
) (client *testSMClient, err error) {
	client = &testSMClient{
		sendMessageChannel:      make(chan *pbsm.SMOutgoingMessages, 10),
		receivedMessagesChannel: make(chan *pbsm.SMIncomingMessages, 10),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if client.connection, err = grpc.DialContext(
		ctx, url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	client.pbClient = pbsm.NewSMServiceClient(client.connection)

	if client.stream, err = pbsm.NewSMServiceClient(client.connection).RegisterSM(context.Background()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err := client.stream.Send(
		&pbsm.SMOutgoingMessages{
			SMOutgoingMessage: &pbsm.SMOutgoingMessages_NodeConfigStatus{NodeConfigStatus: &pbsm.NodeConfigStatus{
				NodeId: nodeConfigStatus.NodeID, NodeType: nodeConfigStatus.NodeType, Version: nodeConfigStatus.Version,
				Error: pbconvert.ErrorInfoToPB(nodeConfigStatus.Error),
			}},
		}); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if runStatus != nil {
		if err := client.stream.Send(
			&pbsm.SMOutgoingMessages{
				SMOutgoingMessage: &pbsm.SMOutgoingMessages_RunInstancesStatus{RunInstancesStatus: runStatus},
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

func (client *testSMClient) waitMessage(expectedMsg *pbsm.SMIncomingMessages, timeout time.Duration) error {
	select {
	case <-time.After(timeout):
		return aoserrors.New("wait message timeout")

	case message := <-client.receivedMessagesChannel:
		if !proto.Equal(message, expectedMsg) {
			return aoserrors.Errorf("incorrect client message received: %v", message)
		}
	}

	return nil
}
