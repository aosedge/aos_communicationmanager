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

package amqphandler_test

import (
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/amqphandler"
)

/***********************************************************************************************************************
 * Const
 **********************************************************************************************************************/

const (
	inQueueName  = "in_queue"
	outQueueName = "out_queue"
	consumerName = "test_consumer"
	exchangeName = "test_exchange"
	systemID     = "systemID"
)

const serviceDiscoveryURL = "http://:8010"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type backendClient struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	delivery   <-chan amqp.Delivery
	errChannel chan *amqp.Error
}

type testCryptoContext struct {
	currentMessage interface{}
}

type testConnectionEventsConsumer struct {
	connectionChannel chan bool
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	testClient backendClient
	amqpURL    *url.URL
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
 * Private
 **********************************************************************************************************************/

func setup() (err error) {
	if err = os.MkdirAll("tmp", 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	amqpURLStr := os.Getenv("AMQP_URL")
	if amqpURLStr == "" {
		amqpURLStr = "amqp://guest:guest@localhost:5672"
	}

	if amqpURL, err = url.Parse(amqpURLStr); err != nil {
		return aoserrors.Wrap(err)
	}

	if testClient.conn, err = amqp.Dial(amqpURL.String()); err != nil {
		return aoserrors.Wrap(err)
	}

	if testClient.channel, err = testClient.conn.Channel(); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = testClient.channel.QueueDeclare(inQueueName, false, false, false, false, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = testClient.channel.QueueDeclare(outQueueName, false, false, false, false, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = testClient.channel.ExchangeDeclare(exchangeName, "fanout", false, false, false, false, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = testClient.channel.QueueBind(inQueueName, "", exchangeName, false, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	if testClient.delivery, err = testClient.channel.Consume(inQueueName, "", true, false, false, false, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	testClient.errChannel = testClient.conn.NotifyClose(make(chan *amqp.Error, 1))

	go startServiceDiscoveryServer()

	time.Sleep(time.Second)

	return nil
}

func cleanup() {
	if testClient.channel != nil {
		_, _ = testClient.channel.QueueDelete(inQueueName, false, false, false)
		_, _ = testClient.channel.QueueDelete(outQueueName, false, false, false)
		_ = testClient.channel.ExchangeDelete(exchangeName, false, false)
		_ = testClient.channel.Close()
	}

	if testClient.conn != nil {
		testClient.conn.Close()
	}

	if err := os.RemoveAll("tmp"); err != nil {
		log.Errorf("Can't remove tmp folder: %v", err)
	}
}

func sendCloudMessage(msgType string, message interface{}) error {
	jsonMsg, err := json.Marshal(message)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	dataToSend := cloudprotocol.ReceivedMessage{
		Header: cloudprotocol.MessageHeader{Version: cloudprotocol.ProtocolVersion},
		Data:   jsonMsg,
	}

	dataJSON, err := json.Marshal(dataToSend)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"message": string(dataJSON)}).Debug("Send message")

	encryptedData := []byte(base64.StdEncoding.EncodeToString(dataJSON))

	return aoserrors.Wrap(testClient.channel.Publish(
		"",
		outQueueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        encryptedData,
		}))
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Error creating service images: %v", err)
	}

	ret := m.Run()

	cleanup()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestReceiveMessages(t *testing.T) {
	cryptoContext := &testCryptoContext{}
	rootfs := "rootfs"

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	if err := amqpHandler.Connect(cryptoContext, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	type testDataType struct {
		messageType  string
		expectedData interface{}
	}

	testTime, err := time.Parse(time.RFC3339, "2016-06-20T12:41:45.14Z")
	if err != nil {
		t.Fatalf("Can't prepare test time %v", err)
	}

	testData := []testDataType{
		{
			messageType: cloudprotocol.StateAcceptanceMessageType,
			expectedData: &cloudprotocol.StateAcceptance{
				MessageType:   cloudprotocol.StateAcceptanceMessageType,
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj0", Instance: 1},
				Checksum:      "0123456890", Result: "accepted", Reason: "just because",
			},
		},
		{
			messageType: cloudprotocol.UpdateStateMessageType,
			expectedData: &cloudprotocol.UpdateState{
				MessageType:   cloudprotocol.UpdateStateMessageType,
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "subj1", Instance: 1},
				Checksum:      "0993478847", State: "This is new state",
			},
		},
		{
			messageType: cloudprotocol.RequestLogMessageType,
			expectedData: &cloudprotocol.RequestLog{
				MessageType: cloudprotocol.RequestLogMessageType,
				LogID:       "someID",
				LogType:     cloudprotocol.ServiceLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service2", "", -1),
					From:           nil, Till: nil,
				},
			},
		},
		{
			messageType: cloudprotocol.RequestLogMessageType,
			expectedData: &cloudprotocol.RequestLog{
				MessageType: cloudprotocol.RequestLogMessageType,
				LogID:       "someID",
				LogType:     cloudprotocol.CrashLog,
				Filter: cloudprotocol.LogFilter{
					InstanceFilter: cloudprotocol.NewInstanceFilter("service3", "", -1),
					From:           nil, Till: nil,
				},
			},
		},
		{
			messageType: cloudprotocol.RequestLogMessageType,
			expectedData: &cloudprotocol.RequestLog{
				MessageType: cloudprotocol.RequestLogMessageType,
				LogID:       "someID",
				LogType:     cloudprotocol.SystemLog,
				Filter: cloudprotocol.LogFilter{
					From: nil, Till: nil,
				},
			},
		},
		{
			messageType: cloudprotocol.RenewCertsNotificationMessageType,
			expectedData: &cloudprotocol.RenewCertsNotification{
				MessageType: cloudprotocol.RenewCertsNotificationMessageType,
				Certificates: []cloudprotocol.RenewCertData{
					{NodeID: "node0", Type: "online", Serial: "1234", ValidTill: testTime},
				},
				UnitSecrets: cloudprotocol.UnitSecrets{Version: "1.0.0", Nodes: map[string]string{"node0": "pwd"}},
			},
		},
		{
			messageType: cloudprotocol.IssuedUnitCertsMessageType,
			expectedData: &cloudprotocol.IssuedUnitCerts{
				MessageType: cloudprotocol.IssuedUnitCertsMessageType,
				Certificates: []cloudprotocol.IssuedCertData{
					{Type: "online", NodeID: "mainNode", CertificateChain: "123456"},
				},
			},
		},
		{
			messageType: cloudprotocol.OverrideEnvVarsMessageType,
			expectedData: &cloudprotocol.OverrideEnvVars{
				MessageType: cloudprotocol.OverrideEnvVarsMessageType,
				Items:       []cloudprotocol.EnvVarsInstanceInfo{},
			},
		},
		{
			messageType: cloudprotocol.DesiredStatusMessageType,
			expectedData: &cloudprotocol.DesiredStatus{
				MessageType: cloudprotocol.DesiredStatusMessageType,
				UnitConfig:  &cloudprotocol.UnitConfig{},
				Components: []cloudprotocol.ComponentInfo{
					{Version: "1.0.0", ComponentID: &rootfs},
				},
				Layers: []cloudprotocol.LayerInfo{
					{Version: "1.0", LayerID: "l1", Digest: "digest"},
				},
				Services: []cloudprotocol.ServiceInfo{
					{Version: "1.0", ServiceID: "serv1", ProviderID: "p1"},
				},
				Instances:    []cloudprotocol.InstanceInfo{{ServiceID: "s1", SubjectID: "subj1", NumInstances: 1}},
				FOTASchedule: cloudprotocol.ScheduleRule{TTL: uint64(100), Type: "type"},
				SOTASchedule: cloudprotocol.ScheduleRule{TTL: uint64(200), Type: "type2"},
			},
		},
		{
			messageType: cloudprotocol.StartProvisioningRequestMessageType,
			expectedData: &cloudprotocol.StartProvisioningRequest{
				MessageType: cloudprotocol.StartProvisioningRequestMessageType,
				NodeID:      "node-1",
				Password:    "password-1",
			},
		},
		{
			messageType: cloudprotocol.FinishProvisioningRequestMessageType,
			expectedData: &cloudprotocol.FinishProvisioningRequest{
				MessageType: cloudprotocol.FinishProvisioningRequestMessageType,
				NodeID:      "node1",
				Certificates: []cloudprotocol.IssuedCertData{
					{NodeID: "node1", Type: "online", CertificateChain: "onlineCSR"},
					{NodeID: "node1", Type: "offline", CertificateChain: "offlineCSR"},
				},
				Password: "password-1",
			},
		},
		{
			messageType: cloudprotocol.DeprovisioningRequestMessageType,
			expectedData: &cloudprotocol.DeprovisioningRequest{
				MessageType: cloudprotocol.DeprovisioningRequestMessageType,
				NodeID:      "node-1",
				Password:    "password-1",
			},
		},
	}

	for _, data := range testData {
		cryptoContext.currentMessage = data.expectedData

		if err = sendCloudMessage(data.messageType, data.expectedData); err != nil {
			t.Errorf("Can't send message: %v", err)
			continue
		}

		select {
		case receiveMessage := <-amqpHandler.MessageChannel:
			if !reflect.DeepEqual(data.expectedData, receiveMessage) {
				t.Errorf("Wrong data received: %v %v", data.expectedData, receiveMessage)
				continue
			}

		case err = <-testClient.errChannel:
			t.Fatalf("AMQP error: %v", err)
			return

		case <-time.After(5 * time.Second):
			t.Error("Waiting data timeout")
			continue
		}
	}
}

func TestSendMessages(t *testing.T) {
	cryptoContext := &testCryptoContext{}

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	if err := amqpHandler.Connect(cryptoContext, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	type messageDesc struct {
		call        func() error
		data        cloudprotocol.Message
		getDataType func() interface{}
	}

	unitConfigData := []cloudprotocol.UnitConfigStatus{{Version: "1.0"}}

	serviceSetupData := []cloudprotocol.ServiceStatus{
		{ServiceID: "service0", Version: "1.0", Status: "running", ErrorInfo: nil},
		{
			ServiceID: "service1", Version: "2.0", Status: "stopped",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "crash"},
		},
		{
			ServiceID: "service2", Version: "3.0", Status: "unknown",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "unknown"},
		},
	}

	instances := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent:  aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
			ServiceVersion: "1.0", StateChecksum: "12345", Status: "running", NodeID: "mainNode",
		},
	}

	layersSetupData := []cloudprotocol.LayerStatus{
		{
			LayerID: "layer0", Digest: "sha256:0", Status: "failed", Version: "1.0",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "bad layer"},
		},
		{LayerID: "layer1", Digest: "sha256:1", Status: "installed", Version: "2.0"},
		{LayerID: "layer2", Digest: "sha256:2", Status: "installed", Version: "3.0"},
	}

	componentSetupData := []cloudprotocol.ComponentStatus{
		{ComponentID: "rootfs", Status: "installed", Version: "1.0"},
		{ComponentID: "firmware", Status: "installed", Version: "5.6"},
		{
			ComponentID: "bootloader", Status: "error", Version: "100",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "install error"},
		},
	}

	nodeConfiguration := []cloudprotocol.NodeInfo{
		{
			NodeID: "main", NodeType: "mainType", TotalRAM: 200,
			CPUs: []cloudprotocol.CPUInfo{
				{ModelName: "Intel(R) Core(TM) i7-1185G7"},
				{ModelName: "Intel(R) Core(TM) i7-1185G7"},
			},
			Partitions: []cloudprotocol.PartitionInfo{{Name: "p1", Types: []string{"t1"}, TotalSize: 200}},
		},
	}

	nodeMonitoring := cloudprotocol.NodeMonitoringData{
		Items: []aostypes.MonitoringData{
			{
				RAM: 1024, CPU: 50, Download: 8192, Upload: 4096, Partitions: []aostypes.PartitionUsage{{
					Name: "p1", UsedSize: 100,
				}},
				Timestamp: time.Now().UTC(),
			},
		},
	}

	instanceMonitoringData := []cloudprotocol.InstanceMonitoringData{
		{
			NodeID: "mainNode", InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
			Items: []aostypes.MonitoringData{
				{RAM: 1024, CPU: 50, Partitions: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}}},
			},
		},
		{
			NodeID: "mainNode", InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "subj1", Instance: 1},
			Items: []aostypes.MonitoringData{
				{RAM: 128, CPU: 60, Partitions: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}}},
			},
		},
		{
			NodeID: "mainNode", InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "subj1", Instance: 1},
			Items: []aostypes.MonitoringData{
				{RAM: 256, CPU: 70, Partitions: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}}},
			},
		},
		{
			NodeID: "mainNode", InstanceIdent: aostypes.InstanceIdent{ServiceID: "service3", SubjectID: "subj1", Instance: 1},
			Items: []aostypes.MonitoringData{
				{RAM: 512, CPU: 80, Partitions: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}}},
			},
		},
	}

	monitoringData := cloudprotocol.Monitoring{
		MessageType:      cloudprotocol.MonitoringMessageType,
		Nodes:            []cloudprotocol.NodeMonitoringData{nodeMonitoring},
		ServiceInstances: instanceMonitoringData,
	}

	pushServiceLogData := cloudprotocol.PushLog{
		MessageType: cloudprotocol.PushLogMessageType,
		LogID:       "log0",
		PartsCount:  2,
		Part:        1,
		Content:     []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		ErrorInfo: &cloudprotocol.ErrorInfo{
			Message: "Error",
		},
	}

	now := time.Now().UTC()
	nowFormatted := now.Format(time.RFC3339Nano)

	alertsData := cloudprotocol.Alerts{
		MessageType: cloudprotocol.AlertsMessageType,
		Items: []interface{}{
			cloudprotocol.SystemAlert{
				AlertItem: cloudprotocol.AlertItem{Timestamp: now, Tag: cloudprotocol.AlertTagSystemError},
				Message:   "System error", NodeID: "mainNode",
			},
			cloudprotocol.SystemAlert{
				AlertItem: cloudprotocol.AlertItem{Timestamp: now, Tag: cloudprotocol.AlertTagSystemError},
				Message:   "Service crashed", NodeID: "mainNode",
			},
			cloudprotocol.SystemQuotaAlert{
				AlertItem: cloudprotocol.AlertItem{Timestamp: now, Tag: cloudprotocol.AlertTagSystemQuota},
				NodeID:    "mainNode",
				Parameter: "cpu",
				Value:     1000,
			},
		},
	}

	overrideEnvStatus := cloudprotocol.OverrideEnvVarsStatus{
		MessageType: cloudprotocol.OverrideEnvVarsStatusMessageType,
		Statuses: []cloudprotocol.EnvVarsInstanceStatus{
			{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
				Statuses: []cloudprotocol.EnvVarStatus{
					{Name: "1234"},
					{Name: "345", ErrorInfo: &cloudprotocol.ErrorInfo{Message: "some error"}},
				},
			},
			{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service1", "subject1", -1),
				Statuses: []cloudprotocol.EnvVarStatus{
					{Name: "0000"},
				},
			},
		},
	}

	startProvisioningResponse := cloudprotocol.StartProvisioningResponse{
		MessageType: cloudprotocol.StartProvisioningResponseMessageType,
		NodeID:      "node-1",
		ErrorInfo:   nil,
		CSRs:        []cloudprotocol.IssueCertData{{Type: "online", Csr: "iam"}, {Type: "offline", Csr: "iam"}},
	}

	finishProvisioningResponse := cloudprotocol.FinishProvisioningResponse{
		MessageType: cloudprotocol.FinishProvisioningResponseMessageType,
		NodeID:      "node-1",
		ErrorInfo:   nil,
	}

	deProvisioningResponse := cloudprotocol.DeprovisioningResponse{
		MessageType: cloudprotocol.DeprovisioningResponseMessageType,
		NodeID:      "node-1",
		ErrorInfo:   nil,
	}

	issueCerts := cloudprotocol.IssueUnitCerts{
		MessageType: cloudprotocol.IssueUnitCertsMessageType,
		Requests: []cloudprotocol.IssueCertData{
			{Type: "online", Csr: "This is online CSR", NodeID: "mainNode"},
			{Type: "offline", Csr: "This is offline CSR", NodeID: "mainNode"},
		},
	}

	installCertsConfirmation := cloudprotocol.InstallUnitCertsConfirmation{
		MessageType: cloudprotocol.InstallUnitCertsConfirmationMessageType,
		Certificates: []cloudprotocol.InstallCertData{
			{Type: "online", Serial: "1234", Status: "ok", Description: "This is online cert", NodeID: "mainNode"},
			{Type: "offline", Serial: "1234", Status: "ok", Description: "This is offline cert", NodeID: "mainNode"},
		},
	}

	testData := []messageDesc{
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendUnitStatus(cloudprotocol.UnitStatus{
					UnitConfig:   unitConfigData,
					Components:   componentSetupData,
					Layers:       layersSetupData,
					Services:     serviceSetupData,
					Instances:    instances,
					Nodes:        nodeConfiguration,
					UnitSubjects: []string{"subject"},
				}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.UnitStatus{
					MessageType:  cloudprotocol.UnitStatusMessageType,
					UnitConfig:   unitConfigData,
					Components:   componentSetupData,
					Layers:       layersSetupData,
					Services:     serviceSetupData,
					Instances:    instances,
					Nodes:        nodeConfiguration,
					UnitSubjects: []string{"subject"},
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.UnitStatus{MessageType: cloudprotocol.UnitStatusMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendMonitoringData(monitoringData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &monitoringData,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.Monitoring{MessageType: cloudprotocol.MonitoringMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(
					amqpHandler.SendInstanceNewState(
						cloudprotocol.NewState{
							InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
							Checksum:      "12345679", State: "This is state",
						}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.NewState{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
					Checksum:      "12345679", State: "This is state",
					MessageType: cloudprotocol.NewStateMessageType,
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.NewState{MessageType: cloudprotocol.NewStateMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendInstanceStateRequest(
					cloudprotocol.StateRequest{
						MessageType:   cloudprotocol.StateRequestMessageType,
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
						Default:       true,
					}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.StateRequest{
					MessageType:   cloudprotocol.StateRequestMessageType,
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
					Default:       true,
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.StateRequest{MessageType: cloudprotocol.StateRequestMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendLog(pushServiceLogData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.PushLog{
					MessageType: cloudprotocol.PushLogMessageType,
					LogID:       pushServiceLogData.LogID,
					PartsCount:  pushServiceLogData.PartsCount,
					Part:        pushServiceLogData.Part,
					Content:     pushServiceLogData.Content,
					ErrorInfo:   pushServiceLogData.ErrorInfo,
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.PushLog{MessageType: cloudprotocol.PushLogMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendAlerts(alertsData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.Alerts{
					MessageType: cloudprotocol.AlertsMessageType,
					Items: []interface{}{
						map[string]interface{}{
							"timestamp": nowFormatted, "tag": "systemAlert",
							"nodeId": "mainNode", "message": "System error",
						},
						map[string]interface{}{
							"timestamp": nowFormatted, "tag": "systemAlert",
							"nodeId": "mainNode", "message": "Service crashed",
						},
						map[string]interface{}{
							"timestamp": nowFormatted, "tag": "systemQuotaAlert",
							"nodeId": "mainNode", "parameter": "cpu", "value": float64(1000),
						},
					},
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.Alerts{MessageType: cloudprotocol.AlertsMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendIssueUnitCerts(issueCerts.Requests))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &issueCerts,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.IssueUnitCerts{MessageType: cloudprotocol.IssueUnitCertsMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendInstallCertsConfirmation(installCertsConfirmation.Certificates))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &installCertsConfirmation,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.InstallUnitCertsConfirmation{
					MessageType: cloudprotocol.InstallUnitCertsConfirmationMessageType,
				}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendOverrideEnvVarsStatus(overrideEnvStatus))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &overrideEnvStatus,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.OverrideEnvVarsStatus{MessageType: cloudprotocol.OverrideEnvVarsStatusMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendStartProvisioningResponse(startProvisioningResponse))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &startProvisioningResponse,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.StartProvisioningResponse{MessageType: cloudprotocol.StartProvisioningResponseMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendFinishProvisioningResponse(finishProvisioningResponse))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &finishProvisioningResponse,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.FinishProvisioningResponse{MessageType: cloudprotocol.FinishProvisioningResponseMessageType}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendDeprovisioningResponse(deProvisioningResponse))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					SystemID: systemID,
					Version:  cloudprotocol.ProtocolVersion,
				},
				Data: &deProvisioningResponse,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.DeprovisioningResponse{MessageType: cloudprotocol.DeprovisioningResponseMessageType}
			},
		},
	}

	for _, message := range testData {
		if err = message.call(); err != nil {
			t.Errorf("Can't perform call: %v", err)
			continue
		}

		select {
		case delivery := <-testClient.delivery:
			receivedHeader := cloudprotocol.Message{}

			if err = json.Unmarshal(delivery.Body, &receivedHeader); err != nil {
				t.Errorf("Error parsing message: %v", err)
				continue
			}

			if !reflect.DeepEqual(receivedHeader.Header, message.data.Header) {
				t.Errorf("Wrong Header received: %v != %v", receivedHeader.Header, message.data.Header)
				continue
			}

			var receivedData struct {
				Data interface{} `json:"data"`
			}

			receivedData.Data = message.getDataType()

			if err = json.Unmarshal(delivery.Body, &receivedData); err != nil {
				t.Errorf("Error parsing message: %v", err)
				continue
			}

			if !reflect.DeepEqual(message.data.Data, receivedData.Data) {
				t.Errorf("Wrong data received: %v != %v", receivedData.Data, message.data.Data)
			}

		case err = <-testClient.errChannel:
			t.Fatalf("AMQP error: %v", err)
			return

		case <-time.After(5 * time.Second):
			t.Error("Waiting data timeout")
			continue
		}
	}
}

func TestConnectionEvents(t *testing.T) {
	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	connectionConsumer := newConnectionEventsConsumer()

	if err := amqpHandler.SubscribeForConnectionEvents(connectionConsumer); err != nil {
		t.Fatalf("Can't subscribe for connection events: %v", err)
	}

	defer func() {
		if err := amqpHandler.UnsubscribeFromConnectionEvents(connectionConsumer); err != nil {
			t.Fatalf("Can't unsubscribe from connection events: %v", err)
		}
	}()

	if err := amqpHandler.Connect(&testCryptoContext{}, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't connect to cloud: %v", err)
	}

	connected, err := connectionConsumer.waitConnectionEvent()
	if err != nil {
		t.Errorf("Error waiting connection event: %v", err)
	}

	if !connected {
		t.Errorf("Wrong connection event: %v", connected)
	}

	if err := amqpHandler.Disconnect(); err != nil {
		t.Errorf("Can't disconnect from cloud: %v", err)
	}

	if connected, err = connectionConsumer.waitConnectionEvent(); err != nil {
		t.Errorf("Error waiting connection event: %v", err)
	}

	if connected {
		t.Errorf("Wrong connection event: %v", connected)
	}
}

func TestConnectionEventsError(t *testing.T) {
	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	connectionConsumer := newConnectionEventsConsumer()

	if err := amqpHandler.UnsubscribeFromConnectionEvents(connectionConsumer); err == nil {
		t.Error("Error expected")
	}

	if err := amqpHandler.SubscribeForConnectionEvents(connectionConsumer); err != nil {
		t.Fatalf("Can't subscribe for connection events: %v", err)
	}

	if err := amqpHandler.SubscribeForConnectionEvents(connectionConsumer); err == nil {
		t.Error("Error expected")
	}

	if err := amqpHandler.UnsubscribeFromConnectionEvents(connectionConsumer); err != nil {
		t.Fatalf("Can't unsubscribe from connection events: %v", err)
	}

	if err := amqpHandler.UnsubscribeFromConnectionEvents(connectionConsumer); err == nil {
		t.Error("Error expected")
	}
}

func TestSendMultipleMessages(t *testing.T) {
	const numMessages = 1000

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	if err = amqpHandler.Connect(&testCryptoContext{}, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	testData := []func() error{
		func() error {
			return aoserrors.Wrap(amqpHandler.SendUnitStatus(
				cloudprotocol.UnitStatus{MessageType: cloudprotocol.UnitStatusMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendDeltaUnitStatus(
				cloudprotocol.DeltaUnitStatus{
					MessageType:  cloudprotocol.UnitStatusMessageType,
					IsDeltaInfo:  true,
					UnitSubjects: []string{"subject"},
				}),
			)
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendMonitoringData(
				cloudprotocol.Monitoring{MessageType: cloudprotocol.MonitoringMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(
				amqpHandler.SendInstanceNewState(cloudprotocol.NewState{MessageType: cloudprotocol.NewStateMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendInstanceStateRequest(
				cloudprotocol.StateRequest{MessageType: cloudprotocol.StateRequestMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(
				amqpHandler.SendLog(cloudprotocol.PushLog{MessageType: cloudprotocol.PushLogMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(
				amqpHandler.SendAlerts(cloudprotocol.Alerts{MessageType: cloudprotocol.AlertsMessageType}),
			)
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendIssueUnitCerts(nil))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendInstallCertsConfirmation(nil))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendOverrideEnvVarsStatus(
				cloudprotocol.OverrideEnvVarsStatus{MessageType: cloudprotocol.OverrideEnvVarsStatusMessageType}),
			)
		},
	}

	for i := 0; i < numMessages; i++ {
		//nolint:gosec // it is enough to use weak random generator in this case
		call := testData[rand.Intn(len(testData))]

		if err = call(); err != nil {
			t.Errorf("Can't perform call: %v", err)
			continue
		}

		select {
		case <-testClient.delivery:

		case err = <-testClient.errChannel:
			t.Errorf("AMQP error: %v", err)

		case <-time.After(5 * time.Second):
			t.Error("Waiting data timeout")
		}
	}
}

func TestSendDisconnectMessages(t *testing.T) {
	const sendQueueSize = 32

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	// Send unimportant message
	err = amqpHandler.SendUnitStatus(cloudprotocol.UnitStatus{MessageType: cloudprotocol.UnitStatusMessageType})
	if !errors.Is(err, amqphandler.ErrNotConnected) {
		t.Errorf("Wrong error type: %v", err)
	}

	// Send number important messages equals to send channel size - should be accepted without error

	for i := 0; i < sendQueueSize; i++ {
		if err := amqpHandler.SendAlerts(cloudprotocol.Alerts{MessageType: cloudprotocol.AlertsMessageType}); err != nil {
			t.Errorf("Can't send important message: %v", err)
		}
	}

	// Next important message should fail due to send channel size

	err = amqpHandler.SendInstanceStateRequest(
		cloudprotocol.StateRequest{MessageType: cloudprotocol.StateRequestMessageType})
	if !errors.Is(err, amqphandler.ErrSendChannelFull) {
		t.Errorf("Wrong error type: %v", err)
	}

	if err = amqpHandler.Connect(&testCryptoContext{}, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	// Server should receive pending important messages

	for i := 0; i < sendQueueSize; i++ {
		select {
		case delivery := <-testClient.delivery:
			// unmarshal type name
			var message struct {
				Data struct {
					MessageType string `json:"messageType"`
				} `json:"data"`
			}

			if err = json.Unmarshal(delivery.Body, &message); err != nil {
				t.Errorf("Can't parse json message: %v", err)
				continue
			}

			if message.Data.MessageType != cloudprotocol.AlertsMessageType {
				t.Errorf("Wrong message type: %s", message.Data.MessageType)
			}

		case err = <-testClient.errChannel:
			t.Errorf("AMQP error: %v", err)

		case <-time.After(5 * time.Second):
			t.Fatal("Waiting message timeout")
		}
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (context *testCryptoContext) GetTLSConfig() (config *tls.Config, err error) {
	return nil, err
}

func (context *testCryptoContext) DecryptMetadata(input []byte) (output []byte, err error) {
	output, err = base64.StdEncoding.DecodeString(string(input))
	if err != nil {
		return output, aoserrors.Wrap(err)
	}

	return output, nil
}

func newConnectionEventsConsumer() *testConnectionEventsConsumer {
	return &testConnectionEventsConsumer{
		connectionChannel: make(chan bool, 1),
	}
}

func (consumer *testConnectionEventsConsumer) CloudConnected() {
	log.Debug("Connected to cloud")

	consumer.connectionChannel <- true
}

func (consumer *testConnectionEventsConsumer) CloudDisconnected() {
	log.Debug("Disconnected from cloud")

	consumer.connectionChannel <- false
}

func (consumer *testConnectionEventsConsumer) waitConnectionEvent() (bool, error) {
	select {
	case connected := <-consumer.connectionChannel:
		return connected, nil

	case <-time.After(1 * time.Second):
		return false, aoserrors.New("wait connection timeout")
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func serviceDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "not found.", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodPost:
		log.Debug("Receive POST")

		password, _ := amqpURL.User.Password()

		response := cloudprotocol.ServiceDiscoveryResponse{
			Version: 4,
			Connection: cloudprotocol.ConnectionInfo{
				SendParams: cloudprotocol.SendParams{
					Host:     amqpURL.Host,
					User:     amqpURL.User.Username(),
					Password: password,
					Exchange: cloudprotocol.ExchangeParams{Name: exchangeName},
				},
				ReceiveParams: cloudprotocol.ReceiveParams{
					Host:     amqpURL.Host,
					User:     amqpURL.User.Username(),
					Password: password,
					Consumer: consumerName,
					Queue:    cloudprotocol.QueueInfo{Name: outQueueName},
				},
			},
		}

		rowResponse, err := json.Marshal(response)
		if err != nil {
			log.Errorf("Can't marshal response: %v", err)
			break
		}

		if _, err := w.Write(rowResponse); err != nil {
			log.Errorf("Can't send http response: %v", err)
		}

	default:
		http.Error(w, "Sorry, only POST methods are supported.", http.StatusNotImplemented)
	}
}

func startServiceDiscoveryServer() {
	http.HandleFunc("/", serviceDiscovery)

	if err := http.ListenAndServe(":8010", nil); err != nil { //nolint:gosec
		log.Fatal(err)
	}
}
