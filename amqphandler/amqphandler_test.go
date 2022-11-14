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
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/amqphandler"
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
	decodedData cloudprotocol.DecodedDesiredStatus
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

var errTimeout = errors.New("wait message timeout")

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

func sendMessage(message interface{}) error {
	dataJSON, err := json.Marshal(message)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"message": string(dataJSON)}).Debug("Send message")

	return aoserrors.Wrap(testClient.channel.Publish(
		"",
		outQueueName,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        dataJSON,
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

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	if err := amqpHandler.Connect(cryptoContext, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	testData := []*cloudprotocol.Message{
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.StateAcceptanceType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.StateAcceptance{
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj0", Instance: 1},
				Checksum:      "0123456890", Result: "accepted", Reason: "just because",
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.UpdateStateType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.UpdateState{
				InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "subj1", Instance: 1},
				Checksum:      "0993478847", State: "This is new state",
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RequestServiceLogType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RequestServiceLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service2", "", -1),
				LogID:          uuid.New().String(), From: &time.Time{}, Till: &time.Time{},
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RequestServiceCrashLogType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RequestServiceCrashLog{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service3", "", -1), LogID: uuid.New().String(),
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RequestSystemLogType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RequestSystemLog{
				LogID: uuid.New().String(), From: &time.Time{}, Till: &time.Time{},
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RenewCertsNotificationType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RenewCertsNotification{
				Certificates: []cloudprotocol.RenewCertData{
					{Type: "online", Serial: "1234", ValidTill: time.Now().UTC()},
				},
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.IssuedUnitCertsType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.IssuedUnitCerts{
				Certificates: []cloudprotocol.IssuedCertData{
					{Type: "online", CertificateChain: "123456"},
				},
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.OverrideEnvVarsType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.OverrideEnvVars{},
		},
	}

	for _, message := range testData {
		if err = sendMessage(message); err != nil {
			t.Errorf("Can't send message: %v", err)
			continue
		}

		select {
		case receiveMessage := <-amqpHandler.MessageChannel:
			switch data := receiveMessage.(type) {
			case *cloudprotocol.DecodedOverrideEnvVars:
				if len(data.OverrideEnvVars) != 0 {
					t.Error("Wrong count of override envs")
				}

			case *cloudprotocol.RenewCertsNotificationWithPwd:
				sentMessage, ok := message.Data.(*cloudprotocol.RenewCertsNotification)
				if !ok {
					t.Errorf("Incorrect message data format")
				}

				if !reflect.DeepEqual(sentMessage.Certificates, data.Certificates) {
					t.Errorf("Wrong data received: %v %v", sentMessage.Certificates, data.Certificates)
					continue
				}

			default:
				if !reflect.DeepEqual(message.Data, receiveMessage) {
					t.Errorf("Wrong data received: %v %v", message.Data, receiveMessage)
					continue
				}
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

func TestDesiredStatusMessages(t *testing.T) {
	type testDesiredMessages struct {
		sendDesiredStatus    cloudprotocol.DesiredStatus
		expectedDesiredStaus cloudprotocol.DecodedDesiredStatus
		expectedError        error
	}

	testData := []testDesiredMessages{
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("error"),
				Services:     []byte("error"),
				Layers:       []byte("error"),
				Instances:    []byte("error"),
				Components:   []byte("error"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("error"),
				Layers:       []byte("error"),
				Instances:    []byte("error"),
				Components:   []byte("error"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("error"),
				Instances:    []byte("error"),
				Components:   []byte("error"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("layers"),
				Instances:    []byte("error"),
				Components:   []byte("error"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("layers"),
				Instances:    []byte("instances"),
				Components:   []byte("error"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("layers"),
				Instances:    []byte("instances"),
				Components:   []byte("components"),
				FOTASchedule: []byte("error"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("layers"),
				Instances:    []byte("instances"),
				Components:   []byte("components"),
				FOTASchedule: []byte("fotaschedule"),
				SOTASchedule: []byte("error"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{},
			expectedError:        errTimeout,
		},
		{
			sendDesiredStatus: cloudprotocol.DesiredStatus{
				UnitConfig:   []byte("unitconfig"),
				Services:     []byte("services"),
				Layers:       []byte("layers"),
				Instances:    []byte("instances"),
				Components:   []byte("components"),
				FOTASchedule: []byte("fotaschedule"),
				SOTASchedule: []byte("sotashedule"),
			},
			expectedDesiredStaus: cloudprotocol.DecodedDesiredStatus{
				UnitConfig: json.RawMessage([]byte("{}")),
				Components: []cloudprotocol.ComponentInfo{
					{VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "rootfs"},
				},
				Layers: []cloudprotocol.LayerInfo{
					{VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "l1", Digest: "digest"},
				},
				Services: []cloudprotocol.ServiceInfo{
					{VersionInfo: aostypes.VersionInfo{AosVersion: 1}, ID: "serv1", ProviderID: "p1"},
				},
				Instances:    []cloudprotocol.InstanceInfo{{ServiceID: "s1", SubjectID: "subj1", NumInstances: 1}},
				FOTASchedule: cloudprotocol.ScheduleRule{TTL: uint64(100), Type: "type"},
				SOTASchedule: cloudprotocol.ScheduleRule{TTL: uint64(200), Type: "type2"},
			},
			expectedError: nil,
		},
	}

	cryptoContext := &testCryptoContext{}

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %v", err)
	}
	defer amqpHandler.Close()

	if err := amqpHandler.Connect(cryptoContext, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	for _, testDataItem := range testData {
		cryptoContext.decodedData = testDataItem.expectedDesiredStaus

		if err = sendMessage(&cloudprotocol.Message{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.DesiredStatusType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &testDataItem.sendDesiredStatus,
		}); err != nil {
			t.Fatalf("Can't send message: %v", err)
		}

		receivedData, err := waitMessage(amqpHandler.MessageChannel, 500*time.Millisecond)
		if !errors.Is(err, testDataItem.expectedError) {
			t.Errorf("Incorrect wait message error: %v", err)

			continue
		}

		if testDataItem.expectedError != nil {
			continue
		}

		if !reflect.DeepEqual(receivedData, &testDataItem.expectedDesiredStaus) {
			t.Error("Incorrect decoded desired status")
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

	unitConfigData := []cloudprotocol.UnitConfigStatus{{VendorVersion: "1.0"}}

	serviceSetupData := []cloudprotocol.ServiceStatus{
		{ID: "service0", AosVersion: 1, Status: "running", ErrorInfo: nil},
		{
			ID: "service1", AosVersion: 2, Status: "stopped",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "crash"},
		},
		{
			ID: "service2", AosVersion: 3, Status: "unknown",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "unknown"},
		},
	}

	instances := []cloudprotocol.InstanceStatus{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
			AosVersion:    1, StateChecksum: "12345", RunState: "running",
		},
	}

	layersSetupData := []cloudprotocol.LayerStatus{
		{
			ID: "layer0", Digest: "sha256:0", Status: "failed", AosVersion: 1,
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "bad layer"},
		},
		{ID: "layer1", Digest: "sha256:1", Status: "installed", AosVersion: 2},
		{ID: "layer2", Digest: "sha256:2", Status: "installed", AosVersion: 3},
	}

	componentSetupData := []cloudprotocol.ComponentStatus{
		{ID: "rootfs", Status: "installed", VendorVersion: "1.0"},
		{ID: "firmware", Status: "installed", VendorVersion: "5", AosVersion: 6},
		{
			ID: "bootloader", Status: "error", VendorVersion: "100",
			ErrorInfo: &cloudprotocol.ErrorInfo{AosCode: 1, ExitCode: 100, Message: "install error"},
		},
	}

	monitoringData := cloudprotocol.MonitoringData{Timestamp: time.Now().UTC()}
	monitoringData.Global = cloudprotocol.GlobalMonitoringData{
		RAM: 1024, CPU: 50, UsedDisk: 2048, InTraffic: 8192, OutTraffic: 4096,
	}
	monitoringData.ServiceInstances = []cloudprotocol.InstanceMonitoringData{
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
			RAM:           1024, CPU: 50, UsedDisk: 100000,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "service1", SubjectID: "subj1", Instance: 1},
			RAM:           128, CPU: 60, UsedDisk: 200000,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "service2", SubjectID: "subj1", Instance: 1},
			RAM:           256, CPU: 70, UsedDisk: 300000,
		},
		{
			InstanceIdent: aostypes.InstanceIdent{ServiceID: "service3", SubjectID: "subj1", Instance: 1},
			RAM:           512, CPU: 80, UsedDisk: 400000,
		},
	}

	pushServiceLogData := cloudprotocol.PushLog{
		LogID:     "log0",
		PartCount: 2,
		Part:      1,
		Data:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		Error:     "Error",
	}

	alertsData := cloudprotocol.Alerts{
		cloudprotocol.AlertItem{
			Timestamp: time.Now().UTC(),
			Tag:       cloudprotocol.AlertTagSystemError,
			Payload:   map[string]interface{}{"Message": "System error"},
		},
		cloudprotocol.AlertItem{
			Timestamp: time.Now().UTC(),
			Tag:       cloudprotocol.AlertTagSystemError,
			Payload:   map[string]interface{}{"Message": "Service crashed"},
		},
		cloudprotocol.AlertItem{
			Timestamp: time.Now().UTC(),
			Tag:       cloudprotocol.AlertTagResourceValidate,
			Payload:   map[string]interface{}{"Parameter": "cpu", "Value": float64(100)},
		},
	}

	overrideEnvStatus := cloudprotocol.OverrideEnvVarsStatus{
		OverrideEnvVarsStatus: []cloudprotocol.EnvVarsInstanceStatus{
			{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service0", "subject0", -1),
				Statuses: []cloudprotocol.EnvVarStatus{
					{ID: "1234"},
					{ID: "345", Error: "some error"},
				},
			},
			{
				InstanceFilter: cloudprotocol.NewInstanceFilter("service1", "subject1", -1),
				Statuses: []cloudprotocol.EnvVarStatus{
					{ID: "0000"},
				},
			},
		},
	}

	issueCerts := cloudprotocol.IssueUnitCerts{
		Requests: []cloudprotocol.IssueCertData{
			{Type: "online", Csr: "This is online CSR"},
			{Type: "offline", Csr: "This is offline CSR"},
		},
	}

	installCertsConfirmation := cloudprotocol.InstallUnitCertsConfirmation{
		Certificates: []cloudprotocol.InstallCertData{
			{Type: "online", Serial: "1234", Status: "ok", Description: "This is online cert"},
			{Type: "offline", Serial: "1234", Status: "ok", Description: "This is offline cert"},
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
					UnitSubjects: []string{"subject"},
				}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.UnitStatusType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.UnitStatus{
					UnitConfig:   unitConfigData,
					Components:   componentSetupData,
					Layers:       layersSetupData,
					Services:     serviceSetupData,
					Instances:    instances,
					UnitSubjects: []string{"subject"},
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.UnitStatus{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendMonitoringData(monitoringData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.MonitoringDataType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &monitoringData,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.MonitoringData{}
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
					MessageType: cloudprotocol.NewStateType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.NewState{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
					Checksum:      "12345679", State: "This is state",
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.NewState{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendInstanceStateRequest(
					cloudprotocol.StateRequest{
						InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
						Default:       true,
					}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.StateRequestType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.StateRequest{
					InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
					Default:       true,
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.StateRequest{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendLog(pushServiceLogData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.PushLogType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.PushLog{
					LogID:     pushServiceLogData.LogID,
					PartCount: pushServiceLogData.PartCount,
					Part:      pushServiceLogData.Part,
					Data:      pushServiceLogData.Data,
					Error:     pushServiceLogData.Error,
				},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.PushLog{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendAlerts(alertsData))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.AlertsType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &alertsData,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.Alerts{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendIssueUnitCerts(issueCerts.Requests))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.IssueUnitCertsType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &issueCerts,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.IssueUnitCerts{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendInstallCertsConfirmation(installCertsConfirmation.Certificates))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.InstallUnitCertsConfirmationType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &installCertsConfirmation,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.InstallUnitCertsConfirmation{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendOverrideEnvVarsStatus(overrideEnvStatus))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.OverrideEnvVarsStatusType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &overrideEnvStatus,
			},
			getDataType: func() interface{} {
				return &cloudprotocol.OverrideEnvVarsStatus{}
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
			var (
				rawData     json.RawMessage
				receiveData = cloudprotocol.Message{Data: &rawData}
			)

			if err = json.Unmarshal(delivery.Body, &receiveData); err != nil {
				t.Errorf("Error parsing message: %v", err)
				continue
			}

			if !reflect.DeepEqual(receiveData.Header, message.data.Header) {
				t.Errorf("Wrong Header received: %v != %v", receiveData.Header, message.data.Header)
				continue
			}

			decodedMsg := message.getDataType()

			if err = json.Unmarshal(rawData, &decodedMsg); err != nil {
				t.Errorf("Error parsing message: %v", err)
				continue
			}

			if !reflect.DeepEqual(message.data.Data, decodedMsg) {
				t.Errorf("Wrong data received: %v != %v", decodedMsg, message.data.Data)
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
			return aoserrors.Wrap(amqpHandler.SendUnitStatus(cloudprotocol.UnitStatus{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendMonitoringData(cloudprotocol.MonitoringData{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendInstanceNewState(cloudprotocol.NewState{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendInstanceStateRequest(cloudprotocol.StateRequest{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendLog(cloudprotocol.PushLog{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendAlerts(cloudprotocol.Alerts{}))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendIssueUnitCerts(nil))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendInstallCertsConfirmation(nil))
		},
		func() error {
			return aoserrors.Wrap(amqpHandler.SendOverrideEnvVarsStatus(cloudprotocol.OverrideEnvVarsStatus{}))
		},
	}

	for i := 0; i < numMessages; i++ {
		// nolint:gosec // it is enough to use weak random generator in this case
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

	if err := amqpHandler.SendUnitStatus(cloudprotocol.UnitStatus{}); !errors.Is(err, amqphandler.ErrNotConnected) {
		t.Errorf("Wrong error type: %v", err)
	}

	// Send number important messages equals to send channel size - should be accepted without error

	for i := 0; i < sendQueueSize; i++ {
		if err := amqpHandler.SendAlerts(cloudprotocol.Alerts{}); err != nil {
			t.Errorf("Can't send important message: %v", err)
		}
	}

	// Next important message should fail due to send channel size

	if err := amqpHandler.SendInstanceStateRequest(
		cloudprotocol.StateRequest{}); !errors.Is(err, amqphandler.ErrSendChannelFull) {
		t.Errorf("Wrong error type: %v", err)
	}

	if err = amqpHandler.Connect(&testCryptoContext{}, serviceDiscoveryURL, systemID, true); err != nil {
		t.Errorf("Can't establish connection: %v", err)
	}

	// Server should receive pending important messages

	for i := 0; i < sendQueueSize; i++ {
		select {
		case delivery := <-testClient.delivery:
			var message cloudprotocol.Message

			if err = json.Unmarshal(delivery.Body, &message); err != nil {
				t.Errorf("Error parsing message: %v", err)
				continue
			}

			if message.Header.MessageType != cloudprotocol.AlertsType {
				t.Errorf("Wrong message type: %s", message.Header.MessageType)
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
	switch string(input) {
	case "unitconfig":
		output, err = json.Marshal(context.decodedData.UnitConfig)

	case "services":
		output, err = json.Marshal(context.decodedData.Services)

	case "layers":
		output, err = json.Marshal(context.decodedData.Layers)

	case "instances":
		output, err = json.Marshal(context.decodedData.Instances)

	case "components":
		output, err = json.Marshal(context.decodedData.Components)

	case "fotaschedule":
		output, err = json.Marshal(context.decodedData.FOTASchedule)

	case "sotashedule":
		output, err = json.Marshal(context.decodedData.SOTASchedule)

	case "error":

	default:
		err = aoserrors.New("Incorrect desired data")
	}

	return output, aoserrors.Wrap(err)
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

func waitMessage(messageChannel <-chan amqphandler.Message, timeout time.Duration) (amqphandler.Message, error) {
	select {
	case <-time.After(timeout):
		return nil, errTimeout

	case message := <-messageChannel:
		return message, nil
	}
}

func serviceDiscovery(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.Error(w, "not found.", http.StatusNotFound)
		return
	}

	switch r.Method {
	case "POST":
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

	if err := http.ListenAndServe(":8010", nil); err != nil {
		log.Fatal(err)
	}
}
