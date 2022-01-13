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
	"encoding/json"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Const
 **********************************************************************************************************************/

const (
	inQueueName  = "in_queue"
	outQueueName = "out_queue"
	consumerName = "test_consumer"
	exchangeName = "test_exchange"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type backendClient struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	delivery   <-chan amqp.Delivery
	errChannel chan *amqp.Error
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
		log.Errorf("Can't remove tmp folder: %s", err)
	}
}

func sendMessage(correlationID string, message interface{}) (err error) {
	dataJSON, err := json.Marshal(message)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	log.Debug(string(dataJSON))

	return aoserrors.Wrap(testClient.channel.Publish(
		"",
		outQueueName,
		false,
		false,
		amqp.Publishing{
			CorrelationId: correlationID,
			ContentType:   "text/plain",
			Body:          dataJSON,
		}))
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Error creating service images: %s", err)
	}

	ret := m.Run()

	cleanup()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestSendMessages(t *testing.T) {
	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %s", err)
	}
	defer amqpHandler.Close()

	password, _ := amqpURL.User.Password()

	if err = amqpHandler.ConnectRabbit("testID", amqpURL.Host, amqpURL.User.Username(), password,
		exchangeName, consumerName, outQueueName); err != nil {
		t.Fatalf("Can't connect to server: %s", err)
	}

	testData := []*cloudprotocol.Message{
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.StateAcceptanceType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.StateAcceptance{
				ServiceID: "service0", Checksum: "0123456890", Result: "accepted", Reason: "just because",
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.UpdateStateType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.UpdateState{
				ServiceID: "service1", Checksum: "0993478847", State: "This is new state",
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RequestServiceLogType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RequestServiceLog{
				ServiceID: "service2", LogID: uuid.New().String(), From: &time.Time{}, Till: &time.Time{},
			},
		},
		{
			Header: cloudprotocol.MessageHeader{
				MessageType: cloudprotocol.RequestServiceCrashLogType, Version: cloudprotocol.ProtocolVersion,
			},
			Data: &cloudprotocol.RequestServiceCrashLog{
				ServiceID: "service3", LogID: uuid.New().String(),
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
		correlationID := uuid.New().String()

		if err = sendMessage(correlationID, message); err != nil {
			t.Errorf("Can't send message: %s", err)
			continue
		}

		select {
		case receiveMessage := <-amqpHandler.MessageChannel:
			switch data := receiveMessage.Data.(type) {
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
				if !reflect.DeepEqual(message.Data, receiveMessage.Data) {
					t.Errorf("Wrong data received: %v %v", message.Data, receiveMessage.Data)
					continue
				}
			}

			if correlationID != receiveMessage.CorrelationID {
				t.Errorf("Wrong correlation ID received: %s %s", correlationID, receiveMessage.CorrelationID)
				continue
			}

		case err = <-testClient.errChannel:
			t.Fatalf("AMQP error: %s", err)
			return

		case <-time.After(5 * time.Second):
			t.Error("Waiting data timeout")
			continue
		}
	}
}

func TestReceiveMessages(t *testing.T) {
	systemID := "testID"

	amqpHandler, err := amqphandler.New()
	if err != nil {
		t.Fatalf("Can't create amqp: %s", err)
	}
	defer amqpHandler.Close()

	password, _ := amqpURL.User.Password()

	if err = amqpHandler.ConnectRabbit(systemID, amqpURL.Host, amqpURL.User.Username(), password,
		exchangeName, consumerName, outQueueName); err != nil {
		t.Fatalf("Can't connect to server: %s", err)
	}

	type messageDesc struct {
		correlationID string
		call          func() error
		data          cloudprotocol.Message
		getDataType   func() interface{}
	}

	boardConfigData := []cloudprotocol.BoardConfigInfo{{VendorVersion: "1.0", Status: "installed"}}

	serviceSetupData := []cloudprotocol.ServiceInfo{
		{ID: "service0", AosVersion: 1, Status: "running", Error: "", StateChecksum: "1234567890"},
		{ID: "service1", AosVersion: 2, Status: "stopped", Error: "crash", StateChecksum: "1234567890"},
		{ID: "service2", AosVersion: 3, Status: "unknown", Error: "unknown", StateChecksum: "1234567890"},
	}

	layersSetupData := []cloudprotocol.LayerInfo{
		{ID: "layer0", Digest: "sha256:0", Status: "installed", AosVersion: 1},
		{ID: "layer1", Digest: "sha256:1", Status: "installed", AosVersion: 2},
		{ID: "layer2", Digest: "sha256:2", Status: "installed", AosVersion: 3},
	}

	componentSetupData := []cloudprotocol.ComponentInfo{
		{ID: "rootfs", Status: "installed", VendorVersion: "1.0"},
		{ID: "firmware", Status: "installed", VendorVersion: "5", AosVersion: 6},
		{ID: "bootloader", Status: "installed", VendorVersion: "100"},
	}

	monitoringData := cloudprotocol.MonitoringData{Timestamp: time.Now().UTC()}
	monitoringData.Global = cloudprotocol.GlobalMonitoringData{
		RAM: 1024, CPU: 50, UsedDisk: 2048, InTraffic: 8192, OutTraffic: 4096,
	}
	monitoringData.ServicesData = []cloudprotocol.ServiceMonitoringData{
		{ServiceID: "service0", RAM: 1024, CPU: 50, UsedDisk: 100000},
		{ServiceID: "service1", RAM: 128, CPU: 60, UsedDisk: 200000},
		{ServiceID: "service2", RAM: 256, CPU: 70, UsedDisk: 300000},
		{ServiceID: "service3", RAM: 512, CPU: 80, UsedDisk: 400000},
	}

	sendNewStateCorrelationID := uuid.New().String()

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
			Source:    "system",
			Payload:   map[string]interface{}{"Message": "System error"},
		},
		cloudprotocol.AlertItem{
			Timestamp:  time.Now().UTC(),
			Tag:        cloudprotocol.AlertTagSystemError,
			Source:     "service 1",
			AosVersion: 2,
			Payload:    map[string]interface{}{"Message": "Service crashed"},
		},
		cloudprotocol.AlertItem{
			Timestamp: time.Now().UTC(),
			Tag:       cloudprotocol.AlertTagResource,
			Source:    "system",
			Payload:   map[string]interface{}{"Parameter": "cpu", "Value": float64(100)},
		},
	}

	overrideEnvStatus := []cloudprotocol.EnvVarInfoStatus{
		{ServiceID: "service0", SubjectID: "subject1", Statuses: []cloudprotocol.EnvVarStatus{
			{ID: "1234"},
			{ID: "345", Error: "some error"},
		}},
		{ServiceID: "service1", SubjectID: "subject1", Statuses: []cloudprotocol.EnvVarStatus{
			{ID: "0000"},
		}},
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
					BoardConfig: boardConfigData,
					Components:  componentSetupData,
					Layers:      layersSetupData,
					Services:    serviceSetupData,
				}))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.UnitStatusType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.UnitStatus{
					BoardConfig: boardConfigData,
					Components:  componentSetupData,
					Layers:      layersSetupData,
					Services:    serviceSetupData,
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
			correlationID: sendNewStateCorrelationID,
			call: func() error {
				return aoserrors.Wrap(
					amqpHandler.SendServiceNewState(
						sendNewStateCorrelationID, "service0", "This is state", "12345679"))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.NewStateType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.NewState{ServiceID: "service0", Checksum: "12345679", State: "This is state"},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.NewState{}
			},
		},
		{
			call: func() error {
				return aoserrors.Wrap(amqpHandler.SendServiceStateRequest("service1", true))
			},
			data: cloudprotocol.Message{
				Header: cloudprotocol.MessageHeader{
					MessageType: cloudprotocol.StateRequestType,
					SystemID:    systemID,
					Version:     cloudprotocol.ProtocolVersion,
				},
				Data: &cloudprotocol.StateRequest{ServiceID: "service1", Default: true},
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
				Data: &cloudprotocol.OverrideEnvVarsStatus{OverrideEnvVarsStatus: overrideEnvStatus},
			},
			getDataType: func() interface{} {
				return &cloudprotocol.OverrideEnvVarsStatus{}
			},
		},
	}

	for _, message := range testData {
		if err = message.call(); err != nil {
			t.Errorf("Can't perform call: %s", err)
			continue
		}

		select {
		case delivery := <-testClient.delivery:
			var (
				rawData     json.RawMessage
				receiveData = cloudprotocol.Message{Data: &rawData}
			)

			if err = json.Unmarshal(delivery.Body, &receiveData); err != nil {
				t.Errorf("Error parsing message: %s", err)
				continue
			}

			if message.correlationID != delivery.CorrelationId {
				t.Errorf("Wrong correlation ID received: %s %s", message.correlationID, delivery.CorrelationId)
			}

			if !reflect.DeepEqual(receiveData.Header, message.data.Header) {
				t.Errorf("Wrong Header received: %v != %v", receiveData.Header, message.data.Header)
				continue
			}

			decodedMsg := message.getDataType()

			if err = json.Unmarshal(rawData, &decodedMsg); err != nil {
				t.Errorf("Error parsing message: %s", err)
				continue
			}

			if !reflect.DeepEqual(message.data.Data, decodedMsg) {
				t.Errorf("Wrong data received: %v != %v", decodedMsg, message.data.Data)
			}

		case err = <-testClient.errChannel:
			t.Fatalf("AMQP error: %s", err)
			return

		case <-time.After(5 * time.Second):
			t.Error("Waiting data timeout")
			continue
		}
	}
}
