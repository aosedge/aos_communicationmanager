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

package monitorcontroller_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/monitorcontroller"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testMonitoringSender struct {
	consumer       amqphandler.ConnectionEventsConsumer
	monitoringData chan cloudprotocol.Monitoring
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

func TestSendMonitorData(t *testing.T) {
	sender := newTestMonitoringSender()

	controller, err := monitorcontroller.New(&config.Config{
		Monitoring: config.Monitoring{MaxOfflineMessages: 8, SendPeriod: aostypes.Duration{Duration: 1 * time.Second}},
	}, sender)
	if err != nil {
		t.Fatalf("Can't create monitoring controller: %v", err)
	}
	defer controller.Close()

	sender.consumer.CloudConnected()

	inputData, expectedData := getTestMonitoringData()
	controller.SendNodeMonitoring(inputData)

	receivedMonitoringData, err := sender.waitMonitoringData()
	if err != nil {
		t.Fatalf("Error waiting for monitoring data: %v", err)
	}

	if !reflect.DeepEqual(receivedMonitoringData, expectedData) {
		t.Errorf("Incorrect monitoring data: %v", receivedMonitoringData)
	}
}

func TestSendMonitorOffline(t *testing.T) {
	const (
		numOfflineMessages = 2
		numExtraMessages   = 1
		maxMessageSize     = 400
	)

	sender := newTestMonitoringSender()

	controller, err := monitorcontroller.New(
		&config.Config{Monitoring: config.Monitoring{
			MaxOfflineMessages: numOfflineMessages,
			SendPeriod:         aostypes.Duration{Duration: 1 * time.Second},
			MaxMessageSize:     maxMessageSize,
		}}, sender)
	if err != nil {
		t.Fatalf("Can't create monitoring controller: %v", err)
	}
	defer controller.Close()

	var sentData []cloudprotocol.Monitoring

	for i := 0; i < numOfflineMessages+numExtraMessages; i++ {
		inputData, expectedData := getTestMonitoringData()

		controller.SendNodeMonitoring(inputData)

		sentData = append(sentData, expectedData)
	}

	if _, err := sender.waitMonitoringData(); err == nil {
		t.Error("Should not be monitoring data received")
	}

	sender.consumer.CloudConnected()

	expectedList := sentData[len(sentData)-numOfflineMessages:]
	for _, message := range expectedList {
		receivedData, err := sender.waitMonitoringData()
		if err != nil {
			t.Fatalf("Error waiting for monitoring data: %v", err)
			return
		}

		if !reflect.DeepEqual(receivedData, message) {
			t.Errorf("Wrong monitoring data received: %v", receivedData)
			return
		}
	}

	if data, err := sender.waitMonitoringData(); err == nil {
		t.Error("Should not be monitoring data received ", data)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newTestMonitoringSender() *testMonitoringSender {
	return &testMonitoringSender{monitoringData: make(chan cloudprotocol.Monitoring)}
}

func (sender *testMonitoringSender) SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
	sender.consumer = consumer

	return nil
}

func (sender *testMonitoringSender) UnsubscribeFromConnectionEvents(
	consumer amqphandler.ConnectionEventsConsumer,
) error {
	return nil
}

func (sender *testMonitoringSender) SendMonitoringData(monitoringData cloudprotocol.Monitoring) error {
	sender.monitoringData <- monitoringData

	return nil
}

func (sender *testMonitoringSender) waitMonitoringData() (cloudprotocol.Monitoring, error) {
	select {
	case monitoringData := <-sender.monitoringData:
		return monitoringData, nil

	case <-time.After(2 * time.Second):
		return cloudprotocol.Monitoring{}, aoserrors.New("wait monitoring data timeout")
	}
}

func getTestMonitoringData() (aostypes.NodeMonitoring, cloudprotocol.Monitoring) {
	timestamp := time.Now().UTC()
	nodeMonitoring := cloudprotocol.NodeMonitoringData{
		NodeID: "mainNode",
		Items: []aostypes.MonitoringData{
			{
				RAM: 1024, CPU: 50, Download: 8192, Upload: 4096, Timestamp: timestamp,
				Disk: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
			},
		},
	}

	instanceMonitoring := cloudprotocol.InstanceMonitoringData{
		NodeID:        "mainNode",
		InstanceIdent: aostypes.InstanceIdent{ServiceID: "service0", SubjectID: "subj1", Instance: 1},
		Items: []aostypes.MonitoringData{
			{
				RAM: 1024, CPU: 50, Timestamp: timestamp,
				Disk: []aostypes.PartitionUsage{{Name: "p1", UsedSize: 100}},
			},
		},
	}

	input := aostypes.NodeMonitoring{
		NodeID:   "mainNode",
		NodeData: nodeMonitoring.Items[0],
		InstancesData: []aostypes.InstanceMonitoring{
			{InstanceIdent: instanceMonitoring.InstanceIdent, MonitoringData: instanceMonitoring.Items[0]},
		},
	}

	output := cloudprotocol.Monitoring{
		Nodes:            []cloudprotocol.NodeMonitoringData{nodeMonitoring},
		ServiceInstances: []cloudprotocol.InstanceMonitoringData{instanceMonitoring},
	}

	return input, output
}
