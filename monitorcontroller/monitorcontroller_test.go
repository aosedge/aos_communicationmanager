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

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/monitorcontroller"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testMonitoringSender struct {
	consumer       amqphandler.ConnectionEventsConsumer
	monitoringData chan cloudprotocol.MonitoringData
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
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	monitorcontroller.MinSendPeriod = 100 * time.Millisecond

	ret := m.Run()

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestSendMonitorData(t *testing.T) {
	sender := newTestMonitoringSender()

	controller, err := monitorcontroller.New(&config.Config{
		Monitoring: config.Monitoring{MaxOfflineMessages: 8},
	}, sender)
	if err != nil {
		t.Fatalf("Can't create monitoring controller: %v", err)
	}
	defer controller.Close()

	sender.consumer.CloudConnected()

	monitoringData := cloudprotocol.MonitoringData{
		Global: cloudprotocol.GlobalMonitoringData{
			RAM:        1100,
			CPU:        35,
			UsedDisk:   2300,
			InTraffic:  150,
			OutTraffic: 150,
		},
	}

	controller.SendMonitoringData(monitoringData)

	receivedMonitoringData, err := sender.waitMonitoringData()
	if err != nil {
		t.Fatalf("Error waiting for monitoring data: %v", err)
	}

	if receivedMonitoringData.Global != monitoringData.Global {
		t.Errorf("Incorrect system monitoring data: %v", receivedMonitoringData.Global)
	}
}

func TestSendMonitorOffline(t *testing.T) {
	const (
		numOfflineMessages = 32
		numExtraMessages   = 16
	)

	sender := newTestMonitoringSender()

	controller, err := monitorcontroller.New(&config.Config{
		Monitoring: config.Monitoring{MaxOfflineMessages: numOfflineMessages},
	}, sender)
	if err != nil {
		t.Fatalf("Can't create monitoring controller: %v", err)
	}
	defer controller.Close()

	var sentData []cloudprotocol.MonitoringData

	for i := 0; i < numOfflineMessages+numExtraMessages; i++ {
		monitoringData := cloudprotocol.MonitoringData{
			Global: cloudprotocol.GlobalMonitoringData{
				RAM:        1100,
				CPU:        35,
				UsedDisk:   uint64(i),
				InTraffic:  150,
				OutTraffic: 150,
			},
		}

		controller.SendMonitoringData(monitoringData)

		sentData = append(sentData, monitoringData)
	}

	if _, err := sender.waitMonitoringData(); err == nil {
		t.Error("Should not be monitoring data received")
	}

	sender.consumer.CloudConnected()

	var receivedData []cloudprotocol.MonitoringData

	for i := 0; i < numOfflineMessages; i++ {
		monitoringData, err := sender.waitMonitoringData()
		if err != nil {
			t.Fatalf("Error waiting for monitoring data: %v", err)
		}

		receivedData = append(receivedData, monitoringData)
	}

	if !reflect.DeepEqual(receivedData, sentData[len(sentData)-numOfflineMessages:]) {
		t.Errorf("Wrong monitoring data received: %v", receivedData)
	}

	if data, err := sender.waitMonitoringData(); err == nil {
		t.Error("Should not be monitoring data received ", data)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newTestMonitoringSender() *testMonitoringSender {
	return &testMonitoringSender{monitoringData: make(chan cloudprotocol.MonitoringData)}
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

func (sender *testMonitoringSender) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) error {
	sender.monitoringData <- monitoringData

	return nil
}

func (sender *testMonitoringSender) waitMonitoringData() (cloudprotocol.MonitoringData, error) {
	select {
	case monitoringData := <-sender.monitoringData:
		return monitoringData, nil

	case <-time.After(1 * time.Second):
		return cloudprotocol.MonitoringData{}, aoserrors.New("wait monitoring data timeout")
	}
}
