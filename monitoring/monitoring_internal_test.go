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

package monitoring

import (
	"os"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
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
 * Types
 **********************************************************************************************************************/

type testAlerts struct {
	callback func(serviceID, resource string, time time.Time, value uint64)
}

type testSender struct {
	monitoringChannel chan cloudprotocol.MonitoringData
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestAlertProcessor(t *testing.T) {
	var sourceValue uint64
	destination := make([]uint64, 0, 2)

	alert := createAlertProcessor(
		"Test",
		&sourceValue,
		func(time time.Time, value uint64) {
			log.Debugf("T: %s, %d", time, value)
			destination = append(destination, value)
		},
		config.AlertRule{
			MinTimeout:   config.Duration{Duration: 3 * time.Second},
			MinThreshold: 80,
			MaxThreshold: 90,
		})

	values := []uint64{50, 91, 79, 92, 93, 94, 95, 94, 79, 91, 92, 93, 94, 32, 91, 92, 93, 94, 95, 96}
	alertsCount := []int{0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3}

	currentTime := time.Now()

	for i, value := range values {
		sourceValue = value
		alert.checkAlertDetection(currentTime)
		if alertsCount[i] != len(destination) {
			t.Errorf("Wrong alert count %d at %d", len(destination), i)
		}
		currentTime = currentTime.Add(time.Second)
	}
}

func TestPeriodicReport(t *testing.T) {
	sendDuration := 2 * time.Second
	testSender := newTestSender()

	monitor, err := New(&config.Config{
		WorkingDir: ".",
		Monitoring: config.Monitoring{
			EnableSystemMonitoring: true,
			MaxOfflineMessages:     10,
			SendPeriod:             config.Duration{Duration: sendDuration},
			PollPeriod:             config.Duration{Duration: 1 * time.Second},
		},
	},
		nil, nil, testSender)
	if err != nil {
		t.Fatalf("Can't create monitoring instance: %s", err)
	}
	defer monitor.Close()

	numSends := 3
	sendTime := time.Now()

	for {
		if _, err := testSender.waitResult(sendDuration * 2); err != nil {
			t.Fatalf("Can't wait monitoring result: %s", err)
		}

		currentTime := time.Now()

		period := currentTime.Sub(sendTime)
		// check is period in +-10% range
		if period > sendDuration*110/100 || period < sendDuration*90/100 {
			t.Errorf("Period mismatch: %s", period)
		}

		sendTime = currentTime

		numSends--
		if numSends == 0 {
			return
		}
	}
}

func TestSystemAlerts(t *testing.T) {
	sendDuration := 1 * time.Second
	testSender := newTestSender()

	alertMap := make(map[string]int)

	monitor, err := New(
		&config.Config{
			WorkingDir: ".",
			Monitoring: config.Monitoring{
				EnableSystemMonitoring: true,
				MaxOfflineMessages:     10,
				SendPeriod:             config.Duration{Duration: sendDuration},
				PollPeriod:             config.Duration{Duration: 1 * time.Second},
				CPU: &config.AlertRule{
					MinTimeout:   config.Duration{},
					MinThreshold: 0,
					MaxThreshold: 0,
				},
				RAM: &config.AlertRule{
					MinTimeout:   config.Duration{},
					MinThreshold: 0,
					MaxThreshold: 0,
				},
				UsedDisk: &config.AlertRule{
					MinTimeout:   config.Duration{},
					MinThreshold: 0,
					MaxThreshold: 0,
				},
				InTraffic: &config.AlertRule{
					MinTimeout:   config.Duration{},
					MinThreshold: 0,
					MaxThreshold: 0,
				},
				OutTraffic: &config.AlertRule{
					MinTimeout:   config.Duration{},
					MinThreshold: 0,
					MaxThreshold: 0,
				},
			},
		},
		&testAlerts{callback: func(serviceID, resource string, time time.Time, value uint64) {
			alertMap[resource] = alertMap[resource] + 1
		}}, nil, testSender)
	if err != nil {
		t.Fatalf("Can't create monitoring instance: %s", err)
	}
	defer monitor.Close()

	if _, err := testSender.waitResult(sendDuration * 2); err != nil {
		t.Fatalf("Can't wait monitoring result: %s", err)
	}

	for resource, numAlerts := range alertMap {
		if numAlerts != 1 {
			t.Errorf("Wrong number of %s alerts: %d", resource, numAlerts)
		}
	}

	if len(alertMap) != 5 {
		t.Error("Not enough alerts")
	}
}

/*******************************************************************************
 * Interfaces
 ******************************************************************************/

func (instance *testAlerts) SendResourceAlert(source, resource string, time time.Time, value uint64) {
	instance.callback(source, resource, time, value)
}

func newTestSender() (sender *testSender) {
	sender = &testSender{
		monitoringChannel: make(chan cloudprotocol.MonitoringData, 1),
	}

	return sender
}

func (sender *testSender) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error) {
	sender.monitoringChannel <- monitoringData

	return nil
}

func (sender *testSender) waitResult(timeout time.Duration) (monitoringData cloudprotocol.MonitoringData, err error) {
	for {
		select {
		case monitoringData = <-sender.monitoringChannel:
			return monitoringData, nil

		case <-time.After(timeout):
			return monitoringData, aoserrors.New("wait result timeout")
		}
	}
}
