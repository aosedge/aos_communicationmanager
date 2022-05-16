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

package alerts_test

import (
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/alerts"
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

type testSender struct {
	alertsChannel chan cloudprotocol.Alerts
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var errTimeout = errors.New("timeout")

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestAlertsMaxOfflineandMessages(t *testing.T) {
	sender := newTestSender()

	alertsHandler, err := alerts.New(config.Alerts{
		SendPeriod:         aostypes.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	},
		sender)
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	expectedAlerts := cloudprotocol.Alerts{}
	testTime := time.Now()

	// alertlen = header 96 + message in bytes*6 (\\u0000)
	alertItem := cloudprotocol.AlertItem{
		Timestamp: testTime,
		Tag:       cloudprotocol.AlertTagSystemError,
		Payload:   cloudprotocol.SystemAlert{Message: string(make([]byte, 150))},
	}

	expectedAlerts = append(expectedAlerts, alertItem)

	for i := 0; i < 2; i++ {
		if i != 0 {
			alertsHandler.SendAlert(cloudprotocol.AlertItem{
				Timestamp: testTime,
				Tag:       cloudprotocol.AlertTagSystemError,
				Payload:   cloudprotocol.SystemAlert{Message: string(make([]byte, 150)) + strconv.Itoa(i)},
			})

			continue
		}

		alertsHandler.SendAlert(alertItem)
	}

	alerts, err := sender.waitResult(2*time.Second, testTime)
	if err != nil {
		t.Fatalf("Wait alerts error: %v", err)
	}

	if !reflect.DeepEqual(alerts, expectedAlerts) {
		t.Error("Incorrect alerts")
	}
}

func TestAlertsDublicationMessages(t *testing.T) {
	sender := newTestSender()

	alertsHandler, err := alerts.New(config.Alerts{
		SendPeriod:         aostypes.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	},
		sender)
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	expectedAlerts := cloudprotocol.Alerts{}
	testTime := time.Now()

	alertItem := cloudprotocol.AlertItem{
		Timestamp: testTime,
		Tag:       cloudprotocol.AlertTagSystemError,
		Payload:   cloudprotocol.SystemAlert{Message: "alert"},
	}

	expectedAlerts = append(expectedAlerts, alertItem)

	for i := 0; i < 2; i++ {
		alertsHandler.SendAlert(alertItem)
	}

	alerts, err := sender.waitResult(2*time.Second, testTime)
	if err != nil {
		t.Fatalf("Wait alerts error: %v", err)
	}

	if !reflect.DeepEqual(alerts, expectedAlerts) {
		t.Error("Incorrect alerts")
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (sender *testSender) SendAlerts(alerts cloudprotocol.Alerts) (err error) {
	sender.alertsChannel <- alerts

	return nil
}

func (sender *testSender) waitResult(timeout time.Duration, testTime time.Time) (cloudprotocol.Alerts, error) {
	for {
		select {
		case alerts := <-sender.alertsChannel:
			for i := range alerts {
				alerts[i].Timestamp = testTime
			}

			return alerts, nil

		case <-time.After(timeout):
			return nil, errTimeout
		}
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newTestSender() (sender *testSender) {
	sender = &testSender{
		alertsChannel: make(chan cloudprotocol.Alerts, 1),
	}

	return sender
}
