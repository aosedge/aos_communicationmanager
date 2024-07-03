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
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/alerts"
	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
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
	consumer      amqphandler.ConnectionEventsConsumer
	alertsChannel chan cloudprotocol.Alerts
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var errTimeout = errors.New("timeout")

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestAlertsMaxMessageSize(t *testing.T) {
	const numMessages = 5

	sender := newTestSender()

	alertsHandler, err := alerts.New(config.Alerts{
		SendPeriod:         aostypes.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     512,
		MaxOfflineMessages: 32,
	},
		sender)
	if err != nil {
		t.Fatalf("Can't create alerts: %v", err)
	}
	defer alertsHandler.Close()

	sender.consumer.CloudConnected()

	expectedAlerts := cloudprotocol.Alerts{}

	for i := 0; i < 2; i++ {
		// alert size = header 96 + message length
		alertItem := cloudprotocol.AlertItem{
			Timestamp: time.Now(),
			Tag:       cloudprotocol.AlertTagSystemError,
			Payload:   cloudprotocol.SystemAlert{Message: randomString(200)},
		}

		alertsHandler.SendAlert(alertItem)

		expectedAlerts.Items = append(expectedAlerts.Items, alertItem)
	}

	for i := 2; i < numMessages; i++ {
		// alert size = header 96 + message length
		alertItem := cloudprotocol.AlertItem{
			Timestamp: time.Now(),
			Tag:       cloudprotocol.AlertTagSystemError,
			Payload:   cloudprotocol.SystemAlert{Message: randomString(200)},
		}

		alertsHandler.SendAlert(alertItem)
	}

	alerts, err := sender.waitResult(2 * time.Second)
	if err != nil {
		t.Fatalf("Wait alerts error: %v", err)
	}

	if !reflect.DeepEqual(alerts, expectedAlerts) {
		t.Error("Incorrect alerts")
	}
}

func TestAlertsDuplicationMessages(t *testing.T) {
	sender := newTestSender()

	alertsHandler, err := alerts.New(config.Alerts{
		SendPeriod:         aostypes.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	},
		sender)
	if err != nil {
		t.Fatalf("Can't create alerts: %v", err)
	}
	defer alertsHandler.Close()

	sender.consumer.CloudConnected()

	expectedAlerts := cloudprotocol.Alerts{}

	alertItem := cloudprotocol.AlertItem{
		Timestamp: time.Now(),
		Tag:       cloudprotocol.AlertTagSystemError,
		Payload:   cloudprotocol.SystemAlert{Message: randomString(32)},
	}

	expectedAlerts.Items = append(expectedAlerts.Items, alertItem)

	for i := 0; i < 2; i++ {
		alertsHandler.SendAlert(alertItem)
	}

	alerts, err := sender.waitResult(2 * time.Second)
	if err != nil {
		t.Fatalf("Wait alerts error: %v", err)
	}

	if !reflect.DeepEqual(alerts, expectedAlerts) {
		t.Error("Incorrect alerts")
	}
}

func TestAlertsOfflineMessages(t *testing.T) {
	const (
		numOfflineMessages = 32
		numExtraMessages   = 8
	)

	sender := newTestSender()

	alertsHandler, err := alerts.New(config.Alerts{
		SendPeriod:         aostypes.Duration{Duration: 100 * time.Millisecond},
		MaxMessageSize:     256,
		MaxOfflineMessages: numOfflineMessages,
	},
		sender)
	if err != nil {
		t.Fatalf("Can't create alerts: %v", err)
	}
	defer alertsHandler.Close()

	expectedAlerts := cloudprotocol.Alerts{}

	for i := 0; i < numOfflineMessages; i++ {
		alertItem := cloudprotocol.AlertItem{
			Timestamp: time.Now(),
			Tag:       cloudprotocol.AlertTagSystemError,
			Payload:   cloudprotocol.SystemAlert{Message: randomString(200)},
		}

		expectedAlerts.Items = append(expectedAlerts.Items, alertItem)

		alertsHandler.SendAlert(alertItem)
	}

	for i := 0; i < numExtraMessages; i++ {
		alertItem := cloudprotocol.AlertItem{
			Timestamp: time.Now(),
			Tag:       cloudprotocol.AlertTagAosCore,
			Payload:   cloudprotocol.SystemAlert{Message: randomString(200)},
		}

		alertsHandler.SendAlert(alertItem)
	}

	// Wait all offline messages are processed: 100 msec for each message + 1 second guard
	if _, err := sender.waitResult(
		numOfflineMessages*100*time.Millisecond + 1*time.Second); !errors.Is(err, errTimeout) {
		t.Error("Timeout error expected")
	}

	sender.consumer.CloudConnected()

	receivedAlerts := cloudprotocol.Alerts{}

	for i := 0; i < numOfflineMessages; i++ {
		alerts, err := sender.waitResult(2 * time.Second)
		if err != nil {
			t.Fatalf("Wait alerts error: %v", err)
		}

		receivedAlerts.Items = append(receivedAlerts.Items, alerts.Items...)
	}

	if !reflect.DeepEqual(receivedAlerts, expectedAlerts) {
		t.Error("Incorrect alerts")
	}

	if _, err := sender.waitResult(1 * time.Second); !errors.Is(err, errTimeout) {
		t.Error("Timeout error expected")
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func newTestSender() (sender *testSender) {
	sender = &testSender{
		alertsChannel: make(chan cloudprotocol.Alerts, 1),
	}

	return sender
}

func (sender *testSender) SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
	sender.consumer = consumer

	return nil
}

func (sender *testSender) UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error {
	return nil
}

func (sender *testSender) SendAlerts(alerts cloudprotocol.Alerts) (err error) {
	sender.alertsChannel <- alerts

	return nil
}

func (sender *testSender) waitResult(timeout time.Duration) (cloudprotocol.Alerts, error) {
	for {
		select {
		case alerts := <-sender.alertsChannel:
			return alerts, nil

		case <-time.After(timeout):
			return cloudprotocol.Alerts{}, errTimeout
		}
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func randomString(n int) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

	s := make([]rune, n)

	for i := range s {
		s[i] = letters[rand.Intn(len(letters))] //nolint:gosec // weak rand is ok in this case
	}

	return string(s)
}
