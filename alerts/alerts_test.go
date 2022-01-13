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
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log/syslog"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-systemd/journal"
	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/alerts"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/*******************************************************************************
 * Init
 ******************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/*******************************************************************************
 * Types
 ******************************************************************************/

type testCursorStorage struct {
	cursor string
}

type testSender struct {
	alertsChannel chan cloudprotocol.Alerts
}

/*******************************************************************************
 * Vars
 ******************************************************************************/

var (
	systemd    *dbus.Conn
	errTimeout = errors.New("timeout")
	tmpDir     string
)

/*******************************************************************************
 * Main
 ******************************************************************************/

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	cleanup()

	os.Exit(ret)
}

/*******************************************************************************
 * Tests
 ******************************************************************************/

func TestGetSystemError(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	sysLog, err := syslog.New(syslog.LOG_CRIT, "")
	if err != nil {
		t.Fatalf("Can't create syslog: %s", err)
	}
	defer sysLog.Close()

	// Check crit message received

	messages := make([]string, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		messages = append(messages, uuid.New().String())

		if err = sysLog.Crit(messages[len(messages)-1]); err != nil {
			t.Errorf("Can't write to syslog: %s", err)
		}

		time.Sleep(500 * time.Millisecond)
	}

	if err = testSender.waitAlerts(5*time.Second, cloudprotocol.AlertTagSystemError, "system", messages); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// Check non crit message not received

	messages = make([]string, 0, numMessages)

	messages = append(messages, uuid.New().String())
	if err = sysLog.Warning(messages[len(messages)-1]); err != nil {
		t.Errorf("Can't write to syslog: %s", err)
	}

	messages = append(messages, uuid.New().String())
	if err = sysLog.Notice(messages[len(messages)-1]); err != nil {
		t.Errorf("Can't write to syslog: %s", err)
	}

	messages = append(messages, uuid.New().String())
	if err = sysLog.Info(messages[len(messages)-1]); err != nil {
		t.Errorf("Can't write to syslog: %s", err)
	}

	messages = append(messages, uuid.New().String())
	if err = sysLog.Debug(messages[len(messages)-1]); err != nil {
		t.Errorf("Can't write to syslog: %s", err)
	}

	if err = testSender.waitResult(5*time.Second,
		func(alert cloudprotocol.AlertItem) (success bool, err error) {
			if alert.Tag == cloudprotocol.AlertTagSystemError {
				for _, originMessage := range messages {
					systemAlert, ok := (alert.Payload.(cloudprotocol.SystemAlert))
					if !ok {
						return false, errors.New("wrong alert type")
					}

					if originMessage == systemAlert.Message {
						return false, fmt.Errorf("unexpected message: %s", systemAlert.Message)
					}
				}
			}

			return false, nil
		}); err != nil && err != errTimeout {
		t.Errorf("Result failed: %s", err)
	}
}

func TestGetOfflineSystemError(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	cursorStorage := &testCursorStorage{}

	// Open and close to store cursor
	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	}}, testSender, cursorStorage)
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	alertsHandler.Close()

	// Send offline messages

	sysLog, err := syslog.New(syslog.LOG_CRIT, "")
	if err != nil {
		t.Fatalf("Can't create syslog: %s", err)
	}
	defer sysLog.Close()

	messages := make([]string, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		messages = append(messages, uuid.New().String())

		if err = sysLog.Emerg(messages[len(messages)-1]); err != nil {
			t.Errorf("Can't write to syslog: %s", err)
		}
	}

	// Wait sys logs are flushed to systemd journal
	time.Sleep(2 * time.Second)

	// Open again
	alertsHandler, err = alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
	}}, testSender, cursorStorage)
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	// Check all offline messages are handled
	if err = testSender.waitAlerts(5*time.Second, cloudprotocol.AlertTagSystemError, "system", messages); err != nil {
		t.Errorf("Result failed: %s", err)
	}
}

func TestGetDowloadsStatusAlerts(t *testing.T) {
	testSender := newTestSender()

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     2048,
		MaxOfflineMessages: 32,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	type downloadAlert struct {
		source          string
		message         string
		progress        string
		url             string
		downloadedBytes string
		totalBytes      string
	}

	downloadStatus := alerts.DownloadStatus{
		Source:     "downloader",
		URL:        "https://testurl",
		TotalBytes: 157_286_400,
	} // 150 MB in binary format

	// send download started alert status
	originAlert := downloadAlert{
		source:          "downloader",
		message:         "Download started",
		progress:        "0%",
		url:             downloadStatus.URL,
		downloadedBytes: "0B",
		totalBytes:      "150M",
	}

	proccessAlertFunc := func(alert cloudprotocol.AlertItem) (success bool, err error) {
		if alert.Tag != cloudprotocol.AlertTagAosCore {
			return false, nil
		}

		receivedAlert, ok := (alert.Payload.(cloudprotocol.DownloadAlert))
		if !ok {
			return false, errors.New("wrong alert type")
		}

		receivedItem := downloadAlert{
			source:          alert.Source,
			message:         receivedAlert.Message,
			progress:        receivedAlert.Progress,
			url:             receivedAlert.URL,
			downloadedBytes: receivedAlert.DownloadedBytes,
			totalBytes:      receivedAlert.TotalBytes,
		}

		if receivedItem == originAlert {
			return true, nil
		}
		return false, nil
	}

	alertsHandler.SendDownloadStartedAlert(downloadStatus)

	if err = testSender.waitResult(5*time.Second,
		proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// send download interrupted alert status
	reason := "problem with Internet connection"
	downloadStatus.DownloadedBytes = 15_728_640 // 15 MB
	downloadStatus.Progress = 10

	originAlert = downloadAlert{
		source:          "downloader",
		message:         "Download interrupted reason: " + reason,
		progress:        strconv.Itoa(downloadStatus.Progress) + "%",
		url:             downloadStatus.URL,
		downloadedBytes: "15M",
		totalBytes:      "150M",
	}

	alertsHandler.SendDownloadInterruptedAlert(downloadStatus, reason)

	if err = testSender.waitResult(5*time.Second, proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// send download resume alert status
	reason = "Internet connection has been fixed"
	originAlert.message = "Download resumed reason: " + reason
	alertsHandler.SendDownloadResumedAlert(downloadStatus, reason)

	if err = testSender.waitResult(5*time.Second, proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// send status download alert
	downloadStatus.DownloadedBytes = 31_457_280 // 30 MB
	downloadStatus.Progress = 20

	originAlert = downloadAlert{
		source:          "downloader",
		message:         "Download status",
		progress:        strconv.Itoa(downloadStatus.Progress) + "%",
		url:             downloadStatus.URL,
		downloadedBytes: "30M",
		totalBytes:      "150M",
	}

	alertsHandler.SendDownloadStatusAlert(downloadStatus)

	if err = testSender.waitResult(5*time.Second, proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// send success download finished alert status
	downloadCode := 200
	downloadStatus.DownloadedBytes = 157_286_400 // 150 MB
	downloadStatus.Progress = 100

	originAlert = downloadAlert{
		source:          "downloader",
		message:         "Download finished code: " + strconv.Itoa(downloadCode),
		progress:        strconv.Itoa(downloadStatus.Progress) + "%",
		url:             downloadStatus.URL,
		downloadedBytes: "150M",
		totalBytes:      "150M",
	}

	alertsHandler.SendDownloadFinishedAlert(downloadStatus, downloadCode)

	if err = testSender.waitResult(5*time.Second, proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}

	// send failed download finished alert status
	downloadCode = 301
	downloadStatus.DownloadedBytes = 52_428_800 // 50 MB
	downloadStatus.Progress = 50

	originAlert = downloadAlert{
		source:          "downloader",
		message:         "Download finished code: " + strconv.Itoa(downloadCode),
		progress:        strconv.Itoa(downloadStatus.Progress) + "%",
		url:             downloadStatus.URL,
		downloadedBytes: "50M",
		totalBytes:      "150M",
	}

	alertsHandler.SendDownloadFinishedAlert(downloadStatus, downloadCode)

	if err = testSender.waitResult(5*time.Second, proccessAlertFunc); err != nil {
		t.Errorf("Result failed: %s", err)
	}
}

func TestGetCommunicationManagerAlerts(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	messages := make([]string, 0, numMessages)

	for i := 0; i < numMessages; i++ {
		messages = append(messages, uuid.New().String())
	}

	command := fmt.Sprintf(
		"/usr/bin/systemd-cat -p3 /bin/bash -c 'for message in %s ; do echo $message ; done'",
		strings.Join(messages, " "))

	if err := createSystemdUnit("oneshot", command,
		path.Join(tmpDir, "aos-communicationmanager.service")); err != nil {
		t.Fatalf("Can't create systemd unit: %s", err)
	}

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     2048,
		MaxOfflineMessages: 32,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	if err = startSystemdUnit("aos-communicationmanager.service"); err != nil {
		t.Fatalf("Can't start systemd unit: %s", err)
	}

	if err = testSender.waitAlerts(5*time.Second, cloudprotocol.AlertTagAosCore,
		"aos-communicationmanager.service", messages); err != nil {
		t.Errorf("Result failed: %s", err)
	}
}

func TestAlertsMaxMessageSize(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	// the size of one message ~154 bytes:
	// {"timestamp":"2019-03-28T16:54:58.500221705+02:00","tag":"aosCore","source":"servicemanager",
	//  "payload":{"message":"884a0472-5ce3-4da6-acff-088ce3959cd3"}}
	// Set MaxMessageSize to 500 to allow only 3 messages to come
	const numExpectedMessages = 3

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     500,
		MaxOfflineMessages: 32,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	for i := 0; i < numMessages; i++ {
		// One message size is: timestamp 24 + tag "system" 6 + source "servicemanager" 14 + uuid 36 = 80
		if err := journal.Send(uuid.New().String(), journal.PriErr, nil); err != nil {
			t.Errorf("Can't send journal log: %s", err)
		}
	}

	select {
	case alerts := <-testSender.alertsChannel:
		if len(alerts) != numExpectedMessages {
			t.Errorf("Wrong message count received: %d", len(alerts))
		}

	case <-time.After(5 * time.Second):
		t.Errorf("Result failed: %s", errTimeout)
	}
}

func TestAlertsMaxOfflineMessages(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 3,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	for i := 0; i < numMessages; i++ {
		if err := journal.Send(uuid.New().String(), journal.PriErr, nil); err != nil {
			t.Errorf("Can't send journal log: %s", err)
		}

		time.Sleep(1500 * time.Millisecond)
	}

	messageCount := 0

	for {
		select {
		case <-testSender.alertsChannel:
			messageCount++

		case <-time.After(1 * time.Second):
			if messageCount != 3 {
				t.Errorf("Wrong message count received: %d", messageCount)
			}
			return
		}
	}
}

func TestDuplicateAlerts(t *testing.T) {
	const numMessages = 5
	testSender := newTestSender()

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 25,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	for i := 0; i < numMessages; i++ {
		if err := journal.Send("This is error message", journal.PriErr, nil); err != nil {
			t.Errorf("Can't send journal log: %s", err)
		}
	}

	select {
	case alerts := <-testSender.alertsChannel:
		if len(alerts) != 1 {
			t.Errorf("Wrong message count received: %d", len(alerts))
		}

	case <-time.After(5 * time.Second):
		t.Errorf("Result failed: %s", errTimeout)
	}
}

func TestMessageFilter(t *testing.T) {
	testSender := newTestSender()

	filter := []string{"test", "regexp"}

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		EnableSystemAlerts: true,
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
		Filter:             filter,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	sysLog, err := syslog.New(syslog.LOG_CRIT, "")
	if err != nil {
		t.Fatalf("Can't create syslog: %s", err)
	}
	defer sysLog.Close()

	validMessage := "message should not be filterout"
	messages := []string{"test mesage to filterout", validMessage, "regexp mesage to filterout"}

	for _, msg := range messages {
		if err = sysLog.Crit(msg); err != nil {
			t.Errorf("Can't write to syslog: %s", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	foundCount := 0

	for i := 0; i < 3; i++ {
		err := testSender.waitResult(1*time.Second,
			func(alert cloudprotocol.AlertItem) (success bool, err error) {
				systemAlert, ok := (alert.Payload.(cloudprotocol.SystemAlert))
				if !ok {
					return false, errors.New("wrong alert type")
				}

				if systemAlert.Message != validMessage {
					return false, errors.New("Receive unexpected alert mesage")
				}

				return true, nil
			})

		if err == nil {
			foundCount++
			continue
		}

		if err != errTimeout {
			t.Errorf("Result failed: %s", err)
		}
	}

	if foundCount != 1 {
		t.Errorf("Incorrect count of received alerts count = %d", foundCount)
	}
}

func TestWrongFilter(t *testing.T) {
	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     1024,
		MaxOfflineMessages: 32,
		Filter:             []string{"", "*(test)^"},
	}}, newTestSender(), &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()
}

func TestGetResourceAlerts(t *testing.T) {
	testSender := newTestSender()

	alertsHandler, err := alerts.New(&config.Config{Alerts: config.Alerts{
		SendPeriod:         config.Duration{Duration: 1 * time.Second},
		MaxMessageSize:     2048,
		MaxOfflineMessages: 32,
	}}, testSender, &testCursorStorage{})
	if err != nil {
		t.Fatalf("Can't create alerts: %s", err)
	}
	defer alertsHandler.Close()

	type resourceAlert struct {
		source   string
		resource string
		time     time.Time
		value    uint64
	}

	resourceAlerts := []resourceAlert{
		{"system", "ram", time.Now(), 93},
		{"system", "ram", time.Now(), 1500},
		{"system", "ram", time.Now(), 1600},
	}

	for _, alert := range resourceAlerts {
		alertsHandler.SendResourceAlert(alert.source, alert.resource, alert.time, alert.value)
	}

	if err = testSender.waitResult(5*time.Second,
		func(alert cloudprotocol.AlertItem) (success bool, err error) {
			if alert.Tag != cloudprotocol.AlertTagResource {
				return false, nil
			}

			for i, originItem := range resourceAlerts {
				receivedAlert, ok := (alert.Payload.(cloudprotocol.ResourceAlert))
				if !ok {
					return false, errors.New("wrong alert type")
				}

				receivedItem := resourceAlert{
					source:   alert.Source,
					resource: receivedAlert.Parameter,
					time:     alert.Timestamp,
					value:    receivedAlert.Value,
				}

				if receivedItem == originItem {
					resourceAlerts = append(resourceAlerts[:i], resourceAlerts[i+1:]...)

					if len(resourceAlerts) == 0 {
						return true, nil
					}

					return false, nil
				}
			}

			return false, nil
		}); err != nil {
		t.Errorf("Result failed: %s", err)
	}
}

/*******************************************************************************
 * Interfaces
 ******************************************************************************/

func (cursorStorage *testCursorStorage) SetJournalCursor(cursor string) (err error) {
	cursorStorage.cursor = cursor

	return nil
}

func (cursorStorage *testCursorStorage) GetJournalCursor() (cursor string, err error) {
	return cursorStorage.cursor, nil
}

func newTestSender() (sender *testSender) {
	sender = &testSender{
		alertsChannel: make(chan cloudprotocol.Alerts, 1),
	}

	return sender
}

func (sender *testSender) SendAlerts(alerts cloudprotocol.Alerts) (err error) {
	sender.alertsChannel <- alerts

	return nil
}

func (sender *testSender) waitResult(timeout time.Duration,
	checkAlert func(alert cloudprotocol.AlertItem) (success bool, err error)) (err error) {
	for {
		select {
		case alerts := <-sender.alertsChannel:
			for _, alert := range alerts {
				success, err := checkAlert(alert)
				if err != nil {
					return err
				}

				if success {
					return nil
				}
			}

		case <-time.After(timeout):
			return errTimeout
		}
	}
}

func (sender *testSender) waitAlerts(timeout time.Duration, tag, source string, data []string) (err error) {
	return sender.waitResult(timeout, func(alert cloudprotocol.AlertItem) (success bool, err error) {
		if alert.Tag != tag {
			return false, nil
		}

		systemAlert, ok := (alert.Payload.(cloudprotocol.SystemAlert))
		if !ok {
			return false, errors.New("wrong alert type")
		}

		for i, message := range data {
			if systemAlert.Message == message {
				data = append(data[:i], data[i+1:]...)

				if alert.Source != source {
					return false, fmt.Errorf("wrong alert source: %s", alert.Source)
				}

				if len(data) == 0 {
					return true, nil
				}

				return false, nil
			}
		}

		return false, nil
	})
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func setup() (err error) {
	tmpDir, err = ioutil.TempDir("", "sm_")
	if err != nil {
		log.Fatalf("Error create temporary dir: %s", err)
	}

	if systemd, err = dbus.NewSystemConnectionContext(context.Background()); err != nil {
		return err
	}

	return nil
}

func cleanup() {
	systemd.Close()

	if err := os.RemoveAll(tmpDir); err != nil {
		log.Errorf("Error removing tmp dir: %s", err)
	}
}

func createSystemdUnit(serviceType, command, fileName string) (err error) {
	serviceTemplate := `[Unit]
	After=network.target
	
	[Service]
	Type=%s
	ExecStart=%s
	
	[Install]
	WantedBy=multi-user.target
`

	serviceContent := fmt.Sprintf(serviceTemplate, serviceType, command)

	if err = ioutil.WriteFile(fileName, []byte(serviceContent), 0o644); err != nil {
		return err
	}

	if _, err = systemd.LinkUnitFilesContext(context.Background(), []string{fileName}, false, true); err != nil {
		return err
	}

	if err = systemd.ReloadContext(context.Background()); err != nil {
		return err
	}

	return nil
}

func startSystemdUnit(name string) (err error) {
	channel := make(chan string)

	if _, err = systemd.RestartUnitContext(context.Background(), name, "replace", channel); err != nil {
		return err
	}

	<-channel

	return nil
}
