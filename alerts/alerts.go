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

// Package alerts provides set of API to send system and services alerts
package alerts

import (
	"context"
	"encoding/json"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/coreos/go-systemd/v22/sdjournal"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	allocAlertItems    = 10
	waitJournalTimeout = 1 * time.Second
	storeCursorPeriod  = 10 * time.Second
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// CursorStorage provides API to set and get journal cursor
type CursorStorage interface {
	SetJournalCursor(cursor string) (err error)
	GetJournalCursor() (cursor string, err error)
}

// Sender sends alerts to the cloud
type Sender interface {
	SendAlerts(alerts cloudprotocol.Alerts) (err error)
}

// Alerts instance
type Alerts struct {
	sync.Mutex

	sender        Sender
	alertsChannel chan cloudprotocol.Alerts

	config        config.Alerts
	cursorStorage CursorStorage
	filterRegexp  []*regexp.Regexp

	alertsSize       int
	skippedAlerts    uint32
	duplicatedAlerts uint32
	alerts           cloudprotocol.Alerts

	cancelFunction context.CancelFunc
}

// DownloadStatus download status structure
type DownloadStatus struct {
	Source          string
	URL             string
	Progress        int
	DownloadedBytes uint64
	TotalBytes      uint64
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new alerts object
func New(config *config.Config, sender Sender, cursorStorage CursorStorage) (instance *Alerts, err error) {
	log.Debug("Create alerts instance")

	instance = &Alerts{sender: sender, config: config.Alerts, cursorStorage: cursorStorage}

	instance.alertsChannel = make(chan cloudprotocol.Alerts, instance.config.MaxOfflineMessages)
	instance.alerts = make([]cloudprotocol.AlertItem, 0, allocAlertItems)

	for _, substr := range instance.config.Filter {
		tmpRegexp, err := regexp.Compile(substr)
		if err != nil {
			log.Errorf("Regexp compile error. Incorrect regexp: %s, error is: %s", substr, err)
			continue
		}

		instance.filterRegexp = append(instance.filterRegexp, tmpRegexp)
	}

	ctx, cancelFunction := context.WithCancel(context.Background())
	instance.cancelFunction = cancelFunction

	sendTicker := time.NewTicker(instance.config.SendPeriod.Duration)

	go func() {
		for {
			select {
			case alerts := <-instance.alertsChannel:
				if err := instance.sender.SendAlerts(alerts); err != nil {
					log.Errorf("Can't send alerts: %s", err)
				}

			case <-sendTicker.C:
				if err = instance.sendAlerts(); err != nil {
					log.Errorf("Send alerts error: %s", err)
				}

			case <-ctx.Done():
				sendTicker.Stop()
				return
			}
		}
	}()

	if config.Alerts.EnableSystemAlerts {
		if err = instance.setupJournal(ctx); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	return instance, nil
}

// SendDownloadStartedAlert sends download started status alert
func (instance *Alerts) SendDownloadStartedAlert(downloadStatus DownloadStatus) {
	message := "Download started"
	payload := instance.prepareDownloadAlert(downloadStatus, message)

	instance.sendDownloadAlert(downloadStatus.Source, payload)
}

// SendDownloadFinishedAlert sends download finished status alert
func (instance *Alerts) SendDownloadFinishedAlert(downloadStatus DownloadStatus, code int) {
	message := "Download finished code: " + strconv.Itoa(code)
	payload := instance.prepareDownloadAlert(downloadStatus, message)

	instance.sendDownloadAlert(downloadStatus.Source, payload)
}

// SendDownloadInterruptedAlert sends download interrupted status alert
func (instance *Alerts) SendDownloadInterruptedAlert(downloadStatus DownloadStatus, reason string) {
	message := "Download interrupted reason: " + reason
	payload := instance.prepareDownloadAlert(downloadStatus, message)

	instance.sendDownloadAlert(downloadStatus.Source, payload)
}

// SendDownloadResumedAlert sends download resumed status alert
func (instance *Alerts) SendDownloadResumedAlert(downloadStatus DownloadStatus, reason string) {
	message := "Download resumed reason: " + reason
	payload := instance.prepareDownloadAlert(downloadStatus, message)

	instance.sendDownloadAlert(downloadStatus.Source, payload)
}

// SendDownloadStatusAlert sends download status alert
func (instance *Alerts) SendDownloadStatusAlert(downloadStatus DownloadStatus) {
	message := "Download status"
	payload := instance.prepareDownloadAlert(downloadStatus, message)

	instance.sendDownloadAlert(downloadStatus.Source, payload)
}

// SendResourceAlert sends resource alert
func (instance *Alerts) SendResourceAlert(source, resource string, time time.Time, value uint64) {
	log.WithFields(log.Fields{
		"timestamp": time,
		"source":    source,
		"resource":  resource,
		"value":     value,
	}).Debug("Resource alert")

	instance.addAlert(cloudprotocol.AlertItem{
		Timestamp: time,
		Tag:       cloudprotocol.AlertTagResource,
		Source:    source,
		Payload: cloudprotocol.ResourceAlert{
			Parameter: resource,
			Value:     value,
		},
	})
}

// SendAlert sends alert
func (instance *Alerts) SendAlert(alert cloudprotocol.AlertItem) (err error) {
	instance.addAlert(alert)

	return nil
}

// Close closes logging
func (instance *Alerts) Close() {
	log.Debug("Close alerts instance")

	instance.cancelFunction()
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Alerts) prepareDownloadAlert(
	downloadStatus DownloadStatus, message string) (downloadAlert cloudprotocol.DownloadAlert) {
	payload := cloudprotocol.DownloadAlert{
		Message:         message,
		Progress:        strconv.Itoa(downloadStatus.Progress) + "%",
		URL:             downloadStatus.URL,
		DownloadedBytes: bytefmt.ByteSize(downloadStatus.DownloadedBytes),
		TotalBytes:      bytefmt.ByteSize(downloadStatus.TotalBytes),
	}

	return payload
}

func (instance *Alerts) sendDownloadAlert(source string, payload cloudprotocol.DownloadAlert) {
	time := time.Now()

	log.WithFields(log.Fields{
		"timestamp":       time,
		"source":          source,
		"progress":        payload.Progress,
		"url":             payload.URL,
		"downloadedBytes": payload.DownloadedBytes,
		"totalBytes":      payload.TotalBytes,
	}).Debug(payload.Message)

	instance.addAlert(cloudprotocol.AlertItem{
		Timestamp: time,
		Tag:       cloudprotocol.AlertTagAosCore,
		Source:    source,
		Payload:   payload,
	})
}

func (instance *Alerts) setupJournal(ctx context.Context) (err error) {
	if instance.cursorStorage == nil {
		return aoserrors.New("cursor storage is not set")
	}

	journal, err := sdjournal.NewJournal()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddMatch("PRIORITY=0"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddMatch("PRIORITY=1"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddMatch("PRIORITY=2"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddMatch("PRIORITY=3"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddDisjunction(); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.AddMatch("_SYSTEMD_UNIT=init.scope"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = journal.SeekTail(); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = journal.Previous(); err != nil {
		return aoserrors.Wrap(err)
	}

	cursor, err := instance.cursorStorage.GetJournalCursor()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if cursor != "" {
		if err = journal.SeekCursor(cursor); err != nil {
			return aoserrors.Wrap(err)
		}

		if _, err = journal.Next(); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		if err = instance.storeCurrentCursor(journal); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	go func() {
		storeTicker := time.NewTicker(storeCursorPeriod)
		journalChanged := false
		result := sdjournal.SD_JOURNAL_APPEND

		for {
			select {
			case <-storeTicker.C:
				if journalChanged {
					journalChanged = false

					if err = instance.storeCurrentCursor(journal); err != nil {
						log.Errorf("Can't store journal cursor: %s", err)
					}
				}

			case <-ctx.Done():
				journal.Close()
				return

			default:
				if result != sdjournal.SD_JOURNAL_NOP {
					journalChanged = true

					if err = instance.processJournal(journal); err != nil {
						log.Errorf("Journal process error: %s", err)
					}
				}

				if result = journal.Wait(waitJournalTimeout); result < 0 {
					log.Errorf("Wait journal error: %s", syscall.Errno(-result))
				}
			}
		}
	}()

	return nil
}

func (instance *Alerts) processJournal(journal *sdjournal.Journal) (err error) {
	for {
		count, err := journal.Next()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if count == 0 {
			return nil
		}

		entry, err := journal.GetEntry()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		source := "system"
		tag := cloudprotocol.AlertTagSystemError
		unit := entry.Fields[sdjournal.SD_JOURNAL_FIELD_SYSTEMD_UNIT]

		if unit == "init.scope" {
			if priority, err := strconv.Atoi(entry.Fields[sdjournal.SD_JOURNAL_FIELD_PRIORITY]); err != nil || priority > 4 {
				continue
			}

			unit = entry.Fields["UNIT"]
		}

		if strings.HasPrefix(unit, "aos") {
			source = unit
			tag = cloudprotocol.AlertTagAosCore
		}

		t := time.Unix(int64(entry.RealtimeTimestamp/1000000),
			int64((entry.RealtimeTimestamp%1000000)*1000))

		skipsend := false

		for _, substr := range instance.filterRegexp {
			skipsend = substr.MatchString(entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE])

			if skipsend {
				break
			}
		}

		if !skipsend {
			log.WithFields(log.Fields{
				"time":    t,
				"message": entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE],
				"tag":     tag,
				"source":  source,
			}).Debug("System alert")

			instance.addAlert(cloudprotocol.AlertItem{
				Timestamp: t,
				Tag:       tag,
				Source:    source,
				Payload:   cloudprotocol.SystemAlert{Message: entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE]},
			})
		}
	}
}

func (instance *Alerts) addAlert(item cloudprotocol.AlertItem) {
	instance.Lock()
	defer instance.Unlock()

	if len(instance.alerts) != 0 &&
		reflect.DeepEqual(instance.alerts[len(instance.alerts)-1].Payload, item.Payload) {
		instance.duplicatedAlerts++
		return
	}

	data, _ := json.Marshal(item)
	instance.alertsSize += len(data)

	if int(instance.alertsSize) <= instance.config.MaxMessageSize {
		instance.alerts = append(instance.alerts, item)
	} else {
		instance.skippedAlerts++
	}
}

func (instance *Alerts) sendAlerts() (err error) {
	instance.Lock()
	defer instance.Unlock()

	if instance.alertsSize != 0 {
		if len(instance.alertsChannel) < cap(instance.alertsChannel) {
			instance.alertsChannel <- instance.alerts

			if instance.skippedAlerts != 0 {
				log.WithField("count", instance.skippedAlerts).Warn("Alerts skipped due to size limit")
			}
			if instance.duplicatedAlerts != 0 {
				log.WithField("count", instance.duplicatedAlerts).Warn("Alerts skipped due to duplication")
			}
		} else {
			log.Warn("Skip sending alerts due to channel is full")
		}

		instance.alerts = make([]cloudprotocol.AlertItem, 0, allocAlertItems)
		instance.skippedAlerts = 0
		instance.duplicatedAlerts = 0
		instance.alertsSize = 0
	}

	return nil
}

func (instance *Alerts) storeCurrentCursor(journal *sdjournal.Journal) (err error) {
	cursor, err := journal.GetCursor()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = instance.cursorStorage.SetJournalCursor(cursor); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
