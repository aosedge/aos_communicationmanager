// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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
package journalalerts

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/coreos/go-systemd/v22/sdjournal"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const aosServicePrefix = "aos-service@"

const (
	waitJournalTimeout = 1 * time.Second
	journalSavePeriod  = 10 * time.Second
)

const microSecondsInSecond = 1000000

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// AlertSender alerts sender.
type AlertSender interface {
	SendAlert(alerts cloudprotocol.AlertItem)
}

// InstanceInfoProvider provides instance info.
type InstanceInfoProvider interface {
	GetInstanceInfoByID(instanceID string) (ident aostypes.InstanceIdent, aosVersion uint64, err error)
}

// CursorStorage provides API to set and get journal cursor.
type CursorStorage interface {
	SetJournalCursor(cursor string) (err error)
	GetJournalCursor() (cursor string, err error)
}

// JournalInterface systemd journal interface.
type JournalInterface interface {
	Close() error
	AddMatch(match string) error
	AddDisjunction() error
	SeekTail() error
	Previous() (uint64, error)
	SeekCursor(cursor string) error
	Next() (uint64, error)
	GetEntry() (*sdjournal.JournalEntry, error)
	Wait(timeout time.Duration) int
	GetCursor() (string, error)
}

// Config alerts configuration.
type Config struct {
	Filter               []string `json:"filter"`
	ServiceAlertPriority int      `json:"serviceAlertPriority"`
	SystemAlertPriority  int      `json:"systemAlertPriority"`
}

// JournalAlerts instance.
type JournalAlerts struct {
	sync.Mutex
	config                Config
	cursorStorage         CursorStorage
	instanceProvider      InstanceInfoProvider
	sender                AlertSender
	filterRegexp          []*regexp.Regexp
	journal               JournalInterface
	journalCancelFunction context.CancelFunc
}

/***********************************************************************************************************************
 * Variable
 **********************************************************************************************************************/

var coreComponents = []string{ //nolint:gochecknoglobals
	"aos-servicemanager",
	"aos-updatemanager",
	"aos-iamanager",
	"aos-communicationmanager",
	"aos-vis",
}

// SDJournal is using to mock systemd journal in unit tests.
var SDJournal JournalInterface //nolint:gochecknoglobals

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new journal alerts object.
func New(
	config Config, instanceProvider InstanceInfoProvider, cursorStorage CursorStorage, sender AlertSender,
) (instance *JournalAlerts, err error) {
	log.Debug("New alerts")

	instance = &JournalAlerts{
		config: config, cursorStorage: cursorStorage,
		instanceProvider: instanceProvider,
		sender:           sender,
	}

	for _, substr := range instance.config.Filter {
		if len(substr) == 0 {
			log.Warning("Filter value has an empty string")

			continue
		}

		tmpRegexp, err := regexp.Compile(substr)
		if err != nil {
			log.Errorf("Regexp compile error. Incorrect regexp: %s, error is: %s", substr, err)

			continue
		}

		instance.filterRegexp = append(instance.filterRegexp, tmpRegexp)
	}

	if err = instance.setupJournal(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return instance, nil
}

// Close closes logging.
func (instance *JournalAlerts) Close() {
	log.Debug("Close alerts")

	if instance.journalCancelFunction != nil {
		instance.journalCancelFunction()

		if err := instance.storeCurrentCursor(); err != nil {
			log.Errorf("Can't store cursor: %s", err)
		}

		instance.journal.Close()
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *JournalAlerts) setupJournal() (err error) {
	if instance.journal = SDJournal; instance.journal == nil {
		if instance.journal, err = sdjournal.NewJournal(); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	for priorityLevel := 0; priorityLevel <= instance.config.SystemAlertPriority; priorityLevel++ {
		if err = instance.journal.AddMatch(fmt.Sprintf("PRIORITY=%d", priorityLevel)); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if err = instance.journal.AddDisjunction(); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = instance.journal.AddMatch("_SYSTEMD_UNIT=init.scope"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = instance.journal.SeekTail(); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err = instance.journal.Previous(); err != nil {
		return aoserrors.Wrap(err)
	}

	cursor, err := instance.cursorStorage.GetJournalCursor()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if cursor != "" {
		if err = instance.journal.SeekCursor(cursor); err != nil {
			return aoserrors.Wrap(err)
		}

		if _, err = instance.journal.Next(); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	go instance.handleChannels()

	return nil
}

func (instance *JournalAlerts) handleChannels() {
	result := sdjournal.SD_JOURNAL_APPEND
	journalTicker := time.NewTicker(journalSavePeriod)

	ctx, cancelFunction := context.WithCancel(context.Background())

	instance.journalCancelFunction = cancelFunction

	for {
		select {
		case <-journalTicker.C:
			if err := instance.storeCurrentCursor(); err != nil {
				log.Error("Can't store journal cursor: ", err)
			}

		case <-ctx.Done():
			journalTicker.Stop()

			return

		default:
			if result != sdjournal.SD_JOURNAL_NOP {
				if err := instance.processJournal(); err != nil {
					log.Errorf("Journal process error: %s", err)
				}
			}

			if result = instance.journal.Wait(waitJournalTimeout); result < 0 {
				log.Errorf("Wait journal error: %s", syscall.Errno(-result))
			}
		}
	}
}

func (instance *JournalAlerts) processJournal() (err error) {
	for {
		count, err := instance.journal.Next()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if count == 0 {
			return nil
		}

		entry, err := instance.journal.GetEntry()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if entry == nil {
			return nil
		}

		unit := entry.Fields[sdjournal.SD_JOURNAL_FIELD_SYSTEMD_UNIT]

		if unit == "init.scope" {
			if priority, err := strconv.Atoi(entry.Fields[sdjournal.SD_JOURNAL_FIELD_PRIORITY]); err != nil ||
				priority > instance.config.ServiceAlertPriority {
				continue
			}

			unit = entry.Fields["UNIT"]
		}

		// with cgroup v2 logs from container do not contains _SYSTEMD_UNIT due to restrictions
		// that's why id should be extracted from _SYSTEMD_CGROUP
		// format: /system.slice/system-aos@service.slice/AOS_INSTANCE_ID
		if len(unit) == 0 {
			systemdCgroup := entry.Fields[sdjournal.SD_JOURNAL_FIELD_SYSTEMD_CGROUP]

			if len(systemdCgroup) == 0 {
				continue
			}

			// add prefix 'aos-service@' and postfix '.service'
			// to service uuid and get proper seervice object from DB
			unit = systemdCgroup
		}

		alert := cloudprotocol.AlertItem{
			Timestamp: time.Unix(int64(entry.RealtimeTimestamp/microSecondsInSecond),
				int64((entry.RealtimeTimestamp%microSecondsInSecond)*1000)),
		}

		if payload := instance.getServiceInstanceAlert(entry, unit); payload != nil {
			alert.Tag = cloudprotocol.AlertTagServiceInstance
			alert.Payload = *payload
		} else if payload := instance.getCoreComponentAlert(entry, unit); payload != nil {
			alert.Tag = cloudprotocol.AlertTagAosCore
			alert.Payload = *payload
		} else if payload := instance.getSystemAlert(entry); payload != nil {
			alert.Tag = cloudprotocol.AlertTagSystemError
			alert.Payload = *payload
		} else {
			continue
		}

		instance.sender.SendAlert(alert)
	}
}

func (instance *JournalAlerts) storeCurrentCursor() (err error) {
	cursor, err := instance.journal.GetCursor()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = instance.cursorStorage.SetJournalCursor(cursor); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *JournalAlerts) getServiceInstanceAlert(
	entry *sdjournal.JournalEntry, unitName string,
) *cloudprotocol.ServiceInstanceAlert {
	if instance.instanceProvider == nil {
		return nil
	}

	if strings.Contains(unitName, aosServicePrefix) {
		instanceID := filepath.Base(unitName)
		instanceID = strings.TrimPrefix(instanceID, aosServicePrefix)
		instanceID = strings.TrimSuffix(instanceID, ".service")

		instanceIdent, aosVersion, err := instance.instanceProvider.GetInstanceInfoByID(instanceID)
		if err != nil {
			log.Errorf("Can't get instance info: %s", err)

			return nil
		}

		return &cloudprotocol.ServiceInstanceAlert{
			Message:       entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE],
			InstanceIdent: instanceIdent,
			AosVersion:    aosVersion,
		}
	}

	return nil
}

func (instance *JournalAlerts) getCoreComponentAlert(
	entry *sdjournal.JournalEntry, unitName string,
) *cloudprotocol.CoreAlert {
	for _, component := range coreComponents {
		if strings.Contains(unitName, component) {
			return &cloudprotocol.CoreAlert{
				CoreComponent: component,
				Message:       entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE],
			}
		}
	}

	return nil
}

func (instance *JournalAlerts) getSystemAlert(entry *sdjournal.JournalEntry) *cloudprotocol.SystemAlert {
	for _, substr := range instance.filterRegexp {
		if substr.MatchString(entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE]) {
			return nil
		}
	}

	return &cloudprotocol.SystemAlert{Message: entry.Fields[sdjournal.SD_JOURNAL_FIELD_MESSAGE]}
}
