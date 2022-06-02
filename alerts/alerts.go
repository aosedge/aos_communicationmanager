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
	"sync"
	"time"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const alertChannelSize = 50

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Sender alerts sender to the cloud interface.
type Sender interface {
	SendAlerts(alerts cloudprotocol.Alerts) (err error)
}

// Alerts instance.
type Alerts struct {
	sync.Mutex
	alertsChannel        chan cloudprotocol.AlertItem
	alertsPackageChannel chan cloudprotocol.Alerts
	currentAlerts        cloudprotocol.Alerts
	senderCancelFunction context.CancelFunc
	config               config.Alerts
	sender               Sender
	alertsSize           int
	skippedAlerts        uint32
	duplicatedAlerts     uint32
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new alerts object.
func New(config config.Alerts, sender Sender) (instance *Alerts, err error) {
	log.Debug("New alerts")

	instance = &Alerts{
		config:        config,
		sender:        sender,
		alertsChannel: make(chan cloudprotocol.AlertItem, alertChannelSize),
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	instance.senderCancelFunction = cancelFunction
	instance.alertsPackageChannel = make(chan cloudprotocol.Alerts, config.MaxOfflineMessages)

	go instance.processAlertChannels(ctx)

	return instance, nil
}

// Close closes logging.
func (instance *Alerts) Close() {
	log.Debug("Close alerts")

	if instance.senderCancelFunction != nil {
		instance.senderCancelFunction()
	}
}

// SendResourceAlert sends resource alert.
func (instance *Alerts) SendAlert(alert cloudprotocol.AlertItem) {
	if len(instance.alertsChannel) >= cap(instance.alertsChannel) {
		log.Warn("Skip alert, channel is full")

		return
	}

	instance.alertsChannel <- alert
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Alerts) processAlertChannels(ctx context.Context) {
	sendTicker := time.NewTicker(instance.config.SendPeriod.Duration)

	for {
		select {
		case alert := <-instance.alertsChannel:
			instance.addAlert(alert)

		case alertsPackage := <-instance.alertsPackageChannel:
			if err := instance.sender.SendAlerts(alertsPackage); err != nil {
				log.Errorf("Can't send alerts: %s", err)
			}

		case <-sendTicker.C:
			instance.prepareAlertsPackage()

		case <-ctx.Done():
			sendTicker.Stop()
			return
		}
	}
}

func (instance *Alerts) addAlert(item cloudprotocol.AlertItem) {
	instance.Lock()
	defer instance.Unlock()

	if len(instance.currentAlerts) != 0 &&
		reflect.DeepEqual(instance.currentAlerts[len(instance.currentAlerts)-1].Payload, item.Payload) {
		instance.duplicatedAlerts++
		return
	}

	data, err := json.Marshal(item)
	if err != nil {
		log.Error("Can't calculate alert size")
	}

	instance.alertsSize += len(data)

	if instance.alertsSize <= instance.config.MaxMessageSize {
		instance.currentAlerts = append(instance.currentAlerts, item)
	} else {
		instance.skippedAlerts++
	}
}

func (instance *Alerts) prepareAlertsPackage() {
	instance.Lock()
	defer instance.Unlock()

	if instance.alertsSize != 0 {
		if len(instance.alertsChannel) < cap(instance.alertsChannel) {
			instance.alertsPackageChannel <- instance.currentAlerts

			if instance.skippedAlerts != 0 {
				log.WithField("count", instance.skippedAlerts).Warn("Alerts skipped due to size limit")
			}

			if instance.duplicatedAlerts != 0 {
				log.WithField("count", instance.duplicatedAlerts).Warn("Alerts skipped due to duplication")
			}
		} else {
			log.Warn("Skip sending alerts due to channel is full")
		}

		instance.currentAlerts = []cloudprotocol.AlertItem{}
		instance.skippedAlerts = 0
		instance.duplicatedAlerts = 0
		instance.alertsSize = 0
	}
}
