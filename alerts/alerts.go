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

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	alertChannelSize = 64
	addAlertTimeout  = 10 * time.Second
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Sender alerts sender to the cloud interface.
type Sender interface {
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	SendAlerts(alerts cloudprotocol.Alerts) error
}

// Alerts instance.
type Alerts struct {
	sync.RWMutex
	alertsChannel        chan cloudprotocol.AlertItem
	alertsPackageChannel chan cloudprotocol.Alerts
	currentAlerts        cloudprotocol.Alerts
	senderCancelFunction context.CancelFunc
	config               config.Alerts
	sender               Sender
	alertsSize           int
	skippedAlerts        uint32
	duplicatedAlerts     uint32
	isConnected          bool
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new alerts object.
func New(config config.Alerts, sender Sender) (instance *Alerts, err error) {
	log.Debug("New alerts")

	instance = &Alerts{
		config:               config,
		sender:               sender,
		alertsChannel:        make(chan cloudprotocol.AlertItem, alertChannelSize),
		alertsPackageChannel: make(chan cloudprotocol.Alerts, config.MaxOfflineMessages),
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	instance.senderCancelFunction = cancelFunction

	if err = instance.sender.SubscribeForConnectionEvents(instance); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go instance.processAlertChannels(ctx)

	return instance, nil
}

// Close closes logging.
func (instance *Alerts) Close() {
	log.Debug("Close alerts")

	if err := instance.sender.SubscribeForConnectionEvents(instance); err != nil {
		log.Errorf("Can't unsubscribe from connection events: %v", err)
	}

	if instance.senderCancelFunction != nil {
		instance.senderCancelFunction()
	}
}

// SendAlert sends alert.
func (instance *Alerts) SendAlert(alert cloudprotocol.AlertItem) {
	select {
	case instance.alertsChannel <- alert:

	case <-time.After(addAlertTimeout):
		log.Warn("Skip alert, channel is full")

		instance.Lock()
		instance.skippedAlerts++
		instance.Unlock()
	}
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

// CloudConnected indicates unit connected to cloud.
func (instance *Alerts) CloudConnected() {
	instance.Lock()
	defer instance.Unlock()

	instance.isConnected = true
}

// CloudDisconnected indicates unit disconnected from cloud.
func (instance *Alerts) CloudDisconnected() {
	instance.Lock()
	defer instance.Unlock()

	instance.isConnected = false
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Alerts) processAlertChannels(ctx context.Context) {
	var (
		sendTicker           = time.NewTicker(instance.config.SendPeriod.Duration)
		alertsChannel        = instance.alertsChannel
		alertsPackageChannel chan cloudprotocol.Alerts
	)

	for {
		select {
		case alert := <-alertsChannel:
			if instance.addAlert(alert) {
				alertsChannel = nil
			}

		case alertsPackage := <-alertsPackageChannel:
			if err := instance.sender.SendAlerts(alertsPackage); err != nil {
				log.Errorf("Can't send alerts: %s", err)
			}

			alertsPackageChannel = nil

		case <-sendTicker.C:
			instance.prepareAlertsPackage()

			if alertsChannel == nil {
				alertsChannel = instance.alertsChannel
			}

			instance.RLock()

			if alertsPackageChannel == nil && instance.isConnected {
				alertsPackageChannel = instance.alertsPackageChannel
			}

			instance.RUnlock()

		case <-ctx.Done():
			sendTicker.Stop()
			return
		}
	}
}

func (instance *Alerts) addAlert(item cloudprotocol.AlertItem) (bufferIsFull bool) {
	instance.Lock()
	defer instance.Unlock()

	if len(instance.currentAlerts) != 0 &&
		reflect.DeepEqual(instance.currentAlerts[len(instance.currentAlerts)-1].Payload, item.Payload) {
		instance.duplicatedAlerts++
		return
	}

	data, err := json.Marshal(item)
	if err != nil {
		log.Errorf("Can't marshal alert: %v", err)
	}

	instance.alertsSize += len(data)
	instance.currentAlerts = append(instance.currentAlerts, item)

	return instance.alertsSize >= instance.config.MaxMessageSize
}

func (instance *Alerts) prepareAlertsPackage() {
	instance.Lock()
	defer instance.Unlock()

	if instance.alertsSize == 0 {
		return
	}

	if len(instance.alertsPackageChannel) < cap(instance.alertsPackageChannel) {
		instance.alertsPackageChannel <- instance.currentAlerts

		if instance.skippedAlerts != 0 {
			log.WithField("count", instance.skippedAlerts).Warn("Alerts skipped due to channel is full")
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
