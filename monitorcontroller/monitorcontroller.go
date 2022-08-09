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

package monitorcontroller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData) error
}

// MonitorController instance.
type MonitorController struct {
	sync.Mutex
	monitoringQueue     []cloudprotocol.MonitoringData
	monitoringQueueSize int
	monitoringSender    MonitoringSender
	cancelFunction      context.CancelFunc
	isConnected         bool
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// MinSendPeriod used to adjust min send period.
// nolint:gochecknoglobals
var MinSendPeriod = 10 * time.Second

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new monitor controller instance.
func New(
	config *config.Config, monitoringSender MonitoringSender,
) (monitor *MonitorController, err error) {
	monitor = &MonitorController{
		monitoringSender:    monitoringSender,
		monitoringQueue:     make([]cloudprotocol.MonitoringData, 0, config.Monitoring.MaxOfflineMessages),
		monitoringQueueSize: config.Monitoring.MaxOfflineMessages,
	}

	if err = monitor.monitoringSender.SubscribeForConnectionEvents(monitor); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	monitor.cancelFunction = cancelFunc

	go monitor.processQueue(ctx)

	return monitor, nil
}

// Close closes monitor controller instance.
func (monitor *MonitorController) Close() {
	if err := monitor.monitoringSender.UnsubscribeFromConnectionEvents(monitor); err != nil {
		log.Errorf("Can't unsubscribe from connection events: %v", err)
	}

	if monitor.cancelFunction != nil {
		monitor.cancelFunction()
	}
}

// SendMonitoringData sends monitoring data.
func (monitor *MonitorController) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) {
	monitor.Lock()
	defer monitor.Unlock()

	if len(monitor.monitoringQueue) >= monitor.monitoringQueueSize {
		monitor.monitoringQueue = monitor.monitoringQueue[1:]
	}

	monitor.monitoringQueue = append(monitor.monitoringQueue, monitoringData)
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

// CloudConnected indicates unit connected to cloud.
func (monitor *MonitorController) CloudConnected() {
	monitor.Lock()
	defer monitor.Unlock()

	monitor.isConnected = true
}

// CloudDisconnected indicates unit disconnected from cloud.
func (monitor *MonitorController) CloudDisconnected() {
	monitor.Lock()
	defer monitor.Unlock()

	monitor.isConnected = false
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (monitor *MonitorController) processQueue(ctx context.Context) {
	sendTicker := time.NewTicker(MinSendPeriod)

	for {
		select {
		case <-sendTicker.C:
			monitor.Lock()

			if len(monitor.monitoringQueue) > 0 && monitor.isConnected {
				var monitoringData cloudprotocol.MonitoringData

				monitoringData, monitor.monitoringQueue = monitor.monitoringQueue[0], monitor.monitoringQueue[1:]

				if err := monitor.monitoringSender.SendMonitoringData(
					monitoringData); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
					log.Errorf("Can't send monitoring data: %v", err)
				}
			}

			monitor.Unlock()

		case <-ctx.Done():
			return
		}
	}
}
