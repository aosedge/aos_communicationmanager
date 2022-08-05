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

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/amqphandler"
	"github.com/aoscloud/aos_communicationmanager/config"

	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error)
}

// MonitorController instance.
type MonitorController struct {
	monitoringChannel chan cloudprotocol.MonitoringData
	monitoringSender  MonitoringSender
	cancelFunction    context.CancelFunc
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new monitor controller instance.
func New(
	config *config.Config, monitoringSender MonitoringSender,
) (monitor *MonitorController, err error) {
	monitor = &MonitorController{
		monitoringSender:  monitoringSender,
		monitoringChannel: make(chan cloudprotocol.MonitoringData, config.Monitoring.MaxOfflineMessages),
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	monitor.cancelFunction = cancelFunc

	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return

			case monitorData := <-monitor.monitoringChannel:
				if err := monitor.monitoringSender.SendMonitoringData(
					monitorData); err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
					log.Errorf("Can't send monitoring data: %v", err)
				}
			}
		}
	}(ctx)

	return monitor, nil
}

// Close closes monitor controller instance.
func (monitor *MonitorController) Close() {
	if monitor.cancelFunction != nil {
		monitor.cancelFunction()
	}
}

// SendMonitoringData sends monitoring data.
func (monitor *MonitorController) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) {
	if len(monitor.monitoringChannel) < cap(monitor.monitoringChannel) {
		monitor.monitoringChannel <- monitoringData
	} else {
		log.Warn("Skip sending monitoring data. Channel full.")
	}
}
