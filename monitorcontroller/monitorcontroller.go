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
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	SendMonitoringData(monitoringData cloudprotocol.Monitoring) error
}

// MonitorController instance.
type MonitorController struct {
	sync.Mutex
	offlineMessages []cloudprotocol.Monitoring

	sendMessageEvent   chan struct{}
	maxMessageSize     int
	currentMessageSize int
	sendPeriod         aostypes.Duration

	monitoringSender MonitoringSender
	cancelFunction   context.CancelFunc
	isConnected      bool
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new monitor controller instance.
func New(
	config *config.Config, monitoringSender MonitoringSender,
) (monitor *MonitorController, err error) {
	monitor = &MonitorController{
		monitoringSender: monitoringSender,
		offlineMessages:  make([]cloudprotocol.Monitoring, 0, config.Monitoring.MaxOfflineMessages),
		sendMessageEvent: make(chan struct{}, 1),
		maxMessageSize:   config.Monitoring.MaxMessageSize,
		sendPeriod:       config.Monitoring.SendPeriod,
	}

	if monitor.sendPeriod.Seconds() < 1.0 {
		log.Warningf("MonitorController send interval is less than 1sec.: %v", monitor.sendPeriod)
		monitor.sendPeriod = aostypes.Duration{Duration: 1 * time.Second}
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

// SendNodeMonitoring sends monitoring data.
func (monitor *MonitorController) SendNodeMonitoring(nodeMonitoring aostypes.NodeMonitoring) {
	monitor.Lock()

	// calculate size of input parameter
	messageSize := 0

	message, err := json.Marshal(nodeMonitoring)
	if err == nil {
		messageSize = len(message)
	} else {
		log.Errorf("Can't marshal nodeMonitoring: %v", err)
	}

	// allocate new offline message
	currentMessageOverflows := monitor.currentMessageSize+messageSize > monitor.maxMessageSize

	if len(monitor.offlineMessages) == 0 || currentMessageOverflows {
		if len(monitor.offlineMessages) == cap(monitor.offlineMessages) {
			monitor.offlineMessages = monitor.offlineMessages[1:]
		}

		monitor.offlineMessages = append(monitor.offlineMessages, cloudprotocol.Monitoring{})
		monitor.currentMessageSize = 0
	}

	monitor.currentMessageSize += messageSize

	// add monitoring data
	monitor.addNodeMonitoring(nodeMonitoring)

	// send notification message
	monitor.Unlock()

	if currentMessageOverflows {
		monitor.sendMessageEvent <- struct{}{}
	}
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
	sendTicker := time.NewTicker(monitor.sendPeriod.Duration)

	for {
		select {
		case <-sendTicker.C:
			monitor.sendMessages()

		case <-monitor.sendMessageEvent:
			monitor.sendMessages()
			sendTicker.Reset(monitor.sendPeriod.Duration)

		case <-ctx.Done():
			return
		}
	}
}

func (monitor *MonitorController) sendMessages() {
	monitor.Lock()
	defer monitor.Unlock()

	if len(monitor.offlineMessages) > 0 && monitor.isConnected {
		for _, offlineMessage := range monitor.offlineMessages {
			err := monitor.monitoringSender.SendMonitoringData(offlineMessage)
			if err != nil && !errors.Is(err, amqphandler.ErrNotConnected) {
				log.Errorf("Can't send monitoring data: %v", err)
			}
		}

		monitor.offlineMessages = make([]cloudprotocol.Monitoring, 0, cap(monitor.offlineMessages))
		monitor.currentMessageSize = 0
	}
}

func (monitor *MonitorController) addNodeMonitoring(nodeMonitoring aostypes.NodeMonitoring) {
	latestMessage := &monitor.offlineMessages[len(monitor.offlineMessages)-1]

	// add node monitoring data
	nodeDataFound := false

	for i, nodeData := range latestMessage.Nodes {
		if nodeData.NodeID == nodeMonitoring.NodeID {
			latestMessage.Nodes[i].Items = append(latestMessage.Nodes[i].Items, nodeMonitoring.NodeData)
			nodeDataFound = true

			break
		}
	}

	if !nodeDataFound {
		latestMessage.Nodes = append(latestMessage.Nodes,
			cloudprotocol.NodeMonitoringData{
				NodeID: nodeMonitoring.NodeID, Items: []aostypes.MonitoringData{nodeMonitoring.NodeData},
			})
	}

	// add instance monitoring data
	for _, instanceData := range nodeMonitoring.InstancesData {
		instanceDataFound := false

		for i, item := range latestMessage.ServiceInstances {
			if item.NodeID == nodeMonitoring.NodeID && item.InstanceIdent == instanceData.InstanceIdent {
				latestMessage.ServiceInstances[i].Items = append(latestMessage.ServiceInstances[i].Items,
					instanceData.MonitoringData)
				instanceDataFound = true

				break
			}
		}

		if !instanceDataFound {
			latestMessage.ServiceInstances = append(latestMessage.ServiceInstances,
				cloudprotocol.InstanceMonitoringData{
					NodeID:        nodeMonitoring.NodeID,
					InstanceIdent: instanceData.InstanceIdent,
					Items:         []aostypes.MonitoringData{instanceData.MonitoringData},
				})
		}
	}
}
