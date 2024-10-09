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

package amqphandler

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	sendChannelSize    = 32
	sendMaxTry         = 3
	sendTimeout        = 1 * time.Minute
	receiveChannelSize = 16
)

const (
	amqpSecureScheme   = "amqps"
	amqpInsecureScheme = "amqp"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// AmqpHandler structure with all amqp connection info.
type AmqpHandler struct { //nolint:stylecheck
	sync.Mutex

	// MessageChannel channel for amqp messages
	MessageChannel chan Message

	sendChannel    chan cloudprotocol.Message
	pendingChannel chan cloudprotocol.Message
	sendTry        int

	sendConnection    *amqp.Connection
	receiveConnection *amqp.Connection

	cryptoContext CryptoContext

	systemID string

	cancelFunc context.CancelFunc

	wg sync.WaitGroup

	isConnected               bool
	connectionEventsConsumers []ConnectionEventsConsumer
}

// CryptoContext interface to access crypto functions.
type CryptoContext interface {
	GetTLSConfig() (*tls.Config, error)
	DecryptMetadata(input []byte) ([]byte, error)
}

// Message AMQP message.
type Message interface{}

// ConnectionEventsConsumer connection events consumer interface.
type ConnectionEventsConsumer interface {
	CloudConnected()
	CloudDisconnected()
}

/***********************************************************************************************************************
 * Variables
 **********************************************************************************************************************/

var messageMap = map[string]func() interface{}{ //nolint:gochecknoglobals
	cloudprotocol.DesiredStatusMessageType: func() interface{} {
		return &cloudprotocol.DesiredStatus{}
	},
	cloudprotocol.RequestLogMessageType: func() interface{} {
		return &cloudprotocol.RequestLog{}
	},
	cloudprotocol.StateAcceptanceMessageType: func() interface{} {
		return &cloudprotocol.StateAcceptance{}
	},
	cloudprotocol.UpdateStateMessageType: func() interface{} {
		return &cloudprotocol.UpdateState{}
	},
	cloudprotocol.RenewCertsNotificationMessageType: func() interface{} {
		return &cloudprotocol.RenewCertsNotification{}
	},
	cloudprotocol.IssuedUnitCertsMessageType: func() interface{} {
		return &cloudprotocol.IssuedUnitCerts{}
	},
	cloudprotocol.OverrideEnvVarsMessageType: func() interface{} {
		return &cloudprotocol.OverrideEnvVars{}
	},
	cloudprotocol.StartProvisioningRequestMessageType: func() interface{} {
		return &cloudprotocol.StartProvisioningRequest{}
	},
	cloudprotocol.FinishProvisioningRequestMessageType: func() interface{} {
		return &cloudprotocol.FinishProvisioningRequest{}
	},
	cloudprotocol.DeprovisioningRequestMessageType: func() interface{} {
		return &cloudprotocol.DeprovisioningRequest{}
	},
}

var (
	// ErrNotConnected indicates AMQP client is not connected.
	ErrNotConnected = errors.New("not connected")
	// ErrSendChannelFull indicates AMQP send channel is full.
	ErrSendChannelFull = errors.New("send channel full")
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new amqp object.
func New() (*AmqpHandler, error) {
	log.Debug("New AMQP")

	handler := &AmqpHandler{
		sendChannel:    make(chan cloudprotocol.Message, sendChannelSize),
		pendingChannel: make(chan cloudprotocol.Message, 1),
	}

	return handler, nil
}

// Connect connects to cloud.
func (handler *AmqpHandler) Connect(cryptoContext CryptoContext, sdURL, systemID string, insecure bool) error {
	handler.Lock()
	defer handler.Unlock()

	log.WithFields(log.Fields{"url": sdURL}).Debug("AMQP connect")

	handler.cryptoContext = cryptoContext
	handler.systemID = systemID

	tlsConfig, err := handler.cryptoContext.GetTLSConfig()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	var (
		connectionInfo cloudprotocol.ConnectionInfo
		ctx            context.Context
	)

	ctx, handler.cancelFunc = context.WithCancel(context.Background())

	if connectionInfo, err = getConnectionInfo(ctx, sdURL,
		handler.createCloudMessage(cloudprotocol.ServiceDiscoveryRequest{}), tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	scheme := amqpSecureScheme

	if insecure {
		scheme = amqpInsecureScheme
	}

	if err := handler.setupConnections(scheme, connectionInfo, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	handler.isConnected = true

	handler.notifyCloudConnected()

	return nil
}

// Disconnect disconnects from cloud.
func (handler *AmqpHandler) Disconnect() error {
	handler.Lock()
	defer handler.Unlock()

	log.Debug("AMQP disconnect")

	if handler.cancelFunc != nil {
		handler.cancelFunc()
	}

	if handler.sendConnection != nil {
		handler.sendConnection.Close()
	}

	if handler.receiveConnection != nil {
		handler.receiveConnection.Close()
	}

	handler.wg.Wait()

	handler.isConnected = false

	handler.notifyCloudDisconnected()

	return nil
}

// SendUnitStatus sends unit status.
func (handler *AmqpHandler) SendUnitStatus(unitStatus cloudprotocol.UnitStatus) error {
	handler.Lock()
	defer handler.Unlock()

	unitStatus.MessageType = cloudprotocol.UnitStatusMessageType

	return handler.scheduleMessage(unitStatus, false)
}

// SendDeltaUnitStatus sends delta unit status.
func (handler *AmqpHandler) SendDeltaUnitStatus(deltaUnitStatus cloudprotocol.DeltaUnitStatus) error {
	handler.Lock()
	defer handler.Unlock()

	deltaUnitStatus.MessageType = cloudprotocol.UnitStatusMessageType

	return handler.scheduleMessage(deltaUnitStatus, false)
}

// SendMonitoringData sends monitoring data.
func (handler *AmqpHandler) SendMonitoringData(monitoringData cloudprotocol.Monitoring) error {
	handler.Lock()
	defer handler.Unlock()

	monitoringData.MessageType = cloudprotocol.MonitoringMessageType

	return handler.scheduleMessage(monitoringData, false)
}

// SendServiceNewState sends new state message.
func (handler *AmqpHandler) SendInstanceNewState(newState cloudprotocol.NewState) error {
	handler.Lock()
	defer handler.Unlock()

	newState.MessageType = cloudprotocol.NewStateMessageType

	return handler.scheduleMessage(newState, false)
}

// SendServiceStateRequest sends state request message.
func (handler *AmqpHandler) SendInstanceStateRequest(request cloudprotocol.StateRequest) error {
	handler.Lock()
	defer handler.Unlock()

	request.MessageType = cloudprotocol.StateRequestMessageType

	return handler.scheduleMessage(request, true)
}

// SendLog sends system or service logs.
func (handler *AmqpHandler) SendLog(serviceLog cloudprotocol.PushLog) error {
	handler.Lock()
	defer handler.Unlock()

	serviceLog.MessageType = cloudprotocol.PushLogMessageType

	return handler.scheduleMessage(serviceLog, true)
}

// SendAlerts sends alerts message.
func (handler *AmqpHandler) SendAlerts(alerts cloudprotocol.Alerts) error {
	handler.Lock()
	defer handler.Unlock()

	alerts.MessageType = cloudprotocol.AlertsMessageType

	return handler.scheduleMessage(alerts, true)
}

// SendIssueUnitCerts sends request to issue new certificates.
func (handler *AmqpHandler) SendIssueUnitCerts(requests []cloudprotocol.IssueCertData) error {
	handler.Lock()
	defer handler.Unlock()

	issueUnitCerts := cloudprotocol.IssueUnitCerts{
		MessageType: cloudprotocol.IssueUnitCertsMessageType, Requests: requests,
	}

	return handler.scheduleMessage(issueUnitCerts, true)
}

// SendInstallCertsConfirmation sends install certificates confirmation.
func (handler *AmqpHandler) SendInstallCertsConfirmation(confirmations []cloudprotocol.InstallCertData) error {
	handler.Lock()
	defer handler.Unlock()

	request := cloudprotocol.InstallUnitCertsConfirmation{
		MessageType: cloudprotocol.InstallUnitCertsConfirmationMessageType, Certificates: confirmations,
	}

	return handler.scheduleMessage(request, true)
}

// SendOverrideEnvVarsStatus overrides env vars status.
func (handler *AmqpHandler) SendOverrideEnvVarsStatus(envs cloudprotocol.OverrideEnvVarsStatus) error {
	handler.Lock()
	defer handler.Unlock()

	envs.MessageType = cloudprotocol.OverrideEnvVarsStatusMessageType

	return handler.scheduleMessage(envs, true)
}

// SendStartProvisioningResponse sends start provisioning response.
func (handler *AmqpHandler) SendStartProvisioningResponse(response cloudprotocol.StartProvisioningResponse) error {
	handler.Lock()
	defer handler.Unlock()

	return handler.scheduleMessage(response, true)
}

// SendFinishProvisioningResponse sends finish provisioning response.
func (handler *AmqpHandler) SendFinishProvisioningResponse(response cloudprotocol.FinishProvisioningResponse) error {
	handler.Lock()
	defer handler.Unlock()

	return handler.scheduleMessage(response, true)
}

// SendDeprovisioningResponse sends deprovisioning response.
func (handler *AmqpHandler) SendDeprovisioningResponse(response cloudprotocol.DeprovisioningResponse) error {
	handler.Lock()
	defer handler.Unlock()

	return handler.scheduleMessage(response, true)
}

// SubscribeForConnectionEvents subscribes for connection events.
func (handler *AmqpHandler) SubscribeForConnectionEvents(consumer ConnectionEventsConsumer) error {
	handler.Lock()
	defer handler.Unlock()

	for _, subscribedConsumer := range handler.connectionEventsConsumers {
		if subscribedConsumer == consumer {
			return aoserrors.New("already subscribed")
		}
	}

	handler.connectionEventsConsumers = append(handler.connectionEventsConsumers, consumer)

	return nil
}

// UnsubscribeFromConnectionEvents unsubscribes from connection events.
func (handler *AmqpHandler) UnsubscribeFromConnectionEvents(consumer ConnectionEventsConsumer) error {
	handler.Lock()
	defer handler.Unlock()

	for i, subscribedConsumer := range handler.connectionEventsConsumers {
		if subscribedConsumer == consumer {
			handler.connectionEventsConsumers = append(handler.connectionEventsConsumers[:i],
				handler.connectionEventsConsumers[i+1:]...)

			return nil
		}
	}

	return aoserrors.New("not subscribed")
}

// Close closes all amqp connection.
func (handler *AmqpHandler) Close() {
	log.Info("Close AMQP")

	if handler.cancelFunc != nil {
		handler.cancelFunc()
	}

	if handler.isConnected {
		if err := handler.Disconnect(); err != nil {
			log.Errorf("Can't disconnect from AMQP server: %s", err)
		}
	}
}

/***************************************************************************************************
 * Private
 **************************************************************************************************/

// service discovery implementation.
func getConnectionInfo(
	ctx context.Context, url string, request cloudprotocol.Message, tlsConfig *tls.Config,
) (info cloudprotocol.ConnectionInfo, err error) {
	reqJSON, err := json.Marshal(request)
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	log.WithField("request", string(reqJSON)).Info("AMQP service discovery request")

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(reqJSON))
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return info, aoserrors.Wrap(err)
	}
	defer resp.Body.Close()

	htmlData, err := io.ReadAll(resp.Body)
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	if resp.StatusCode != http.StatusOK {
		return info, aoserrors.Errorf("%s: %s", resp.Status, string(htmlData))
	}

	var jsonResp cloudprotocol.ServiceDiscoveryResponse

	err = json.Unmarshal(htmlData, &jsonResp)
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	return jsonResp.Connection, nil
}

func (handler *AmqpHandler) setupConnections(
	scheme string, info cloudprotocol.ConnectionInfo, tlsConfig *tls.Config,
) error {
	handler.MessageChannel = make(chan Message, receiveChannelSize)

	if err := handler.setupSendConnection(scheme, info.SendParams, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.setupReceiveConnection(scheme, info.ReceiveParams, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *AmqpHandler) setupSendConnection(
	scheme string, params cloudprotocol.SendParams, tlsConfig *tls.Config,
) error {
	urlRabbitMQ := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(params.User, params.Password),
		Host:   params.Host,
	}

	log.WithField("url", urlRabbitMQ.String()).Debug("Sender connection url")

	connection, err := amqp.DialConfig(urlRabbitMQ.String(), amqp.Config{
		TLSClientConfig: tlsConfig,
		SASL:            nil,
		Heartbeat:       10 * time.Second,
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	amqpChannel, err := connection.Channel()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	handler.sendConnection = connection

	if err = amqpChannel.Confirm(false); err != nil {
		return aoserrors.Wrap(err)
	}

	handler.wg.Add(1)

	go handler.runSender(amqpChannel, params)

	return nil
}

func (handler *AmqpHandler) runSender(amqpChannel *amqp.Channel, params cloudprotocol.SendParams) {
	log.Info("Start AMQP sender")

	defer func() {
		log.Info("AMQP sender closed")

		handler.wg.Done()
	}()

	errorChannel := handler.sendConnection.NotifyClose(make(chan *amqp.Error, 1))
	confirmChannel := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))
	sendChannel := handler.sendChannel

	if len(handler.pendingChannel) > 0 {
		sendChannel = nil
	}

	for {
		select {
		case err := <-errorChannel:
			if err != nil {
				handler.MessageChannel <- aoserrors.New(err.Reason)
			}

			return

		case message := <-sendChannel:
			handler.sendTry = 0
			sendChannel = nil
			handler.pendingChannel <- message

		case message := <-handler.pendingChannel:
			if err := handler.sendMessage(message, amqpChannel, params); err != nil {
				log.Warnf("Can't send message: %v", err)

				sendChannel = handler.sendChannel

				break
			}

			if confirm, ok := <-confirmChannel; !ok || !confirm.Ack {
				handler.pendingChannel <- message

				break
			}

			sendChannel = handler.sendChannel
		}
	}
}

func (handler *AmqpHandler) setupReceiveConnection(
	scheme string, params cloudprotocol.ReceiveParams, tlsConfig *tls.Config,
) error {
	urlRabbitMQ := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(params.User, params.Password),
		Host:   params.Host,
	}

	log.WithField("url", urlRabbitMQ.String()).Debug("Consumer connection url")

	connection, err := amqp.DialConfig(urlRabbitMQ.String(), amqp.Config{
		TLSClientConfig: tlsConfig,
		SASL:            nil,
		Heartbeat:       10 * time.Second,
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	amqpChannel, err := connection.Channel()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	deliveryChannel, err := amqpChannel.Consume(
		params.Queue.Name, // queue
		params.Consumer,   // consumer
		true,              // auto-ack param.AutoAck
		params.Exclusive,  // exclusive
		params.NoLocal,    // no-local
		params.NoWait,     // no-wait
		nil,               // args
	)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	handler.receiveConnection = connection

	handler.wg.Add(1)

	go handler.runReceiver(deliveryChannel, params)

	return nil
}

func (handler *AmqpHandler) runReceiver(deliveryChannel <-chan amqp.Delivery, param cloudprotocol.ReceiveParams) {
	log.Info("Start AMQP receiver")

	defer func() {
		log.Info("AMQP receiver closed")

		handler.wg.Done()
	}()

	errorChannel := handler.receiveConnection.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case err := <-errorChannel:
			if err != nil {
				handler.MessageChannel <- aoserrors.New(err.Reason)
			}

			return

		case delivery, ok := <-deliveryChannel:
			if !ok {
				handler.MessageChannel <- aoserrors.New("delivery channel is closed")
				return
			}

			decryptData, err := handler.cryptoContext.DecryptMetadata(delivery.Body)
			if err != nil {
				log.Errorf("Can't decrypt message: %v", err)

				continue
			}

			var incomingMsg cloudprotocol.ReceivedMessage
			if err := json.Unmarshal(decryptData, &incomingMsg); err != nil {
				log.Errorf("Can't parse message: %v", err)

				continue
			}

			if incomingMsg.Header.Version != cloudprotocol.ProtocolVersion {
				log.Errorf("Unsupported protocol version: %d", incomingMsg.Header.Version)

				continue
			}

			decodedData, err := handler.unmarshalReceiveData(incomingMsg.Data)
			if err != nil {
				log.Errorf("Can't unmarshal incoming message %s", err)

				continue
			}

			handler.MessageChannel <- decodedData
		}
	}
}

func (handler *AmqpHandler) unmarshalReceiveData(data []byte) (Message, error) {
	if len(data) == 0 {
		//nolint:nilnil
		return nil, nil
	}

	// unmarshal type name
	var messageType struct {
		Type string `json:"messageType"`
	}

	if err := json.Unmarshal(data, &messageType); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	// unmarshal type message
	messageTypeFunc, ok := messageMap[messageType.Type]
	if !ok {
		log.Warnf("AMQP unsupported message type: %s", messageType.Type)

		return nil, aoserrors.New("AMQP unsupported message type")
	}

	messageData := messageTypeFunc()

	if err := json.Unmarshal(data, &messageData); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	// print DesiredStatus message
	desiredStatus, ok := messageData.(*cloudprotocol.DesiredStatus)
	if !ok {
		return messageData, nil
	}

	log.Debug("Decrypted data:")

	if desiredStatus.UnitConfig != nil {
		log.Debugf("UnitConfig: %v", desiredStatus.UnitConfig)
	}

	for _, service := range desiredStatus.Services {
		log.WithFields(log.Fields{
			"id":      service.ServiceID,
			"version": service.Version,
		}).Debug("Service")
	}

	for _, layer := range desiredStatus.Layers {
		log.WithFields(log.Fields{
			"id":      layer.LayerID,
			"digest":  layer.Digest,
			"version": layer.Version,
		}).Debug("Layer")
	}

	for _, component := range desiredStatus.Components {
		log.WithFields(log.Fields{
			"id":          component.ComponentID,
			"annotations": string(component.Annotations),
			"version":     component.Version,
		}).Debug("Component")
	}

	for _, instance := range desiredStatus.Instances {
		log.WithFields(log.Fields{
			"serviceID":    instance.ServiceID,
			"subjectID":    instance.SubjectID,
			"priority":     instance.Priority,
			"numInstances": instance.NumInstances,
			"labels":       instance.Labels,
		}).Debug("Instance")
	}

	if schedule, err := json.Marshal(desiredStatus.FOTASchedule); err == nil {
		log.Debugf("Fota schedule: %s", schedule)
	}

	if schedule, err := json.Marshal(desiredStatus.SOTASchedule); err == nil {
		log.Debugf("Sota schedule: %s", schedule)
	}

	return messageData, nil
}

func (handler *AmqpHandler) createCloudMessage(data interface{}) cloudprotocol.Message {
	return cloudprotocol.Message{
		Header: cloudprotocol.MessageHeader{
			Version:  cloudprotocol.ProtocolVersion,
			SystemID: handler.systemID,
		},
		Data: data,
	}
}

func (handler *AmqpHandler) notifyCloudConnected() {
	for _, consumer := range handler.connectionEventsConsumers {
		consumer.CloudConnected()
	}
}

func (handler *AmqpHandler) notifyCloudDisconnected() {
	for _, consumer := range handler.connectionEventsConsumers {
		consumer.CloudDisconnected()
	}
}

func (handler *AmqpHandler) scheduleMessage(data interface{}, important bool) error {
	if !important && !handler.isConnected {
		return ErrNotConnected
	}

	select {
	case handler.sendChannel <- handler.createCloudMessage(data):
		return nil

	case <-time.After(sendTimeout):
		return ErrSendChannelFull
	}
}

func (handler *AmqpHandler) sendMessage(
	message cloudprotocol.Message, amqpChannel *amqp.Channel, params cloudprotocol.SendParams,
) error {
	data, err := json.Marshal(message)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if handler.sendTry > 1 {
		log.WithField("data", string(data)).Debug("AMQP retry message")
	} else {
		log.WithField("data", string(data)).Debug("AMQP send message")
	}

	if handler.sendTry++; handler.sendTry > sendMaxTry {
		return aoserrors.New("sending message max try reached")
	}

	if err := amqpChannel.Publish(
		params.Exchange.Name, // exchange
		"",                   // routing key
		params.Mandatory,     // mandatory
		params.Immediate,     // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			UserId:       params.User,
			Body:         data,
		}); err != nil {
		// Do not return error in this case for purpose rescheduling message
		log.Errorf("Error publishing AMQP message: %v", err)
	}

	return nil
}
