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
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"

	"aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	sendChannelSize    = 32
	receiveChannelSize = 16
	retryChannelSize   = 8
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// AmqpHandler structure with all amqp connection info
type AmqpHandler struct {
	sync.Mutex

	// MessageChannel channel for amqp messages
	MessageChannel chan Message

	sendChannel  chan Message
	retryChannel chan Message

	sendConnection    *amqp.Connection
	receiveConnection *amqp.Connection

	cryptoContext CryptoContext

	systemID string

	ctx        context.Context
	cancelFunc context.CancelFunc

	wg sync.WaitGroup
}

// CryptoContext interface to access crypto functions
type CryptoContext interface {
	GetTLSConfig() (config *tls.Config, err error)
	DecryptMetadata(input []byte) (output []byte, err error)
}

// Message AMQP message with correlation ID
type Message struct {
	CorrelationID string
	Data          interface{}
}

/***********************************************************************************************************************
 * Variables
 **********************************************************************************************************************/

var messageMap = map[string]func() interface{}{
	cloudprotocol.DesiredStatusType: func() interface{} {
		return &cloudprotocol.DesiredStatus{}
	},
	cloudprotocol.RequestServiceCrashLogType: func() interface{} {
		return &cloudprotocol.RequestServiceCrashLog{}
	},
	cloudprotocol.RequestServiceLogType: func() interface{} {
		return &cloudprotocol.RequestServiceLog{}
	},
	cloudprotocol.RequestSystemLogType: func() interface{} {
		return &cloudprotocol.RequestSystemLog{}
	},
	cloudprotocol.StateAcceptanceType: func() interface{} {
		return &cloudprotocol.StateAcceptance{}
	},
	cloudprotocol.UpdateStateType: func() interface{} {
		return &cloudprotocol.UpdateState{}
	},
	cloudprotocol.RenewCertsNotificationType: func() interface{} {
		return &cloudprotocol.RenewCertsNotification{}
	},
	cloudprotocol.IssuedUnitCertsType: func() interface{} {
		return &cloudprotocol.IssuedUnitCerts{}
	},
	cloudprotocol.OverrideEnvVarsType: func() interface{} {
		return &cloudprotocol.OverrideEnvVars{}
	},
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new amqp object
func New() (handler *AmqpHandler, err error) {
	log.Debug("New AMQP")

	handler = &AmqpHandler{
		sendChannel:  make(chan Message, sendChannelSize),
		retryChannel: make(chan Message, retryChannelSize),
	}

	handler.ctx, handler.cancelFunc = context.WithCancel(context.Background())

	return handler, nil
}

// Connect connects to cloud
func (handler *AmqpHandler) Connect(cryptoContext CryptoContext, sdURL, systemID string, users []string) (err error) {
	handler.Lock()
	defer handler.Unlock()

	log.WithFields(log.Fields{"url": sdURL, "users": users}).Debug("AMQP connect")

	handler.cryptoContext = cryptoContext
	handler.systemID = systemID

	tlsConfig, err := handler.cryptoContext.GetTLSConfig()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	var connectionInfo cloudprotocol.ConnectionInfo

	if connectionInfo, err = getConnectionInfo(handler.ctx, sdURL,
		handler.createCloudMessage(cloudprotocol.ServiceDiscoveryType,
			cloudprotocol.ServiceDiscoveryRequest{Users: users}), tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = handler.setupConnections("amqps", connectionInfo, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// ConnectRabbit connects directly to RabbitMQ server without service discovery
func (handler *AmqpHandler) ConnectRabbit(systemID, host, user, password, exchange, consumer, queue string) (err error) {
	handler.Lock()
	defer handler.Unlock()

	log.WithFields(log.Fields{
		"host": host,
		"user": user}).Debug("AMQP direct connect")

	handler.systemID = systemID

	connectionInfo := cloudprotocol.ConnectionInfo{
		SendParams: cloudprotocol.SendParams{
			Host:     host,
			User:     user,
			Password: password,
			Exchange: cloudprotocol.ExchangeParams{Name: exchange}},
		ReceiveParams: cloudprotocol.ReceiveParams{
			Host:     host,
			User:     user,
			Password: password,
			Consumer: consumer,
			Queue:    cloudprotocol.QueueInfo{Name: queue}}}

	if err = handler.setupConnections("amqp", connectionInfo, nil); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// Disconnect disconnects from cloud
func (handler *AmqpHandler) Disconnect() (err error) {
	handler.Lock()
	defer handler.Unlock()

	log.Debug("AMQP disconnect")

	if handler.sendConnection != nil {
		handler.sendConnection.Close()
	}

	if handler.receiveConnection != nil {
		handler.receiveConnection.Close()
	}

	handler.wg.Wait()

	return nil
}

// SendUnitStatus sends unit status
func (handler *AmqpHandler) SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error) {
	unitStatusMsg := handler.createCloudMessage(cloudprotocol.UnitStatusType, unitStatus)

	handler.sendChannel <- Message{"", unitStatusMsg}

	return nil
}

// SendMonitoringData sends monitoring data
func (handler *AmqpHandler) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error) {
	monitoringMsg := handler.createCloudMessage(cloudprotocol.MonitoringDataType, monitoringData)

	handler.sendChannel <- Message{"", monitoringMsg}

	return nil
}

// SendServiceNewState sends new state message
func (handler *AmqpHandler) SendServiceNewState(correlationID, serviceID, state, checksum string) (err error) {
	newStateMsg := handler.createCloudMessage(cloudprotocol.NewStateType,
		cloudprotocol.NewState{ServiceID: serviceID, State: state, Checksum: checksum})

	handler.sendChannel <- Message{correlationID, newStateMsg}

	return nil
}

// SendServiceStateRequest sends state request message
func (handler *AmqpHandler) SendServiceStateRequest(serviceID string, defaultState bool) (err error) {
	stateRequestMsg := handler.createCloudMessage(cloudprotocol.StateRequestType,
		cloudprotocol.StateRequest{ServiceID: serviceID, Default: defaultState})

	handler.sendChannel <- Message{"", stateRequestMsg}

	return nil
}

// SendLog sends system or service logs
func (handler *AmqpHandler) SendLog(serviceLog cloudprotocol.PushLog) (err error) {
	serviceLogMsg := handler.createCloudMessage(cloudprotocol.PushLogType, serviceLog)

	handler.sendChannel <- Message{"", serviceLogMsg}

	return nil
}

// SendAlerts sends alerts message
func (handler *AmqpHandler) SendAlerts(alerts cloudprotocol.Alerts) (err error) {
	alertMsg := handler.createCloudMessage(cloudprotocol.AlertsType, alerts)

	handler.sendChannel <- Message{"", alertMsg}

	return nil
}

// SendIssueUnitCerts sends request to issue new certificates
func (handler *AmqpHandler) SendIssueUnitCerts(requests []cloudprotocol.IssueCertData) (err error) {
	request := handler.createCloudMessage(
		cloudprotocol.IssueUnitCertsType, cloudprotocol.IssueUnitCerts{Requests: requests})

	handler.sendChannel <- Message{"", request}

	return nil
}

// SendInstallCertsConfirmation sends install certificates confirmation
func (handler *AmqpHandler) SendInstallCertsConfirmation(
	confirmations []cloudprotocol.InstallCertData) (err error) {
	response := handler.createCloudMessage(cloudprotocol.InstallUnitCertsConfirmationType,
		cloudprotocol.InstallUnitCertsConfirmation{Certificates: confirmations})

	handler.sendChannel <- Message{"", response}

	return nil
}

// SendOverrideEnvVarsStatus overrides env vars status
func (handler *AmqpHandler) SendOverrideEnvVarsStatus(envs []cloudprotocol.EnvVarInfoStatus) (err error) {
	handler.sendChannel <- Message{"", handler.createCloudMessage(cloudprotocol.OverrideEnvVarsStatusType,
		cloudprotocol.OverrideEnvVarsStatus{OverrideEnvVarsStatus: envs})}

	return nil
}

// Close closes all amqp connection
func (handler *AmqpHandler) Close() {
	log.Info("Close AMQP")

	handler.cancelFunc()
	handler.Disconnect()
}

/***************************************************************************************************
 * Private
 **************************************************************************************************/

// service discovery implementation
func getConnectionInfo(ctx context.Context, url string,
	request cloudprotocol.Message, tlsConfig *tls.Config) (info cloudprotocol.ConnectionInfo, err error) {
	reqJSON, err := json.Marshal(request)
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	log.WithField("request", string(reqJSON)).Info("AMQP service discovery request")

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	client := &http.Client{Transport: transport}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(reqJSON))
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return info, aoserrors.Wrap(err)
	}
	defer resp.Body.Close()

	htmlData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	if resp.StatusCode != 200 {
		return info, aoserrors.Errorf("%s: %s", resp.Status, string(htmlData))
	}

	var jsonResp cloudprotocol.ServiceDiscoveryResponse

	err = json.Unmarshal(htmlData, &jsonResp) // TODO: add check
	if err != nil {
		return info, aoserrors.Wrap(err)
	}

	return jsonResp.Connection, nil
}

func (handler *AmqpHandler) setupConnections(scheme string,
	info cloudprotocol.ConnectionInfo, tlsConfig *tls.Config) (err error) {
	handler.MessageChannel = make(chan Message, receiveChannelSize)

	if err = handler.setupSendConnection(scheme, info.SendParams, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = handler.setupReceiveConnection(scheme, info.ReceiveParams, tlsConfig); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *AmqpHandler) setupSendConnection(scheme string,
	params cloudprotocol.SendParams, tlsConfig *tls.Config) (err error) {
	urlRabbitMQ := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(params.User, params.Password),
		Host:   params.Host,
	}

	log.WithField("url", urlRabbitMQ.String()).Debug("Sender connection url")

	connection, err := amqp.DialConfig(urlRabbitMQ.String(), amqp.Config{
		TLSClientConfig: tlsConfig,
		SASL:            nil,
		Heartbeat:       10 * time.Second})
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

	go handler.runSender(params, amqpChannel)

	return nil
}

func (handler *AmqpHandler) runSender(params cloudprotocol.SendParams, amqpChannel *amqp.Channel) {
	log.Info("Start AMQP sender")

	defer func() {
		log.Info("AMQP sender closed")

		handler.wg.Done()
	}()

	errorChannel := handler.sendConnection.NotifyClose(make(chan *amqp.Error, 1))
	confirmChannel := amqpChannel.NotifyPublish(make(chan amqp.Confirmation, 1))

	for {
		var message Message
		retry := false

		select {
		case err := <-errorChannel:
			if err != nil {
				handler.MessageChannel <- Message{"", aoserrors.New(err.Reason)}
			}

			return

		case message = <-handler.retryChannel:
			retry = true

		case message = <-handler.sendChannel:
		}

		data, err := json.Marshal(message.Data)
		if err != nil {
			log.Errorf("Can't parse message: %s", err)
			continue
		}

		if retry {
			log.WithFields(log.Fields{
				"correlationID": message.CorrelationID,
				"data":          string(data)}).Debug("AMQP retry message")
		} else {
			log.WithFields(log.Fields{
				"correlationID": message.CorrelationID,
				"data":          string(data)}).Debug("AMQP send message")
		}

		if err := amqpChannel.Publish(
			params.Exchange.Name, // exchange
			"",                   // routing key
			params.Mandatory,     // mandatory
			params.Immediate,     // immediate
			amqp.Publishing{
				ContentType:   "application/json",
				DeliveryMode:  amqp.Persistent,
				CorrelationId: message.CorrelationID,
				UserId:        params.User,
				Body:          data,
			}); err != nil {
			log.Errorf("Error publishing AMQP message: %s", err)
		}

		// Handle retry packets
		confirm, ok := <-confirmChannel
		if !ok || !confirm.Ack {
			log.WithFields(log.Fields{
				"correlationID": message.CorrelationID,
				"data":          string(data)}).Warning("AMQP data is not sent. Put into retry queue")

			handler.retryChannel <- message
		}
	}
}

func (handler *AmqpHandler) setupReceiveConnection(scheme string,
	params cloudprotocol.ReceiveParams, tlsConfig *tls.Config) (err error) {
	urlRabbitMQ := url.URL{
		Scheme: scheme,
		User:   url.UserPassword(params.User, params.Password),
		Host:   params.Host,
	}

	log.WithField("url", urlRabbitMQ.String()).Debug("Consumer connection url")

	connection, err := amqp.DialConfig(urlRabbitMQ.String(), amqp.Config{
		TLSClientConfig: tlsConfig,
		SASL:            nil,
		Heartbeat:       10 * time.Second})
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

	go handler.runReceiver(params, deliveryChannel)

	return nil
}

func (handler *AmqpHandler) runReceiver(param cloudprotocol.ReceiveParams, deliveryChannel <-chan amqp.Delivery) {
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
				handler.MessageChannel <- Message{"", aoserrors.New(err.Reason)}
			}

			return

		case delivery, ok := <-deliveryChannel:
			if !ok {
				handler.MessageChannel <- Message{"", aoserrors.New("delivery channel is closed")}
				return
			}

			var rawData json.RawMessage
			incomingMsg := cloudprotocol.Message{Data: &rawData}

			if err := json.Unmarshal(delivery.Body, &incomingMsg); err != nil {
				log.Errorf("Can't parse message header: %s", err)
				continue
			}

			log.WithFields(log.Fields{
				"corrlationId": delivery.CorrelationId,
				"version":      incomingMsg.Header.Version,
				"type":         incomingMsg.Header.MessageType}).Debug("AMQP received message")

			if incomingMsg.Header.Version != cloudprotocol.ProtocolVersion {
				log.Errorf("Unsupported protocol version: %d", incomingMsg.Header.Version)
				continue
			}

			messageType, ok := messageMap[incomingMsg.Header.MessageType]
			if !ok {
				log.Warnf("AMQP unsupported message type: %s", incomingMsg.Header.MessageType)
				continue
			}

			data := messageType()

			if err := json.Unmarshal(rawData, data); err != nil {
				log.Errorf("Can't parse message body: %s", err)
				continue
			}

			switch incomingMsg.Header.MessageType {
			case cloudprotocol.DesiredStatusType:
				encodedStatus, ok := data.(*cloudprotocol.DesiredStatus)
				if !ok {
					log.Error("Wrong data type: expect desired status")
					continue
				}

				decodedStatus, err := handler.decodeDesiredStatus(encodedStatus)
				if err != nil {
					log.Errorf("Can't decode desired status: %s", err)
					continue
				}

				data = decodedStatus

			case cloudprotocol.RenewCertsNotificationType:
				notification, ok := data.(*cloudprotocol.RenewCertsNotification)
				if !ok {
					log.Error("Wrong data type: expect renew certificate notification")
					continue
				}

				notificationWithPwd, err := handler.decodeRenewCertsNotification(notification)
				if err != nil {
					log.Errorf("Can't decode renew certificate notification: %s", err)
					continue
				}

				data = notificationWithPwd

			case cloudprotocol.OverrideEnvVarsType:
				encodedEnvVars, ok := data.(*cloudprotocol.OverrideEnvVars)
				if !ok {
					log.Error("Wrong data type: expect override env")
					continue
				}

				decodedEnvVars, err := handler.decodeEnvVars(encodedEnvVars)
				if err != nil {
					log.Errorf("Can't decode env vars: %s", err)
					continue
				}

				data = decodedEnvVars
			}

			handler.MessageChannel <- Message{delivery.CorrelationId, data}
		}
	}
}

func (handler *AmqpHandler) decodeDesiredStatus(
	encodedStatus *cloudprotocol.DesiredStatus) (decodedStatus *cloudprotocol.DecodedDesiredStatus, err error) {
	decodedStatus = &cloudprotocol.DecodedDesiredStatus{
		CertificateChains: encodedStatus.CertificateChains,
		Certificates:      encodedStatus.Certificates}

	if err = handler.decodeData(encodedStatus.BoardConfig, &decodedStatus.BoardConfig); err != nil {
		return nil, err
	}

	if err = handler.decodeData(encodedStatus.Services, &decodedStatus.Services); err != nil {
		return nil, err
	}

	if err = handler.decodeData(encodedStatus.Layers, &decodedStatus.Layers); err != nil {
		return nil, err
	}

	if err = handler.decodeData(encodedStatus.Components, &decodedStatus.Components); err != nil {
		return nil, err
	}

	if err = handler.decodeData(encodedStatus.FOTASchedule, &decodedStatus.FOTASchedule); err != nil {
		return nil, err
	}

	if err = handler.decodeData(encodedStatus.SOTASchedule, &decodedStatus.SOTASchedule); err != nil {
		return nil, err
	}

	return decodedStatus, nil
}

func (handler *AmqpHandler) decodeRenewCertsNotification(
	encodedNotification *cloudprotocol.RenewCertsNotification) (
	decodedNotification *cloudprotocol.RenewCertsNotificationWithPwd, err error) {
	var secret cloudprotocol.UnitSecret

	if len(encodedNotification.UnitSecureData) > 0 {
		if err = handler.decodeData(encodedNotification.UnitSecureData, &secret); err != nil {
			return nil, err
		}

		if secret.Version != cloudprotocol.UnitSecretVersion {
			return nil, aoserrors.New("unit secure version missmatch")
		}
	}

	return &cloudprotocol.RenewCertsNotificationWithPwd{
		Certificates: encodedNotification.Certificates,
		Password:     secret.Data.OwnerPassword}, nil
}

func (handler *AmqpHandler) decodeEnvVars(
	encodedEnvVars *cloudprotocol.OverrideEnvVars) (decodedEnvVars *cloudprotocol.DecodedOverrideEnvVars, err error) {
	decodedEnvVars = &cloudprotocol.DecodedOverrideEnvVars{}

	if err = handler.decodeData(encodedEnvVars.OverrideEnvVars, decodedEnvVars); err != nil {
		return nil, err
	}

	return decodedEnvVars, nil
}

func (handler *AmqpHandler) decodeData(data []byte, result interface{}) (err error) {
	if len(data) == 0 {
		return nil
	}

	decryptData, err := handler.cryptoContext.DecryptMetadata(data)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(decryptData, result); err != nil {
		return aoserrors.Wrap(err)
	}

	if rawJSON, ok := result.(*json.RawMessage); ok {
		log.WithField("data", string(*rawJSON)).Debug("Decrypted data")
	} else {
		log.WithField("data", result).Debug("Decrypted data")
	}

	return nil
}

func (handler *AmqpHandler) createCloudMessage(messageType string, data interface{}) (message cloudprotocol.Message) {
	return cloudprotocol.Message{
		Header: cloudprotocol.MessageHeader{
			Version:     cloudprotocol.ProtocolVersion,
			SystemID:    handler.systemID,
			MessageType: messageType},
		Data: data}
}
