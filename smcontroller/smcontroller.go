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

package smcontroller

import (
	"errors"
	"io"
	"net"
	"sync"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/servicemanager"
	"github.com/aosedge/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/aosedge/aos_communicationmanager/amqphandler"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const statusChanSize = 10

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller SM controller instance.
type Controller struct {
	sync.Mutex

	nodes map[string]*smHandler

	logHandler map[string]func(logRequest cloudprotocol.RequestLog) error

	messageSender             MessageSender
	alertSender               AlertSender
	monitoringSender          MonitoringSender
	updateInstancesStatusChan chan []cloudprotocol.InstanceStatus
	runInstancesStatusChan    chan launcher.NodeRunInstanceStatus
	systemQuotaAlertChan      chan cloudprotocol.SystemQuotaAlert
	nodeConfigStatusChan      chan unitconfig.NodeConfigStatus

	isCloudConnected bool
	grpcServer       *grpc.Server
	listener         net.Listener
	pb.UnimplementedSMServiceServer
}

// AlertSender sends alert.
type AlertSender interface {
	SendAlert(alert interface{})
}

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendNodeMonitoring(monitoring aostypes.NodeMonitoring)
}

// MessageSender sends messages to the cloud.
type MessageSender interface {
	SubscribeForConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	UnsubscribeFromConnectionEvents(consumer amqphandler.ConnectionEventsConsumer) error
	SendOverrideEnvVarsStatus(envs cloudprotocol.OverrideEnvVarsStatus) error
	SendLog(serviceLog cloudprotocol.PushLog) error
}

// CertificateProvider certificate and key provider interface.
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, keyURL string, err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new SM controller.
func New(
	cfg *config.Config, messageSender MessageSender, alertSender AlertSender, monitoringSender MonitoringSender,
	certProvider CertificateProvider, cryptcoxontext *cryptutils.CryptoContext,
	insecureConn bool,
) (controller *Controller, err error) {
	log.Debug("Create SM controller")

	controller = &Controller{
		messageSender:             messageSender,
		alertSender:               alertSender,
		monitoringSender:          monitoringSender,
		runInstancesStatusChan:    make(chan launcher.NodeRunInstanceStatus, statusChanSize),
		updateInstancesStatusChan: make(chan []cloudprotocol.InstanceStatus, statusChanSize),
		systemQuotaAlertChan:      make(chan cloudprotocol.SystemQuotaAlert, statusChanSize),
		nodeConfigStatusChan:      make(chan unitconfig.NodeConfigStatus, statusChanSize),
		nodes:                     make(map[string]*smHandler),
	}

	if controller.messageSender != nil {
		if err = controller.messageSender.SubscribeForConnectionEvents(controller); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	controller.logHandler = map[string]func(cloudprotocol.RequestLog) error{
		cloudprotocol.SystemLog:  controller.getSystemLog,
		cloudprotocol.ServiceLog: controller.getServiceLog,
		cloudprotocol.CrashLog:   controller.getCrashLog,
	}

	var opts []grpc.ServerOption

	if !insecureConn {
		certURL, keyURL, err := certProvider.GetCertificate(cfg.CertStorage, nil, "")
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		tlsConfig, err := cryptcoxontext.GetClientMutualTLSConfig(certURL, keyURL)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Info("GRPC server starts in insecure mode")
	}

	controller.grpcServer = grpc.NewServer(opts...)

	pb.RegisterSMServiceServer(controller.grpcServer, controller)

	go func() {
		if err := controller.startServer(cfg.SMController.CMServerURL); err != nil {
			log.Errorf("Can't start SM controller server: %v", err)
		}
	}()

	return controller, nil
}

// Close closes SM controller.
func (controller *Controller) Close() error {
	log.Debug("Close SM controller")

	controller.stopServer()

	if controller.messageSender != nil {
		if err := controller.messageSender.UnsubscribeFromConnectionEvents(controller); err != nil {
			log.Errorf("Can't unsubscribe from connection events: %v", err)
		}
	}

	close(controller.nodeConfigStatusChan)

	return nil
}

// GetNodeConfigStatus gets node configuration status.
func (controller *Controller) GetNodeConfigStatus(nodeID string) (unitconfig.NodeConfigStatus, error) {
	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return unitconfig.NodeConfigStatus{}, err
	}

	return handler.nodeConfigStatus, nil
}

// CheckNodeConfig checks node config.
func (controller *Controller) CheckNodeConfig(version string, nodeConfig cloudprotocol.NodeConfig) error {
	if nodeConfig.NodeID == nil {
		return aoserrors.Errorf("node ID is not set")
	}

	handler, err := controller.getNodeHandlerByID(*nodeConfig.NodeID)
	if err != nil {
		return err
	}

	if err = handler.checkNodeConfig(version, nodeConfig); err != nil {
		return err
	}

	return nil
}

// SetNodeConfig sets node config.
func (controller *Controller) SetNodeConfig(version string, nodeConfig cloudprotocol.NodeConfig) error {
	if nodeConfig.NodeID == nil {
		return aoserrors.Errorf("node ID is not set")
	}

	handler, err := controller.getNodeHandlerByID(*nodeConfig.NodeID)
	if err != nil {
		return err
	}

	if err = handler.setNodeConfig(version, nodeConfig); err != nil {
		return err
	}

	if handler.nodeConfigStatus, err = handler.getNodeConfigStatus(); err != nil {
		return err
	}

	return nil
}

// GetNodeConfigStatuses returns node configuration statuses.
func (controller *Controller) GetNodeConfigStatuses() ([]unitconfig.NodeConfigStatus, error) {
	controller.Lock()
	defer controller.Unlock()

	statuses := make([]unitconfig.NodeConfigStatus, 0, len(controller.nodes))

	for _, handler := range controller.nodes {
		statuses = append(statuses, handler.nodeConfigStatus)
	}

	return statuses, nil
}

// NodeConfigStatusChannel returns channel used to send new node configuration statuses.
func (controller *Controller) NodeConfigStatusChannel() <-chan unitconfig.NodeConfigStatus {
	return controller.nodeConfigStatusChan
}

// RunInstances runs desired services instances.
func (controller *Controller) RunInstances(nodeID string,
	services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo, forceRestart bool,
) error {
	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	return handler.runInstances(services, layers, instances, forceRestart)
}

// UpdateNetwork updates node networks configuration.
func (controller *Controller) UpdateNetwork(nodeID string, networkParameters []aostypes.NetworkParameters) error {
	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	return handler.updateNetworks(networkParameters)
}

// OverrideEnvVars overrides instance env vars.
func (controller *Controller) OverrideEnvVars(nodeID string, envVars cloudprotocol.OverrideEnvVars) error {
	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.overrideEnvVars(envVars); err != nil {
		return err
	}

	return nil
}

// GetLog requests log from SM.
func (controller *Controller) GetLog(logRequest cloudprotocol.RequestLog) error {
	handler, ok := controller.logHandler[logRequest.LogType]
	if !ok {
		return aoserrors.Errorf("Unexpected log type: %s", logRequest.LogType)
	}

	return handler(logRequest)
}

// GetAverageMonitoring returns average monitoring data for the node.
func (controller *Controller) GetAverageMonitoring(nodeID string) (aostypes.NodeMonitoring, error) {
	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return aostypes.NodeMonitoring{}, err
	}

	return handler.getAverageMonitoring()
}

// GetUpdateInstancesStatusChannel returns channel with update instances status.
func (controller *Controller) GetUpdateInstancesStatusChannel() <-chan []cloudprotocol.InstanceStatus {
	return controller.updateInstancesStatusChan
}

// GetRunInstancesStatusChannel returns channel with run instances status.
func (controller *Controller) GetRunInstancesStatusChannel() <-chan launcher.NodeRunInstanceStatus {
	return controller.runInstancesStatusChan
}

// GetSystemQuoteAlertChannel returns channel with alerts about RAM, CPU system limits.
func (controller *Controller) GetSystemQuoteAlertChannel() <-chan cloudprotocol.SystemQuotaAlert {
	return controller.systemQuotaAlertChan
}

// RegisterSM registers new SM client connection.
func (controller *Controller) RegisterSM(stream pb.SMService_RegisterSMServer) error {
	var handler *smHandler

	for {
		message, err := stream.Recv()
		if err != nil {
			if handler != nil {
				controller.handleCloseConnection(handler.nodeID)
			}

			if !errors.Is(err, io.EOF) && codes.Canceled != status.Code(err) {
				log.Errorf("Close SM client connection error: %v", err)

				return aoserrors.Wrap(err)
			}

			return nil
		}

		if handler == nil {
			nodeConfigStatus, ok := message.GetSMOutgoingMessage().(*pb.SMOutgoingMessages_NodeConfigStatus)
			if !ok {
				log.Error("Unexpected first message from stream")

				continue
			}

			nodeID := nodeConfigStatus.NodeConfigStatus.GetNodeId()
			nodeType := nodeConfigStatus.NodeConfigStatus.GetNodeType()

			log.WithFields(log.Fields{
				"nodeID":   nodeID,
				"nodeType": nodeType,
			}).Debug("Register SM")

			handler, err = newSMHandler(nodeID, nodeType, stream, controller.messageSender, controller.alertSender,
				controller.monitoringSender, controller.runInstancesStatusChan, controller.updateInstancesStatusChan,
				controller.systemQuotaAlertChan)
			if err != nil {
				log.Errorf("Can't crate SM handler: %v", err)

				return err
			}

			if err := controller.handleNewConnection(
				nodeConfigStatusFromPB(nodeConfigStatus.NodeConfigStatus), handler); err != nil {
				log.Errorf("Can't register new SM connection: %v", err)

				return err
			}

			continue
		}

		handler.processSMMessages(message)
	}
}

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

// CloudConnected indicates unit connected to cloud.
func (controller *Controller) CloudConnected() {
	controller.Lock()
	defer controller.Unlock()

	controller.isCloudConnected = true

	controller.sendConnectionStatus()
}

// CloudDisconnected indicates unit disconnected from cloud.
func (controller *Controller) CloudDisconnected() {
	controller.Lock()
	defer controller.Unlock()

	controller.isCloudConnected = false

	controller.sendConnectionStatus()
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (controller *Controller) getSystemLog(logRequest cloudprotocol.RequestLog) error {
	handlers, err := controller.getNodeHandlersByIDs(logRequest.Filter.NodeIDs)
	if err != nil {
		return err
	}

	for _, handler := range handlers {
		if err := handler.getSystemLog(logRequest); err != nil {
			return err
		}
	}

	return nil
}

func (controller *Controller) getServiceLog(logRequest cloudprotocol.RequestLog) error {
	handlers, err := controller.getNodeHandlersByIDs(logRequest.Filter.NodeIDs)
	if err != nil {
		return err
	}

	for _, handler := range handlers {
		if err := handler.getInstanceLog(logRequest); err != nil {
			return err
		}
	}

	return nil
}

func (controller *Controller) getCrashLog(logRequest cloudprotocol.RequestLog) error {
	handlers, err := controller.getNodeHandlersByIDs(logRequest.Filter.NodeIDs)
	if err != nil {
		return err
	}

	for _, handler := range handlers {
		if err := handler.getInstanceCrashLog(logRequest); err != nil {
			return err
		}
	}

	return nil
}

func (controller *Controller) getNodeHandlersByIDs(nodeIDs []string) (handlers []*smHandler, err error) {
	if len(nodeIDs) > 0 {
		for _, nodeID := range nodeIDs {
			handler, err := controller.getNodeHandlerByID(nodeID)
			if err != nil {
				return nil, err
			}

			handlers = append(handlers, handler)
		}

		return handlers, nil
	}

	controller.Lock()
	defer controller.Unlock()

	for _, handler := range controller.nodes {
		if handler != nil {
			handlers = append(handlers, handler)
		}
	}

	return handlers, nil
}

func (controller *Controller) startServer(serverURL string) (err error) {
	controller.listener, err = net.Listen("tcp", serverURL)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := controller.grpcServer.Serve(controller.listener); err != nil {
		log.Errorf("Can't serve gRPC server: %s", err)

		return aoserrors.Wrap(err)
	}

	return nil
}

func (controller *Controller) stopServer() {
	if controller.grpcServer != nil {
		controller.grpcServer.Stop()
	}

	if controller.listener != nil {
		controller.listener.Close()
	}
}

func (controller *Controller) handleNewConnection(
	nodeConfigStatus unitconfig.NodeConfigStatus, newHandler *smHandler,
) error {
	controller.Lock()
	defer controller.Unlock()

	if _, ok := controller.nodes[nodeConfigStatus.NodeID]; ok {
		return aoserrors.Errorf("connection for node ID %s already exist", nodeConfigStatus.NodeID)
	}

	controller.nodes[nodeConfigStatus.NodeID] = newHandler
	newHandler.nodeConfigStatus = nodeConfigStatus
	controller.nodeConfigStatusChan <- nodeConfigStatus

	if err := newHandler.sendConnectionStatus(controller.isCloudConnected); err != nil {
		log.Errorf("Can't send connection status: %v", err)
	}

	return nil
}

func (controller *Controller) handleCloseConnection(nodeID string) {
	controller.Lock()
	defer controller.Unlock()

	if _, ok := controller.nodes[nodeID]; !ok {
		log.Errorf("Connection for nodeID %s doesn't exist", nodeID)

		return
	}

	delete(controller.nodes, nodeID)
}

func (controller *Controller) getNodeHandlerByID(nodeID string) (*smHandler, error) {
	controller.Lock()
	defer controller.Unlock()

	handler, ok := controller.nodes[nodeID]
	if !ok {
		return nil, aoserrors.Errorf("unknown nodeID %s", nodeID)
	}

	if handler == nil {
		return nil, aoserrors.Errorf("connection for nodeID %s doesn't exist", nodeID)
	}

	return handler, nil
}

func (controller *Controller) sendConnectionStatus() {
	for _, handler := range controller.nodes {
		if handler == nil {
			continue
		}

		if err := handler.sendConnectionStatus(controller.isCloudConnected); err != nil {
			log.Errorf("Can't send connection status: %v", err)
		}
	}
}
