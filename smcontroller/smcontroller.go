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
	"context"
	"net"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v3"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/launcher"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const statusChanSize = 10

const (
	openConnection = iota
	closeConnection
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller SM controller instance.
type Controller struct {
	sync.Mutex

	nodes map[string]*smHandler

	messageSender             MessageSender
	alertSender               AlertSender
	monitoringSender          MonitoringSender
	updateInstancesStatusChan chan []cloudprotocol.InstanceStatus
	runInstancesStatusChan    chan launcher.NodeRunInstanceStatus
	cancelFunction            context.CancelFunc

	grpcServer *grpc.Server
	listener   net.Listener
	url        string
	pb.UnimplementedSMServiceServer
}

type smCtrlInternalMsg struct {
	nodeID      string
	handler     *smHandler
	requestType int
}

// AlertSender sends alert.
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem)
}

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendMonitoringData(monitoringData cloudprotocol.NodeMonitoringData)
}

// MessageSender sends messages to the cloud.
type MessageSender interface {
	SendInstanceNewState(newState cloudprotocol.NewState) error
	SendInstanceStateRequest(request cloudprotocol.StateRequest) error
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
		nodes:                     make(map[string]*smHandler),
		url:                       cfg.SMController.SMServerURL,
	}

	for _, nodeID := range cfg.SMController.NodeIDs {
		controller.nodes[nodeID] = nil
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
		if err := controller.startServer(); err != nil {
			log.Errorf("Can't start SM controller server: %s", err)
		}
	}()

	return controller, nil
}

// Close closes SM controller.
func (controller *Controller) Close() error {
	log.Debug("Close SM controller")

	controller.stopServer()

	return nil
}

// GetNodeConfiguration gets node static configuration.
func (controller *Controller) GetNodeConfiguration(nodeID string) (cfg launcher.NodeConfiguration, err error) {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return cfg, aoserrors.Wrap(err)
	}

	return handler.config, nil
}

// GetUnitConfigStatus gets unit configuration status fot he node.
func (controller *Controller) GetUnitConfigStatus(nodeID string) (string, error) {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return handler.getUnitConfigState()
}

// CheckUnitConfig checks unit config for the node.
func (controller *Controller) CheckUnitConfig(nodeCfg aostypes.NodeConfig, vendorVersion string) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeCfg.NodeID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return handler.checkUnitConfigState(nodeCfg, vendorVersion)
}

// SetUnitConfig sets usint config for the node.
func (controller *Controller) SetUnitConfig(nodeCfg aostypes.NodeConfig, vendorVersion string) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeCfg.NodeID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return handler.setUnitConfig(nodeCfg, vendorVersion)
}

// RunInstances runs desired services instances.
func (controller *Controller) RunInstances(nodeID string,
	services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo,
) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	return handler.runInstances(services, layers, instances)
}

// InstanceStateAcceptance handles instance state acceptance.
func (controller *Controller) InstanceStateAcceptance(
	nodeID string, stateAcceptance cloudprotocol.StateAcceptance,
) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.instanceStateAcceptance(stateAcceptance); err != nil {
		return err
	}

	return nil
}

// SetServiceState sets instance state.
func (controller *Controller) SetInstanceState(nodeID string, state cloudprotocol.UpdateState) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.setInstanceState(state); err != nil {
		return err
	}

	return nil
}

// OverrideEnvVars overrides instance env vars.
func (controller *Controller) OverrideEnvVars(nodeID string, envVars cloudprotocol.OverrideEnvVars) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.overrideEnvVars(envVars); err != nil {
		return err
	}

	return nil
}

// GetSystemLog requests system log from SM.
func (controller *Controller) GetSystemLog(nodeID string, logRequest cloudprotocol.RequestSystemLog) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.getSystemLog(logRequest); err != nil {
		return err
	}

	return nil
}

// GetInstanceLog requests service instance log from SM.
func (controller *Controller) GetInstanceLog(nodeID string, logRequest cloudprotocol.RequestServiceLog) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.getInstanceLog(logRequest); err != nil {
		return err
	}

	return nil
}

// GetInstanceCrashLog requests service instance crash log from SM.
func (controller *Controller) GetInstanceCrashLog(
	nodeID string, logRequest cloudprotocol.RequestServiceCrashLog,
) error {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return err
	}

	if err := handler.getInstanceCrashLog(logRequest); err != nil {
		return err
	}

	return nil
}

// GetInstanceCrashLog requests service instance crash log from SM.
func (controller *Controller) GetNodeMonitoringData(nodeID string) (data cloudprotocol.NodeMonitoringData, err error) {
	controller.Lock()
	defer controller.Unlock()

	handler, err := controller.getNodeHandlerByID(nodeID)
	if err != nil {
		return data, err
	}

	return handler.getNodeMonitoring()
}

// GetUpdateInstancesStatusChannel returns channel with update instances status.
func (controller *Controller) GetUpdateInstancesStatusChannel() <-chan []cloudprotocol.InstanceStatus {
	return controller.updateInstancesStatusChan
}

// GetRunInstancesStatusChannel returns channel with run instances status.
func (controller *Controller) GetRunInstancesStatusChannel() <-chan launcher.NodeRunInstanceStatus {
	return controller.runInstancesStatusChan
}

// RegisterSM registers new SM client connection.
func (controller *Controller) RegisterSM(stream pb.SMService_RegisterSMServer) error {
	message, err := stream.Recv()
	if err != nil {
		log.Errorf("Error receive message from SM: %v", err)

		return aoserrors.Wrap(err)
	}

	nodeConfig, ok := message.SMOutgoingMessage.(*pb.SMOutgoingMessages_NodeConfiguration)
	if !ok {
		log.Error("unexpected first messager from stream")

		return aoserrors.New("Unexpected first messager from stream")
	}

	log.Debugf("Register SM id %s", nodeConfig.NodeConfiguration.NodeId)

	nodeCfg := launcher.NodeConfiguration{
		NodeInfo: cloudprotocol.NodeInfo{
			NodeID: nodeConfig.NodeConfiguration.NodeId, NumCPUs: nodeConfig.NodeConfiguration.NumCpus,
			TotalRAM:   nodeConfig.NodeConfiguration.TotalRam,
			Partitions: make([]cloudprotocol.PartitionInfo, len(nodeConfig.NodeConfiguration.Partitions)),
		},
		RemoteNode:    nodeConfig.NodeConfiguration.RemoteNode,
		RunnerFeature: message.GetNodeConfiguration().RunnerFeatures,
	}

	for i, pbPartition := range nodeConfig.NodeConfiguration.Partitions {
		nodeCfg.Partitions[i] = cloudprotocol.PartitionInfo{
			Name: pbPartition.Name,
			Type: pbPartition.Type, TotalSize: pbPartition.TotalSize,
		}
	}

	handler, err := newSMHandler(stream, controller.messageSender, controller.alertSender, controller.monitoringSender, nodeCfg,
		controller.runInstancesStatusChan, controller.updateInstancesStatusChan)
	if err != nil {
		return err
	}

	controller.handleNewConnection(nodeConfig.NodeConfiguration.NodeId, handler)

	// wait for close
	<-handler.closeChannel

	controller.handleCloseConnection(nodeConfig.NodeConfiguration.NodeId)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (controller *Controller) establishGRPCServer(
	cfg *config.Config, certProvider CertificateProvider, cryptcoxontext *cryptutils.CryptoContext, insecure bool,
) (err error) {
	log.WithField("host", cfg.SMController.SMServerURL).Debug("Start SM server")

	return nil
}

func (controller *Controller) startServer() (err error) {
	controller.listener, err = net.Listen("tcp", controller.url)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := controller.grpcServer.Serve(controller.listener); err != nil {
		log.Errorf("Can't serve gRPC server: %s", err)

		return err
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

func (controller *Controller) handleNewConnection(nodeID string, newHandler *smHandler) {
	controller.Lock()
	defer controller.Unlock()

	if handler, ok := controller.nodes[nodeID]; ok {
		if handler != nil {
			log.Warnf("Connection for nodeID %s already exist. Replace it", nodeID)
		}
	} else {
		log.Warnf("Unknown nodeID connection with nodeID %s", nodeID)
	}

	controller.nodes[nodeID] = newHandler

	for _, node := range controller.nodes {
		if node == nil {
			return
		}
	}

	log.Info("All SM client connected")
}

func (controller *Controller) handleCloseConnection(nodeID string) {
	controller.Lock()
	defer controller.Unlock()

	if _, ok := controller.nodes[nodeID]; !ok {
		log.Errorf("Connection for nodeID %s doesn't exist.", nodeID)

		return
	}

	controller.nodes[nodeID] = nil
}

func (controller *Controller) getNodeHandlerByID(nodeID string) (*smHandler, error) {
	handler, ok := controller.nodes[nodeID]
	if !ok {
		return nil, aoserrors.Errorf("unknown nodeID %s", nodeID)
	}

	if handler == nil {
		return nil, aoserrors.Errorf("connection for nodeID %s doesn't exist", nodeID)
	}

	return handler, nil
}
