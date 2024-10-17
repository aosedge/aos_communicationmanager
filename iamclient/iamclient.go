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

package iamclient

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/iamanager"
	"github.com/aosedge/aos_common/utils/cryptutils"
	"github.com/aosedge/aos_common/utils/grpchelpers"
	pbconvert "github.com/aosedge/aos_common/utils/pbconvert"
	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	iamRequestTimeout    = 30 * time.Second
	iamReconnectInterval = 10 * time.Second
)

const iamCertType = "iam"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Client IAM client instance.
type Client struct {
	sync.Mutex
	*grpchelpers.IAMPublicServiceClient

	config        *config.Config
	sender        Sender
	cryptocontext *cryptutils.CryptoContext
	insecure      bool

	nodeID   string
	systemID string

	publicConnection    *grpchelpers.BlockingGRPCConn
	protectedConnection *grpc.ClientConn

	identService        pb.IAMPublicIdentityServiceClient
	certificateService  pb.IAMCertificateServiceClient
	provisioningService pb.IAMProvisioningServiceClient
	publicNodesService  pb.IAMPublicNodesServiceClient
	nodesService        pb.IAMNodesServiceClient

	nodeInfoSubs *nodeInfoChangeSub

	tlsCertChan      <-chan *pb.CertInfo
	closeChannel     chan struct{}
	disableReconnect atomic.Bool
	reconnectChannel chan struct{}

	reconnectTimer *time.Timer
}

// certSubscription generic subscription for IAM public messages.
type iamSubscription[grpcStream *pb.IAMPublicService_SubscribeCertChangedClient |
	*pb.IAMPublicNodesService_SubscribeNodeChangedClient, T any] struct {
	grpcStream grpcStream
	listeners  []chan T
	stopWG     sync.WaitGroup
}

type (
	nodeInfoChangeSub = iamSubscription[*pb.IAMPublicNodesService_SubscribeNodeChangedClient, cloudprotocol.NodeInfo]
)

// Sender provides API to send messages to the cloud.
type Sender interface {
	SendIssueUnitCerts(requests []cloudprotocol.IssueCertData) (err error)
	SendInstallCertsConfirmation(confirmations []cloudprotocol.InstallCertData) (err error)
	SendStartProvisioningResponse(response cloudprotocol.StartProvisioningResponse) (err error)
	SendFinishProvisioningResponse(response cloudprotocol.FinishProvisioningResponse) (err error)
	SendDeprovisioningResponse(response cloudprotocol.DeprovisioningResponse) (err error)
}

// CertificateProvider provides certificate info.
type CertificateProvider interface {
	GetCertSerial(certURL string) (serial string, err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new IAM client.
func New(
	config *config.Config, sender Sender, cryptocontext *cryptutils.CryptoContext, insecure bool,
) (client *Client, err error) {
	if sender == nil {
		return nil, aoserrors.New("sender is nil")
	}

	reconnectChannel := make(chan struct{}, 1)
	localClient := &Client{
		IAMPublicServiceClient: grpchelpers.NewIAMPublicServiceClient(iamRequestTimeout),
		config:                 config,
		sender:                 sender,
		cryptocontext:          cryptocontext,
		insecure:               insecure,
		publicConnection:       grpchelpers.NewBlockingGRPCConn(),
		nodeInfoSubs: &nodeInfoChangeSub{
			listeners: make([]chan cloudprotocol.NodeInfo, 0),
		},

		tlsCertChan:      make(<-chan *pb.CertInfo),
		closeChannel:     make(chan struct{}, 1),
		reconnectChannel: reconnectChannel,
	}

	defer func() {
		if err != nil {
			localClient.Close()
		}
	}()

	if err = localClient.openGRPCConnection(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if !insecure {
		if ch, err := localClient.SubscribeCertChanged(config.CertStorage); err != nil {
			return nil, aoserrors.Wrap(err)
		} else {
			localClient.tlsCertChan = ch
		}
	}

	if nodeInfo, err := localClient.GetCurrentNodeInfo(); err != nil {
		return nil, aoserrors.Wrap(err)
	} else {
		localClient.nodeID = nodeInfo.NodeID
	}

	if localClient.systemID, err = localClient.getSystemID(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go localClient.processEvents()

	return localClient, nil
}

// GetNodeID returns node ID.
func (client *Client) GetNodeID() string {
	return client.nodeID
}

// GetSystemID returns system ID.
func (client *Client) GetSystemID() (systemID string) {
	return client.systemID
}

// GetNodeInfo returns node info.
func (client *Client) GetNodeInfo(nodeID string) (nodeInfo cloudprotocol.NodeInfo, err error) {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Get node info")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	request := &pb.GetNodeInfoRequest{NodeId: nodeID}

	response, err := client.publicNodesService.GetNodeInfo(ctx, request)
	if err != nil {
		return cloudprotocol.NodeInfo{}, aoserrors.Wrap(err)
	}

	return pbconvert.NodeInfoFromPB(response), nil
}

// GetAllNodeIDs returns node ids.
func (client *Client) GetAllNodeIDs() (nodeIDs []string, err error) {
	client.Lock()
	defer client.Unlock()

	log.Debug("Get all node ids")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.publicNodesService.GetAllNodeIDs(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return response.GetIds(), err
}

// SubscribeNodeInfoChange subscribes client on NodeInfoChange events.
func (client *Client) SubscribeNodeInfoChange() <-chan cloudprotocol.NodeInfo {
	client.Lock()
	defer client.Unlock()

	log.Debug("Subscribe on node info change event")

	ch := make(chan cloudprotocol.NodeInfo)
	client.nodeInfoSubs.listeners = append(client.nodeInfoSubs.listeners, ch)

	return ch
}

// RenewCertificatesNotification renew certificates notification.
func (client *Client) RenewCertificatesNotification(secrets cloudprotocol.UnitSecrets,
	certInfo []cloudprotocol.RenewCertData,
) (err error) {
	client.Lock()
	defer client.Unlock()

	newCerts := make([]cloudprotocol.IssueCertData, 0, len(certInfo))

	for _, cert := range certInfo {
		log.WithFields(log.Fields{
			"type": cert.Type, "serial": cert.Serial, "nodeID": cert.NodeID, "validTill": cert.ValidTill,
		}).Debug("Renew certificate")

		ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
		defer cancel()

		pwd, ok := secrets.Nodes[cert.NodeID]
		if !ok {
			return aoserrors.New("not found password for node: " + cert.NodeID)
		}

		request := &pb.CreateKeyRequest{Type: cert.Type, Password: pwd, NodeId: cert.NodeID}

		response, err := client.certificateService.CreateKey(ctx, request)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		newCerts = append(newCerts, cloudprotocol.IssueCertData{
			Type: response.GetType(), Csr: response.GetCsr(), NodeID: cert.NodeID,
		})
	}

	if len(newCerts) == 0 {
		return nil
	}

	if err := client.sender.SendIssueUnitCerts(newCerts); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// InstallCertificates applies new issued certificates.
func (client *Client) InstallCertificates(
	certInfo []cloudprotocol.IssuedCertData, certProvider CertificateProvider,
) error {
	client.Lock()
	defer client.Unlock()

	confirmations := make([]cloudprotocol.InstallCertData, len(certInfo))

	// IAM cert type of secondary nodes should be sent the latest among certificates for that node.
	// And IAM certificate for the main node should be send in the end. Otherwise IAM client/server
	// restart will fail the following certificates to apply.
	slices.SortStableFunc(certInfo, func(a, b cloudprotocol.IssuedCertData) bool {
		if a.NodeID == b.NodeID {
			return b.Type == iamCertType
		}

		if a.NodeID == client.nodeID && a.Type == iamCertType {
			return false
		}

		if b.NodeID == client.nodeID && b.Type == iamCertType {
			return true
		}

		return false
	})

	for i, cert := range certInfo {
		log.WithFields(log.Fields{"type": cert.Type}).Debug("Install certificate")

		ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
		defer cancel()

		request := &pb.ApplyCertRequest{Type: cert.Type, Cert: cert.CertificateChain, NodeId: cert.NodeID}
		certConfirmation := cloudprotocol.InstallCertData{Type: cert.Type, NodeID: cert.NodeID}

		response, err := client.certificateService.ApplyCert(ctx, request)
		if err == nil {
			certConfirmation.Status = "installed"
		} else if err != nil {
			certConfirmation.Status = "not installed"
			certConfirmation.Description = err.Error()

			log.WithFields(log.Fields{"type": cert.Type}).Errorf("Can't install certificate: %s", err)
		}

		if response != nil {
			certConfirmation.Serial = response.GetSerial()
		}

		confirmations[i] = certConfirmation
	}

	if len(confirmations) == 0 {
		return nil
	}

	if err := client.sender.SendInstallCertsConfirmation(confirmations); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// StartProvisioning starts provisioning.
func (client *Client) StartProvisioning(nodeID, password string) (err error) {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Start provisioning")

	var (
		errorInfo *cloudprotocol.ErrorInfo
		csrs      []cloudprotocol.IssueCertData
	)

	defer func() {
		errSend := client.sender.SendStartProvisioningResponse(cloudprotocol.StartProvisioningResponse{
			MessageType: cloudprotocol.StartProvisioningResponseMessageType,
			NodeID:      nodeID,
			ErrorInfo:   errorInfo,
			CSRs:        csrs,
		})
		if errSend != nil && err == nil {
			err = aoserrors.Wrap(errSend)
		}
	}()

	errorInfo = client.startProvisioning(nodeID, password)
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	csrs, errorInfo = client.createKeys(nodeID, password)
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	return nil
}

// FinishProvisioning starts provisioning.
func (client *Client) FinishProvisioning(
	nodeID, password string, certificates []cloudprotocol.IssuedCertData,
) (err error) {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Finish provisioning")

	var errorInfo *cloudprotocol.ErrorInfo

	defer func() {
		errSend := client.sender.SendFinishProvisioningResponse(cloudprotocol.FinishProvisioningResponse{
			MessageType: cloudprotocol.FinishProvisioningResponseMessageType,
			NodeID:      nodeID,
			ErrorInfo:   errorInfo,
		})
		if errSend != nil && err == nil {
			err = aoserrors.Wrap(errSend)
		}
	}()

	errorInfo = client.applyCertificates(nodeID, certificates)
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	errorInfo = client.finishProvisioning(nodeID, password)
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	return nil
}

// Deprovision deprovisions node.
func (client *Client) Deprovision(nodeID, password string) (err error) {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Deprovision node")

	var errorInfo *cloudprotocol.ErrorInfo

	defer func() {
		errSend := client.sender.SendDeprovisioningResponse(cloudprotocol.DeprovisioningResponse{
			MessageType: cloudprotocol.DeprovisioningResponseMessageType,
			NodeID:      nodeID,
			ErrorInfo:   errorInfo,
		})
		if errSend != nil && err == nil {
			err = aoserrors.Wrap(errSend)
		}
	}()

	if nodeID == client.GetNodeID() {
		err = aoserrors.New("Can't deprovision main node")
		errorInfo = &cloudprotocol.ErrorInfo{
			Message: err.Error(),
		}

		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.provisioningService.Deprovision(
		ctx, &pb.DeprovisionRequest{NodeId: nodeID, Password: password})
	if err != nil {
		errorInfo = &cloudprotocol.ErrorInfo{Message: err.Error()}

		return aoserrors.Wrap(err)
	}

	errorInfo = pbconvert.ErrorInfoFromPB(response.GetError())
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	return nil
}

// PauseNode pauses node.
func (client *Client) PauseNode(nodeID string) error {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Pause node")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.nodesService.PauseNode(ctx, &pb.PauseNodeRequest{NodeId: nodeID})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	errorInfo := pbconvert.ErrorInfoFromPB(response.GetError())
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	return nil
}

// ResumeNode resumes node.
func (client *Client) ResumeNode(nodeID string) error {
	client.Lock()
	defer client.Unlock()

	log.WithField("nodeID", nodeID).Debug("Resume node")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.nodesService.ResumeNode(ctx, &pb.ResumeNodeRequest{NodeId: nodeID})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	errorInfo := pbconvert.ErrorInfoFromPB(response.GetError())
	if errorInfo != nil {
		return aoserrors.New(errorInfo.Message)
	}

	return nil
}

// Close closes IAM client.
func (client *Client) Close() (err error) {
	client.Lock()
	defer client.Unlock()

	client.disableReconnect.Store(true)
	client.closeChannel <- struct{}{}

	if client.reconnectTimer != nil {
		client.reconnectTimer.Stop()
		client.reconnectTimer = nil
	}

	client.closeGRPCConnection()
	client.publicConnection.Close()

	log.Debug("Disconnected from IAM")

	return nil
}

// OnConnectionLost grpchelpers.ConnectionLossHandler::OnConnectionLost implementation.
func (client *Client) OnConnectionLost() {
	if !client.disableReconnect.Load() {
		client.reconnectChannel <- struct{}{}
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (client *Client) openGRPCConnection() (err error) {
	log.Debug("Connecting to IAM...")

	var publicConn *grpc.ClientConn

	publicConn, err = grpchelpers.CreatePublicConnection(
		client.config.IAMPublicServerURL, iamRequestTimeout, client.cryptocontext, client.insecure)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	client.publicConnection.Set(publicConn)

	client.RegisterIAMPublicServiceClient(client.publicConnection, client)
	client.identService = pb.NewIAMPublicIdentityServiceClient(client.publicConnection)
	client.publicNodesService = pb.NewIAMPublicNodesServiceClient(client.publicConnection)

	client.protectedConnection, err = grpchelpers.CreateProtectedConnection(client.config.CertStorage,
		client.config.IAMProtectedServerURL, iamRequestTimeout, client.cryptocontext, client, client.insecure)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	client.certificateService = pb.NewIAMCertificateServiceClient(client.protectedConnection)
	client.provisioningService = pb.NewIAMProvisioningServiceClient(client.protectedConnection)
	client.nodesService = pb.NewIAMNodesServiceClient(client.protectedConnection)

	// Restore GRPC NodeInfo subscription
	if err = client.subscribeNodeInfoChange(); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *Client) closeGRPCConnection() {
	log.Debug("Closing IAM connection...")

	if client.publicConnection != nil {
		client.publicConnection.Stop()
	}

	if client.protectedConnection != nil {
		client.protectedConnection.Close()
	}

	client.WaitIAMPublicServiceClient()
	client.nodeInfoSubs.stopWG.Wait()
}

func (client *Client) processEvents() {
	for {
		select {
		case <-client.closeChannel:
			return

		case <-client.tlsCertChan:
			client.Lock()
			client.reconnect()
			client.Unlock()

		case <-client.reconnectChannel:
			client.Lock()
			client.reconnect()
			client.Unlock()
		}
	}
}

func (client *Client) reconnect() {
	if client.disableReconnect.CompareAndSwap(false, true) {
		return
	}

	log.Debug("Reconnecting to IAM server...")

	client.closeGRPCConnection()

	if err := client.openGRPCConnection(); err != nil {
		log.WithField("err", err).Error("Reconnection to IAM failed")

		client.reconnectTimer = time.AfterFunc(iamReconnectInterval, func() {
			client.Lock()
			defer client.Unlock()

			client.reconnectTimer = nil

			client.reconnect()
		})
	} else {
		client.disableReconnect.Store(false)
	}
}

func (client *Client) getSystemID() (systemID string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	request := &empty.Empty{}

	response, err := client.identService.GetSystemInfo(ctx, request)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"systemID": response.GetSystemId()}).Debug("Get system ID")

	return response.GetSystemId(), nil
}

func (client *Client) processNodeInfoChange(sub *nodeInfoChangeSub) {
	defer sub.stopWG.Done()

	for {
		nodeInfo, err := (*sub.grpcStream).Recv()
		if err != nil {
			client.OnConnectionLost()

			return
		}

		client.Lock()
		for _, listener := range sub.listeners {
			listener <- pbconvert.NodeInfoFromPB(nodeInfo)
		}
		client.Unlock()
	}
}

func (client *Client) subscribeNodeInfoChange() error {
	listener, err := client.publicNodesService.SubscribeNodeChanged(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.WithField("error", err).Error("Can't subscribe on NodeChange event")

		return aoserrors.Wrap(err)
	}

	client.nodeInfoSubs.grpcStream = &listener

	client.nodeInfoSubs.stopWG.Add(1)
	go client.processNodeInfoChange(client.nodeInfoSubs)

	return nil
}

func (client *Client) getCertTypes(nodeID string) ([]string, error) {
	log.WithField("nodeID", nodeID).Debug("Get certificate types")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.provisioningService.GetCertTypes(
		ctx, &pb.GetCertTypesRequest{NodeId: nodeID})
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return response.GetTypes(), nil
}

func (client *Client) createKeys(nodeID, password string) (
	certs []cloudprotocol.IssueCertData, errorInfo *cloudprotocol.ErrorInfo,
) {
	certTypes, err := client.getCertTypes(nodeID)
	if err != nil {
		return nil, &cloudprotocol.ErrorInfo{Message: err.Error()}
	}

	for _, certType := range certTypes {
		log.WithFields(log.Fields{"nodeID": nodeID, "type": certType}).Debug("Create key")

		ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
		defer cancel()

		response, err := client.certificateService.CreateKey(ctx, &pb.CreateKeyRequest{
			Type:     certType,
			NodeId:   nodeID,
			Password: password,
		})
		if err != nil {
			return nil, &cloudprotocol.ErrorInfo{Message: err.Error()}
		}

		if response.GetError() != nil {
			return nil, pbconvert.ErrorInfoFromPB(response.GetError())
		}

		certs = append(certs, cloudprotocol.IssueCertData{
			Type:   certType,
			Csr:    response.GetCsr(),
			NodeID: nodeID,
		})
	}

	return certs, nil
}

func (client *Client) startProvisioning(nodeID, password string) (errorInfo *cloudprotocol.ErrorInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.provisioningService.StartProvisioning(
		ctx, &pb.StartProvisioningRequest{NodeId: nodeID, Password: password})
	if err != nil {
		return &cloudprotocol.ErrorInfo{Message: err.Error()}
	}

	return pbconvert.ErrorInfoFromPB(response.GetError())
}

func (client *Client) applyCertificates(
	nodeID string, certificates []cloudprotocol.IssuedCertData,
) (errorInfo *cloudprotocol.ErrorInfo) {
	for _, certificate := range certificates {
		log.WithFields(log.Fields{"nodeID": nodeID, "type": certificate.Type}).Debug("Apply certificate")

		ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
		defer cancel()

		response, err := client.certificateService.ApplyCert(
			ctx, &pb.ApplyCertRequest{
				NodeId: nodeID,
				Type:   certificate.Type,
				Cert:   certificate.CertificateChain,
			})
		if err != nil {
			return &cloudprotocol.ErrorInfo{Message: err.Error()}
		}

		if response.GetError() != nil {
			return pbconvert.ErrorInfoFromPB(response.GetError())
		}
	}

	return nil
}

func (client *Client) finishProvisioning(nodeID, password string) (errorInfo *cloudprotocol.ErrorInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.provisioningService.FinishProvisioning(
		ctx, &pb.FinishProvisioningRequest{NodeId: nodeID, Password: password})
	if err != nil {
		return &cloudprotocol.ErrorInfo{Message: err.Error()}
	}

	return pbconvert.ErrorInfoFromPB(response.GetError())
}
