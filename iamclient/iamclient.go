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
	"encoding/base64"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/iamanager"
	"github.com/aosedge/aos_common/utils/cryptutils"
	pbconvert "github.com/aosedge/aos_common/utils/pbconvert"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	iamRequestTimeout    = 30 * time.Second
	iamReconnectInterval = 10 * time.Second
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Client IAM client instance.
type Client struct {
	sync.Mutex

	sender Sender

	nodeID   string
	systemID string

	publicConnection    *grpc.ClientConn
	protectedConnection *grpc.ClientConn
	publicService       pb.IAMPublicServiceClient
	identService        pb.IAMPublicIdentityServiceClient
	certificateService  pb.IAMCertificateServiceClient
	provisioningService pb.IAMProvisioningServiceClient

	closeChannel       chan struct{}
	publicNodesService pb.IAMPublicNodesServiceClient
	nodesService       pb.IAMNodesServiceClient
	nodeInfoListeners  []chan cloudprotocol.NodeInfo
}

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
	log.Debug("Connecting to IAM...")

	if sender == nil {
		return nil, aoserrors.New("sender is nil")
	}

	localClient := &Client{
		sender:            sender,
		closeChannel:      make(chan struct{}, 1),
		nodeInfoListeners: make([]chan cloudprotocol.NodeInfo, 0),
	}

	defer func() {
		if err != nil {
			localClient.Close()
		}
	}()

	if localClient.publicConnection, err = createPublicConnection(
		config.IAMPublicServerURL, cryptocontext, insecure); err != nil {
		return nil, err
	}

	localClient.publicService = pb.NewIAMPublicServiceClient(localClient.publicConnection)
	localClient.identService = pb.NewIAMPublicIdentityServiceClient(localClient.publicConnection)
	localClient.publicNodesService = pb.NewIAMPublicNodesServiceClient(localClient.publicConnection)

	if localClient.protectedConnection, err = localClient.createProtectedConnection(
		config, cryptocontext, insecure); err != nil {
		return nil, err
	}

	localClient.certificateService = pb.NewIAMCertificateServiceClient(localClient.protectedConnection)
	localClient.provisioningService = pb.NewIAMProvisioningServiceClient(localClient.protectedConnection)
	localClient.nodesService = pb.NewIAMNodesServiceClient(localClient.protectedConnection)

	log.Debug("Connected to IAM")

	if localClient.nodeID, _, err = localClient.getNodeInfo(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if localClient.systemID, err = localClient.getSystemID(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go localClient.connectPublicNodeService()

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

// GetCurrentNodeInfo returns info for current node.
func (client *Client) GetCurrentNodeInfo() (nodeInfo cloudprotocol.NodeInfo, err error) {
	return client.GetNodeInfo(client.GetNodeID())
}

// GetNodeInfo returns node info.
func (client *Client) GetNodeInfo(nodeID string) (nodeInfo cloudprotocol.NodeInfo, err error) {
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
	client.nodeInfoListeners = append(client.nodeInfoListeners, ch)

	return ch
}

// RenewCertificatesNotification renew certificates notification.
func (client *Client) RenewCertificatesNotification(pwd string, certInfo []cloudprotocol.RenewCertData) (err error) {
	newCerts := make([]cloudprotocol.IssueCertData, 0, len(certInfo))

	for _, cert := range certInfo {
		log.WithFields(log.Fields{
			"type": cert.Type, "serial": cert.Serial, "nodeID": cert.NodeID, "validTill": cert.ValidTill,
		}).Debug("Renew certificate")

		ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
		defer cancel()

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
	confirmations := make([]cloudprotocol.InstallCertData, len(certInfo))

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

// GetCertificate gets certificate by issuer.
func (client *Client) GetCertificate(
	certType string, issuer []byte, serial string,
) (certURL, keyURL string, err error) {
	log.WithFields(log.Fields{
		"type":   certType,
		"issuer": base64.StdEncoding.EncodeToString(issuer),
		"serial": serial,
	}).Debug("Get certificate")

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.publicService.GetCert(
		ctx, &pb.GetCertRequest{Type: certType, Issuer: issuer, Serial: serial})
	if err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{
		"certURL": response.GetCertUrl(), "keyURL": response.GetKeyUrl(),
	}).Debug("Certificate info")

	return response.GetCertUrl(), response.GetKeyUrl(), nil
}

// StartProvisioning starts provisioning.
func (client *Client) StartProvisioning(nodeID, password string) (err error) {
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
	if client.publicConnection != nil || client.protectedConnection != nil {
		client.closeChannel <- struct{}{}
	}

	if client.publicConnection != nil {
		client.publicConnection.Close()
	}

	if client.protectedConnection != nil {
		client.protectedConnection.Close()
	}

	client.nodeInfoListeners = nil

	log.Debug("Disconnected from IAM")

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createPublicConnection(serverURL string, cryptocontext *cryptutils.CryptoContext, insecureConn bool) (
	connection *grpc.ClientConn, err error,
) {
	var secureOpt grpc.DialOption

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	if insecureConn {
		secureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		tlsConfig, err := cryptocontext.GetClientTLSConfig()
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		secureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	if connection, err = grpc.DialContext(ctx, serverURL, secureOpt, grpc.WithBlock()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return connection, nil
}

func (client *Client) getNodeInfo() (nodeID, nodeType string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	response, err := client.publicService.GetNodeInfo(ctx, &empty.Empty{})
	if err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{
		"nodeID":   response.GetNodeId(),
		"nodeType": response.GetNodeType(),
	}).Debug("Get node Info")

	return response.GetNodeId(), response.GetNodeType(), nil
}

func (client *Client) createProtectedConnection(
	config *config.Config, cryptocontext *cryptutils.CryptoContext, insecureConn bool) (
	connection *grpc.ClientConn, err error,
) {
	var secureOpt grpc.DialOption

	if insecureConn {
		secureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		certURL, keyURL, err := client.GetCertificate(config.CertStorage, nil, "")
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		tlsConfig, err := cryptocontext.GetClientMutualTLSConfig(certURL, keyURL)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		secureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	ctx, cancel := context.WithTimeout(context.Background(), iamRequestTimeout)
	defer cancel()

	if connection, err = grpc.DialContext(ctx, config.IAMProtectedServerURL, secureOpt, grpc.WithBlock()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return connection, nil
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

func (client *Client) processMessages(listener pb.IAMPublicNodesService_SubscribeNodeChangedClient) (err error) {
	for {
		nodeInfo, err := listener.Recv()
		if err != nil {
			if code, ok := status.FromError(err); ok {
				if code.Code() == codes.Canceled {
					log.Debug("IAM client connection closed")
					return nil
				}
			}

			return aoserrors.Wrap(err)
		}

		client.Lock()
		for _, listener := range client.nodeInfoListeners {
			listener <- pbconvert.NodeInfoFromPB(nodeInfo)
		}
		client.Unlock()
	}
}

func (client *Client) connectPublicNodeService() {
	listener, err := client.subscribeNodeInfoChange()

	for {
		if err != nil {
			log.Errorf("Error register to IAM: %v", aoserrors.Wrap(err))
		} else {
			if err = client.processMessages(listener); err != nil {
				if errors.Is(err, io.EOF) {
					log.Debug("Connection is closed")
				} else {
					log.Errorf("Connection error: %v", aoserrors.Wrap(err))
				}
			}
		}

		log.Debugf("Reconnect to IAM in %v...", iamReconnectInterval)

		select {
		case <-client.closeChannel:
			log.Debugf("Disconnected from IAM")

			return

		case <-time.After(iamReconnectInterval):
			listener, err = client.subscribeNodeInfoChange()
		}
	}
}

func (client *Client) subscribeNodeInfoChange() (
	listener pb.IAMPublicNodesService_SubscribeNodeChangedClient, err error,
) {
	client.Lock()
	defer client.Unlock()

	client.publicNodesService = pb.NewIAMPublicNodesServiceClient(client.publicConnection)

	listener, err = client.publicNodesService.SubscribeNodeChanged(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.WithField("error", err).Error("Can't subscribe on NodeChange event")

		return nil, aoserrors.Wrap(err)
	}

	return listener, aoserrors.Wrap(err)
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
