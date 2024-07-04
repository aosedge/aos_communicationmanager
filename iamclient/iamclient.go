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
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/iamanager"
	"github.com/aosedge/aos_common/utils/cryptutils"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const iamRequestTimeout = 30 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Client IAM client instance.
type Client struct {
	sync.Mutex

	sender Sender

	nodeID string

	systemID string

	publicConnection    *grpc.ClientConn
	protectedConnection *grpc.ClientConn
	publicService       pb.IAMPublicServiceClient
	identService        pb.IAMPublicIdentityServiceClient
	certificateService  pb.IAMCertificateServiceClient

	closeChannel chan struct{}
}

// Sender provides API to send messages to the cloud.
type Sender interface {
	SendIssueUnitCerts(requests []cloudprotocol.IssueCertData) (err error)
	SendInstallCertsConfirmation(confirmations []cloudprotocol.InstallCertData) (err error)
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
		sender:       sender,
		closeChannel: make(chan struct{}, 1),
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

	if localClient.protectedConnection, err = localClient.createProtectedConnection(
		config, cryptocontext, insecure); err != nil {
		return nil, err
	}

	localClient.certificateService = pb.NewIAMCertificateServiceClient(localClient.protectedConnection)

	log.Debug("Connected to IAM")

	if localClient.nodeID, _, err = localClient.getNodeInfo(); err != nil {
		return client, aoserrors.Wrap(err)
	}

	if localClient.systemID, err = localClient.getSystemID(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

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
