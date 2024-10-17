// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2024 Renesas Electronics Corporation.
// Copyright (C) 2024 EPAM Systems, Inc.
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

package grpchelpers

import (
	"context"
	"encoding/base64"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/api/iamanager"
	"github.com/aosedge/aos_common/utils/pbconvert"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// IAMPublicServiceClient implements IAM public service client interface.
type IAMPublicServiceClient struct {
	sync.Mutex

	connLossHandler   ConnectionLossHandler
	iamRequestTimeout time.Duration

	currentNodeInfo *cloudprotocol.NodeInfo
	publicService   iamanager.IAMPublicServiceClient
	certChangeSub   map[string]*certChangeSub
}

type ConnectionLossHandler interface {
	OnConnectionLost()
}

// iamSubscription subscription on CertChanged event.
type certChangeSub struct {
	grpcStream *iamanager.IAMPublicService_SubscribeCertChangedClient
	listeners  []chan *iamanager.CertInfo
	stopWG     sync.WaitGroup
}

// NewIAMPublicServiceClient creates IAMPublicServiceClient instance.
func NewIAMPublicServiceClient(iamRequestTimeout time.Duration) *IAMPublicServiceClient {
	localClient := &IAMPublicServiceClient{
		iamRequestTimeout: iamRequestTimeout,
		certChangeSub:     make(map[string]*certChangeSub),
	}

	return localClient
}

// Register registers IAMPublicServiceClient for grpc connection.
func (client *IAMPublicServiceClient) RegisterIAMPublicServiceClient(connection grpc.ClientConnInterface,
	connLossHandler ConnectionLossHandler,
) {
	client.Lock()
	defer client.Unlock()

	client.publicService = iamanager.NewIAMPublicServiceClient(connection)
	client.connLossHandler = connLossHandler

	client.restoreCertInfoSubs()
}

// GetCertificate gets certificate by issuer.
func (client *IAMPublicServiceClient) GetCertificate(
	certType string, issuer []byte, serial string,
) (certURL, keyURL string, err error) {
	client.Lock()
	defer client.Unlock()

	log.WithFields(log.Fields{
		"type":   certType,
		"issuer": base64.StdEncoding.EncodeToString(issuer),
		"serial": serial,
	}).Debug("Get certificate")

	ctx, cancel := context.WithTimeout(context.Background(), client.iamRequestTimeout)
	defer cancel()

	response, err := client.publicService.GetCert(
		ctx, &iamanager.GetCertRequest{Type: certType, Issuer: issuer, Serial: serial})
	if err != nil {
		return "", "", aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{
		"certURL": response.GetCertUrl(), "keyURL": response.GetKeyUrl(),
	}).Debug("Certificate info")

	return response.GetCertUrl(), response.GetKeyUrl(), nil
}

// SubscribeCertChanged subscribes client on CertChange events.
func (client *IAMPublicServiceClient) SubscribeCertChanged(certType string) (<-chan *iamanager.CertInfo, error) {
	client.Lock()
	defer client.Unlock()

	ch := make(chan *iamanager.CertInfo)

	if _, ok := client.certChangeSub[certType]; !ok {
		grpcStream, err := client.subscribeCertChange(certType)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		subscription := &certChangeSub{
			grpcStream: &grpcStream,
			listeners:  []chan *iamanager.CertInfo{ch},
		}

		client.certChangeSub[certType] = subscription

		subscription.stopWG.Add(1)

		go client.processCertInfoChange(subscription)
	} else {
		subscription := client.certChangeSub[certType]
		subscription.listeners = append(subscription.listeners, ch)
	}

	return ch, nil
}

// UnsubscribeCertChanged unsubscribes client from CertChange event.
func (client *IAMPublicServiceClient) UnsubscribeCertChanged(listener <-chan *iamanager.CertInfo) error {
	client.Lock()
	defer client.Unlock()

	for _, subscription := range client.certChangeSub {
		for ind, curListener := range subscription.listeners {
			if curListener == listener {
				subscription.listeners = append(subscription.listeners[:ind], subscription.listeners[ind+1:]...)

				return nil
			}
		}
	}

	return aoserrors.New("not found")
}

func (client *IAMPublicServiceClient) GetCurrentNodeInfo() (cloudprotocol.NodeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.iamRequestTimeout)
	defer cancel()

	if client.currentNodeInfo != nil {
		return *client.currentNodeInfo, nil
	}

	response, err := client.publicService.GetNodeInfo(ctx, &empty.Empty{})
	if err != nil {
		return cloudprotocol.NodeInfo{}, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{
		"nodeID":   response.GetNodeId(),
		"nodeType": response.GetNodeType(),
	}).Debug("Get node Info")

	nodeInfo := pbconvert.NodeInfoFromPB(response)
	client.currentNodeInfo = &nodeInfo

	return *client.currentNodeInfo, nil
}

// Close closes client.
func (client *IAMPublicServiceClient) WaitIAMPublicServiceClient() {
	for _, sub := range client.certChangeSub {
		sub.stopWG.Wait()
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (client *IAMPublicServiceClient) subscribeCertChange(certType string) (
	listener iamanager.IAMPublicService_SubscribeCertChangedClient, err error,
) {
	listener, err = client.publicService.SubscribeCertChanged(context.Background(),
		&iamanager.SubscribeCertChangedRequest{Type: certType})
	if err != nil {
		log.WithField("error", err).Error("Can't subscribe on CertChange event")

		return nil, aoserrors.Wrap(err)
	}

	return listener, aoserrors.Wrap(err)
}

func (client *IAMPublicServiceClient) processCertInfoChange(sub *certChangeSub) {
	log.Debug("Start process CertInfo change")

	defer sub.stopWG.Done()

	for {
		cert, err := (*sub.grpcStream).Recv()
		if err != nil {
			log.WithField("err", err).Error("Process CertInfo change failed")

			if client.connLossHandler != nil {
				client.connLossHandler.OnConnectionLost()
			}

			return
		}

		client.Lock()
		for _, listener := range sub.listeners {
			listener <- cert
		}
		client.Unlock()
	}
}

func (client *IAMPublicServiceClient) restoreCertInfoSubs() {
	for certType, sub := range client.certChangeSub {
		grpcStream, err := client.subscribeCertChange(certType)
		if err != nil {
			log.WithField("certType", certType).Error("Failed restore CertChange subscription")
		}

		sub.grpcStream = &grpcStream

		sub.stopWG.Add(1)

		go client.processCertInfoChange(sub)
	}
}
