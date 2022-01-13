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

package umcontroller

import (
	"io"
	"net"
	"strings"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/aoscloud/aos_common/aoserrors"
	pb "github.com/aoscloud/aos_common/api/updatemanager/v1"
	"github.com/aoscloud/aos_common/utils/cryptutils"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// UmCtrlServer gRPC update managers controller server
type umCtrlServer struct {
	pb.UnimplementedUMServiceServer

	url          string
	grpcServer   *grpc.Server
	listener     net.Listener
	controllerCh chan umCtrlInternalMsg
}

/***********************************************************************************************************************
 * public
 **********************************************************************************************************************/

// NewServer create update controller server
func newServer(cfg *config.Config, ch chan umCtrlInternalMsg, insecure bool) (server *umCtrlServer, err error) {
	log.WithField("host", cfg.UMController.ServerURL).Debug("Start UM server")
	server = &umCtrlServer{controllerCh: ch}

	var opts []grpc.ServerOption

	if !insecure {
		tlsConfig, err := cryptutils.GetServerMutualTLSConfig(cfg.Crypt.CACert, cfg.CertStorage)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	} else {
		log.Info("GRPC server starts in insecure mode")
	}

	server.url = cfg.UMController.ServerURL

	server.grpcServer = grpc.NewServer(opts...)

	pb.RegisterUMServiceServer(server.grpcServer, server)

	return server, nil
}

// Start start update controller server
func (server *umCtrlServer) Start() (err error) {
	server.listener, err = net.Listen("tcp", server.url)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	go func() {
		if err := server.grpcServer.Serve(server.listener); err != nil {
			log.Errorf("Can't serve gRPC server: %s", err)
		}
	}()

	return nil
}

// Stop stop update controller server
func (server *umCtrlServer) Stop() {
	if server.grpcServer != nil {
		server.grpcServer.Stop()
	}

	if server.listener != nil {
		server.listener.Close()
	}
}

// RegisterUM stop update controller server call back
func (server *umCtrlServer) RegisterUM(stream pb.UMService_RegisterUMServer) (err error) {
	statusMsg, err := stream.Recv()
	if err == io.EOF {
		log.Warn("Unexpected end of UM stream")
	}

	if err != nil {
		log.Error("Error receive message from UM ", err)
		return aoserrors.Wrap(err)
	}

	log.Debugf("Register UM id %s status %s", statusMsg.GetUmId(), statusMsg.GetUmState().String())

	handler, ch, err := newUmHandler(statusMsg.GetUmId(), stream, server.controllerCh, statusMsg.GetUmState())
	if err != nil {
		return aoserrors.Wrap(err)
	}

	openConnectionMsg := umCtrlInternalMsg{
		umID:        statusMsg.GetUmId(),
		handler:     handler,
		requestType: openConnection,
		status:      getUmStatusFromUmMessage(statusMsg),
	}

	server.controllerCh <- openConnectionMsg

	// wait for close
	<-ch

	closeConnectionMsg := umCtrlInternalMsg{
		umID:        statusMsg.GetUmId(),
		requestType: closeConnection,
	}
	server.controllerCh <- closeConnectionMsg

	return nil
}

func getUmStatusFromUmMessage(msg *pb.UpdateStatus) (status umStatus) {
	status.umState = msg.GetUmState().String()

	for _, component := range msg.GetComponents() {
		if component.GetId() == "" {
			continue
		}

		status.componsStatus = append(status.componsStatus, systemComponentStatus{
			id:            component.GetId(),
			vendorVersion: component.GetVendorVersion(),
			aosVersion:    component.GetAosVersion(),
			status:        strings.ToLower(component.GetStatus().String()),
			err:           component.GetError(),
		})
	}

	return status
}
