// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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

package cmserver

import (
	"net"

	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	pb "gitpct.epam.com/epmd-aepr/aos_common/api/communicationmanager/v1"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/cryptutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// CMServer CM server instance
type CMServer struct {
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedUpdateSchedulerServiceServer
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new IAM server instance
func New(cfg *config.Config, insecure bool) (server *CMServer, err error) {
	server = &CMServer{}

	if cfg.CMServerURL != "" {
		var opts []grpc.ServerOption

		if !insecure {
			tlsConfig, err := cryptutils.GetServerMutualTLSConfig(cfg.Crypt.CACert, cfg.CertStorage)
			if err != nil {
				return nil, aoserrors.Wrap(err)
			}

			opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
		} else {
			log.Info("CM GRPC server starts in insecure mode")
		}

		server.grpcServer = grpc.NewServer(opts...)

		pb.RegisterUpdateSchedulerServiceServer(server.grpcServer, server)

		log.Debug("Start update scheduler grpc server")

		server.listener, err = net.Listen("tcp", cfg.CMServerURL)
		if err != nil {
			return server, aoserrors.Wrap(err)
		}

		go server.grpcServer.Serve(server.listener)
	}

	return server, nil
}

// Close stops CM server
func (server *CMServer) Close() {
	log.Debug("Close update scheduler grpc server")

	if server.grpcServer != nil {
		server.grpcServer.Stop()
	}

	if server.listener != nil {
		server.listener.Close()
	}
}
