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

package cmserver

import (
	"context"
	"net"
	"sync"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/communicationmanager"
	"github.com/aosedge/aos_common/utils/cryptutils"
	"github.com/aosedge/aos_common/utils/pbconvert"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// Update states.
const (
	NoUpdate UpdateState = iota
	Downloading
	ReadyToUpdate
	Updating
)

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// UpdateState type for update state.
type UpdateState int

// UpdateStatus represents SOTA/FOTA status.
type UpdateStatus struct {
	State UpdateState
	Error *cloudprotocol.ErrorInfo
}

// UpdateFOTAStatus FOTA update status for update scheduler service.
type UpdateFOTAStatus struct {
	Components []cloudprotocol.ComponentStatus
	UpdateStatus
}

// UpdateSOTAStatus SOTA update status for update scheduler service.
type UpdateSOTAStatus struct {
	UnitConfig       *cloudprotocol.UnitConfigStatus
	InstallServices  []cloudprotocol.ServiceStatus
	RemoveServices   []cloudprotocol.ServiceStatus
	InstallLayers    []cloudprotocol.LayerStatus
	RemoveLayers     []cloudprotocol.LayerStatus
	RebalanceRequest bool
	UpdateStatus
}

// UpdateHandler interface for SOTA/FOTA update.
type UpdateHandler interface {
	GetFOTAStatusChannel() (channel <-chan UpdateFOTAStatus)
	GetSOTAStatusChannel() (channel <-chan UpdateSOTAStatus)
	GetFOTAStatus() (status UpdateFOTAStatus)
	GetSOTAStatus() (status UpdateSOTAStatus)
	StartFOTAUpdate() (err error)
	StartSOTAUpdate() (err error)
}

// CMServer CM server instance.
type CMServer struct {
	grpcServer *grpc.Server
	listener   net.Listener
	pb.UnimplementedUpdateSchedulerServiceServer
	clients           []pb.UpdateSchedulerService_SubscribeNotificationsServer
	currentFOTAStatus UpdateFOTAStatus
	currentSOTAStatus UpdateSOTAStatus
	stopChannel       chan bool
	updatehandler     UpdateHandler
	sync.Mutex
}

// CertificateProvider certificate and key provider interface.
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, keyURL string, err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new IAM server instance.
func New(
	cfg *config.Config, handler UpdateHandler, certProvider CertificateProvider,
	cryptcoxontext *cryptutils.CryptoContext, insecure bool,
) (server *CMServer, err error) {
	server = &CMServer{
		currentFOTAStatus: handler.GetFOTAStatus(),
		currentSOTAStatus: handler.GetSOTAStatus(),
		stopChannel:       make(chan bool, 1),
		updatehandler:     handler,
	}

	if cfg.CMServerURL != "" {
		var opts []grpc.ServerOption

		if !insecure {
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
			log.Info("CM GRPC server starts in insecure mode")
		}

		server.grpcServer = grpc.NewServer(opts...)

		pb.RegisterUpdateSchedulerServiceServer(server.grpcServer, server)

		log.Debug("Start update scheduler gRPC server")

		server.clients = []pb.UpdateSchedulerService_SubscribeNotificationsServer{}

		server.listener, err = net.Listen("tcp", cfg.CMServerURL)
		if err != nil {
			return server, aoserrors.Wrap(err)
		}

		go func() {
			if err := server.grpcServer.Serve(server.listener); err != nil {
				log.Errorf("Can't serve gRPC server: %s", err)
			}
		}()
	}

	go server.handleChannels()

	return server, nil
}

// Close stops CM server.
func (server *CMServer) Close() {
	log.Debug("Close update scheduler gRPC server")

	if server.grpcServer != nil {
		server.grpcServer.Stop()
	}

	if server.listener != nil {
		server.listener.Close()
	}

	server.clients = nil

	server.stopChannel <- true
}

// SubscribeNotifications subscribes on SOTA FOTA packages status changes.
func (server *CMServer) SubscribeNotifications(
	req *empty.Empty, stream pb.UpdateSchedulerService_SubscribeNotificationsServer,
) (err error) {
	log.Debug("New CM client subscribed to schedule update notification")

	server.Lock()

	// send current status of sota and fota packages
	fotaNotification := pb.SchedulerNotifications{
		SchedulerNotification: &pb.SchedulerNotifications_FotaStatus{
			FotaStatus: server.currentFOTAStatus.convertToPBStatus(),
		},
	}

	if err = stream.Send(&fotaNotification); err != nil {
		server.Unlock()

		log.Error("Can't send FOTA notification: ", err)

		return aoserrors.Wrap(err)
	}

	sotaNotification := pb.SchedulerNotifications{
		SchedulerNotification: &pb.SchedulerNotifications_SotaStatus{
			SotaStatus: server.currentSOTAStatus.convertToPBStatus(),
		},
	}

	if err := stream.Send(&sotaNotification); err != nil {
		server.Unlock()

		log.Error("Can't send SOTA notification: ", err)

		return aoserrors.Wrap(err)
	}

	server.clients = append(server.clients, stream)

	server.Unlock()

	<-stream.Context().Done()

	server.Lock()

	for i, item := range server.clients {
		if stream == item {
			server.clients[i] = server.clients[len(server.clients)-1]
			server.clients = server.clients[:len(server.clients)-1]

			break
		}
	}

	server.Unlock()

	return nil
}

// StartFOTAUpdate triggers FOTA update.
func (server *CMServer) StartFOTAUpdate(ctx context.Context, req *empty.Empty) (ret *empty.Empty, err error) {
	return &emptypb.Empty{}, aoserrors.Wrap(server.updatehandler.StartFOTAUpdate())
}

// StartSOTAUpdate triggers SOTA update.
func (server *CMServer) StartSOTAUpdate(ctx context.Context, req *empty.Empty) (ret *empty.Empty, err error) {
	return &emptypb.Empty{}, aoserrors.Wrap(server.updatehandler.StartSOTAUpdate())
}

func (state UpdateState) String() string {
	return [...]string{"no update", "downloading", "ready to update", "updating"}[state]
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (server *CMServer) handleChannels() {
	for {
		notification := pb.SchedulerNotifications{}

		select {
		case fotaStatus, ok := <-server.updatehandler.GetFOTAStatusChannel():
			if !ok {
				break
			}

			server.Lock()

			server.currentFOTAStatus = fotaStatus

			notification.SchedulerNotification = &pb.SchedulerNotifications_FotaStatus{
				FotaStatus: fotaStatus.convertToPBStatus(),
			}

			server.notifyAllClients(&notification)

			server.Unlock()

		case sotaStatus, ok := <-server.updatehandler.GetSOTAStatusChannel():
			if !ok {
				break
			}

			server.Lock()

			server.currentSOTAStatus = sotaStatus

			notification.SchedulerNotification = &pb.SchedulerNotifications_SotaStatus{
				SotaStatus: sotaStatus.convertToPBStatus(),
			}

			server.notifyAllClients(&notification)

			server.Unlock()

		case <-server.stopChannel:
			return
		}
	}
}

func (server *CMServer) notifyAllClients(notification *pb.SchedulerNotifications) {
	for _, client := range server.clients {
		if err := client.Send(notification); err != nil {
			log.Error("Can't send notification: ", err)
		}
	}
}

func (updateStatus *UpdateSOTAStatus) convertToPBStatus() (pbStatus *pb.UpdateSOTAStatus) {
	pbStatus = &pb.UpdateSOTAStatus{
		Error:            pbconvert.ErrorInfoToPB(updateStatus.Error),
		State:            updateStatus.State.getPbState(),
		RebalanceRequest: updateStatus.RebalanceRequest,
	}

	if updateStatus.UnitConfig != nil {
		pbStatus.UnitConfig = &pb.UnitConfigInfo{Version: updateStatus.UnitConfig.Version}
	}

	for _, layer := range updateStatus.InstallLayers {
		pbStatus.InstallLayers = append(pbStatus.GetInstallLayers(), &pb.LayerInfo{
			LayerId: layer.LayerID,
			Digest:  layer.Digest,
			Version: layer.Version,
		})
	}

	for _, layer := range updateStatus.RemoveLayers {
		pbStatus.RemoveLayers = append(pbStatus.GetRemoveLayers(), &pb.LayerInfo{
			LayerId: layer.LayerID,
			Digest:  layer.Digest,
			Version: layer.Version,
		})
	}

	for _, service := range updateStatus.InstallServices {
		pbStatus.InstallServices = append(pbStatus.GetInstallServices(), &pb.ServiceInfo{
			ServiceId: service.ServiceID,
			Version:   service.Version,
		})
	}

	for _, service := range updateStatus.RemoveServices {
		pbStatus.RemoveServices = append(pbStatus.GetRemoveServices(), &pb.ServiceInfo{
			ServiceId: service.ServiceID,
			Version:   service.Version,
		})
	}

	return pbStatus
}

func (updateStatus *UpdateFOTAStatus) convertToPBStatus() (pbStatus *pb.UpdateFOTAStatus) {
	pbStatus = &pb.UpdateFOTAStatus{
		Error: pbconvert.ErrorInfoToPB(updateStatus.Error),
		State: updateStatus.State.getPbState(),
	}

	for _, component := range updateStatus.Components {
		pbStatus.Components = append(pbStatus.GetComponents(), &pb.ComponentInfo{
			ComponentId:   component.ComponentID,
			ComponentType: component.ComponentType,
			Version:       component.Version,
		})
	}

	return pbStatus
}

func (state UpdateState) getPbState() (pbState pb.UpdateState) {
	return [...]pb.UpdateState{
		pb.UpdateState_NO_UPDATE, pb.UpdateState_DOWNLOADING,
		pb.UpdateState_READY_TO_UPDATE, pb.UpdateState_UPDATING,
	}[state]
}
