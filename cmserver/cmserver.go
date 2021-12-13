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

	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	pb "gitpct.epam.com/epmd-aepr/aos_common/api/communicationmanager/v1"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/cryptutils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// Update states
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

// UpdateState type for update state
type UpdateState int

// UpdateStatus represents SOTA/FOTA status
type UpdateStatus struct {
	State UpdateState
	Error string
}

// UpdateFOTAStatus struct with fota parts information
type UpdateFOTAStatus struct {
	Components  []cloudprotocol.ComponentInfo
	BoardConfig *cloudprotocol.BoardConfigInfo
	UpdateStatus
}

// UpdateSOTAStatus struct with sota parts information
type UpdateSOTAStatus struct {
	InstallServices []cloudprotocol.ServiceInfo
	RemoveServices  []cloudprotocol.ServiceInfo
	InstallLayers   []cloudprotocol.LayerInfo
	RemoveLayers    []cloudprotocol.LayerInfo
	UpdateStatus
}

// UpdateHandler interface for SOTA/FOTA update
type UpdateHandler interface {
	GetFOTAStatusChannel() (channel <-chan UpdateFOTAStatus)
	GetSOTAStatusChannel() (channel <-chan UpdateSOTAStatus)
	GetFOTAStatus() (status UpdateFOTAStatus)
	GetSOTAStatus() (status UpdateSOTAStatus)
	StartFOTAUpdate() (err error)
	StartSOTAUpdate() (err error)
}

// CMServer CM server instance
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

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new IAM server instance
func New(cfg *config.Config, handler UpdateHandler, insecure bool) (server *CMServer, err error) {
	server = &CMServer{
		currentFOTAStatus: handler.GetFOTAStatus(),
		currentSOTAStatus: handler.GetSOTAStatus(),
		stopChannel:       make(chan bool, 1),
		updatehandler:     handler,
	}

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

		server.clients = []pb.UpdateSchedulerService_SubscribeNotificationsServer{}

		server.listener, err = net.Listen("tcp", cfg.CMServerURL)
		if err != nil {
			return server, aoserrors.Wrap(err)
		}

		go server.grpcServer.Serve(server.listener)
	}

	go server.handleChannels()

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

	server.clients = nil

	server.stopChannel <- true
}

// SubscribeNotifications sunscribes on SOTA FOTA packages status changes
func (server *CMServer) SubscribeNotifications(req *empty.Empty, stream pb.UpdateSchedulerService_SubscribeNotificationsServer) (err error) {
	log.Debug("New CM client subscribed to schedule update notification")

	server.Lock()

	// send current status of sota and fota packages
	fotaNotification := pb.SchedulerNotifications{
		SchedulerNotification: &pb.SchedulerNotifications_FotaStatus{
			FotaStatus: server.currentFOTAStatus.convertToPBStatus()},
	}

	if err = stream.Send(&fotaNotification); err != nil {
		server.Unlock()

		log.Error("Can't send FOTA notification: ", err)

		return err
	}

	sotaNotification := pb.SchedulerNotifications{
		SchedulerNotification: &pb.SchedulerNotifications_SotaStatus{
			SotaStatus: server.currentSOTAStatus.convertToPBStatus()},
	}

	if err := stream.Send(&sotaNotification); err != nil {
		server.Unlock()

		log.Error("Can't send SOTA notification: ", err)

		return err
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

// StartFOTAUpdate triggers FOTA update
func (server *CMServer) StartFOTAUpdate(ctx context.Context, req *empty.Empty) (ret *empty.Empty, err error) {
	return &emptypb.Empty{}, server.updatehandler.StartFOTAUpdate()
}

// StartSOTAUpdate triggers SOTA update
func (server *CMServer) StartSOTAUpdate(ctx context.Context, req *empty.Empty) (ret *empty.Empty, err error) {
	return &emptypb.Empty{}, server.updatehandler.StartSOTAUpdate()
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
				FotaStatus: fotaStatus.convertToPBStatus()}

			server.notifyAllClients(&notification)

			server.Unlock()

		case sotaStatus, ok := <-server.updatehandler.GetSOTAStatusChannel():
			if !ok {
				break
			}

			server.Lock()

			server.currentSOTAStatus = sotaStatus

			notification.SchedulerNotification = &pb.SchedulerNotifications_SotaStatus{
				SotaStatus: sotaStatus.convertToPBStatus()}

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
	pbStatus = new(pb.UpdateSOTAStatus)

	pbStatus.Error = updateStatus.Error

	pbStatus.State = updateStatus.State.getPbState()

	for _, layer := range updateStatus.InstallLayers {
		pbStatus.InstallLayers = append(pbStatus.InstallLayers, &pb.LayerInfo{Id: layer.ID,
			AosVersion: layer.AosVersion, Digest: layer.Digest})
	}

	for _, layer := range updateStatus.RemoveLayers {
		pbStatus.RemoveLayers = append(pbStatus.RemoveLayers, &pb.LayerInfo{Id: layer.ID,
			AosVersion: layer.AosVersion, Digest: layer.Digest})
	}

	for _, service := range updateStatus.InstallServices {
		pbStatus.InstallServices = append(pbStatus.InstallServices, &pb.ServiceInfo{Id: service.ID,
			AosVersion: service.AosVersion})
	}

	for _, service := range updateStatus.RemoveServices {
		pbStatus.RemoveServices = append(pbStatus.RemoveServices, &pb.ServiceInfo{Id: service.ID,
			AosVersion: service.AosVersion})
	}

	return pbStatus
}

func (updateStatus *UpdateFOTAStatus) convertToPBStatus() (pbStatus *pb.UpdateFOTAStatus) {
	pbStatus = new(pb.UpdateFOTAStatus)
	pbStatus.Error = updateStatus.Error
	pbStatus.State = updateStatus.State.getPbState()

	for _, component := range updateStatus.Components {
		pbStatus.Components = append(pbStatus.Components, &pb.ComponentInfo{Id: component.ID,
			AosVersion: component.AosVersion, VendorVersion: component.VendorVersion})
	}

	if updateStatus.BoardConfig != nil {
		pbStatus.BoardConfig = &pb.BoardConfigInfo{VendorVersion: updateStatus.BoardConfig.VendorVersion}
	}

	return pbStatus
}

func (state UpdateState) getPbState() (pbState pb.UpdateState) {
	return [...]pb.UpdateState{
		pb.UpdateState_NO_UPDATE, pb.UpdateState_DOWNLOADING,
		pb.UpdateState_READY_TO_UPDATE, pb.UpdateState_UPDATING}[state]
}
