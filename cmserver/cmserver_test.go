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

package cmserver_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	pb "github.com/aosedge/aos_common/api/communicationmanager"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/aosedge/aos_communicationmanager/cmserver"
	"github.com/aosedge/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	serverURL = "localhost:8094"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testClient struct {
	connection *grpc.ClientConn
	pbclient   pb.UpdateSchedulerServiceClient
}

type testUpdateHandler struct {
	fotaChannel chan cmserver.UpdateFOTAStatus
	sotaChannel chan cmserver.UpdateSOTAStatus
	startFOTA   bool
	startSOTA   bool
}

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestConnection(t *testing.T) {
	cmConfig := config.Config{
		CMServerURL: serverURL,
	}

	unitStatusHandler := testUpdateHandler{
		sotaChannel: make(chan cmserver.UpdateSOTAStatus, 10),
		fotaChannel: make(chan cmserver.UpdateFOTAStatus, 10),
	}

	cmServer, err := cmserver.New(&cmConfig, &unitStatusHandler, nil, nil, true)
	if err != nil {
		t.Fatalf("Can't create CM server: %s", err)
	}
	defer cmServer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := newTestClient(serverURL)
	if err != nil {
		t.Fatalf("Can't create test client: %s", err)
	}

	stream, err := client.pbclient.SubscribeNotifications(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Can't subscribe: %s", err)
	}

	for i := 0; i < 2; i++ {
		notification, err := stream.Recv()
		if err != nil {
			t.Fatalf("Can't receive notification: %s", err)
		}

		switch notification.GetSchedulerNotification().(type) {
		case *pb.SchedulerNotifications_FotaStatus:
			if notification.GetFotaStatus().GetState() != pb.UpdateState_NO_UPDATE {
				t.Error("Incorrect state: ", notification.GetFotaStatus().GetState().String())
			}

		case *pb.SchedulerNotifications_SotaStatus:
			if notification.GetSotaStatus().GetState() != pb.UpdateState_NO_UPDATE {
				t.Error("Incorrect state: ", notification.GetSotaStatus().GetState().String())
			}
		}
	}

	statusFotaNotification := cmserver.UpdateFOTAStatus{
		Components: []cloudprotocol.ComponentStatus{
			{
				ComponentID:   "testComponent",
				ComponentType: "testType",
				Version:       "2.0.0",
			},
		},
		UnitConfig:   &cloudprotocol.UnitConfigStatus{Version: "bc_version"},
		UpdateStatus: cmserver.UpdateStatus{State: cmserver.ReadyToUpdate},
	}

	unitStatusHandler.fotaChannel <- statusFotaNotification

	notification, err := stream.Recv()
	if err != nil {
		t.Fatalf("Can't receive notification: %s", err)
	}

	status := notification.GetFotaStatus()
	if status == nil {
		t.Fatalf("No FOTA status")
	}

	if status.GetState() != pb.UpdateState_READY_TO_UPDATE {
		t.Error("Incorrect state: ", status.GetState().String())
	}

	if len(status.GetComponents()) != 1 {
		t.Fatal("Incorrect count of components")
	}

	if status.GetComponents()[0].GetComponentId() != "testComponent" {
		t.Error("Incorrect component id")
	}

	if status.GetComponents()[0].GetComponentType() != "testType" {
		t.Error("Incorrect component type")
	}

	if status.GetComponents()[0].GetVersion() != "2.0.0" {
		t.Error("Incorrect version")
	}

	if status.GetUnitConfig() == nil {
		t.Fatal("Unit Config is nil")
	}

	if status.GetUnitConfig().GetVersion() != "bc_version" {
		t.Error("Incorrect unit config version")
	}

	statusNotification := cmserver.UpdateSOTAStatus{
		InstallServices: []cloudprotocol.ServiceStatus{{ServiceID: "s1", Version: "1.0.0"}},
		RemoveServices:  []cloudprotocol.ServiceStatus{{ServiceID: "s2", Version: "2.0.0"}},
		InstallLayers:   []cloudprotocol.LayerStatus{{LayerID: "l1", Digest: "someSha", Version: "3.0.0"}},
		RemoveLayers:    []cloudprotocol.LayerStatus{{LayerID: "l2", Digest: "someSha", Version: "4.0.0"}},
		UpdateStatus: cmserver.UpdateStatus{State: cmserver.Downloading, Error: &cloudprotocol.ErrorInfo{
			Message: "SOTA error",
		}},
	}

	unitStatusHandler.sotaChannel <- statusNotification

	notification, err = stream.Recv()
	if err != nil {
		t.Fatalf("Can't receive notification: %s", err)
	}

	sotaStatus := notification.GetSotaStatus()
	if sotaStatus == nil {
		t.Fatalf("No SOTA status")
	}

	if sotaStatus.GetState() != pb.UpdateState_DOWNLOADING {
		t.Error("Incorrect state: ", status.GetState().String())
	}

	if sotaStatus.GetError().Message != "SOTA error" {
		t.Error("Incorrect error message: ", status.GetError())
	}

	if len(sotaStatus.GetInstallServices()) != 1 {
		t.Fatal("Incorrect count of services")
	}

	if sotaStatus.GetInstallServices()[0].GetServiceId() != "s1" {
		t.Error("Incorrect service id")
	}

	if sotaStatus.GetInstallServices()[0].GetVersion() != "1.0.0" {
		t.Error("Incorrect service aos version")
	}

	if len(sotaStatus.GetInstallLayers()) != 1 {
		t.Fatal("Incorrect count of layers")
	}

	if sotaStatus.GetInstallLayers()[0].GetLayerId() != "l1" {
		t.Error("Incorrect layer id")
	}

	if sotaStatus.GetInstallLayers()[0].GetDigest() != "someSha" {
		t.Error("Incorrect layer digest")
	}

	if sotaStatus.GetInstallLayers()[0].GetVersion() != "3.0.0" {
		t.Error("Incorrect layer aos version")
	}

	if _, err := client.pbclient.StartFOTAUpdate(ctx, &emptypb.Empty{}); err != nil {
		t.Fatalf("Can't start FOTA update: %v", err)
	}

	if !unitStatusHandler.startFOTA {
		t.Error("FOTA update should be started")
	}

	if _, err := client.pbclient.StartSOTAUpdate(ctx, &emptypb.Empty{}); err != nil {
		t.Fatalf("Can't start SOTA update: %v", err)
	}

	if !unitStatusHandler.startSOTA {
		t.Error("SOTA update should be started")
	}

	client.close()

	time.Sleep(time.Second)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newTestClient(url string) (client *testClient, err error) {
	client = &testClient{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if client.connection, err = grpc.DialContext(
		ctx, url, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	client.pbclient = pb.NewUpdateSchedulerServiceClient(client.connection)

	return client, nil
}

func (client *testClient) close() {
	if client.connection != nil {
		client.connection.Close()
	}
}

func (handler *testUpdateHandler) GetFOTAStatusChannel() (channel <-chan cmserver.UpdateFOTAStatus) {
	return handler.fotaChannel
}

func (handler *testUpdateHandler) GetSOTAStatusChannel() (channel <-chan cmserver.UpdateSOTAStatus) {
	return handler.sotaChannel
}

func (handler *testUpdateHandler) GetFOTAStatus() (status cmserver.UpdateFOTAStatus) {
	status.State = cmserver.NoUpdate

	return status
}

func (handler *testUpdateHandler) GetSOTAStatus() (status cmserver.UpdateSOTAStatus) {
	status.State = cmserver.NoUpdate

	return status
}

func (handler *testUpdateHandler) StartFOTAUpdate() (err error) {
	handler.startFOTA = true

	return nil
}

func (handler *testUpdateHandler) StartSOTAUpdate() (err error) {
	handler.startSOTA = true

	return nil
}
