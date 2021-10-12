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

package cmserver_test

import (
	"context"
	"os"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	pb "gitpct.epam.com/epmd-aepr/aos_common/api/communicationmanager/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"aos_communicationmanager/cmserver"
	"aos_communicationmanager/config"
)

/*******************************************************************************
 * Consts
 ******************************************************************************/

const (
	serverURL = "localhost:8094"
)

/*******************************************************************************
 * Types
 ******************************************************************************/

type testClient struct {
	connection *grpc.ClientConn
	pbclient   pb.UpdateSchedulerServiceClient
}

type testUpdateHandler struct {
	fotaChannel chan cmserver.UpdateStatus
	sotaChannel chan cmserver.UpdateStatus
}

/*******************************************************************************
 * Init
 ******************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/*******************************************************************************
 * Main
 ******************************************************************************/

/*******************************************************************************
 * Tests
 ******************************************************************************/

func TestConnection(t *testing.T) {
	cmConfig := config.Config{
		CMServerURL: serverURL,
	}

	unitStatusHandler := testUpdateHandler{
		sotaChannel: make(chan cmserver.UpdateStatus, 10),
		fotaChannel: make(chan cmserver.UpdateStatus, 10)}

	cmServer, err := cmserver.New(&cmConfig, &unitStatusHandler, true)
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

		switch notification.SchedulerNotification.(type) {
		case *pb.SchedulerNotifications_FotaStatus:
			if notification.GetFotaStatus().State != pb.UpdateState_NO_UPDATE {
				t.Error("Incorrect state: ", notification.GetFotaStatus().State.String())
			}

		case *pb.SchedulerNotifications_SotaStatus:
			if notification.GetSotaStatus().State != pb.UpdateState_NO_UPDATE {
				t.Error("Incorrect state: ", notification.GetSotaStatus().State.String())
			}
		}
	}

	unitStatusHandler.fotaChannel <- cmserver.UpdateStatus{State: cmserver.ReadyToUpdate}

	notification, err := stream.Recv()
	if err != nil {
		t.Fatalf("Can't receive notification: %s", err)
	}

	status := notification.GetFotaStatus()
	if status == nil {
		t.Fatalf("No FOTA status")
	}

	if status.State != pb.UpdateState_READY_TO_UPDATE {
		t.Error("Incorrect state: ", status.State.String())
	}

	unitStatusHandler.sotaChannel <- cmserver.UpdateStatus{State: cmserver.Downloading, Error: "SOTA error"}

	notification, err = stream.Recv()
	if err != nil {
		t.Fatalf("Can't receive notification: %s", err)
	}

	status = notification.GetSotaStatus()
	if status == nil {
		t.Fatalf("No SOTA status")
	}

	if status.State != pb.UpdateState_DOWNLOADING {
		t.Error("Incorrect state: ", status.State.String())
	}

	if status.Error != "SOTA error" {
		t.Error("Incorrect error message: ", status.Error)
	}

	client.close()
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func newTestClient(url string) (client *testClient, err error) {
	client = &testClient{}

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)

	if client.connection, err = grpc.DialContext(ctx, url, grpc.WithInsecure(), grpc.WithBlock()); err != nil {
		return nil, err
	}

	client.pbclient = pb.NewUpdateSchedulerServiceClient(client.connection)

	return client, nil
}

func (client *testClient) close() {
	if client.connection != nil {
		client.connection.Close()
	}
}

func (handler *testUpdateHandler) GetFOTAStatusChannel() (channel <-chan cmserver.UpdateStatus) {
	return handler.fotaChannel
}

func (handler *testUpdateHandler) GetSOTAStatusChannel() (channel <-chan cmserver.UpdateStatus) {
	return handler.sotaChannel
}

func (handler *testUpdateHandler) GetFOTAStatus() (status cmserver.UpdateStatus) {
	return cmserver.UpdateStatus{State: cmserver.NoUpdate}

}

func (handler *testUpdateHandler) GetSOTAStatus() (status cmserver.UpdateStatus) {
	return cmserver.UpdateStatus{State: cmserver.NoUpdate}
}
