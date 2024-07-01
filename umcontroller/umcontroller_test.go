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

package umcontroller_test

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aosedge/aos_common/aoserrors"
	pb "github.com/aosedge/aos_common/api/updatemanager"
	"github.com/aosedge/aos_common/image"

	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/fcrypt"
	"github.com/aosedge/aos_communicationmanager/umcontroller"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	serverURL = "localhost:8091"

	kilobyte = uint64(1 << 10)
)

const (
	prepareStep = "prepare"
	updateStep  = "update"
	applyStep   = "apply"
	revertStep  = "revert"
	finishStep  = "finish"
	rebootStep  = "reboot"
)

type testStorage struct {
	updateInfo []umcontroller.ComponentStatus
}

type testUmConnection struct {
	stream         pb.UMService_RegisterUMClient
	notifyTestChan chan bool
	continueChan   chan bool
	step           string
	test           *testing.T
	umID           string
	components     []*pb.ComponentStatus
	conn           *grpc.ClientConn
}

type testCryptoContext struct{}

type testNodeInfoProvider struct {
	umcontroller.NodeInfoProvider
	nodeInfo          []*cloudprotocol.NodeInfo
	nodeInfoListeners []chan *cloudprotocol.NodeInfo
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var tmpDir string

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
 * testNodeInfoProvider implementation
 **********************************************************************************************************************/

func NewTestNodeInfoProvider(nodeIds []string) *testNodeInfoProvider {
	provider := &testNodeInfoProvider{
		nodeInfo:          make([]*cloudprotocol.NodeInfo, 0),
		nodeInfoListeners: make([]chan *cloudprotocol.NodeInfo, 0),
	}

	for _, nodeId := range nodeIds {
		nodeInfo := &cloudprotocol.NodeInfo{
			NodeID: nodeId,
			Status: "provisioned",
			Attrs: map[string]interface{}{
				cloudprotocol.NodeAttrAosComponents: interface{}(cloudprotocol.AosComponentUM),
			},
		}
		provider.nodeInfo = append(provider.nodeInfo, nodeInfo)
	}

	return provider
}

func (provider *testNodeInfoProvider) GetNodeID() string {
	return "test"
}

func (provider *testNodeInfoProvider) GetAllNodeIDs() (nodesId []string, err error) {
	result := make([]string, 0)

	for _, nodeInfo := range provider.nodeInfo {
		result = append(result, nodeInfo.NodeID)
	}

	return result, nil
}

func (provider *testNodeInfoProvider) GetNodeInfo(nodeID string) (nodeInfo *cloudprotocol.NodeInfo, err error) {
	for _, nodeInfo := range provider.nodeInfo {
		if nodeInfo.NodeID == nodeID {
			return nodeInfo, nil
		}
	}

	return nil, aoserrors.Errorf("not found")
}

func (provider *testNodeInfoProvider) SubscribeNodeInfoChange() <-chan *cloudprotocol.NodeInfo {
	listener := make(chan *cloudprotocol.NodeInfo)
	provider.nodeInfoListeners = append(provider.nodeInfoListeners, listener)

	return listener
}

func (provider *testNodeInfoProvider) addNode(nodeID string) {
	nodeInfo := &cloudprotocol.NodeInfo{
		NodeID: nodeID,
		Status: "provisioned",
		Attrs: map[string]interface{}{
			cloudprotocol.NodeAttrAosComponents: interface{}(cloudprotocol.AosComponentUM),
		},
	}
	provider.nodeInfo = append(provider.nodeInfo, nodeInfo)

	for _, listener := range provider.nodeInfoListeners {
		listener <- nodeInfo
	}
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	var err error

	if err = setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	if err = cleanup(); err != nil {
		log.Errorf("Error cleaning up: %s", err)
	}

	os.Exit(ret)
}

func setup() (err error) {
	tmpDir, err = os.MkdirTemp("", "cm_")
	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func cleanup() (err error) {
	if err = os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func TestConnection(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8092",
	}

	nodeInfoProvider := NewTestNodeInfoProvider([]string{"umID1"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	umCtrl, err := umcontroller.New(
		&smConfig, &testStorage{}, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Fatalf("Can't create: UM controller %s", err)
	}

	components := []*pb.ComponentStatus{
		{ComponentId: "component1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "component2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	streamUM1, connUM1, err := createClientConnection("umID1", pb.UpdateState_IDLE, components)
	if err != nil {
		t.Errorf("Error connect %s", err)
	}

	nodeInfoProvider.addNode("umID2")

	components2 := []*pb.ComponentStatus{
		{ComponentId: "component3", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "component4", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	streamUM2, connUM2, err := createClientConnection("umID2", pb.UpdateState_IDLE, components2)
	if err != nil {
		t.Errorf("Error connect %s", err)
	}

	streamUM1Copy, connUM1Copy, err := createClientConnection("umID1", pb.UpdateState_IDLE, components)
	if err != nil {
		t.Errorf("Error connect %s", err)
	}

	newComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Errorf("Can't get system components %s", err)
	}

	if len(newComponents) != 4 {
		t.Errorf("Incorrect count of components %d", len(newComponents))
	}

	umCtrl.Close()

	_ = streamUM1.CloseSend()

	connUM1.Close()

	_ = streamUM2.CloseSend()

	connUM2.Close()

	_ = streamUM1Copy.CloseSend()

	connUM1Copy.Close()

	time.Sleep(1 * time.Second)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func createClientConnection(
	clientID string, state pb.UpdateState, components []*pb.ComponentStatus,
) (stream pb.UMService_RegisterUMClient, conn *grpc.ClientConn, err error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithBlock())

	conn, err = grpc.Dial(serverURL, opts...)
	if err != nil {
		return stream, nil, aoserrors.Wrap(err)
	}

	client := pb.NewUMServiceClient(conn)

	stream, err = client.RegisterUM(context.Background())
	if err != nil {
		log.Errorf("Fail call RegisterUM %s", err)
		return stream, nil, aoserrors.Wrap(err)
	}

	umMsg := &pb.UpdateStatus{NodeId: clientID, UpdateState: state, Components: components}

	if err = stream.Send(umMsg); err != nil {
		log.Errorf("Fail send update status message %s", err)
	}

	time.Sleep(1 * time.Second)

	return stream, conn, nil
}

func TestFullUpdate(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM1", "testUM2"})

	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um1Components := []*pb.ComponentStatus{
		{ComponentId: "um1C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um1C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um1 := newTestUM(t, "testUM1", pb.UpdateState_IDLE, "init", um1Components)
	go um1.processMessages()

	um2Components := []*pb.ComponentStatus{
		{ComponentId: "um2C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um2C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um2 := newTestUM(t, "testUM2", pb.UpdateState_IDLE, "init", um2Components)
	go um2.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um1C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um2C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um2C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func(finChan chan bool) {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err != nil {
			t.Errorf("Can't update components: %s", err)
		}
		finChan <- true
	}(finishChannel)

	um1Components = append(um1Components, &pb.ComponentStatus{
		ComponentId: "um1C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING,
	})
	um1.setComponents(um1Components)

	um1.step = prepareStep
	um1.continueChan <- true
	<-um1.notifyTestChan // receive prepare
	um1.sendState(pb.UpdateState_PREPARED)

	um2Components = append(um2Components,
		&pb.ComponentStatus{ComponentId: "um2C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um2Components = append(um2Components,
		&pb.ComponentStatus{ComponentId: "um2C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um2.setComponents(um2Components)

	um2.step = prepareStep
	um2.continueChan <- true
	<-um2.notifyTestChan
	um2.sendState(pb.UpdateState_PREPARED)

	um1.step = updateStep
	um1.continueChan <- true
	<-um1.notifyTestChan // um1 updated
	um1.sendState(pb.UpdateState_UPDATED)

	um2.step = updateStep
	um2.continueChan <- true
	<-um2.notifyTestChan // um2 updated
	um2.sendState(pb.UpdateState_UPDATED)

	um1Components = []*pb.ComponentStatus{
		{ComponentId: "um1C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um1C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}
	um1.setComponents(um1Components)

	um1.step = applyStep
	um1.continueChan <- true
	<-um1.notifyTestChan // um1 apply
	um1.sendState(pb.UpdateState_IDLE)

	um2Components = []*pb.ComponentStatus{
		{ComponentId: "um2C1", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um2C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}
	um2.setComponents(um2Components)

	um2.step = applyStep
	um2.continueChan <- true
	<-um2.notifyTestChan // um1 apply
	um2.sendState(pb.UpdateState_IDLE)

	time.Sleep(1 * time.Second)

	um1.step = finishStep
	um2.step = finishStep

	<-finishChannel

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um1C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um1C2", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um2C1", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um2C2", Version: "2.0.0", Status: "installed"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
	}

	um1.closeConnection()
	um2.closeConnection()

	<-um1.notifyTestChan
	<-um2.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestFullUpdateWithDisconnect(t *testing.T) {
	// fix the test on CI
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}

	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM3", "testUM4"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um3Components := []*pb.ComponentStatus{
		{ComponentId: "um3C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um3C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um3 := newTestUM(t, "testUM3", pb.UpdateState_IDLE, "init", um3Components)
	go um3.processMessages()

	um4Components := []*pb.ComponentStatus{
		{ComponentId: "um4C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um4C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um4 := newTestUM(t, "testUM4", pb.UpdateState_IDLE, "init", um4Components)
	go um4.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um3C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um4C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um4C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err != nil {
			t.Errorf("Can't update components: %s", err)
		}

		close(finishChannel)
	}()

	// prepare UM3
	um3Components = append(um3Components,
		&pb.ComponentStatus{ComponentId: "um3C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um3.setComponents(um3Components)

	um3.step = prepareStep
	um3.continueChan <- true
	<-um3.notifyTestChan // receive prepare
	um3.sendState(pb.UpdateState_PREPARED)

	// prepare UM4
	um4Components = append(um4Components,
		&pb.ComponentStatus{ComponentId: "um4C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um4Components = append(um4Components,
		&pb.ComponentStatus{ComponentId: "um4C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um4.setComponents(um4Components)

	um4.step = prepareStep
	um4.continueChan <- true
	<-um4.notifyTestChan
	um4.sendState(pb.UpdateState_PREPARED)

	um3.step = updateStep
	um3.continueChan <- true
	<-um3.notifyTestChan

	// um3  reboot
	um3.step = rebootStep
	um3.closeConnection()
	<-um3.notifyTestChan

	um3 = newTestUM(t, "testUM3", pb.UpdateState_UPDATED, applyStep, um3Components)
	go um3.processMessages()

	um4.step = updateStep
	um4.continueChan <- true
	<-um4.notifyTestChan
	// full reboot
	um4.step = rebootStep
	um4.closeConnection()

	<-um4.notifyTestChan

	um4 = newTestUM(t, "testUM4", pb.UpdateState_UPDATED, applyStep, um4Components)
	go um4.processMessages()

	um3.step = applyStep
	um3.continueChan <- true
	<-um3.notifyTestChan

	// um3  reboot
	um3.step = rebootStep
	um3.closeConnection()
	<-um3.notifyTestChan

	um3Components = []*pb.ComponentStatus{
		{ComponentId: "um3C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um3C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}

	um3 = newTestUM(t, "testUM3", pb.UpdateState_IDLE, "init", um3Components)
	go um3.processMessages()

	// um4  reboot
	um4.step = rebootStep
	um4.closeConnection()
	<-um4.notifyTestChan

	um4Components = []*pb.ComponentStatus{
		{ComponentId: "um4C1", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um4C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}

	um4 = newTestUM(t, "testUM4", pb.UpdateState_IDLE, "init", um4Components)
	go um4.processMessages()

	um3.step = finishStep
	um4.step = finishStep

	<-finishChannel

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um3C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um3C2", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um4C1", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um4C2", Version: "2.0.0", Status: "installed"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
	}

	um3.closeConnection()
	um4.closeConnection()

	<-um3.notifyTestChan
	<-um4.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestFullUpdateWithReboot(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM5", "testUM6"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um5Components := []*pb.ComponentStatus{
		{ComponentId: "um5C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um5C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um5 := newTestUM(t, "testUM5", pb.UpdateState_IDLE, "init", um5Components)
	go um5.processMessages()

	um6Components := []*pb.ComponentStatus{
		{ComponentId: "um6C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um6C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um6 := newTestUM(t, "testUM6", pb.UpdateState_IDLE, "init", um6Components)
	go um6.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um5C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um6C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um6C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err != nil {
			t.Errorf("Can't update components: %s", err)
		}

		close(finishChannel)
	}()

	// prepare UM5
	um5Components = append(um5Components,
		&pb.ComponentStatus{ComponentId: "um5C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um5.setComponents(um5Components)

	um5.step = prepareStep
	um5.continueChan <- true
	<-um5.notifyTestChan // receive prepare
	um5.sendState(pb.UpdateState_PREPARED)

	// prepare UM6
	um6Components = append(um6Components,
		&pb.ComponentStatus{ComponentId: "um6C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um6Components = append(um6Components,
		&pb.ComponentStatus{ComponentId: "um6C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um6.setComponents(um6Components)

	um6.step = prepareStep
	um6.continueChan <- true
	<-um6.notifyTestChan
	um6.sendState(pb.UpdateState_PREPARED)

	um5.step = updateStep
	um5.continueChan <- true
	<-um5.notifyTestChan

	// full reboot
	um5.step = rebootStep
	um6.step = rebootStep

	um5.closeConnection()
	um6.closeConnection()
	umCtrl.Close()

	<-um5.notifyTestChan
	<-um6.notifyTestChan
	<-finishChannel

	umCtrl, err = umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um5 = newTestUM(t, "testUM5", pb.UpdateState_UPDATED, applyStep, um5Components)
	go um5.processMessages()

	um6 = newTestUM(t, "testUM6", pb.UpdateState_PREPARED, updateStep, um6Components)
	go um6.processMessages()

	um6.continueChan <- true
	<-um6.notifyTestChan

	um6.step = rebootStep
	um6.closeConnection()

	um6 = newTestUM(t, "testUM6", pb.UpdateState_UPDATED, applyStep, um6Components)
	go um6.processMessages()

	// um5 apply and full reboot
	um5.continueChan <- true
	<-um5.notifyTestChan

	// full reboot
	um5.step = rebootStep
	um6.step = rebootStep

	um5.closeConnection()
	um6.closeConnection()
	umCtrl.Close()

	<-um5.notifyTestChan
	<-um6.notifyTestChan

	umCtrl, err = umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um5Components = []*pb.ComponentStatus{
		{ComponentId: "um5C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um5C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}

	um5 = newTestUM(t, "testUM5", pb.UpdateState_IDLE, "init", um5Components)
	go um5.processMessages()

	um6 = newTestUM(t, "testUM6", pb.UpdateState_UPDATED, applyStep, um6Components)
	go um6.processMessages()

	um6.step = rebootStep
	um6.closeConnection()
	<-um6.notifyTestChan

	um6Components = []*pb.ComponentStatus{
		{ComponentId: "um6C1", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um6C2", Version: "2.0.0", State: pb.ComponentState_INSTALLED},
	}

	um6 = newTestUM(t, "testUM6", pb.UpdateState_IDLE, "init", um6Components)
	go um6.processMessages()

	um5.step = finishStep
	um6.step = finishStep

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um5C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um5C2", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um6C1", Version: "2.0.0", Status: "installed"},
		{ComponentID: "um6C2", Version: "2.0.0", Status: "installed"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
	}

	um5.closeConnection()
	um6.closeConnection()

	<-um5.notifyTestChan
	<-um6.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestRevertOnPrepare(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM7", "testUM8"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um7Components := []*pb.ComponentStatus{
		{ComponentId: "um7C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um7C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um7 := newTestUM(t, "testUM7", pb.UpdateState_IDLE, "init", um7Components)
	go um7.processMessages()

	um8Components := []*pb.ComponentStatus{
		{ComponentId: "um8C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um8C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um8 := newTestUM(t, "testUM8", pb.UpdateState_IDLE, "init", um8Components)
	go um8.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um7C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um8C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um8C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err == nil {
			t.Errorf("Should fail")
		}

		close(finishChannel)
	}()

	um7Components = append(um7Components,
		&pb.ComponentStatus{ComponentId: "um7C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um7.setComponents(um7Components)

	um7.step = prepareStep
	um7.continueChan <- true
	<-um7.notifyTestChan // receive prepare
	um7.sendState(pb.UpdateState_PREPARED)

	um8Components = append(um8Components,
		&pb.ComponentStatus{ComponentId: "um8C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um8Components = append(um8Components,
		&pb.ComponentStatus{ComponentId: "um8C2", Version: "2.0.0", State: pb.ComponentState_ERROR})
	um8.setComponents(um8Components)

	um8.step = prepareStep
	um8.continueChan <- true
	<-um8.notifyTestChan
	um8.sendState(pb.UpdateState_FAILED)

	um7.step = revertStep
	um7.continueChan <- true
	<-um7.notifyTestChan // um7 revert received
	um7.sendState(pb.UpdateState_IDLE)

	um8.step = revertStep
	um8.continueChan <- true
	<-um8.notifyTestChan // um8 revert received
	um8.sendState(pb.UpdateState_IDLE)

	um7.step = finishStep
	um8.step = finishStep

	<-finishChannel

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um7C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um7C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um8C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um8C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um8C2", Version: "2.0.0", Status: "error"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
		log.Debug(etalonComponents)
	}

	um7.closeConnection()
	um8.closeConnection()

	<-um7.notifyTestChan
	<-um8.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestRevertOnUpdate(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM9", "testUM10"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um9Components := []*pb.ComponentStatus{
		{ComponentId: "um9C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um9C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um9 := newTestUM(t, "testUM9", pb.UpdateState_IDLE, "init", um9Components)
	go um9.processMessages()

	um10Components := []*pb.ComponentStatus{
		{ComponentId: "um10C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um10C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um10 := newTestUM(t, "testUM10", pb.UpdateState_IDLE, "init", um10Components)
	go um10.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um9C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um10C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um10C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err == nil {
			t.Errorf("Should fail")
		}

		close(finishChannel)
	}()

	um9Components = append(um9Components,
		&pb.ComponentStatus{ComponentId: "um9C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um9.setComponents(um9Components)

	um9.step = prepareStep
	um9.continueChan <- true
	<-um9.notifyTestChan // receive prepare
	um9.sendState(pb.UpdateState_PREPARED)

	um10Components = append(um10Components,
		&pb.ComponentStatus{ComponentId: "um10C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um10Components = append(um10Components,
		&pb.ComponentStatus{ComponentId: "um10C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um10.setComponents(um10Components)

	um10.step = prepareStep
	um10.continueChan <- true
	<-um10.notifyTestChan
	um10.sendState(pb.UpdateState_PREPARED)

	um9.step = updateStep
	um9.continueChan <- true
	<-um9.notifyTestChan // um9 updated
	um9.sendState(pb.UpdateState_UPDATED)

	um10Components = []*pb.ComponentStatus{
		{ComponentId: "um10C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um10C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um10C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING},
		{ComponentId: "um10C2", Version: "2.0.0", State: pb.ComponentState_ERROR},
	}
	um10.setComponents(um10Components)

	um10.step = updateStep
	um10.continueChan <- true
	<-um10.notifyTestChan // um10 updated
	um10.sendState(pb.UpdateState_FAILED)

	um9Components = []*pb.ComponentStatus{
		{ComponentId: "um9C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um9C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}
	um9.setComponents(um9Components)

	um9.step = revertStep
	um9.continueChan <- true
	<-um9.notifyTestChan // um9 revert received
	um9.sendState(pb.UpdateState_IDLE)

	um10.step = revertStep
	um10.continueChan <- true
	<-um10.notifyTestChan // um10 revert received
	um10.sendState(pb.UpdateState_IDLE)

	um9.step = finishStep
	um10.step = finishStep

	<-finishChannel

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um9C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um9C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um10C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um10C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um10C2", Version: "2.0.0", Status: "error"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
		log.Debug(etalonComponents)
	}

	um9.closeConnection()
	um10.closeConnection()

	<-um9.notifyTestChan
	<-um10.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestRevertOnUpdateWithDisconnect(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}
	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM11", "testUM12"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um11Components := []*pb.ComponentStatus{
		{ComponentId: "um11C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um11C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um11 := newTestUM(t, "testUM11", pb.UpdateState_IDLE, "init", um11Components)
	go um11.processMessages()

	um12Components := []*pb.ComponentStatus{
		{ComponentId: "um12C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um12C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um12 := newTestUM(t, "testUM12", pb.UpdateState_IDLE, "init", um12Components)
	go um12.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um11C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um12C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um12C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err != nil {
			t.Errorf("Can't update components: %s", err)
		}

		close(finishChannel)
	}()

	um11Components = append(um11Components,
		&pb.ComponentStatus{ComponentId: "um11C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um11.setComponents(um11Components)

	um11.step = prepareStep
	um11.continueChan <- true
	<-um11.notifyTestChan // receive prepare
	um11.sendState(pb.UpdateState_PREPARED)

	um12Components = append(um12Components,
		&pb.ComponentStatus{ComponentId: "um12C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um12Components = append(um12Components,
		&pb.ComponentStatus{ComponentId: "um12C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um12.setComponents(um12Components)

	um12.step = prepareStep
	um12.continueChan <- true
	<-um12.notifyTestChan
	um12.sendState(pb.UpdateState_PREPARED)

	um11.step = updateStep
	um11.continueChan <- true
	<-um11.notifyTestChan // um11 updated
	um11.sendState(pb.UpdateState_UPDATED)

	um12Components = []*pb.ComponentStatus{
		{ComponentId: "um12C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um12C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um12C2", Version: "2.0.0", State: pb.ComponentState_ERROR},
	}
	um12.setComponents(um12Components)

	um12.step = updateStep
	um12.continueChan <- true
	<-um12.notifyTestChan // um12 updated
	// um12  reboot
	um12.step = rebootStep
	um12.closeConnection()
	<-um12.notifyTestChan

	um12 = newTestUM(t, "testUM12", pb.UpdateState_FAILED, revertStep, um12Components)
	go um12.processMessages()

	um11Components = []*pb.ComponentStatus{
		{ComponentId: "um11C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um11C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}
	um11.setComponents(um11Components)

	um11.step = revertStep
	um11.continueChan <- true
	<-um11.notifyTestChan // um11 revert received
	um11.sendState(pb.UpdateState_IDLE)

	um12.step = revertStep
	um12.continueChan <- true
	<-um12.notifyTestChan // um12 revert received
	um12.sendState(pb.UpdateState_IDLE)

	um11.step = finishStep
	um12.step = finishStep

	<-finishChannel

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um11C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um11C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um12C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um12C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um12C2", Version: "2.0.0", Status: "error"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
		log.Debug(etalonComponents)
	}

	um11.closeConnection()
	um12.closeConnection()

	<-um11.notifyTestChan
	<-um12.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

func TestRevertOnUpdateWithReboot(t *testing.T) {
	umCtrlConfig := config.UMController{
		CMServerURL:   "localhost:8091",
		FileServerURL: "localhost:8093",
	}

	nodeInfoProvider := NewTestNodeInfoProvider([]string{"testUM13", "testUM14"})
	smConfig := config.Config{UMController: umCtrlConfig, ComponentsDir: tmpDir}

	var updateStorage testStorage

	umCtrl, err := umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um13Components := []*pb.ComponentStatus{
		{ComponentId: "um13C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um13C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um13 := newTestUM(t, "testUM13", pb.UpdateState_IDLE, "init", um13Components)
	go um13.processMessages()

	um14Components := []*pb.ComponentStatus{
		{ComponentId: "um14C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um14C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}

	um14 := newTestUM(t, "testUM14", pb.UpdateState_IDLE, "init", um14Components)
	go um14.processMessages()

	componentDir, err := os.MkdirTemp("", "aosComponent_")
	if err != nil {
		t.Fatalf("Can't create component dir: %v", componentDir)
	}

	defer os.RemoveAll(componentDir)

	updateComponents := []cloudprotocol.ComponentInfo{
		{
			ComponentID: convertToComponentID("um13C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile1"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um14C1"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile2"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
		{
			ComponentID: convertToComponentID("um14C2"), Version: "2.0.0",
			DownloadInfo:   prepareDownloadInfo(path.Join(componentDir, "someFile3"), kilobyte*2),
			DecryptionInfo: prepareDecryptionInfo(),
		},
	}

	finishChannel := make(chan bool)

	go func() {
		if _, err := umCtrl.UpdateComponents(updateComponents, nil, nil); err != nil {
			t.Errorf("Can't update components: %s", err)
		}

		close(finishChannel)
	}()

	um13Components = append(um13Components,
		&pb.ComponentStatus{ComponentId: "um13C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um13.setComponents(um13Components)

	um13.step = prepareStep
	um13.continueChan <- true
	<-um13.notifyTestChan // receive prepare
	um13.sendState(pb.UpdateState_PREPARED)

	um14Components = append(um14Components,
		&pb.ComponentStatus{ComponentId: "um14C1", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um14Components = append(um14Components,
		&pb.ComponentStatus{ComponentId: "um14C2", Version: "2.0.0", State: pb.ComponentState_INSTALLING})
	um14.setComponents(um14Components)

	um14.step = prepareStep
	um14.continueChan <- true
	<-um14.notifyTestChan
	um14.sendState(pb.UpdateState_PREPARED)

	um13.step = updateStep
	um13.continueChan <- true
	<-um13.notifyTestChan // um13 updated
	um13.sendState(pb.UpdateState_UPDATED)

	um14Components = []*pb.ComponentStatus{
		{ComponentId: "um14C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um14C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um14C2", Version: "2.0.0", State: pb.ComponentState_ERROR},
	}
	um14.setComponents(um14Components)

	um14.step = updateStep
	um14.continueChan <- true
	<-um14.notifyTestChan // um14 updated

	// full reboot
	um13.step = rebootStep
	um14.step = rebootStep

	um13.closeConnection()
	um14.closeConnection()
	umCtrl.Close()

	<-um13.notifyTestChan
	<-um14.notifyTestChan
	<-finishChannel
	// um14  reboot

	umCtrl, err = umcontroller.New(
		&smConfig, &updateStorage, nil, nodeInfoProvider, nil, &testCryptoContext{}, true)
	if err != nil {
		t.Errorf("Can't create: UM controller %s", err)
	}

	um13 = newTestUM(t, "testUM13", pb.UpdateState_UPDATED, revertStep, um13Components)
	go um13.processMessages()

	um14 = newTestUM(t, "testUM14", pb.UpdateState_FAILED, revertStep, um14Components)
	go um14.processMessages()

	um13Components = []*pb.ComponentStatus{
		{ComponentId: "um13C1", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
		{ComponentId: "um13C2", Version: "1.0.0", State: pb.ComponentState_INSTALLED},
	}
	um13.setComponents(um13Components)

	um13.step = revertStep
	um13.continueChan <- true
	<-um13.notifyTestChan // um13 revert received
	um13.sendState(pb.UpdateState_IDLE)

	um14.step = revertStep
	um14.continueChan <- true
	<-um14.notifyTestChan // um14 revert received
	um14.sendState(pb.UpdateState_IDLE)

	um13.step = finishStep
	um14.step = finishStep

	time.Sleep(time.Second)

	etalonComponents := []cloudprotocol.ComponentStatus{
		{ComponentID: "um13C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um13C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um14C1", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um14C2", Version: "1.0.0", Status: "installed"},
		{ComponentID: "um14C2", Version: "2.0.0", Status: "error"},
	}

	currentComponents, err := umCtrl.GetStatus()
	if err != nil {
		t.Fatalf("Can't get components info: %s", err)
	}

	if !reflect.DeepEqual(etalonComponents, currentComponents) {
		log.Debug(currentComponents)
		t.Error("incorrect result component list")
		log.Debug(etalonComponents)
	}

	um13.closeConnection()
	um14.closeConnection()

	<-um13.notifyTestChan
	<-um14.notifyTestChan

	umCtrl.Close()

	time.Sleep(time.Second)
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (storage *testStorage) GetComponentsUpdateInfo() (updateInfo []umcontroller.ComponentStatus, err error) {
	return storage.updateInfo, err
}

func (storage *testStorage) SetComponentsUpdateInfo(updateInfo []umcontroller.ComponentStatus) (err error) {
	storage.updateInfo = updateInfo
	return aoserrors.Wrap(err)
}

func (um *testUmConnection) processMessages() {
	defer func() { um.notifyTestChan <- true }()

	for {
		<-um.continueChan

		msg, err := um.stream.Recv()
		if err != nil {
			return
		}

		switch um.step {
		case finishStep:
			fallthrough

		case rebootStep:
			if errors.Is(err, io.EOF) {
				log.Debug("[test] End of connection ", um.umID)
				return
			}

			if err != nil {
				log.Debug("[test] End of connection with error ", err, um.umID)
				return
			}

		case prepareStep:
			if msg.GetPrepareUpdate() == nil {
				um.test.Error("Expect prepare update request ", um.umID)
			}

		case updateStep:
			if msg.GetStartUpdate() == nil {
				um.test.Error("Expect start update ", um.umID)
			}

		case applyStep:
			if msg.GetApplyUpdate() == nil {
				um.test.Error("Expect apply update ", um.umID)
			}

		case revertStep:
			if msg.GetRevertUpdate() == nil {
				um.test.Error("Expect revert update ", um.umID)
			}

		default:
			um.test.Error("unexpected message at step", um.step)
		}
		um.notifyTestChan <- true
	}
}

func (context *testCryptoContext) DecryptAndValidate(
	encryptedFile, decryptedFile string, params fcrypt.DecryptParams,
) error {
	srcFile, err := os.Open(encryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(decryptedFile, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newTestUM(t *testing.T, id string, updateStatus pb.UpdateState, testState string, components []*pb.ComponentStatus) (
	umTest *testUmConnection,
) {
	t.Helper()

	stream, conn, err := createClientConnection(id, updateStatus, components)
	if err != nil {
		t.Errorf("Error connect %s", err)
		return umTest
	}

	umTest = &testUmConnection{
		notifyTestChan: make(chan bool),
		continueChan:   make(chan bool),
		step:           testState,
		test:           t,
		stream:         stream,
		umID:           id,
		conn:           conn,
		components:     components,
	}

	return umTest
}

func (um *testUmConnection) setComponents(components []*pb.ComponentStatus) {
	um.components = components
}

func (um *testUmConnection) sendState(state pb.UpdateState) {
	umMsg := &pb.UpdateStatus{NodeId: um.umID, UpdateState: state, Components: um.components}

	if err := um.stream.Send(umMsg); err != nil {
		um.test.Errorf("Fail send update status message %s", err)
	}
}

func (um *testUmConnection) closeConnection() {
	um.continueChan <- true
	um.conn.Close()
	_ = um.stream.CloseSend()
}

func prepareDownloadInfo(filePath string, size uint64) cloudprotocol.DownloadInfo {
	if err := generateFile(filePath, size); err != nil {
		return cloudprotocol.DownloadInfo{}
	}

	imageFileInfo, err := image.CreateFileInfo(context.Background(), filePath)
	if err != nil {
		return cloudprotocol.DownloadInfo{}
	}

	url := url.URL{
		Scheme: "file",
		Path:   filePath,
	}

	return cloudprotocol.DownloadInfo{
		URLs:   []string{url.String()},
		Sha256: imageFileInfo.Sha256,
		Size:   imageFileInfo.Size,
	}
}

func prepareDecryptionInfo() cloudprotocol.DecryptionInfo {
	recInfo := struct {
		Serial string `json:"serial"`
		Issuer []byte `json:"issuer"`
	}{
		Serial: "string",
		Issuer: []byte("issuer"),
	}

	return cloudprotocol.DecryptionInfo{
		BlockAlg:     "AES256/CBC/pkcs7",
		BlockIv:      []byte{},
		BlockKey:     []byte{},
		AsymAlg:      "RSA/PKCS1v1_5",
		ReceiverInfo: &recInfo,
	}
}

func generateFile(fileName string, size uint64) (err error) {
	if output, err := exec.Command("dd", "if=/dev/urandom", "of="+fileName, "bs=1",
		"count="+strconv.FormatUint(size, 10)).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

func convertToComponentID(id string) *string {
	return &id
}
