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
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/image"
	"github.com/aosedge/aos_common/spaceallocator"
	"github.com/aosedge/aos_common/utils/cryptutils"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/api/iamanager"
	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/fcrypt"
	"github.com/aosedge/aos_communicationmanager/fileserver"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller update managers controller.
type Controller struct {
	sync.Mutex
	config        *config.Config
	storage       storage
	certProvider  CertificateProvider
	cryptocontext *cryptutils.CryptoContext
	insecure      bool

	server *umCtrlServer

	eventChannel     chan umCtrlInternalMsg
	nodeInfoProvider NodeInfoProvider
	nodeInfoChannel  <-chan cloudprotocol.NodeInfo
	certChannel      <-chan *iamanager.CertInfo

	stopChannel  chan bool
	componentDir string

	decrypter Decrypter

	allocator spaceallocator.Allocator

	connections          []umConnection
	currentComponents    []cloudprotocol.ComponentStatus
	newComponentsChannel chan []cloudprotocol.ComponentStatus

	fsm               *fsm.FSM
	connectionMonitor allConnectionMonitor
	operable          bool
	updateFinishCond  *sync.Cond

	updateError   error
	currentNodeID string

	fileServer *fileserver.FileServer

	restartTimer *time.Timer
}

// ComponentStatus information about system component update.
type ComponentStatus struct {
	ComponentID   string `json:"componentId"`
	ComponentType string `json:"componentType"`
	Version       string `json:"version"`
	Annotations   string `json:"annotations,omitempty"`
	URL           string `json:"url"`
	Sha256        []byte `json:"sha256"`
	Size          uint64 `json:"size"`
}

type umConnection struct {
	umID           string
	isLocalClient  bool
	handler        *umHandler
	updatePriority uint32
	state          string
	components     []string
	updatePackages []ComponentStatus
}

type umCtrlInternalMsg struct {
	umID        string
	handler     *umHandler
	requestType int
	status      umStatus
	close       closeReason
}

type umStatus struct {
	updateStatus  string
	nodePriority  uint32
	componsStatus []systemComponentStatus
}

type systemComponentStatus struct {
	componentID   string
	componentType string
	version       string
	status        string
	err           string
}

type allConnectionMonitor struct {
	sync.Mutex
	connTimer     *time.Timer
	timeoutChan   chan bool
	stopTimerChan chan bool
	wg            sync.WaitGroup
}

type storage interface {
	GetComponentsUpdateInfo() (updateInfo []ComponentStatus, err error)
	SetComponentsUpdateInfo(updateInfo []ComponentStatus) (err error)
}

// CertificateProvider certificate and key provider interface.
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, keyURL string, err error)
	SubscribeCertChanged(certType string) (<-chan *iamanager.CertInfo, error)
}

// Decrypter interface to decrypt and validate image.
type Decrypter interface {
	DecryptAndValidate(encryptedFile, decryptedFile string, params fcrypt.DecryptParams) error
}

// NodeInfoProvider provides information about the nodes.
type NodeInfoProvider interface {
	GetNodeID() string
	GetAllNodeIDs() (nodeIds []string, err error)
	GetNodeInfo(nodeID string) (nodeInfo cloudprotocol.NodeInfo, err error)
	SubscribeNodeInfoChange() <-chan cloudprotocol.NodeInfo
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	openConnection = iota
	closeConnection
	umStatusUpdate
)

// FSM states.
const (
	stateInit                          = "init"
	stateIdle                          = "idle"
	stateReconnecting                  = "reconnecting"
	stateFaultState                    = "fault"
	statePrepareUpdate                 = "prepareUpdate"
	stateUpdateUmStatusOnPrepareUpdate = "updateUmStatusOnPrepareUpdate"
	stateStartUpdate                   = "startUpdate"
	stateUpdateUmStatusOnStartUpdate   = "updateUmStatusOnStartUpdate"
	stateStartApply                    = "startApply"
	stateUpdateUmStatusOnStartApply    = "updateUmStatusOnStartApply"
	stateStartRevert                   = "startRevert"
	stateUpdateUmStatusOnRevert        = "updateUmStatusOnRevert"
)

// FSM events.
const (
	evAllClientsConnected    = "allClientsConnected"
	evAllClientsDisconnected = "allClientsDisconnected"

	evConnectionTimeout   = "connectionTimeout"
	evUpdateRequest       = "updateRequest"
	evContinue            = "continue"
	evUpdatePrepared      = "updatePrepared"
	evUpdateStatusUpdated = "updateStatusUpdated"
	evSystemUpdated       = "systemUpdated"
	evApplyComplete       = "applyComplete"

	evContinuePrepare = "continuePrepare"
	evContinueUpdate  = "continueUpdate"
	evContinueApply   = "continueApply"
	evContinueRevert  = "continueRevert"

	evUpdateFailed   = "updateFailed"
	evSystemReverted = "systemReverted"

	evTLSCertUpdated = "tlsCertUpdated"
)

// client sates.
const (
	umIdle     = "IDLE"
	umPrepared = "PREPARED"
	umUpdated  = "UPDATED"
	umFailed   = "FAILED"
)

const (
	enterPrefix  = "enter_"
	leavePrefix  = "leave_"
	beforePrefix = "before_"
)

const (
	connectionTimeout = 600 * time.Second
	umRestartInterval = 10 * time.Second
)

const lowestUpdatePriority = 1000

const fileScheme = "file"

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new update managers controller.
func New(config *config.Config, storage storage, certProvider CertificateProvider, nodeInfoProvider NodeInfoProvider,
	cryptocontext *cryptutils.CryptoContext, decrypter Decrypter, insecure bool) (
	umCtrl *Controller, err error,
) {
	umCtrl = &Controller{
		config:        config,
		storage:       storage,
		certProvider:  certProvider,
		cryptocontext: cryptocontext,
		insecure:      insecure,

		eventChannel:         make(chan umCtrlInternalMsg),
		nodeInfoProvider:     nodeInfoProvider,
		nodeInfoChannel:      nodeInfoProvider.SubscribeNodeInfoChange(),
		certChannel:          make(<-chan *iamanager.CertInfo),
		stopChannel:          make(chan bool),
		componentDir:         config.ComponentsDir,
		connectionMonitor:    allConnectionMonitor{stopTimerChan: make(chan bool, 1), timeoutChan: make(chan bool, 1)},
		operable:             true,
		updateFinishCond:     sync.NewCond(&sync.Mutex{}),
		newComponentsChannel: make(chan []cloudprotocol.ComponentStatus, 1),
		decrypter:            decrypter,
	}

	if err := os.MkdirAll(umCtrl.componentDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if umCtrl.fileServer, err = fileserver.New(
		config.UMController.FileServerURL, config.ComponentsDir); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	umCtrl.currentNodeID = nodeInfoProvider.GetNodeID()

	if umCtrl.allocator, err = spaceallocator.New(umCtrl.componentDir, 0, nil); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	umCtrl.fsm = fsm.NewFSM(umCtrl.createStateMachine())

	if err := umCtrl.createConnections(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err := umCtrl.createUMServer(); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if !insecure {
		if umCtrl.certChannel, err = certProvider.SubscribeCertChanged(config.CertStorage); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	go umCtrl.processInternalMessages()

	return umCtrl, nil
}

// Close close server.
func (umCtrl *Controller) Close() {
	if umCtrl.fileServer != nil {
		if err := umCtrl.fileServer.Close(); err != nil {
			log.Errorf("Can't close file server: %v", err)
		}
	}

	close(umCtrl.newComponentsChannel)
	umCtrl.operable = false
	umCtrl.stopChannel <- true
}

// GetStatus returns list of system components information.
func (umCtrl *Controller) GetStatus() ([]cloudprotocol.ComponentStatus, error) {
	currentState := umCtrl.fsm.Current()
	if currentState != stateInit {
		return umCtrl.currentComponents, nil
	}

	umCtrl.connectionMonitor.wg.Wait()

	return umCtrl.currentComponents, nil
}

// UpdateComponents updates components.
//
//nolint:funlen
func (umCtrl *Controller) UpdateComponents(
	components []cloudprotocol.ComponentInfo, chains []cloudprotocol.CertificateChain,
	certs []cloudprotocol.Certificate,
) ([]cloudprotocol.ComponentStatus, error) {
	log.Debug("Update components")

	if umCtrl.fsm.Current() == stateIdle {
		umCtrl.updateError = nil

		if len(components) == 0 {
			return umCtrl.currentComponents, nil
		}

		componentsUpdateInfo := []ComponentStatus{}

		for _, component := range components {
			if component.ComponentID == nil {
				continue
			}

			componentStatus := systemComponentStatus{
				componentID:   *component.ComponentID,
				componentType: component.ComponentType,
				version:       component.Version,
				status:        cloudprotocol.DownloadedStatus,
			}

			encryptedFile, err := getFilePath(component.URLs[0])
			if err != nil {
				return umCtrl.currentComponents, aoserrors.Wrap(err)
			}

			decryptedFile := path.Join(umCtrl.componentDir, base64.URLEncoding.EncodeToString(component.Sha256))

			space, err := umCtrl.allocator.AllocateSpace(component.Size)
			if err != nil {
				return umCtrl.currentComponents, aoserrors.Wrap(err)
			}

			defer func() {
				if err != nil {
					releaseAllocatedSpace(decryptedFile, space)

					return
				}

				if err := space.Accept(); err != nil {
					log.Errorf("Can't accept memory: %v", err)
				}
			}()

			if err = umCtrl.decrypter.DecryptAndValidate(encryptedFile, decryptedFile,
				fcrypt.DecryptParams{
					Chains:         chains,
					Certs:          certs,
					DecryptionInfo: component.DecryptionInfo,
					Signs:          component.Signs,
				}); err != nil {
				return umCtrl.currentComponents, aoserrors.Wrap(err)
			}

			fileInfo, err := image.CreateFileInfo(context.Background(), decryptedFile)
			if err != nil {
				return umCtrl.currentComponents, aoserrors.Wrap(err)
			}

			url := url.URL{
				Scheme: fileScheme,
				Path:   decryptedFile,
			}

			componentInfo := ComponentStatus{
				ComponentID:   *component.ComponentID,
				ComponentType: component.ComponentType,
				Version:       component.Version,
				Annotations:   string(component.Annotations),
				Sha256:        fileInfo.Sha256, Size: fileInfo.Size,
				URL: url.String(),
			}

			if err = umCtrl.addComponentForUpdateToUm(componentInfo); err != nil {
				return umCtrl.currentComponents, aoserrors.Wrap(err)
			}

			componentsUpdateInfo = append(componentsUpdateInfo, componentInfo)

			umCtrl.updateComponentElement(componentStatus)
		}

		if err := umCtrl.storage.SetComponentsUpdateInfo(componentsUpdateInfo); err != nil {
			go umCtrl.generateFSMEvent(evUpdateFailed, aoserrors.Wrap(err))

			return umCtrl.currentComponents, aoserrors.Wrap(err)
		}

		umCtrl.generateFSMEvent(evUpdateRequest, nil)
	}

	umCtrl.updateFinishCond.L.Lock()
	defer umCtrl.updateFinishCond.L.Unlock()

	umCtrl.updateFinishCond.Wait()

	return umCtrl.currentComponents, umCtrl.updateError
}

func (umCtrl *Controller) NewComponentsChannel() <-chan []cloudprotocol.ComponentStatus {
	return umCtrl.newComponentsChannel
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (umCtrl *Controller) processInternalMessages() {
	for {
		select {
		case <-umCtrl.connectionMonitor.timeoutChan:
			if len(umCtrl.connections) == 0 {
				umCtrl.generateFSMEvent(evAllClientsConnected)
			} else {
				log.Error("Ums connection timeout")
				umCtrl.generateFSMEvent(evConnectionTimeout)
			}

		case internalMsg := <-umCtrl.eventChannel:
			log.Debug("Internal Event ", internalMsg.requestType)

			switch internalMsg.requestType {
			case openConnection:
				umCtrl.handleNewConnection(internalMsg.umID, internalMsg.handler, internalMsg.status)

			case closeConnection:
				umCtrl.handleCloseConnection(internalMsg.umID, internalMsg.close)

			case umStatusUpdate:
				umCtrl.generateFSMEvent(evUpdateStatusUpdated, internalMsg.umID, internalMsg.status)

			default:
				log.Error("Unsupported internal message ", internalMsg.requestType)
			}

		case nodeInfo := <-umCtrl.nodeInfoChannel:
			umCtrl.handleNodeInfoChange(nodeInfo)

		case <-umCtrl.certChannel:
			umCtrl.handleCertChange()

		case <-umCtrl.stopChannel:
			log.Debug("Close all connections")

			umCtrl.closeUMServer()

			umCtrl.updateFinishCond.Broadcast()

			return
		}
	}
}

func (umCtrl *Controller) handleNewConnection(umID string, handler *umHandler, status umStatus) {
	if handler == nil {
		log.Error("Handler is nil")
		return
	}

	umIDfound := false

	for i, value := range umCtrl.connections {
		if value.umID != umID {
			continue
		}

		umCtrl.updateCurrentComponentsStatus(status.componsStatus)

		if umCtrl.fsm.Current() != stateInit {
			umCtrl.notifyNewComponents(umID, status.componsStatus)
		}

		umIDfound = true

		if value.handler != nil {
			log.Warn("Connection already available umID = ", umID)
			value.handler.Close(Reconnect)
		}

		umCtrl.connections[i].handler = handler
		umCtrl.connections[i].state = handler.GetInitialState()
		umCtrl.connections[i].updatePriority = status.nodePriority
		umCtrl.connections[i].components = []string{}

		for _, newComponent := range status.componsStatus {
			idExist := false

			for _, value := range umCtrl.connections[i].components {
				if value == newComponent.componentID {
					idExist = true
					break
				}
			}

			if idExist {
				continue
			}

			umCtrl.connections[i].components = append(umCtrl.connections[i].components, newComponent.componentID)
		}

		break
	}

	if !umIDfound {
		log.Error("Unexpected new UM connection with ID = ", umID)
		handler.Close(ConnectionClose)

		return
	}

	for _, value := range umCtrl.connections {
		if value.handler == nil {
			return
		}
	}

	if umCtrl.fsm.Current() == stateInit {
		log.Debug("All connection to Ums established")

		umCtrl.connectionMonitor.stopConnectionTimer()

		if err := umCtrl.getUpdateComponentsFromStorage(); err != nil {
			log.Error("Can't read update components from storage: ", err)
		}

		umCtrl.generateFSMEvent(evAllClientsConnected)
	}
}

func (umCtrl *Controller) handleCloseConnection(umID string, reason closeReason) {
	log.WithFields(log.Fields{"state": umCtrl.fsm.Current(), "umID": umID}).Debug("Close UM connection")

	if umCtrl.fsm.Current() == stateReconnecting {
		for i, value := range umCtrl.connections {
			if value.umID == umID {
				umCtrl.connections[i].handler = nil

				break
			}
		}

		allClientsDisconnected := !slices.ContainsFunc(umCtrl.connections, func(uc umConnection) bool {
			return uc.handler != nil
		})

		if allClientsDisconnected {
			umCtrl.generateFSMEvent(evAllClientsDisconnected)
		}

		return
	}

	for i, value := range umCtrl.connections {
		if value.umID == umID {
			if reason == ConnectionClose {
				nodeInfo, err := umCtrl.nodeInfoProvider.GetNodeInfo(umID)
				provisionedNode := err == nil && nodeInfo.Status != cloudprotocol.NodeStatusUnprovisioned

				if provisionedNode {
					umCtrl.connections[i].handler = nil

					umCtrl.fsm.SetState(stateInit)

					umCtrl.connectionMonitor.startConnectionTimer(len(umCtrl.connections))
				} else {
					umCtrl.connections = append(umCtrl.connections[:i], umCtrl.connections[i+1:]...)
				}
			}

			return
		}
	}
}

func (umCtrl *Controller) updateCurrentComponentsStatus(componsStatus []systemComponentStatus) {
	log.Debug("Receive components: ", componsStatus)

	for _, value := range componsStatus {
		if value.status == cloudprotocol.InstalledStatus {
			toRemove := []int{}

			for i, curStatus := range umCtrl.currentComponents {
				if value.componentID == curStatus.ComponentID && value.componentType == curStatus.ComponentType {
					if curStatus.Status != cloudprotocol.InstalledStatus {
						continue
					}

					if value.version != curStatus.Version {
						toRemove = append(toRemove, i)
						continue
					}
				}
			}

			sort.Ints(toRemove)

			for i, value := range toRemove {
				umCtrl.currentComponents = append(umCtrl.currentComponents[:value-i],
					umCtrl.currentComponents[value-i+1:]...)
			}
		}

		umCtrl.updateComponentElement(value)
	}
}

func (umCtrl *Controller) updateComponentElement(component systemComponentStatus) {
	for i, curElement := range umCtrl.currentComponents {
		if curElement.ComponentID == component.componentID && curElement.ComponentType == component.componentType &&
			curElement.Version == component.version {
			if curElement.Status == cloudprotocol.InstalledStatus && component.status != cloudprotocol.InstalledStatus {
				break
			}

			if curElement.Status != component.status {
				umCtrl.currentComponents[i].Status = component.status

				if component.err != "" {
					umCtrl.currentComponents[i].ErrorInfo = &cloudprotocol.ErrorInfo{Message: component.err}
				}
			}

			return
		}
	}

	newComponentStatus := cloudprotocol.ComponentStatus{
		ComponentID:   component.componentID,
		ComponentType: component.componentType,
		Version:       component.version,
		Status:        component.status,
	}

	if component.err != "" {
		newComponentStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: component.err}
	}

	umCtrl.currentComponents = append(umCtrl.currentComponents, newComponentStatus)
}

func (umCtrl *Controller) cleanupCurrentComponentStatus() {
	i := 0

	for _, component := range umCtrl.currentComponents {
		if component.Status == cloudprotocol.InstalledStatus || component.Status == cloudprotocol.ErrorStatus {
			umCtrl.currentComponents[i] = component
			i++
		}
	}

	umCtrl.currentComponents = umCtrl.currentComponents[:i]
}

func (umCtrl *Controller) getCurrentUpdateState() (state string) {
	var onPrepareState, onApplyState bool

	for _, conn := range umCtrl.connections {
		switch conn.state {
		case umFailed:
			return stateFaultState

		case umPrepared:
			onPrepareState = true

		case umUpdated:
			onApplyState = true

		default:
			continue
		}
	}

	if onPrepareState {
		return statePrepareUpdate
	}

	if onApplyState {
		return stateStartApply
	}

	return stateIdle
}

func (umCtrl *Controller) getUpdateComponentsFromStorage() (err error) {
	for i := range umCtrl.connections {
		umCtrl.connections[i].updatePackages = []ComponentStatus{}
	}

	updateComponents, err := umCtrl.storage.GetComponentsUpdateInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, component := range updateComponents {
		if addErr := umCtrl.addComponentForUpdateToUm(component); addErr != nil {
			if err == nil {
				err = aoserrors.Wrap(addErr)
			}
		}
	}

	return aoserrors.Wrap(err)
}

func (umCtrl *Controller) addComponentForUpdateToUm(componentInfo ComponentStatus) (err error) {
	for i := range umCtrl.connections {
		for _, id := range umCtrl.connections[i].components {
			if id == componentInfo.ComponentID {
				newURL, err := umCtrl.fileServer.TranslateURL(umCtrl.connections[i].isLocalClient, componentInfo.URL)
				if err != nil {
					return aoserrors.Wrap(err)
				}

				componentInfo.URL = newURL

				umCtrl.connections[i].updatePackages = append(umCtrl.connections[i].updatePackages, componentInfo)

				return nil
			}
		}
	}

	return aoserrors.Errorf("component id %s not found", componentInfo.ComponentID)
}

func (umCtrl *Controller) cleanupUpdateData() {
	for i := range umCtrl.connections {
		for _, updatePackage := range umCtrl.connections[i].updatePackages {
			umCtrl.allocator.FreeSpace(updatePackage.Size)
		}

		umCtrl.connections[i].updatePackages = []ComponentStatus{}
	}

	entries, err := os.ReadDir(umCtrl.componentDir)
	if err != nil {
		log.Errorf("Can't read component directory: %v", err)

		return
	}

	for _, entry := range entries {
		fileInfo, err := entry.Info()
		if err != nil {
			log.Errorf("Can't get file info: %v", err)

			continue
		}

		umCtrl.allocator.FreeSpace(uint64(fileInfo.Size()))

		if err := os.RemoveAll(filepath.Join(umCtrl.componentDir, entry.Name())); err != nil {
			log.Errorf("Can't remove decrypted file: %v", err)
		}
	}

	updateComponents, err := umCtrl.storage.GetComponentsUpdateInfo()
	if err != nil {
		log.Error("Can't get components update info ", err)
		return
	}

	if len(updateComponents) == 0 {
		return
	}

	if err := umCtrl.storage.SetComponentsUpdateInfo([]ComponentStatus{}); err != nil {
		log.Error("Can't clean components update info ", err)
	}
}

func (umCtrl *Controller) generateFSMEvent(event string, args ...interface{}) {
	if !umCtrl.operable {
		log.Error("Update controller in shutdown state")
		return
	}

	if err := umCtrl.fsm.Event(context.Background(), event, args...); err != nil {
		log.Error("Error transaction ", err)
	}
}

func (umCtrl *Controller) createStateMachine() (string, []fsm.EventDesc, map[string]fsm.Callback) {
	return stateInit,
		fsm.Events{
			// process Init state
			{Name: evTLSCertUpdated, Src: []string{stateInit}, Dst: stateReconnecting},

			// process Idle state
			{Name: evAllClientsConnected, Src: []string{stateInit}, Dst: stateIdle},
			{Name: evUpdateRequest, Src: []string{stateIdle}, Dst: statePrepareUpdate},
			{Name: evContinuePrepare, Src: []string{stateIdle}, Dst: statePrepareUpdate},
			{Name: evContinueUpdate, Src: []string{stateIdle}, Dst: stateStartUpdate},
			{Name: evContinueApply, Src: []string{stateIdle}, Dst: stateStartApply},
			{Name: evContinueRevert, Src: []string{stateIdle}, Dst: stateStartRevert},
			{Name: evTLSCertUpdated, Src: []string{stateIdle}, Dst: stateReconnecting},

			// process Reconnecting state
			{Name: evAllClientsDisconnected, Src: []string{stateReconnecting}, Dst: stateInit},

			// process prepare
			{Name: evUpdateStatusUpdated, Src: []string{statePrepareUpdate}, Dst: stateUpdateUmStatusOnPrepareUpdate},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnPrepareUpdate}, Dst: statePrepareUpdate},
			// process start update
			{Name: evUpdatePrepared, Src: []string{statePrepareUpdate}, Dst: stateStartUpdate},
			{Name: evUpdateStatusUpdated, Src: []string{stateStartUpdate}, Dst: stateUpdateUmStatusOnStartUpdate},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnStartUpdate}, Dst: stateStartUpdate},
			// process start apply
			{Name: evSystemUpdated, Src: []string{stateStartUpdate}, Dst: stateStartApply},
			{Name: evUpdateStatusUpdated, Src: []string{stateStartApply}, Dst: stateUpdateUmStatusOnStartApply},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnStartApply}, Dst: stateStartApply},
			{Name: evApplyComplete, Src: []string{stateStartApply}, Dst: stateIdle},
			// process revert
			{Name: evUpdateFailed, Src: []string{statePrepareUpdate}, Dst: stateStartRevert},
			{Name: evUpdateFailed, Src: []string{stateStartUpdate}, Dst: stateStartRevert},
			{Name: evUpdateFailed, Src: []string{stateStartApply}, Dst: stateStartRevert},
			{Name: evUpdateStatusUpdated, Src: []string{stateStartRevert}, Dst: stateUpdateUmStatusOnRevert},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnRevert}, Dst: stateStartRevert},
			{Name: evSystemReverted, Src: []string{stateStartRevert}, Dst: stateIdle},

			{Name: evConnectionTimeout, Src: []string{stateInit}, Dst: stateFaultState},
		},
		fsm.Callbacks{
			enterPrefix + stateIdle:                          umCtrl.processIdleState,
			enterPrefix + stateReconnecting:                  umCtrl.processEnterReconnectingState,
			leavePrefix + stateReconnecting:                  umCtrl.processLeaveReconnectingState,
			enterPrefix + statePrepareUpdate:                 umCtrl.processPrepareState,
			enterPrefix + stateUpdateUmStatusOnPrepareUpdate: umCtrl.processUpdateUpdateStatus,
			enterPrefix + stateStartUpdate:                   umCtrl.processStartUpdateState,
			enterPrefix + stateUpdateUmStatusOnStartUpdate:   umCtrl.processUpdateUpdateStatus,
			enterPrefix + stateStartApply:                    umCtrl.processStartApplyState,
			enterPrefix + stateUpdateUmStatusOnStartApply:    umCtrl.processUpdateUpdateStatus,
			enterPrefix + stateStartRevert:                   umCtrl.processStartRevertState,
			enterPrefix + stateUpdateUmStatusOnRevert:        umCtrl.processUpdateUpdateStatus,
			enterPrefix + stateFaultState:                    umCtrl.processFaultState,

			beforePrefix + "event":          umCtrl.onEvent,
			beforePrefix + evApplyComplete:  umCtrl.updateComplete,
			beforePrefix + evSystemReverted: umCtrl.revertComplete,
			beforePrefix + evUpdateFailed:   umCtrl.processError,
		}
}

func (monitor *allConnectionMonitor) startConnectionTimer(connectionsCount int) {
	monitor.Lock()
	defer monitor.Unlock()

	if connectionsCount == 0 {
		monitor.timeoutChan <- true
		return
	}

	if monitor.connTimer != nil {
		log.Debug("Timer already started")
		return
	}

	monitor.connTimer = time.NewTimer(connectionTimeout)

	monitor.wg.Add(1)

	go func() {
		defer monitor.wg.Done()

		select {
		case <-monitor.connTimer.C:
			monitor.timeoutChan <- true

		case <-monitor.stopTimerChan:
			monitor.connTimer.Stop()
		}

		monitor.Lock()
		defer monitor.Unlock()

		monitor.connTimer = nil
	}()
}

func (monitor *allConnectionMonitor) stopConnectionTimer() {
	monitor.Lock()
	defer monitor.Unlock()

	if monitor.connTimer != nil {
		monitor.stopTimerChan <- true
	}
}

func releaseAllocatedSpace(filePath string, space spaceallocator.Space) {
	if err := os.RemoveAll(filePath); err != nil {
		log.Errorf("Can't remove decrypted file: %v", err)
	}

	if err := space.Release(); err != nil {
		log.Errorf("Can't release memory: %v", err)
	}
}

func getFilePath(fileURL string) (string, error) {
	urlData, err := url.Parse(fileURL)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	if urlData.Scheme != fileScheme {
		return "", aoserrors.New("unexpected schema")
	}

	return urlData.Path, nil
}

/***********************************************************************************************************************
 * FSM callbacks
 **********************************************************************************************************************/

func (umCtrl *Controller) onEvent(ctx context.Context, e *fsm.Event) {
	log.Debugf("[CtrlFSM] %s -> %s : Event: %s", e.Src, e.Dst, e.Event)
}

func (umCtrl *Controller) processEnterReconnectingState(context.Context, *fsm.Event) {
	umCtrl.closeUMServer()

	allClientsDisconnected := !slices.ContainsFunc(umCtrl.connections, func(uc umConnection) bool {
		return uc.handler != nil
	})

	if allClientsDisconnected {
		umCtrl.generateFSMEvent(evAllClientsDisconnected)
	}
}

func (umCtrl *Controller) processLeaveReconnectingState(ctx context.Context, event *fsm.Event) {
	if umCtrl.restartTimer != nil {
		umCtrl.restartTimer.Stop()
		umCtrl.restartTimer = nil
	}

	if err := umCtrl.createUMServer(); err != nil {
		log.WithField("err", err).Debug("Create UM server failed")

		umCtrl.restartTimer = time.AfterFunc(umRestartInterval, func() {
			umCtrl.processLeaveReconnectingState(ctx, event)
		})
	}
}

func (umCtrl *Controller) processIdleState(ctx context.Context, e *fsm.Event) {
	updateStatus := umCtrl.getCurrentUpdateState()

	switch updateStatus {
	case stateFaultState:
		go umCtrl.generateFSMEvent(evContinueRevert)
		return

	case statePrepareUpdate:
		go umCtrl.generateFSMEvent(evContinuePrepare)
		return

	case stateStartApply:
		go umCtrl.generateFSMEvent(evContinueApply)
		return
	}

	umCtrl.cleanupUpdateData()

	umCtrl.updateFinishCond.Broadcast()
}

func (umCtrl *Controller) processFaultState(ctx context.Context, e *fsm.Event) {
}

func (umCtrl *Controller) processPrepareState(ctx context.Context, e *fsm.Event) {
	for i := range umCtrl.connections {
		if len(umCtrl.connections[i].updatePackages) > 0 {
			if umCtrl.connections[i].state == umFailed {
				go umCtrl.generateFSMEvent(evUpdateFailed, aoserrors.New("preparUpdate failure umID = "+umCtrl.connections[i].umID))
				return
			}

			if umCtrl.connections[i].handler == nil {
				log.Warnf("Connection to um %s closed", umCtrl.connections[i].umID)
				return
			}

			if err := umCtrl.connections[i].handler.PrepareUpdate(umCtrl.connections[i].updatePackages); err == nil {
				return
			}
		}
	}

	go umCtrl.generateFSMEvent(evUpdatePrepared)
}

func (umCtrl *Controller) processStartUpdateState(ctx context.Context, e *fsm.Event) {
	log.Debug("processStartUpdateState")

	for i := range umCtrl.connections {
		if len(umCtrl.connections[i].updatePackages) > 0 {
			if umCtrl.connections[i].state == umFailed {
				go umCtrl.generateFSMEvent(evUpdateFailed, aoserrors.New("update failure umID = "+umCtrl.connections[i].umID))
				return
			}
		}

		if umCtrl.connections[i].handler == nil {
			log.Warnf("Connection to um %s closed", umCtrl.connections[i].umID)
			return
		}

		if err := umCtrl.connections[i].handler.StartUpdate(); err == nil {
			return
		}
	}

	go umCtrl.generateFSMEvent(evSystemUpdated)
}

func (umCtrl *Controller) processStartRevertState(ctx context.Context, e *fsm.Event) {
	errAvailable := false

	for i := range umCtrl.connections {
		log.Debug(len(umCtrl.connections[i].updatePackages))

		if len(umCtrl.connections[i].updatePackages) > 0 || umCtrl.connections[i].state == umFailed {
			if umCtrl.connections[i].handler == nil {
				log.Warnf("Connection to um %s closed", umCtrl.connections[i].umID)
				return
			}

			if len(umCtrl.connections[i].updatePackages) == 0 {
				log.Warnf("No update components but UM %s is in failure state", umCtrl.connections[i].umID)
			}

			if err := umCtrl.connections[i].handler.StartRevert(); err == nil {
				return
			}

			if umCtrl.connections[i].state == umFailed {
				errAvailable = true
			}
		}
	}

	if errAvailable {
		log.Error("System maintenance is required") // think about cyclic revert
		return
	}

	go umCtrl.generateFSMEvent(evSystemReverted)
}

func (umCtrl *Controller) processStartApplyState(ctx context.Context, e *fsm.Event) {
	for i := range umCtrl.connections {
		if len(umCtrl.connections[i].updatePackages) > 0 {
			if umCtrl.connections[i].state == umFailed {
				go umCtrl.generateFSMEvent(evUpdateFailed, aoserrors.New("apply failure umID = "+umCtrl.connections[i].umID))
				return
			}
		}

		if umCtrl.connections[i].handler == nil {
			log.Warnf("Connection to um %s closed", umCtrl.connections[i].umID)
			return
		}

		if err := umCtrl.connections[i].handler.StartApply(); err == nil {
			return
		}
	}

	go umCtrl.generateFSMEvent(evApplyComplete)
}

func (umCtrl *Controller) processUpdateUpdateStatus(ctx context.Context, e *fsm.Event) {
	log.Debug("processUpdateUpdateStatus")

	umID, ok := e.Args[0].(string)
	if !ok {
		log.Error("Incorrect UM ID in update state")
		return
	}

	status, ok := e.Args[1].(umStatus)
	if !ok {
		log.Error("Incorrect UM status in update state")
		return
	}

	for i, v := range umCtrl.connections {
		if v.umID == umID {
			umCtrl.connections[i].state = status.updateStatus
			log.Debugf("umID = %s  state = %s", umID, status.updateStatus)

			break
		}
	}

	umCtrl.updateCurrentComponentsStatus(status.componsStatus)

	go umCtrl.generateFSMEvent(evContinue)
}

func (umCtrl *Controller) processError(ctx context.Context, e *fsm.Event) {
	var ok bool

	if umCtrl.updateError, ok = e.Args[0].(error); !ok {
		umCtrl.updateError = aoserrors.New("unknown error")
	}

	log.Error("Update error: ", umCtrl.updateError)

	umCtrl.cleanupCurrentComponentStatus()
}

func (umCtrl *Controller) revertComplete(ctx context.Context, e *fsm.Event) {
	log.Debug("Revert complete")

	umCtrl.cleanupCurrentComponentStatus()
}

func (umCtrl *Controller) updateComplete(ctx context.Context, e *fsm.Event) {
	log.Debug("Update finished")

	umCtrl.cleanupCurrentComponentStatus()
}

func (umCtrl *Controller) createConnections() error {
	umCtrl.connections = []umConnection{}

	ids, err := umCtrl.nodeInfoProvider.GetAllNodeIDs()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, id := range ids {
		nodeInfo, err := umCtrl.nodeInfoProvider.GetNodeInfo(id)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if nodeInfo.Status == "unprovisioned" {
			continue
		}

		nodeHasUM, err := umCtrl.nodeHasUMComponent(nodeInfo)
		if err != nil {
			log.WithField("nodeID", nodeInfo.NodeID).Errorf("Failed to check UM component: %v", err)
		}

		if nodeHasUM {
			umCtrl.connections = append(umCtrl.connections, umConnection{
				umID:           nodeInfo.NodeID,
				isLocalClient:  nodeInfo.NodeID == umCtrl.currentNodeID,
				updatePriority: lowestUpdatePriority,
				handler:        nil,
			})
		}
	}

	sort.Slice(umCtrl.connections, func(i, j int) bool {
		return umCtrl.connections[i].updatePriority < umCtrl.connections[j].updatePriority
	})

	return nil
}

func (umCtrl *Controller) handleNodeInfoChange(nodeInfo cloudprotocol.NodeInfo) {
	if nodeInfo.Status == "unprovisioned" {
		return
	}

	nodeHasUM, err := umCtrl.nodeHasUMComponent(nodeInfo)
	if err != nil {
		log.WithField("nodeID", nodeInfo.NodeID).Errorf("Failed to check UM component: %v", err)
	}

	if !nodeHasUM {
		return
	}

	for _, connection := range umCtrl.connections {
		if connection.umID == nodeInfo.NodeID {
			return
		}
	}

	umCtrl.connections = append(umCtrl.connections, umConnection{
		umID:          nodeInfo.NodeID,
		isLocalClient: nodeInfo.NodeID == umCtrl.currentNodeID, updatePriority: lowestUpdatePriority, handler: nil,
	})
}

func (umCtrl *Controller) handleCertChange() {
	umCtrl.generateFSMEvent(evTLSCertUpdated)
}

func (status systemComponentStatus) String() string {
	return fmt.Sprintf("{id: %s, status: %s, version: %s }", status.componentID, status.status, status.version)
}

func (umCtrl *Controller) notifyNewComponents(umID string, componsStatus []systemComponentStatus) {
	newComponents := make([]cloudprotocol.ComponentStatus, 0, len(componsStatus))

	for _, status := range componsStatus {
		newComponent := cloudprotocol.ComponentStatus{
			ComponentID:   status.componentID,
			ComponentType: status.componentType,
			Version:       status.version,
			NodeID:        &umID,
			Status:        status.status,
		}

		if status.err != "" {
			newComponent.ErrorInfo = &cloudprotocol.ErrorInfo{Message: status.err}
		}

		newComponents = append(newComponents, newComponent)
	}

	if len(newComponents) == 0 {
		return
	}

	umCtrl.newComponentsChannel <- newComponents
}

func (umCtrl *Controller) nodeHasUMComponent(
	nodeInfo cloudprotocol.NodeInfo,
) (bool, error) {
	components, err := nodeInfo.GetAosComponents()
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	for _, component := range components {
		if component == cloudprotocol.AosComponentUM {
			return true, nil
		}
	}

	return false, nil
}

func (umCtrl *Controller) createUMServer() error {
	log.Debug("Create UM Server")

	var err error

	umCtrl.server, err = newServer(umCtrl.config, umCtrl.eventChannel, umCtrl.certProvider,
		umCtrl.cryptocontext, umCtrl.insecure)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	umCtrl.connectionMonitor.startConnectionTimer(len(umCtrl.connections))

	return nil
}

func (umCtrl *Controller) closeUMServer() {
	log.Debug("Close UM Server")

	umCtrl.connectionMonitor.stopConnectionTimer()

	umCtrl.server.Stop()
}
