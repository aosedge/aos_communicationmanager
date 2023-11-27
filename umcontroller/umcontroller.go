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

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/image"
	"github.com/aoscloud/aos_common/spaceallocator"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/fcrypt"
	"github.com/aoscloud/aos_communicationmanager/fileserver"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller update managers controller.
type Controller struct {
	storage      storage
	server       *umCtrlServer
	eventChannel chan umCtrlInternalMsg
	stopChannel  chan bool
	componentDir string

	decrypter Decrypter

	allocator spaceallocator.Allocator

	connections       []umConnection
	currentComponents []cloudprotocol.ComponentStatus
	fsm               *fsm.FSM
	connectionMonitor allConnectionMonitor
	operable          bool
	updateFinishCond  *sync.Cond

	updateError error

	fileServer *fileserver.FileServer
}

// SystemComponent information about system component update.
type SystemComponent struct {
	ID            string `json:"id"`
	VendorVersion string `json:"vendorVersion"`
	AosVersion    uint64 `json:"aosVersion"`
	Annotations   string `json:"annotations,omitempty"`
	URL           string `json:"url"`
	Sha256        []byte `json:"sha256"`
	Sha512        []byte `json:"sha512"`
	Size          uint64 `json:"size"`
}

type umConnection struct {
	umID           string
	isLocalClient  bool
	handler        *umHandler
	updatePriority uint32
	state          string
	components     []string
	updatePackages []SystemComponent
}

type umCtrlInternalMsg struct {
	umID        string
	handler     *umHandler
	requestType int
	status      umStatus
}

type umStatus struct {
	umState       string
	componsStatus []systemComponentStatus
}

type systemComponentStatus struct {
	id            string
	vendorVersion string
	aosVersion    uint64
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
	GetComponentsUpdateInfo() (updateInfo []SystemComponent, err error)
	SetComponentsUpdateInfo(updateInfo []SystemComponent) (err error)
}

// CertificateProvider certificate and key provider interface.
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, keyURL string, err error)
}

// Decrypter interface to decrypt and validate image.
type Decrypter interface {
	DecryptAndValidate(encryptedFile, decryptedFile string, params fcrypt.DecryptParams) error
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
	evAllClientsConnected = "allClientsConnected"
	evConnectionTimeout   = "connectionTimeout"
	evUpdateRequest       = "updateRequest"
	evContinue            = "continue"
	evUpdatePrepared      = "updatePrepared"
	evUmStateUpdated      = "umStateUpdated"
	evSystemUpdated       = "systemUpdated"
	evApplyComplete       = "applyComplete"

	evContinuePrepare = "continuePrepare"
	evContinueUpdate  = "continueUpdate"
	evContinueApply   = "continueApply"
	evContinueRevert  = "continueRevert"

	evUpdateFailed   = "updateFailed"
	evSystemReverted = "systemReverted"
)

// client sates.
const (
	umIdle     = "IDLE"
	umPrepared = "PREPARED"
	umUpdated  = "UPDATED"
	umFailed   = "FAILED"
)

const connectionTimeout = 600 * time.Second

const fileScheme = "file"

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new update managers controller.
func New(config *config.Config, storage storage, certProvider CertificateProvider,
	cryptocontext *cryptutils.CryptoContext, decrypter Decrypter, insecure bool) (
	umCtrl *Controller, err error,
) {
	umCtrl = &Controller{
		storage:           storage,
		eventChannel:      make(chan umCtrlInternalMsg),
		stopChannel:       make(chan bool),
		componentDir:      config.ComponentsDir,
		connectionMonitor: allConnectionMonitor{stopTimerChan: make(chan bool, 1), timeoutChan: make(chan bool, 1)},
		operable:          true,
		updateFinishCond:  sync.NewCond(&sync.Mutex{}),
		decrypter:         decrypter,
	}

	if err := os.MkdirAll(umCtrl.componentDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if umCtrl.fileServer, err = fileserver.New(
		config.UMController.FileServerURL, config.ComponentsDir); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, client := range config.UMController.UMClients {
		umCtrl.connections = append(umCtrl.connections, umConnection{
			umID:          client.UMID,
			isLocalClient: client.IsLocal, updatePriority: client.Priority, handler: nil,
		})
	}

	sort.Slice(umCtrl.connections, func(i, j int) bool {
		return umCtrl.connections[i].updatePriority < umCtrl.connections[j].updatePriority
	})

	if umCtrl.allocator, err = spaceallocator.New(umCtrl.componentDir, 0, nil); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	umCtrl.fsm = fsm.NewFSM(umCtrl.createStateMachine())

	umCtrl.server, err = newServer(config, umCtrl.eventChannel, certProvider, cryptocontext, insecure)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	go umCtrl.processInternalMessages()

	umCtrl.connectionMonitor.wg.Add(1)

	go umCtrl.connectionMonitor.startConnectionTimer(len(umCtrl.connections))
	go func() {
		if err := umCtrl.server.Start(); err != nil {
			log.Errorf("Can't start UM controller server: %s", err)
		}
	}()

	return umCtrl, nil
}

// Close close server.
func (umCtrl *Controller) Close() {
	if umCtrl.fileServer != nil {
		if err := umCtrl.fileServer.Close(); err != nil {
			log.Errorf("Can't close file server: %v", err)
		}
	}

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

		componentsUpdateInfo := []SystemComponent{}

		for _, component := range components {
			componentStatus := systemComponentStatus{
				id: component.ID, vendorVersion: component.VendorVersion,
				aosVersion: component.AosVersion, status: cloudprotocol.DownloadedStatus,
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

			componentInfo := SystemComponent{
				ID: component.ID, VendorVersion: component.VendorVersion,
				AosVersion: component.AosVersion, Annotations: string(component.Annotations),
				Sha256: fileInfo.Sha256, Sha512: fileInfo.Sha512, Size: fileInfo.Size,
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
				umCtrl.handleCloseConnection(internalMsg.umID)

			case umStatusUpdate:
				umCtrl.generateFSMEvent(evUmStateUpdated, internalMsg.umID, internalMsg.status)

			default:
				log.Error("Unsupported internal message ", internalMsg.requestType)
			}

		case <-umCtrl.stopChannel:
			log.Debug("Close all connections")

			umCtrl.server.Stop()
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

		umIDfound = true

		if value.handler != nil {
			log.Warn("Connection already available umID = ", umID)
			value.handler.Close()
		}

		umCtrl.connections[i].handler = handler
		umCtrl.connections[i].state = handler.GetInitialState()
		umCtrl.connections[i].components = []string{}

		for _, newComponent := range status.componsStatus {
			idExist := false

			for _, value := range umCtrl.connections[i].components {
				if value == newComponent.id {
					idExist = true
					break
				}
			}

			if idExist {
				continue
			}

			umCtrl.connections[i].components = append(umCtrl.connections[i].components, newComponent.id)
		}

		break
	}

	if !umIDfound {
		log.Error("Unexpected new UM connection with ID = ", umID)
		handler.Close()

		return
	}

	for _, value := range umCtrl.connections {
		if value.handler == nil {
			return
		}
	}

	log.Debug("All connection to Ums established")

	umCtrl.connectionMonitor.stopConnectionTimer()

	if err := umCtrl.getUpdateComponentsFromStorage(); err != nil {
		log.Error("Can't read update components from storage: ", err)
	}

	umCtrl.generateFSMEvent(evAllClientsConnected)
}

func (umCtrl *Controller) handleCloseConnection(umID string) {
	log.Debug("Close UM connection umid = ", umID)

	for i, value := range umCtrl.connections {
		if value.umID == umID {
			umCtrl.connections[i].handler = nil

			umCtrl.fsm.SetState(stateInit)
			umCtrl.connectionMonitor.wg.Add(1)

			go umCtrl.connectionMonitor.startConnectionTimer(len(umCtrl.connections))

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
				if value.id == curStatus.ID {
					if curStatus.Status != cloudprotocol.InstalledStatus {
						continue
					}

					if value.vendorVersion != curStatus.VendorVersion {
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
		if curElement.ID == component.id && curElement.VendorVersion == component.vendorVersion {
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
		ID:            component.id,
		VendorVersion: component.vendorVersion,
		AosVersion:    component.aosVersion,
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
		umCtrl.connections[i].updatePackages = []SystemComponent{}
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

func (umCtrl *Controller) addComponentForUpdateToUm(componentInfo SystemComponent) (err error) {
	for i := range umCtrl.connections {
		for _, id := range umCtrl.connections[i].components {
			if id == componentInfo.ID {
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

	return aoserrors.Errorf("component id %s not found", componentInfo.ID)
}

func (umCtrl *Controller) cleanupUpdateData() {
	for i := range umCtrl.connections {
		for _, updatePackage := range umCtrl.connections[i].updatePackages {
			umCtrl.allocator.FreeSpace(updatePackage.Size)
		}

		umCtrl.connections[i].updatePackages = []SystemComponent{}
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

	if err := umCtrl.storage.SetComponentsUpdateInfo([]SystemComponent{}); err != nil {
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
			// process Idle state
			{Name: evAllClientsConnected, Src: []string{stateInit}, Dst: stateIdle},
			{Name: evUpdateRequest, Src: []string{stateIdle}, Dst: statePrepareUpdate},
			{Name: evContinuePrepare, Src: []string{stateIdle}, Dst: statePrepareUpdate},
			{Name: evContinueUpdate, Src: []string{stateIdle}, Dst: stateStartUpdate},
			{Name: evContinueApply, Src: []string{stateIdle}, Dst: stateStartApply},
			{Name: evContinueRevert, Src: []string{stateIdle}, Dst: stateStartRevert},
			// process prepare
			{Name: evUmStateUpdated, Src: []string{statePrepareUpdate}, Dst: stateUpdateUmStatusOnPrepareUpdate},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnPrepareUpdate}, Dst: statePrepareUpdate},
			// process start update
			{Name: evUpdatePrepared, Src: []string{statePrepareUpdate}, Dst: stateStartUpdate},
			{Name: evUmStateUpdated, Src: []string{stateStartUpdate}, Dst: stateUpdateUmStatusOnStartUpdate},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnStartUpdate}, Dst: stateStartUpdate},
			// process start apply
			{Name: evSystemUpdated, Src: []string{stateStartUpdate}, Dst: stateStartApply},
			{Name: evUmStateUpdated, Src: []string{stateStartApply}, Dst: stateUpdateUmStatusOnStartApply},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnStartApply}, Dst: stateStartApply},
			{Name: evApplyComplete, Src: []string{stateStartApply}, Dst: stateIdle},
			// process revert
			{Name: evUpdateFailed, Src: []string{statePrepareUpdate}, Dst: stateStartRevert},
			{Name: evUpdateFailed, Src: []string{stateStartUpdate}, Dst: stateStartRevert},
			{Name: evUpdateFailed, Src: []string{stateStartApply}, Dst: stateStartRevert},
			{Name: evUmStateUpdated, Src: []string{stateStartRevert}, Dst: stateUpdateUmStatusOnRevert},
			{Name: evContinue, Src: []string{stateUpdateUmStatusOnRevert}, Dst: stateStartRevert},
			{Name: evSystemReverted, Src: []string{stateStartRevert}, Dst: stateIdle},

			{Name: evConnectionTimeout, Src: []string{stateInit}, Dst: stateFaultState},
		},
		fsm.Callbacks{
			"enter_" + stateIdle:                          umCtrl.processIdleState,
			"enter_" + statePrepareUpdate:                 umCtrl.processPrepareState,
			"enter_" + stateUpdateUmStatusOnPrepareUpdate: umCtrl.processUpdateUmState,
			"enter_" + stateStartUpdate:                   umCtrl.processStartUpdateState,
			"enter_" + stateUpdateUmStatusOnStartUpdate:   umCtrl.processUpdateUmState,
			"enter_" + stateStartApply:                    umCtrl.processStartApplyState,
			"enter_" + stateUpdateUmStatusOnStartApply:    umCtrl.processUpdateUmState,
			"enter_" + stateStartRevert:                   umCtrl.processStartRevertState,
			"enter_" + stateUpdateUmStatusOnRevert:        umCtrl.processUpdateUmState,
			"enter_" + stateFaultState:                    umCtrl.processFaultState,

			"before_event":               umCtrl.onEvent,
			"before_" + evApplyComplete:  umCtrl.updateComplete,
			"before_" + evSystemReverted: umCtrl.revertComplete,
			"before_" + evUpdateFailed:   umCtrl.processError,
		}
}

func (monitor *allConnectionMonitor) startConnectionTimer(connectionsCount int) {
	monitor.Lock()
	defer monitor.Unlock()

	defer monitor.wg.Done()

	if connectionsCount == 0 {
		monitor.timeoutChan <- true
		return
	}

	if monitor.connTimer != nil {
		log.Debug("Timer already started")
		return
	}

	monitor.connTimer = time.NewTimer(connectionTimeout)

	monitor.Unlock()

	select {
	case <-monitor.connTimer.C:
		monitor.timeoutChan <- true

	case <-monitor.stopTimerChan:
		monitor.connTimer.Stop()
	}

	monitor.Lock()

	monitor.connTimer = nil
}

func (monitor *allConnectionMonitor) stopConnectionTimer() {
	monitor.stopTimerChan <- true
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

func (umCtrl *Controller) processIdleState(ctx context.Context, e *fsm.Event) {
	umState := umCtrl.getCurrentUpdateState()

	switch umState {
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

func (umCtrl *Controller) processUpdateUmState(ctx context.Context, e *fsm.Event) {
	log.Debug("processUpdateUmState")

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
			umCtrl.connections[i].state = status.umState
			log.Debugf("UMid = %s  state= %s", umID, status.umState)

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

func (status systemComponentStatus) String() string {
	return fmt.Sprintf("{id: %s, status: %s, vendorVersion: %s aosVersion: %d }",
		status.id, status.status, status.vendorVersion, status.aosVersion)
}
