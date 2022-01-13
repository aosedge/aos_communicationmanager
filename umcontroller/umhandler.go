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

	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_common/aoserrors"
	pb "github.com/aoscloud/aos_common/api/updatemanager/v1"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type umHandler struct {
	umID           string
	stream         pb.UMService_RegisterUMServer
	messageChannel chan umCtrlInternalMsg
	closeChannel   chan bool
	FSM            *fsm.FSM
	initialUmState string
}

type prepareRequest struct {
	components []SystemComponent
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	hStateIdle                = "Idle"
	hStateWaitForPrepareResp  = "WaitForPrepareResp"
	hStateWaitForStartUpdate  = "WaitForStartUpdate"
	hStateWaitForUpdateStatus = "WaitForUpdateStatus"
	hStateWaitForApply        = "WaitForApply"
	hStateWaitForApplyStatus  = "WaitForApplyStatus"
	hStateWaitForRevert       = "WaitForRevert"
	hStateWaitForRevertStatus = "WaitForRevertStatus"
)

const (
	eventPrepareUpdate  = "PrepareUpdate"
	eventPrepareSuccess = "PrepareSuccess"
	eventStartUpdate    = "StartUpdate"
	eventUpdateSuccess  = "UpdateSuccess"
	eventStartApply     = "StartApply"
	eventIdleState      = "IdleState"
	eventUpdateError    = "UpdateError"
	eventStartRevert    = "StartRevert"
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// NewUmHandler create update manager connection handler
func newUmHandler(id string, umStream pb.UMService_RegisterUMServer,
	messageChannel chan umCtrlInternalMsg, state pb.UmState) (handler *umHandler, closeChannel chan bool, err error,
) {
	handler = &umHandler{umID: id, stream: umStream, messageChannel: messageChannel}
	handler.closeChannel = make(chan bool)
	handler.initialUmState = state.String()

	initFsmState := hStateIdle
	switch state {
	case pb.UmState_IDLE:
		initFsmState = hStateIdle
	case pb.UmState_PREPARED:
		initFsmState = hStateWaitForStartUpdate
	case pb.UmState_UPDATED:
		initFsmState = hStateWaitForApply
	case pb.UmState_FAILED:
		initFsmState = hStateWaitForRevert
		log.Error("UM in failure state")
	}

	handler.FSM = fsm.NewFSM(
		initFsmState,
		fsm.Events{
			{Name: eventPrepareUpdate, Src: []string{hStateIdle}, Dst: hStateWaitForPrepareResp},
			{Name: eventPrepareSuccess, Src: []string{hStateWaitForPrepareResp}, Dst: hStateWaitForStartUpdate},
			{Name: eventStartUpdate, Src: []string{hStateWaitForStartUpdate}, Dst: hStateWaitForUpdateStatus},
			{Name: eventUpdateSuccess, Src: []string{hStateWaitForUpdateStatus}, Dst: hStateWaitForApply},
			{Name: eventStartApply, Src: []string{hStateWaitForApply}, Dst: hStateWaitForApplyStatus},
			{Name: eventIdleState, Src: []string{hStateWaitForApplyStatus, hStateWaitForRevertStatus}, Dst: hStateIdle},

			{
				Name: eventUpdateError, Src: []string{hStateWaitForPrepareResp, hStateWaitForUpdateStatus},
				Dst: hStateWaitForRevert,
			},
			{
				Name: eventStartRevert, Src: []string{hStateWaitForRevert, hStateWaitForStartUpdate, hStateWaitForApply},
				Dst: hStateWaitForRevertStatus,
			},
		},
		fsm.Callbacks{
			"enter_" + hStateWaitForPrepareResp:  handler.sendPrepareUpdateRequest,
			"enter_" + hStateWaitForUpdateStatus: handler.sendStartUpdateToUM,
			"enter_" + hStateWaitForApplyStatus:  handler.sendApplyUpdateToUM,
			"enter_" + hStateWaitForRevertStatus: handler.sendRevertUpdateToUM,
			// notify umcontroller about current state
			"before_" + eventPrepareSuccess: handler.updateStatusNtf,
			"before_" + eventUpdateSuccess:  handler.updateStatusNtf,
			"before_" + eventIdleState:      handler.updateStatusNtf,
			"before_" + eventUpdateError:    handler.updateStatusNtf,

			"before_event": func(e *fsm.Event) {
				log.Debugf("[UMID %s]: %s -> %s : Event: %s", handler.umID, e.Src, e.Dst, e.Event)
			},
		},
	)

	go handler.receiveData()

	return handler, handler.closeChannel, aoserrors.Wrap(err)
}

// Close close connection
func (handler *umHandler) Close() {
	log.Debug("Close umhandler with UMID = ", handler.umID)
	handler.closeChannel <- true
}

func (handler *umHandler) GetInitilState() (state string) {
	return handler.initialUmState
}

// Close close connection
func (handler *umHandler) PrepareUpdate(prepareComponents []SystemComponent) (err error) {
	log.Debug("PrepareUpdate for UMID ", handler.umID)

	request := prepareRequest{components: prepareComponents}

	err = handler.FSM.Event(eventPrepareUpdate, request)

	return aoserrors.Wrap(err)
}

func (handler *umHandler) StartUpdate() (err error) {
	log.Debug("StartUpdate for UMID ", handler.umID)
	err = handler.FSM.Event(eventStartUpdate)
	return aoserrors.Wrap(err)
}

func (handler *umHandler) StartApply() (err error) {
	log.Debug("StartApply for UMID ", handler.umID)
	err = handler.FSM.Event(eventStartApply)
	return aoserrors.Wrap(err)
}

func (handler *umHandler) StartRevert() (err error) {
	log.Debug("StartRevert for UMID ", handler.umID)
	err = handler.FSM.Event(eventStartRevert)
	return aoserrors.Wrap(err)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (handler *umHandler) receiveData() {
	defer func() { handler.closeChannel <- true }()

	for {
		statusMsg, err := handler.stream.Recv()
		if err == io.EOF {
			log.Debug("End of connection ", handler.umID)
			return
		}

		if err != nil {
			log.Debugf("End of connection %s with error: %s", handler.umID, err)
			return
		}

		var evt string
		state := statusMsg.GetUmState()
		switch state {
		case pb.UmState_IDLE:
			evt = eventIdleState
		case pb.UmState_PREPARED:
			evt = eventPrepareSuccess
		case pb.UmState_UPDATED:
			evt = eventUpdateSuccess
		case pb.UmState_FAILED:
			evt = eventUpdateError
			log.Error("Update failure status: ", statusMsg.GetError())
		}

		err = handler.FSM.Event(evt, getUmStatusFromUmMessage(statusMsg))
		if err != nil {
			log.Errorf("Can't make transition umID %s %s", handler.umID, err.Error())
		}
		continue
	}
}

func (handler *umHandler) sendPrepareUpdateRequest(e *fsm.Event) {
	log.Debug("Send prepare request for UMID = ", handler.umID)

	request := e.Args[0].(prepareRequest)

	componetForUpdate := []*pb.PrepareComponentInfo{}
	for _, value := range request.components {
		componetInfo := pb.PrepareComponentInfo{
			Id:            value.ID,
			VendorVersion: value.VendorVersion,
			AosVersion:    value.AosVersion,
			Annotations:   value.Annotations,
			Url:           value.URL,
			Sha256:        value.Sha256,
			Sha512:        value.Sha512,
			Size:          value.Size,
		}

		componetForUpdate = append(componetForUpdate, &componetInfo)
	}

	cmMsg := &pb.CMMessages_PrepareUpdate{PrepareUpdate: &pb.PrepareUpdate{Components: componetForUpdate}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send Prepare update: ", err)

		go func() {
			err := handler.FSM.Event(eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) updateStatusNtf(e *fsm.Event) {
	handler.messageChannel <- umCtrlInternalMsg{
		umID:        handler.umID,
		requestType: umStatusUpdate,
		status:      e.Args[0].(umStatus),
	}
}

func (handler *umHandler) sendStartUpdateToUM(e *fsm.Event) {
	log.Debug("Send start update UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_StartUpdate{StartUpdate: &pb.StartUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send start update: ", err)

		go func() {
			err := handler.FSM.Event(eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) sendApplyUpdateToUM(e *fsm.Event) {
	log.Debug("Send apply UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_ApplyUpdate{ApplyUpdate: &pb.ApplyUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send apply update: ", err)

		go func() {
			err := handler.FSM.Event(eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) sendRevertUpdateToUM(e *fsm.Event) {
	log.Debug("Send revert UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_RevertUpdate{RevertUpdate: &pb.RevertUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send revert update: ", err)

		go func() {
			err := handler.FSM.Event(eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}
