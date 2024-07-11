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
	"errors"
	"io"

	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_common/aoserrors"
	pb "github.com/aosedge/aos_common/api/updatemanager"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type closeReason int

type umHandler struct {
	umID                string
	stream              pb.UMService_RegisterUMServer
	messageChannel      chan umCtrlInternalMsg
	closeChannel        chan closeReason
	FSM                 *fsm.FSM
	initialUpdateStatus string
}

type prepareRequest struct {
	components []ComponentStatus
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	Reconnect closeReason = iota
	ConnectionClose
)

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

// NewUmHandler create update manager connection handler.
func newUmHandler(id string, umStream pb.UMService_RegisterUMServer,
	messageChannel chan umCtrlInternalMsg, state pb.UpdateState,
) (handler *umHandler, closeChannel chan closeReason, err error) {
	handler = &umHandler{umID: id, stream: umStream, messageChannel: messageChannel}
	handler.closeChannel = make(chan closeReason)
	handler.initialUpdateStatus = state.String()

	initFsmState := hStateIdle

	switch state {
	case pb.UpdateState_IDLE:
		initFsmState = hStateIdle
	case pb.UpdateState_PREPARED:
		initFsmState = hStateWaitForStartUpdate
	case pb.UpdateState_UPDATED:
		initFsmState = hStateWaitForApply
	case pb.UpdateState_FAILED:
		log.Error("UM in failure state")

		initFsmState = hStateWaitForRevert
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
			enterPrefix + hStateWaitForPrepareResp:  handler.sendPrepareUpdateRequest,
			enterPrefix + hStateWaitForUpdateStatus: handler.sendStartUpdateToUM,
			enterPrefix + hStateWaitForApplyStatus:  handler.sendApplyUpdateToUM,
			enterPrefix + hStateWaitForRevertStatus: handler.sendRevertUpdateToUM,
			// notify umcontroller about current state
			beforePrefix + eventPrepareSuccess: handler.updateStatusNtf,
			beforePrefix + eventUpdateSuccess:  handler.updateStatusNtf,
			beforePrefix + eventIdleState:      handler.updateStatusNtf,
			beforePrefix + eventUpdateError:    handler.updateStatusNtf,

			beforePrefix + "event": func(ctx context.Context, e *fsm.Event) {
				log.Debugf("[UMID %s]: %s -> %s : Event: %s", handler.umID, e.Src, e.Dst, e.Event)
			},
		},
	)

	go handler.receiveData()

	return handler, handler.closeChannel, aoserrors.Wrap(err)
}

// Close close connection.
func (handler *umHandler) Close(reason closeReason) {
	log.Debug("Close umhandler with UMID = ", handler.umID)
	handler.closeChannel <- reason
}

func (handler *umHandler) GetInitialState() (state string) {
	return handler.initialUpdateStatus
}

// Close close connection.
func (handler *umHandler) PrepareUpdate(prepareComponents []ComponentStatus) (err error) {
	log.Debug("PrepareUpdate for UMID ", handler.umID)

	request := prepareRequest{components: prepareComponents}

	err = handler.FSM.Event(context.Background(), eventPrepareUpdate, request)

	return aoserrors.Wrap(err)
}

func (handler *umHandler) StartUpdate() (err error) {
	log.Debug("StartUpdate for UMID ", handler.umID)
	return aoserrors.Wrap(handler.FSM.Event(context.Background(), eventStartUpdate))
}

func (handler *umHandler) StartApply() (err error) {
	log.Debug("StartApply for UMID ", handler.umID)
	return aoserrors.Wrap(handler.FSM.Event(context.Background(), eventStartApply))
}

func (handler *umHandler) StartRevert() (err error) {
	log.Debug("StartRevert for UMID ", handler.umID)
	return aoserrors.Wrap(handler.FSM.Event(context.Background(), eventStartRevert))
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (handler *umHandler) receiveData() {
	defer func() { handler.closeChannel <- ConnectionClose }()

	for {
		statusMsg, err := handler.stream.Recv()
		if errors.Is(err, io.EOF) {
			log.Debug("End of connection ", handler.umID)
			return
		}

		if err != nil {
			log.Debugf("End of connection %s with error: %s", handler.umID, err)
			return
		}

		var evt string

		state := statusMsg.GetUpdateState()
		switch state {
		case pb.UpdateState_IDLE:
			evt = eventIdleState
		case pb.UpdateState_PREPARED:
			evt = eventPrepareSuccess
		case pb.UpdateState_UPDATED:
			evt = eventUpdateSuccess
		case pb.UpdateState_FAILED:
			log.Error("Update failure status: ", statusMsg.GetError())

			evt = eventUpdateError
		}

		err = handler.FSM.Event(context.Background(), evt, getUmStatusFromUmMessage(statusMsg))
		if err != nil {
			log.Errorf("Can't make transition umID %s %s", handler.umID, err.Error())
		}

		continue
	}
}

func (handler *umHandler) sendPrepareUpdateRequest(ctx context.Context, e *fsm.Event) {
	log.Debug("Send prepare request for UMID = ", handler.umID)

	request, ok := e.Args[0].(prepareRequest)
	if !ok {
		log.Error("Incorrect arg type in prepare update request")
		return
	}

	componetForUpdate := []*pb.PrepareComponentInfo{}

	for _, value := range request.components {
		componetInfo := pb.PrepareComponentInfo{
			ComponentId: value.ID,
			Version:     value.Version,
			Annotations: value.Annotations,
			Url:         value.URL,
			Sha256:      value.Sha256,
			Size:        value.Size,
		}

		componetForUpdate = append(componetForUpdate, &componetInfo)
	}

	cmMsg := &pb.CMMessages_PrepareUpdate{PrepareUpdate: &pb.PrepareUpdate{Components: componetForUpdate}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send Prepare update: ", err)

		go func() {
			err := handler.FSM.Event(context.Background(), eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) updateStatusNtf(ctx context.Context, e *fsm.Event) {
	status, ok := e.Args[0].(umStatus)

	if !ok {
		log.Error("Unexpected fsm event status type")
	}

	handler.messageChannel <- umCtrlInternalMsg{
		umID:        handler.umID,
		requestType: umStatusUpdate,
		status:      status,
	}
}

func (handler *umHandler) sendStartUpdateToUM(ctx context.Context, e *fsm.Event) {
	log.Debug("Send start update UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_StartUpdate{StartUpdate: &pb.StartUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send start update: ", err)

		go func() {
			err := handler.FSM.Event(context.Background(), eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) sendApplyUpdateToUM(ctx context.Context, e *fsm.Event) {
	log.Debug("Send apply UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_ApplyUpdate{ApplyUpdate: &pb.ApplyUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send apply update: ", err)

		go func() {
			err := handler.FSM.Event(context.Background(), eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}

func (handler *umHandler) sendRevertUpdateToUM(ctx context.Context, e *fsm.Event) {
	log.Debug("Send revert UMID = ", handler.umID)

	cmMsg := &pb.CMMessages_RevertUpdate{RevertUpdate: &pb.RevertUpdate{}}

	if err := handler.stream.Send(&pb.CMMessages{CMMessage: cmMsg}); err != nil {
		log.Error("Fail send revert update: ", err)

		go func() {
			err := handler.FSM.Event(context.Background(), eventUpdateError, err.Error())
			if err != nil {
				log.Error("Can't make transition: ", err)
			}
		}()
	}
}
