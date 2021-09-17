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

package umcontroller

import (
	"io"
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "gitpct.epam.com/epmd-aepr/aos_common/api/updatemanager/v1"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/
type normalUpdateStream struct {
	grpc.ServerStream
	test       *testing.T
	step       int
	continueCh chan bool
}

type failureUpdateStream struct {
	grpc.ServerStream
	test       *testing.T
	step       int
	continueCh chan bool
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	StepPrepare = iota + 1
	StepUpdate
	StepRebootOnUpdate
	StepApplyUpdate
	StepRebootOnApply
	StepRevertUpdate
	StepRebootOnRevert
	StepFinish
)

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/
func TestNormalUpdate(t *testing.T) {
	eventChannel := make(chan umCtrlInternalMsg)

	stream := normalUpdateStream{test: t, continueCh: make(chan bool)}

	handler, stopCh, err := newUmHandler("testUM", &stream, eventChannel, pb.UmState_IDLE)
	if err != nil {
		t.Errorf("erro create handler %s", err)
	}
	stream.step = StepPrepare

	components := []SystemComponent{SystemComponent{URL: "file:///path/to/update",
		VendorVersion: "vendorversion1", AosVersion: 1}}

	handler.PrepareUpdate(components)
	for {
		select {
		case internalEvent := <-eventChannel:
			switch stream.step {
			case StepPrepare:
				if internalEvent.requestType != umStatusUpdate {
					t.Errorf("Unexpected internl message %d != %d", internalEvent.requestType, umStatusUpdate)
					break
				}
				if internalEvent.status.umState != pb.UmState_PREPARED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_PREPARED.String())
					break
				}
				stream.step = StepUpdate
				handler.StartUpdate()

			case StepUpdate:
				if internalEvent.requestType != umStatusUpdate {
					t.Errorf("Unexpected internl message %d != %d", internalEvent.requestType, umStatusUpdate)
					break
				}
				if internalEvent.status.umState != pb.UmState_UPDATED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_UPDATED.String())
					break
				}
				stream.step = StepApplyUpdate
				handler.StartApply()

			case StepApplyUpdate:
				if internalEvent.requestType != umStatusUpdate {
					t.Errorf("Unexpected internl message %d != %d", internalEvent.requestType, umStatusUpdate)
					break
				}
				if internalEvent.status.umState != pb.UmState_IDLE.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_IDLE.String())
					break
				}
				stream.step = StepFinish
				stream.continueCh <- true
			}
		case <-stopCh:
			if stream.step != StepFinish {
				t.Error("Unexpected close connection")
			}

			return
		}
	}
}

func TestNormalUpdateWithReboot(t *testing.T) {
	eventChannel := make(chan umCtrlInternalMsg)

	stream := normalUpdateStream{test: t, continueCh: make(chan bool)}

	handler, stopCh, err := newUmHandler("testUM2", &stream, eventChannel, pb.UmState_IDLE)
	if err != nil {
		t.Errorf("error create handler %s", err)
	}
	stream.step = StepPrepare

	components := []SystemComponent{SystemComponent{URL: "file:///path/to/update",
		VendorVersion: "vendorversion2", AosVersion: 1}}

	handler.PrepareUpdate(components)
	for {
		select {
		case internalEvent := <-eventChannel:
			switch stream.step {
			case StepPrepare:
				if internalEvent.status.umState != pb.UmState_PREPARED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_PREPARED.String())
					break
				}
				stream.step = StepRebootOnUpdate
				handler.StartUpdate()

			default:
				log.Warn("Receive ", internalEvent.status.umState)
			}

		case <-stopCh:
			if stream.step == StepRebootOnUpdate {
				handler, stopCh, err = newUmHandler("testUM2", &stream, eventChannel, pb.UmState_UPDATED)
				stream.step = StepRebootOnApply
				handler.StartApply()
				continue
			}

			if stream.step == StepRebootOnApply {
				handler, stopCh, err = newUmHandler("testUM2", &stream, eventChannel, pb.UmState_IDLE)
				stream.step = StepFinish
				stream.continueCh <- true
				continue
			}

			if stream.step != StepFinish {
				t.Error("Unexpected close connection")
			}

			return
		}
	}
}

func TestRevert(t *testing.T) {
	eventChannel := make(chan umCtrlInternalMsg)

	stream := failureUpdateStream{test: t, continueCh: make(chan bool)}

	handler, stopCh, err := newUmHandler("testUM3", &stream, eventChannel, pb.UmState_IDLE)
	if err != nil {
		t.Errorf("error create handler %s", err)
	}
	stream.step = StepPrepare

	components := []SystemComponent{SystemComponent{URL: "file:///path/to/update",
		VendorVersion: "vendorversion3", AosVersion: 1}}

	handler.PrepareUpdate(components)
	for {
		select {
		case internalEvent := <-eventChannel:
			switch stream.step {
			case StepPrepare:
				if internalEvent.status.umState != pb.UmState_PREPARED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_PREPARED.String())
					break
				}
				stream.step = StepUpdate
				handler.StartUpdate()

			case StepUpdate:
				if internalEvent.status.umState != pb.UmState_FAILED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_FAILED.String())
					break
				}
				stream.step = StepRevertUpdate
				handler.StartRevert()

			case StepRevertUpdate:
				if internalEvent.status.umState != pb.UmState_IDLE.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_IDLE.String())
					break
				}
				stream.step = StepFinish
				stream.continueCh <- true
			}
		case <-stopCh:
			if stream.step != StepFinish {
				t.Error("Unexpected close connection")
			}

			return
		}
	}
}

func TestRevertWithReboot(t *testing.T) {
	eventChannel := make(chan umCtrlInternalMsg)

	stream := failureUpdateStream{test: t, continueCh: make(chan bool)}

	handler, stopCh, err := newUmHandler("testUM4", &stream, eventChannel, pb.UmState_IDLE)
	if err != nil {
		t.Errorf("error create handler %s", err)
	}
	stream.step = StepPrepare

	components := []SystemComponent{SystemComponent{URL: "file:///path/to/update",
		VendorVersion: "vendorversion4", AosVersion: 1}}

	handler.PrepareUpdate(components)
	for {
		select {
		case internalEvent := <-eventChannel:
			switch stream.step {
			case StepPrepare:
				if internalEvent.status.umState != pb.UmState_PREPARED.String() {
					t.Errorf("Unexpected UM update State  %s != %s", internalEvent.status.umState,
						pb.UmState_PREPARED.String())
					break
				}
				stream.step = StepRebootOnUpdate
				handler.StartUpdate()

			default:
				log.Warn("Receive ", internalEvent.status.umState)
			}

		case <-stopCh:
			if stream.step == StepRebootOnUpdate {
				handler, stopCh, err = newUmHandler("testUM4", &stream, eventChannel, pb.UmState_FAILED)
				stream.step = StepRebootOnRevert
				handler.StartRevert()
				continue
			}

			if stream.step == StepRebootOnRevert {
				handler, stopCh, err = newUmHandler("testUM2", &stream, eventChannel, pb.UmState_IDLE)
				stream.step = StepFinish
				stream.continueCh <- true
				continue
			}

			if stream.step != StepFinish {
				t.Error("Unexpected close connection")
			}

			return
		}
	}
}

/*******************************************************************************
 * Interfaces
 ******************************************************************************/

func (stream *normalUpdateStream) Send(msg *pb.CMMessages) (err error) {
	switch stream.step {
	case StepPrepare:
		if msg.GetPrepareUpdate() == nil {
			stream.test.Error("Expect prepare update request")
		}

	case StepUpdate:
		if msg.GetStartUpdate() == nil {
			stream.test.Error("Expect start update")
		}

	case StepRebootOnApply:
		if msg.GetApplyUpdate() == nil {
			stream.test.Error("Expect apply update")
		}

	case StepApplyUpdate:
		if msg.GetApplyUpdate() == nil {
			stream.test.Error("Expect apply update")
		}
	}

	stream.continueCh <- true

	return err
}

func (stream *normalUpdateStream) Recv() (*pb.UpdateStatus, error) {
	<-stream.continueCh
	var messageToSend *pb.UpdateStatus
	switch stream.step {
	case StepPrepare:
		messageToSend = &pb.UpdateStatus{UmState: pb.UmState_PREPARED}

	case StepRebootOnUpdate:
		return nil, io.EOF

	case StepRebootOnApply:
		return nil, io.EOF

	case StepUpdate:
		messageToSend = &pb.UpdateStatus{UmState: pb.UmState_UPDATED}

	case StepApplyUpdate:
		messageToSend = &pb.UpdateStatus{UmState: pb.UmState_IDLE}

	case StepFinish:
		return nil, io.EOF
	}

	return messageToSend, nil
}

func (stream *failureUpdateStream) Send(msg *pb.CMMessages) (err error) {
	switch stream.step {
	case StepPrepare:
		if msg.GetPrepareUpdate() == nil {
			stream.test.Error("Expect prepare update request")
		}

	case StepUpdate:
		if msg.GetStartUpdate() == nil {
			stream.test.Error("Expect start update")
		}

	case StepRebootOnRevert:
		if msg.GetRevertUpdate() == nil {
			stream.test.Error("Expect revert update")
		}

	case StepRevertUpdate:
		if msg.GetRevertUpdate() == nil {
			stream.test.Error("Expect revert update")
		}
	}

	stream.continueCh <- true

	return err
}

func (stream *failureUpdateStream) Recv() (*pb.UpdateStatus, error) {
	<-stream.continueCh
	var messageToSend *pb.UpdateStatus
	switch stream.step {
	case StepPrepare:
		messageToSend = &pb.UpdateStatus{UmState: pb.UmState_PREPARED}

	case StepRebootOnUpdate:
		return nil, io.EOF

	case StepUpdate:
		messageToSend = &pb.UpdateStatus{UmState: pb.UmState_FAILED}

	case StepRebootOnRevert:
		return nil, io.EOF

	case StepFinish:
		return nil, io.EOF
	}

	return messageToSend, nil
}
