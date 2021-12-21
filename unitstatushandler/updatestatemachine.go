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

package unitstatushandler

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/looplab/fsm"
	log "github.com/sirupsen/logrus"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/cmserver"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	stateNoUpdate      = "noUpdate"
	stateDownloading   = "downloading"
	stateReadyToUpdate = "readyToUpdate"
	stateUpdating      = "updating"
)

const (
	eventStartDownload  = "startDownload"
	eventFinishDownload = "finishDownload"
	eventStartUpdate    = "startUpdate"
	eventFinishUpdate   = "finishUpdate"
	eventCancel         = "cancel"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type updateStateMachine struct {
	manager updateManager

	fsm        *fsm.FSM
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc

	updateTimer *time.Timer
	ttlTimer    *time.Timer

	defaultTTL time.Duration
}

type updateManager interface {
	stateChanged(event, state, updateErr string)
	download(ctx context.Context)
	readyToUpdate()
	update(ctx context.Context)
	noUpdate()
	startUpdate() (err error)
}

type syncExecutor struct {
	sync.Mutex
	inProgress bool
	waitQueue  *list.List
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var updateSynchronizer = newSyncExecutor()

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func newUpdateStateMachine(initState string, events []fsm.EventDesc,
	manager updateManager, defaultTTL time.Duration) (stateMachine *updateStateMachine) {
	stateMachine = &updateStateMachine{
		manager:    manager,
		defaultTTL: defaultTTL,
	}

	stateMachine.fsm = fsm.NewFSM(
		initState, events,
		fsm.Callbacks{
			"before_event":     stateMachine.onBeforeEvent,
			stateNoUpdate:      stateMachine.onStateNoUpdate,
			stateDownloading:   stateMachine.onStateDownloading,
			stateReadyToUpdate: stateMachine.onStateReadyToUpdate,
			stateUpdating:      stateMachine.onStateUpdating,
		},
	)

	return stateMachine
}

func (stateMachine *updateStateMachine) close() (err error) {
	stateMachine.resetTimers()
	stateMachine.cancel()

	return nil
}

func (stateMachine *updateStateMachine) init(ttlDate time.Time) (err error) {
	switch stateMachine.fsm.Current() {
	case stateDownloading:
		stateMachine.onStateDownloading(nil)

	case stateReadyToUpdate:
		stateMachine.onStateReadyToUpdate(nil)

	case stateUpdating:
		stateMachine.onStateUpdating(nil)
	}

	if stateMachine.fsm.Current() != stateNoUpdate && !ttlDate.IsZero() {
		stateMachine.setTTLTimer(ttlDate.Sub(time.Now()))
	}

	return nil
}

func (stateMachine *updateStateMachine) canTransit(event string) (result bool) {
	return stateMachine.fsm.Can(event)
}

func (stateMachine *updateStateMachine) sendEvent(event string, managerErr string) (err error) {
	if stateMachine.canTransit(event) {
		stateMachine.cancel()
	}

	if err = stateMachine.fsm.Event(event, managerErr); err != nil {
		log.Errorf("Can't send event: %s", err)
		return aoserrors.Wrap(err)
	}

	return nil
}

func (stateMachine *updateStateMachine) scheduleUpdate(schedule cloudprotocol.ScheduleRule) {
	var updateTime time.Duration

	switch schedule.Type {
	case cloudprotocol.TriggerUpdate:
		log.Debug("Wait for update trigger")
		return

	case cloudprotocol.TimetableUpdate:
		updateTime, _ = getAvailableTimetableTime(time.Now(), schedule.Timetable)

		log.WithFields(log.Fields{"in": updateTime}).Debug("Schedule timetable update")

	default:
		// Schedule forces update by default
		updateTime = 0

		log.WithFields(log.Fields{"in": updateTime}).Debug("Schedule forced update")
	}

	stateMachine.updateTimer = time.AfterFunc(updateTime, func() {
		if err := stateMachine.manager.startUpdate(); err != nil {
			log.Errorf("Can't start update: %s", err)
		}
	})
}

func (stateMachine *updateStateMachine) finishOperation(ctx context.Context, finishEvent string, operationErr string) {
	// Do nothing if context canceled
	if ctx.Err() != nil {
		return
	}

	stateMachine.sendEvent(finishEvent, operationErr)
}

func (stateMachine *updateStateMachine) startNewUpdate(ttlTime time.Duration) (ttlDate time.Time, err error) {
	if ttlTime == 0 {
		ttlTime = stateMachine.defaultTTL
	}

	// if TTL is not received and default value is zero then do not set TTL timer
	if ttlTime != 0 {
		ttlDate = time.Now().Add(ttlTime)
		stateMachine.setTTLTimer(ttlTime)
	}

	if err = stateMachine.sendEvent(eventStartDownload, ""); err != nil {
		return ttlDate, aoserrors.Wrap(err)
	}

	return ttlDate, nil
}

func convertState(state string) (updateState cmserver.UpdateState) {
	switch state {
	case stateDownloading:
		return cmserver.Downloading

	case stateReadyToUpdate:
		return cmserver.ReadyToUpdate

	case stateUpdating:
		return cmserver.Updating

	default:
		return cmserver.NoUpdate
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (stateMachine *updateStateMachine) setTTLTimer(ttlTime time.Duration) {
	stateMachine.ttlTimer = time.AfterFunc(ttlTime, func() {
		if stateMachine.canTransit(eventCancel) {
			if err := stateMachine.sendEvent(eventCancel, aoserrors.New("update timeout").Error()); err != nil {
				log.Errorf("Can't cancel update: %s", err)
			}
		}
	})
}

func (stateMachine *updateStateMachine) cancel() {
	if stateMachine.cancelFunc != nil {
		stateMachine.cancelFunc()
	}

	stateMachine.wg.Wait()
}

func (stateMachine *updateStateMachine) onBeforeEvent(event *fsm.Event) {
	var managerErr string

	if len(event.Args) != 0 {
		if errorStr, ok := event.Args[0].(string); ok {
			managerErr = errorStr
		}
	}

	stateMachine.manager.stateChanged(event.Event, event.Dst, managerErr)
}

func (stateMachine *updateStateMachine) onStateNoUpdate(event *fsm.Event) {
	stateMachine.resetTimers()
	stateMachine.manager.noUpdate()
}

func (stateMachine *updateStateMachine) onStateDownloading(event *fsm.Event) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	stateMachine.cancelFunc = cancelFunc

	stateMachine.wg.Add(1)

	go func() {
		defer stateMachine.wg.Done()
		stateMachine.manager.download(ctx)
	}()
}

func (stateMachine *updateStateMachine) onStateReadyToUpdate(event *fsm.Event) {
	stateMachine.manager.readyToUpdate()
}

func (stateMachine *updateStateMachine) onStateUpdating(event *fsm.Event) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	stateMachine.cancelFunc = cancelFunc

	stateMachine.wg.Add(1)

	updateSynchronizer.execute(ctx, func() {
		defer stateMachine.wg.Done()
		stateMachine.manager.update(ctx)
	})

	go func() {
	}()
}

func (stateMachine *updateStateMachine) resetTimers() {
	// Reset update timer
	if stateMachine.updateTimer != nil {
		stateMachine.updateTimer.Stop()
		stateMachine.updateTimer = nil
	}

	// Reset TTL timer
	if stateMachine.ttlTimer != nil {
		stateMachine.ttlTimer.Stop()
		stateMachine.ttlTimer = nil
	}
}

/***********************************************************************************************************************
 * syncExecutor
 **********************************************************************************************************************/

func newSyncExecutor() (executor *syncExecutor) {
	executor = &syncExecutor{
		waitQueue: list.New(),
	}

	return executor
}

func (executor *syncExecutor) execute(ctx context.Context, f func()) {
	executor.Lock()
	defer executor.Unlock()

	type executeData struct {
		f func()
		c chan struct{}
	}

	if executor.inProgress {
		channelDone := make(chan struct{}, 1)

		element := executor.waitQueue.PushBack(executeData{f: f, c: channelDone})

		go func() {
			select {
			case <-ctx.Done():
				executor.Lock()
				defer executor.Unlock()

				executor.waitQueue.Remove(element)

			case <-channelDone:
			}
		}()

		return
	}

	executor.inProgress = true

	go func() {
		for executeFunc := f; executeFunc != nil; {
			executeFunc()

			executor.Lock()

			element := executor.waitQueue.Front()

			if element != nil {
				data := executor.waitQueue.Remove(element).(executeData)
				data.c <- struct{}{}
				executeFunc = data.f
			} else {
				executor.inProgress = false
				executeFunc = nil
			}

			executor.Unlock()
		}
	}()
}
