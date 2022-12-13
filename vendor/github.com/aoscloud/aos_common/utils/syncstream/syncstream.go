// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 EPAM Systems, Inc.
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

package syncstream

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type SyncStream struct {
	sync.Mutex
	requestID int
	requests  []request
}

type request struct {
	id           int
	responseType reflect.Type
	channel      chan<- interface{}
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var ErrTimeout = errors.New("wait for response timeout")

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func New() *SyncStream {
	return &SyncStream{}
}

func (stream *SyncStream) Send(ctx context.Context, sendFunc func() error,
	responseType reflect.Type,
) (response interface{}, err error) {
	stream.Lock()

	channel := make(chan interface{}, 1)

	request := request{
		id:           stream.requestID,
		responseType: responseType,
		channel:      channel,
	}

	stream.requests = append(stream.requests, request)

	stream.requestID++

	stream.Unlock()

	defer func() {
		if err != nil {
			stream.Lock()
			stream.deleteRequest(stream.requestID)
			stream.Unlock()
		}
	}()

	if sendFunc != nil {
		if err := sendFunc(); err != nil {
			return nil, err
		}
	}

	select {
	case response := <-channel:
		return response, nil

	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

func (stream *SyncStream) ProcessMessages(message interface{}) (processed bool) {
	stream.Lock()
	defer stream.Unlock()

	for _, request := range stream.requests {
		if request.responseType == reflect.TypeOf(message) {
			request.channel <- message

			stream.deleteRequest(request.id)

			return true
		}
	}

	return false
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (stream *SyncStream) deleteRequest(id int) {
	i := 0

	for _, request := range stream.requests {
		if request.id != id {
			stream.requests[i] = request
			i++

			continue
		}

		close(request.channel)
	}

	stream.requests = stream.requests[:i]
}
