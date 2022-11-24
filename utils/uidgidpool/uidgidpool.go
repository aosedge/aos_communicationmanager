// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
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

// Package launcher provides set of API to controls services lifecycle
package uidgidpool

import (
	"fmt"
	"os/user"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

const (
	idsRangeBegin int = 5000
	idsRangeEnd   int = 10000
)

/**********************************************************************************************************************
* Types
**********************************************************************************************************************/

type IdentifierPool struct {
	sync.Mutex
	lockedIDs          []int
	systemAvailability func(int) bool
}

/**********************************************************************************************************************
* Public
**********************************************************************************************************************/

func NewGroupIDPool() (pool *IdentifierPool) {
	pool = &IdentifierPool{
		systemAvailability: func(gid int) bool {
			if group, err := user.LookupGroupId(fmt.Sprint(gid)); err == nil || group != nil {
				return false
			}

			return true
		},
	}

	return pool
}

func NewUserIDPool() (pool *IdentifierPool) {
	pool = &IdentifierPool{
		systemAvailability: func(uid int) bool {
			if user, err := user.LookupId(fmt.Sprint(uid)); err == nil || user != nil {
				return false
			}

			return true
		},
	}

	return pool
}

func (pool *IdentifierPool) GetFreeID() (id int, err error) {
	pool.Lock()
	defer pool.Unlock()

	if id, err = pool.getFreeIDFromPool(pool.systemAvailability); err != nil {
		return 0, err
	}

	pool.lockedIDs = append(pool.lockedIDs, id)

	return id, nil
}

func (pool *IdentifierPool) AddID(id int) error {
	pool.Lock()
	defer pool.Unlock()

	if isInPool(pool.lockedIDs, id) {
		return aoserrors.New("given ID already exist in pool")
	}

	pool.lockedIDs = append(pool.lockedIDs, id)

	return nil
}

func (pool *IdentifierPool) RemoveID(id int) (err error) {
	pool.Lock()
	defer pool.Unlock()

	for i, value := range pool.lockedIDs {
		if value == id {
			pool.lockedIDs = append(pool.lockedIDs[:i], pool.lockedIDs[i+1:]...)
			return nil
		}
	}

	return aoserrors.New("can't remove ID from pool: UID/GID is not found")
}

/**********************************************************************************************************************
* Private
**********************************************************************************************************************/

func (pool *IdentifierPool) getFreeIDFromPool(systemAvailability func(int) bool) (id int, err error) {
	for i := idsRangeBegin; i <= idsRangeEnd; i++ {
		if isInPool(pool.lockedIDs, i) {
			continue
		}

		if !systemAvailability(i) {
			continue
		}

		return i, nil
	}

	return 0, aoserrors.New("can't get free id")
}

func isInPool(pool []int, id int) (exist bool) {
	for _, value := range pool {
		if id == value {
			return true
		}
	}

	return false
}
