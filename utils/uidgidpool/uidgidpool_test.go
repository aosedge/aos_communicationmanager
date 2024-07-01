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

package uidgidpool_test

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/utils/uidgidpool"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/**********************************************************************************************************************
 * Consts
 *********************************************************************************************************************/

/**********************************************************************************************************************
 * Types
 *********************************************************************************************************************/

/**********************************************************************************************************************
 * Init
 *********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/**********************************************************************************************************************
* Main
**********************************************************************************************************************/

func TestMain(m *testing.M) {
	ret := m.Run()

	os.Exit(ret)
}

/**********************************************************************************************************************
* Tests
**********************************************************************************************************************/

func TestUIDs(t *testing.T) {
	pool := uidgidpool.NewUserIDPool()

	testFunction(t, pool, "user id")
}

func TestGIDs(t *testing.T) {
	pool := uidgidpool.NewGroupIDPool()

	testFunction(t, pool, "group id")
}

/**********************************************************************************************************************
* Private
**********************************************************************************************************************/

func testFunction(t *testing.T, pool *uidgidpool.IdentifierPool, msgID string) {
	t.Helper()

	uid, err := pool.GetFreeID()
	if err != nil {
		t.Fatalf("Can't get %s: %s", msgID, err)
	}

	if err = pool.AddID(uid); err == nil {
		t.Error("Should be error")
	}

	if err = pool.RemoveID(uid); err != nil {
		t.Errorf("Can't remove %s: %s", msgID, err)
	}

	if err = pool.RemoveID(uid); err == nil {
		t.Error("Should be error")
	}

	if err = pool.AddID(uid); err != nil {
		t.Errorf("Can't add %s: %s", msgID, err)
	}

	_, err = pool.GetFreeID()
	if err != nil {
		t.Fatalf("Can't get %s: %s", msgID, err)
	}
}
