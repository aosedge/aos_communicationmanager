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

package cloudprotocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestTimeMarshalign(t *testing.T) {
	var timeSlot cloudprotocol.TimeSlot

	if err := json.Unmarshal([]byte(`{"Start":"01:02:03", "Finish":"04:05:06"}`), &timeSlot); err != nil {
		t.Fatalf("Can't unmarshal json: %s", err)
	}

	if timeSlot.Start.Hour() != 1 || timeSlot.Start.Minute() != 2 || timeSlot.Start.Second() != 3 {
		t.Errorf("Wrong time slot start value: %v", timeSlot.Start)
	}

	timeSlot.Start = cloudprotocol.Time{time.Date(1, 1, 1, 7, 8, 9, 0, time.Local)}
	timeSlot.Start = cloudprotocol.Time{time.Date(1, 1, 1, 10, 11, 12, 0, time.Local)}

	dataJSON, err := json.Marshal(timeSlot)
	if err != nil {
		t.Fatalf("Can't marshal json: %s", err)
	}

	if string(dataJSON) != `{"start":"10:11:12","finish":"04:05:06"}` {
		t.Errorf("Wrong json data: %s", string(dataJSON))
	}
}
