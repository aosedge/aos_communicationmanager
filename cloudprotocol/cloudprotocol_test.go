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

package cloudprotocol_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestTimeMarshalign(t *testing.T) {
	type testData struct {
		rawJSON  string
		timeSlot cloudprotocol.TimeSlot
	}

	var timeSlot cloudprotocol.TimeSlot

	unmarshalData := []testData{
		{
			rawJSON: `{"start":"T01","finish":"T04"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 0, 0, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 0, 0, 0, time.Local)},
			},
		},
		{
			rawJSON: `{"start":"T0102","finish":"T0405"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 2, 0, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 5, 0, 0, time.Local)},
			},
		},
		{
			rawJSON: `{"start":"T010203","finish":"T040506"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 2, 3, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 5, 6, 0, time.Local)},
			},
		},
		{
			rawJSON: `{"start":"01:02","finish":"04:05"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 2, 0, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 5, 0, 0, time.Local)},
			},
		},
		{
			rawJSON: `{"start":"01:02:03","finish":"04:05:06"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 2, 3, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 5, 6, 0, time.Local)},
			},
		},
	}

	for _, item := range unmarshalData {
		if err := json.Unmarshal([]byte(item.rawJSON), &timeSlot); err != nil {
			t.Errorf("Can't unmarshal json: %s", err)
			continue
		}

		if timeSlot != item.timeSlot {
			t.Errorf("Wrong time slot value: %v", timeSlot)
		}
	}

	marshalData := []testData{
		{
			rawJSON: `{"start":"01:02:03","finish":"04:05:06"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 1, 2, 3, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 4, 5, 6, 0, time.Local)},
			},
		},
		{
			rawJSON: `{"start":"07:08:09","finish":"10:11:12"}`,
			timeSlot: cloudprotocol.TimeSlot{
				Start:  cloudprotocol.Time{Time: time.Date(0, 1, 1, 7, 8, 9, 0, time.Local)},
				Finish: cloudprotocol.Time{Time: time.Date(0, 1, 1, 10, 11, 12, 0, time.Local)},
			},
		},
	}

	for _, item := range marshalData {
		rawJSON, err := json.Marshal(item.timeSlot)
		if err != nil {
			t.Errorf("Can't marshal json: %s", err)
			continue
		}

		if string(rawJSON) != item.rawJSON {
			t.Errorf("Wrong json data: %s", string(rawJSON))
		}
	}
}
