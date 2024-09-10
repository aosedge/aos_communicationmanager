// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2024 Renesas Electronics Corporation.
// Copyright (C) 2024 EPAM Systems, Inc.
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

package alertutils

import (
	"reflect"

	"github.com/aosedge/aos_common/api/cloudprotocol"
)

// AlertsPayloadEqual compares alerts ignoring timestamp.
//
//nolint:funlen
func AlertsPayloadEqual(alert1, alert2 interface{}) bool {
	switch alert1casted := alert1.(type) {
	case cloudprotocol.SystemAlert:
		alert2casted, ok := alert2.(cloudprotocol.SystemAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.CoreAlert:
		alert2casted, ok := alert2.(cloudprotocol.CoreAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.DownloadAlert:
		alert2casted, ok := alert2.(cloudprotocol.DownloadAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.SystemQuotaAlert:
		alert2casted, ok := alert2.(cloudprotocol.SystemQuotaAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.InstanceQuotaAlert:
		alert2casted, ok := alert2.(cloudprotocol.InstanceQuotaAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.DeviceAllocateAlert:
		alert2casted, ok := alert2.(cloudprotocol.DeviceAllocateAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.ResourceValidateAlert:
		alert2casted, ok := alert2.(cloudprotocol.ResourceValidateAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)

	case cloudprotocol.ServiceInstanceAlert:
		alert2casted, ok := alert2.(cloudprotocol.ServiceInstanceAlert)
		if !ok {
			return false
		}

		newAlert1 := alert1casted
		newAlert1.Timestamp = alert2casted.Timestamp

		return reflect.DeepEqual(newAlert1, alert2casted)
	}

	return false
}
