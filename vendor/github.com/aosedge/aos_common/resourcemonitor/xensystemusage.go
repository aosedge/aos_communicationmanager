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

package resourcemonitor

import (
	"github.com/aosedge/aos_common/utils/xentop"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type xenSystemUsage struct {
	systemInfos map[string]xentop.SystemInfo
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func (xen *xenSystemUsage) CacheSystemInfos() {
	instanceInfos, err := xentop.GetSystemInfos()
	if err != nil {
		log.Errorf("Can't get system infos: %v", err)

		return
	}

	xen.systemInfos = instanceInfos
}

func (xen *xenSystemUsage) FillSystemInfo(instanceID string, instance *instanceMonitoring) error {
	systemInfo, ok := xen.systemInfos[instanceID]
	if ok {
		instance.monitoring.CPU = uint64(systemInfo.CPUFraction)
		instance.monitoring.RAM = uint64(systemInfo.Memory) * 1024 //nolint:mnd
	}

	return nil
}
