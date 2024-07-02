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

package cloudprotocol

import (
	"time"

	"github.com/aosedge/aos_common/aostypes"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// MonitoringMessageType monitoring message type.
const MonitoringMessageType = "monitoringData"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// PartitionUsage partition usage information.
type PartitionUsage struct {
	Name     string `json:"name"`
	UsedSize uint64 `json:"usedSize"`
}

// MonitoringData monitoring data.
type MonitoringData struct {
	RAM        uint64           `json:"ram"`
	CPU        uint64           `json:"cpu"`
	InTraffic  uint64           `json:"inTraffic"`
	OutTraffic uint64           `json:"outTraffic"`
	Disk       []PartitionUsage `json:"disk"`
}

// NodeMonitoringData node monitoring data.
type NodeMonitoringData struct {
	MonitoringData
	NodeID           string                   `json:"nodeId"`
	Timestamp        time.Time                `json:"timestamp"`
	ServiceInstances []InstanceMonitoringData `json:"serviceInstances"`
}

// InstanceMonitoringData monitoring data for service.
type InstanceMonitoringData struct {
	aostypes.InstanceIdent
	NodeID string `json:"nodeId"`
	MonitoringData
}

// Monitoring monitoring message structure.
type Monitoring struct {
	MessageType      string                   `json:"messageType"`
	Nodes            []NodeMonitoringData     `json:"nodes"`
	ServiceInstances []InstanceMonitoringData `json:"serviceInstances"`
}
