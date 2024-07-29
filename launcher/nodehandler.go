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

package launcher

import (
	"errors"
	"slices"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type runRequest struct {
	Services  []aostypes.ServiceInfo  `json:"services"`
	Layers    []aostypes.LayerInfo    `json:"layers"`
	Instances []aostypes.InstanceInfo `json:"instances"`
}

type nodeHandler struct {
	nodeInfo          cloudprotocol.NodeInfo
	nodeConfig        cloudprotocol.NodeConfig
	deviceAllocations map[string]int
	runStatus         []cloudprotocol.InstanceStatus
	runRequest        runRequest
	isLocalNode       bool
	waitStatus        bool
	availableCPU      uint64
	availableRAM      uint64
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

//nolint:gochecknoglobals
var defaultRunners = []string{"crun", "runc"}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newNodeHandler(
	nodeInfo cloudprotocol.NodeInfo, resourceManager ResourceManager, isLocalNode bool,
) (*nodeHandler, error) {
	log.WithFields(log.Fields{"nodeID": nodeInfo.NodeID}).Debug("Init node handler")

	node := &nodeHandler{
		nodeInfo:    nodeInfo,
		isLocalNode: isLocalNode,
		waitStatus:  true,
	}

	nodeConfig, err := resourceManager.GetNodeConfig(node.nodeInfo.NodeID, node.nodeInfo.NodeType)
	if err != nil && !errors.Is(err, unitconfig.ErrNotFound) {
		return nil, aoserrors.Wrap(err)
	}

	node.nodeConfig = nodeConfig
	node.resetDeviceAllocations()

	node.availableCPU = node.nodeInfo.MaxDMIPs
	node.availableRAM = node.nodeInfo.TotalRAM

	return node, nil
}

func (node *nodeHandler) resetDeviceAllocations() {
	node.deviceAllocations = make(map[string]int)

	for _, device := range node.nodeConfig.Devices {
		node.deviceAllocations[device.Name] = device.SharedCount
	}
}

func (node *nodeHandler) allocateDevices(serviceDevices []aostypes.ServiceDevice) error {
	for _, serviceDevice := range serviceDevices {
		count, ok := node.deviceAllocations[serviceDevice.Name]
		if !ok {
			return aoserrors.Errorf("device not found: %s", serviceDevice.Name)
		}

		if count == 0 {
			return aoserrors.Errorf("can't allocate device: %s", serviceDevice.Name)
		}

		node.deviceAllocations[serviceDevice.Name] = count - 1
	}

	return nil
}

func (node *nodeHandler) nodeHasDesiredDevices(desiredDevices []aostypes.ServiceDevice) bool {
	for _, desiredDevice := range desiredDevices {
		count, ok := node.deviceAllocations[desiredDevice.Name]
		if !ok || count == 0 {
			return false
		}
	}

	return true
}

func (node *nodeHandler) addRunRequest(instanceInfo aostypes.InstanceInfo, service imagemanager.ServiceInfo,
	layers []imagemanager.LayerInfo,
) error {
	log.WithFields(instanceIdentLogFields(
		instanceInfo.InstanceIdent, log.Fields{"node": node.nodeInfo.NodeID})).Debug("Schedule instance on node")

	if err := node.allocateDevices(service.Config.Devices); err != nil {
		return err
	}

	node.runRequest.Instances = append(node.runRequest.Instances, instanceInfo)

	node.addService(service)
	node.addLayers(layers)

	requestedCPU := node.getRequestedCPU(service.Config)
	if requestedCPU > node.availableCPU {
		return aoserrors.Errorf("not enough CPU")
	}

	requestedRAM := node.getRequestedRAM(service.Config)
	if requestedRAM > node.availableRAM {
		return aoserrors.Errorf("not enough CPU")
	}

	node.availableCPU -= requestedCPU
	node.availableRAM -= requestedRAM

	return nil
}

func (node *nodeHandler) addService(service imagemanager.ServiceInfo) {
	serviceInfo := service.ServiceInfo

	if !node.isLocalNode {
		serviceInfo.URL = service.RemoteURL
	}

	if slices.ContainsFunc(node.runRequest.Services, func(info aostypes.ServiceInfo) bool {
		return info.ServiceID == serviceInfo.ServiceID
	}) {
		return
	}

	log.WithFields(log.Fields{
		"serviceID": serviceInfo.ServiceID, "node": node.nodeInfo.NodeID,
	}).Debug("Schedule service on node")

	node.runRequest.Services = append(node.runRequest.Services, serviceInfo)
}

func (node *nodeHandler) addLayers(layers []imagemanager.LayerInfo) {
	for _, layer := range layers {
		layerInfo := layer.LayerInfo

		if !node.isLocalNode {
			layerInfo.URL = layer.RemoteURL
		}

		if slices.ContainsFunc(node.runRequest.Layers, func(info aostypes.LayerInfo) bool {
			return info.Digest == layerInfo.Digest
		}) {
			continue
		}

		log.WithFields(log.Fields{
			"digest": layerInfo.Digest, "node": node.nodeInfo.NodeID,
		}).Debug("Schedule layer on node")

		node.runRequest.Layers = append(node.runRequest.Layers, layerInfo)
	}
}

func (node *nodeHandler) getPartitionSize(partitionType string) uint64 {
	partitionIndex := slices.IndexFunc(node.nodeInfo.Partitions, func(partition cloudprotocol.PartitionInfo) bool {
		return slices.Contains(partition.Types, partitionType)
	})

	if partitionIndex == -1 {
		return 0
	}

	return node.nodeInfo.Partitions[partitionIndex].TotalSize
}

func (node *nodeHandler) getRequestedCPU(serviceConfig aostypes.ServiceConfig) uint64 {
	requestedCPU := uint64(0)

	if serviceConfig.Quotas.CPULimit != nil {
		requestedCPU = uint64(float64(*serviceConfig.Quotas.CPULimit)*getCPURequestRatio(
			serviceConfig.ResourceRatios, node.nodeConfig.ResourceRatios) + 0.5)
	}

	return requestedCPU
}

func (node *nodeHandler) getRequestedRAM(serviceConfig aostypes.ServiceConfig) uint64 {
	requestedRAM := uint64(0)

	if serviceConfig.Quotas.RAMLimit != nil {
		requestedRAM = uint64(float64(*serviceConfig.Quotas.RAMLimit)*getRAMRequestRatio(
			serviceConfig.ResourceRatios, node.nodeConfig.ResourceRatios) + 0.5)
	}

	return requestedRAM
}

func getNodesByStaticResources(nodes []*nodeHandler,
	serviceConfig aostypes.ServiceConfig, instanceInfo cloudprotocol.InstanceInfo,
) ([]*nodeHandler, error) {
	resultNodes := getNodeByRunners(nodes, serviceConfig.Runners)
	if len(resultNodes) == 0 {
		return resultNodes, aoserrors.Errorf("no nodes with runner: %s", serviceConfig.Runners)
	}

	resultNodes = getNodesByLabels(resultNodes, instanceInfo.Labels)
	if len(resultNodes) == 0 {
		return resultNodes, aoserrors.Errorf("no nodes with labels %v", instanceInfo.Labels)
	}

	resultNodes = getNodesByResources(resultNodes, serviceConfig.Resources)
	if len(resultNodes) == 0 {
		return resultNodes, aoserrors.Errorf("no nodes with resources %v", serviceConfig.Resources)
	}

	return resultNodes, nil
}

func getNodesByDevices(nodes []*nodeHandler, desiredDevices []aostypes.ServiceDevice) []*nodeHandler {
	if len(desiredDevices) == 0 {
		return nodes
	}

	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if !node.nodeHasDesiredDevices(desiredDevices) {
			continue
		}

		resultNodes = append(resultNodes, node)
	}

	return resultNodes
}

func getNodesByResources(nodes []*nodeHandler, desiredResources []string) []*nodeHandler {
	if len(desiredResources) == 0 {
		return nodes
	}

	resultNodes := make([]*nodeHandler, 0)

nodeLoop:
	for _, node := range nodes {
		if len(node.nodeConfig.Resources) == 0 {
			continue
		}

		for _, resource := range desiredResources {
			if !slices.ContainsFunc(node.nodeConfig.Resources, func(info cloudprotocol.ResourceInfo) bool {
				return info.Name == resource
			}) {
				continue nodeLoop
			}
		}

		resultNodes = append(resultNodes, node)
	}

	return resultNodes
}

func getNodesByLabels(nodes []*nodeHandler, desiredLabels []string) []*nodeHandler {
	if len(desiredLabels) == 0 {
		return nodes
	}

	resultNodes := make([]*nodeHandler, 0)

nodeLoop:
	for _, node := range nodes {
		if len(node.nodeConfig.Labels) == 0 {
			continue
		}

		for _, label := range desiredLabels {
			if !slices.Contains(node.nodeConfig.Labels, label) {
				continue nodeLoop
			}
		}

		resultNodes = append(resultNodes, node)
	}

	return resultNodes
}

func getNodeByRunners(nodes []*nodeHandler, runners []string) []*nodeHandler {
	if len(runners) == 0 {
		runners = defaultRunners
	}

	resultNodes := make([]*nodeHandler, 0)

	for _, runner := range runners {
		for _, node := range nodes {
			nodeRunners, err := node.nodeInfo.GetNodeRunners()
			if err != nil {
				log.WithField("nodeID", node.nodeInfo.NodeID).Errorf("Can't get node runners: %v", err)

				continue
			}

			if (len(nodeRunners) == 0 && slices.Contains(defaultRunners, runner)) ||
				slices.Contains(nodeRunners, runner) {
				resultNodes = append(resultNodes, node)
			}
		}
	}

	return resultNodes
}

func getNodesByCPU(nodes []*nodeHandler, serviceConfig aostypes.ServiceConfig) []*nodeHandler {
	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if node.availableCPU >= node.getRequestedCPU(serviceConfig) {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
}

func getNodesByRAM(nodes []*nodeHandler, serviceConfig aostypes.ServiceConfig) []*nodeHandler {
	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if node.availableRAM >= node.getRequestedRAM(serviceConfig) {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
}

func getTopPriorityNodes(nodes []*nodeHandler) []*nodeHandler {
	if len(nodes) == 0 {
		return nodes
	}

	topPriority := nodes[0].nodeConfig.Priority

	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if node.nodeConfig.Priority == topPriority {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
}

func getInstanceNode(nodes []*nodeHandler, serviceConfig aostypes.ServiceConfig) (*nodeHandler, error) {
	resultNodes := getNodesByDevices(nodes, serviceConfig.Devices)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with devices %v", serviceConfig.Devices)
	}

	resultNodes = getNodesByCPU(resultNodes, serviceConfig)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with available CPU")
	}

	resultNodes = getNodesByRAM(resultNodes, serviceConfig)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with available RAM")
	}

	resultNodes = getTopPriorityNodes(resultNodes)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("can't get top priority nodes")
	}

	slices.SortStableFunc(resultNodes, func(node1, node2 *nodeHandler) int {
		if node1.availableCPU < node2.availableCPU {
			return 1
		}

		if node1.availableCPU > node2.availableCPU {
			return -1
		}

		return 0
	})

	return resultNodes[0], nil
}
