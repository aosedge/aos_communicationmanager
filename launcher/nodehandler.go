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
	"math"

	"golang.org/x/exp/slices"

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
	averageMonitoring aostypes.NodeMonitoring
	needRebalancing   bool
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
	nodeInfo cloudprotocol.NodeInfo, nodeManager NodeManager, resourceManager ResourceManager,
	isLocalNode bool, rebalancing bool,
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

	node.initAvailableResources(nodeManager, rebalancing)

	return node, nil
}

func (node *nodeHandler) initAvailableResources(nodeManager NodeManager, rebalancing bool) {
	var err error

	node.averageMonitoring = aostypes.NodeMonitoring{}

	if rebalancing && node.nodeConfig.AlertRules != nil &&
		(node.nodeConfig.AlertRules.CPU != nil || node.nodeConfig.AlertRules.RAM != nil) {
		node.averageMonitoring, err = nodeManager.GetAverageMonitoring(node.nodeInfo.NodeID)
		if err != nil {
			log.WithField("nodeID", node.nodeInfo.NodeID).Errorf("Can't get average monitoring: %v", err)
		}

		if (node.nodeConfig.AlertRules.CPU != nil &&
			node.averageMonitoring.NodeData.CPU >
				uint64(math.Round(float64(node.nodeInfo.MaxDMIPs)*
					node.nodeConfig.AlertRules.CPU.MaxThreshold/100.0))) ||
			(node.nodeConfig.AlertRules.RAM != nil &&
				node.averageMonitoring.NodeData.RAM >
					uint64(math.Round(float64(node.nodeInfo.TotalRAM)*
						node.nodeConfig.AlertRules.RAM.MaxThreshold/100.0))) {
			node.needRebalancing = true
		}
	}

	nodeCPU := node.getNodeCPU()
	nodeRAM := node.getNodeRAM()
	totalCPU := node.nodeInfo.MaxDMIPs
	totalRAM := node.nodeInfo.TotalRAM

	// For nodes required rebalancing, we need to decrease resource consumption below the low threshold
	if node.needRebalancing {
		if node.nodeConfig.AlertRules.CPU != nil {
			totalCPU = uint64(math.Round(float64(node.nodeInfo.MaxDMIPs) *
				node.nodeConfig.AlertRules.CPU.MinThreshold / 100.0))
		}

		if node.nodeConfig.AlertRules.RAM != nil {
			totalRAM = uint64(math.Round(float64(node.nodeInfo.TotalRAM) *
				node.nodeConfig.AlertRules.RAM.MinThreshold / 100.0))
		}
	}

	if nodeCPU > totalCPU {
		node.availableCPU = 0
	} else {
		node.availableCPU = totalCPU - nodeCPU
	}

	if nodeRAM > totalRAM {
		node.availableRAM = 0
	} else {
		node.availableRAM = totalRAM - nodeRAM
	}

	if node.needRebalancing {
		log.WithFields(log.Fields{
			"nodeID": node.nodeInfo.NodeID, "RAM": nodeRAM, "CPU": nodeCPU,
		}).Debug("Node resources usage")
	}

	log.WithFields(log.Fields{
		"nodeID": node.nodeInfo.NodeID, "RAM": node.availableRAM, "CPU": node.availableCPU,
	}).Debug("Available resources on node")
}

func (node *nodeHandler) getNodeCPU() uint64 {
	instancesCPU := uint64(0)

	for _, instance := range node.averageMonitoring.InstancesData {
		instancesCPU += instance.CPU
	}

	if instancesCPU > node.averageMonitoring.NodeData.CPU {
		return 0
	}

	return node.averageMonitoring.NodeData.CPU - instancesCPU
}

func (node *nodeHandler) getNodeRAM() uint64 {
	instancesRAM := uint64(0)

	for _, instance := range node.averageMonitoring.InstancesData {
		instancesRAM += instance.RAM
	}

	if instancesRAM > node.averageMonitoring.NodeData.RAM {
		return 0
	}

	return node.averageMonitoring.NodeData.RAM - instancesRAM
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

	requestedCPU := node.getRequestedCPU(instanceInfo.InstanceIdent, service.Config)
	if requestedCPU > node.availableCPU && !service.Config.SkipResourceLimits {
		return aoserrors.Errorf("not enough CPU")
	}

	requestedRAM := node.getRequestedRAM(instanceInfo.InstanceIdent, service.Config)
	if requestedRAM > node.availableRAM && !service.Config.SkipResourceLimits {
		return aoserrors.Errorf("not enough RAM")
	}

	if !service.Config.SkipResourceLimits {
		node.availableCPU -= requestedCPU
		node.availableRAM -= requestedRAM
	}

	node.runRequest.Instances = append(node.runRequest.Instances, instanceInfo)
	node.addService(service)
	node.addLayers(layers)

	log.WithFields(log.Fields{
		"nodeID": node.nodeInfo.NodeID, "RAM": node.availableRAM, "CPU": node.availableCPU,
	}).Debug("Remaining resources on node")

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

func (node *nodeHandler) getRequestedCPU(
	instanceIdent aostypes.InstanceIdent, serviceConfig aostypes.ServiceConfig,
) uint64 {
	requestedCPU := uint64(0)
	cpuQuota := serviceConfig.Quotas.CPUDMIPSLimit

	if serviceConfig.RequestedResources != nil && serviceConfig.RequestedResources.CPU != nil {
		requestedCPU = clampResource(*serviceConfig.RequestedResources.CPU, cpuQuota)
	} else {
		requestedCPU = getReqCPUFromNodeConf(cpuQuota, node.nodeConfig.ResourceRatios)
	}

	if node.needRebalancing {
		index := slices.IndexFunc(node.averageMonitoring.InstancesData, func(instance aostypes.InstanceMonitoring) bool {
			return instance.InstanceIdent == instanceIdent
		})

		if index != -1 {
			if node.averageMonitoring.InstancesData[index].CPU > requestedCPU {
				return node.averageMonitoring.InstancesData[index].CPU
			}
		}
	}

	return requestedCPU
}

func (node *nodeHandler) getRequestedRAM(
	instanceIdent aostypes.InstanceIdent, serviceConfig aostypes.ServiceConfig,
) uint64 {
	requestedRAM := uint64(0)
	ramQuota := serviceConfig.Quotas.RAMLimit

	if serviceConfig.RequestedResources != nil && serviceConfig.RequestedResources.RAM != nil {
		requestedRAM = clampResource(*serviceConfig.RequestedResources.RAM, ramQuota)
	} else {
		requestedRAM = getReqRAMFromNodeConf(ramQuota, node.nodeConfig.ResourceRatios)
	}

	if node.needRebalancing {
		index := slices.IndexFunc(node.averageMonitoring.InstancesData, func(instance aostypes.InstanceMonitoring) bool {
			return instance.InstanceIdent == instanceIdent
		})

		if index != -1 {
			if node.averageMonitoring.InstancesData[index].RAM > requestedRAM {
				return node.averageMonitoring.InstancesData[index].RAM
			}
		}
	}

	return requestedRAM
}

func getNodesByStaticResources(nodes []*nodeHandler,
	serviceConfig aostypes.ServiceConfig, instanceInfo cloudprotocol.InstanceInfo,
) ([]*nodeHandler, error) {
	resultNodes := getActiveNodes(nodes)
	if len(resultNodes) == 0 {
		return resultNodes, aoserrors.Errorf("no active nodes")
	}

	resultNodes = getNodeByRunners(resultNodes, serviceConfig.Runners)
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

func getActiveNodes(nodes []*nodeHandler) []*nodeHandler {
	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if node.nodeInfo.Status == cloudprotocol.NodeStatusProvisioned {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
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

func getNodesByCPU(
	nodes []*nodeHandler, instanceIdent aostypes.InstanceIdent, serviceConfig aostypes.ServiceConfig,
) []*nodeHandler {
	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		requestedCPU := node.getRequestedCPU(instanceIdent, serviceConfig)

		log.WithFields(instanceIdentLogFields(instanceIdent, log.Fields{
			"CPU": requestedCPU, "nodeID": node.nodeInfo.NodeID,
		})).Debug("Instance CPU request")

		if node.availableCPU >= requestedCPU || serviceConfig.SkipResourceLimits {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
}

func getNodesByRAM(
	nodes []*nodeHandler, instanceIdent aostypes.InstanceIdent, serviceConfig aostypes.ServiceConfig,
) []*nodeHandler {
	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		requestedRAM := node.getRequestedRAM(instanceIdent, serviceConfig)

		log.WithFields(instanceIdentLogFields(instanceIdent, log.Fields{
			"RAM": requestedRAM, "nodeID": node.nodeInfo.NodeID,
		})).Debug("Instance RAM request")

		if node.availableRAM >= node.getRequestedRAM(instanceIdent, serviceConfig) || serviceConfig.SkipResourceLimits {
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

func excludeNodes(nodes []*nodeHandler, excludeNodes []string) []*nodeHandler {
	if len(excludeNodes) == 0 {
		return nodes
	}

	resultNodes := make([]*nodeHandler, 0)

	for _, node := range nodes {
		if !slices.Contains(excludeNodes, node.nodeInfo.NodeID) {
			resultNodes = append(resultNodes, node)
		}
	}

	return resultNodes
}

func getInstanceNode(
	nodes []*nodeHandler, instanceIdent aostypes.InstanceIdent, serviceConfig aostypes.ServiceConfig,
) (*nodeHandler, error) {
	resultNodes := getNodesByDevices(nodes, serviceConfig.Devices)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with devices %v", serviceConfig.Devices)
	}

	resultNodes = getNodesByCPU(resultNodes, instanceIdent, serviceConfig)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with available CPU")
	}

	resultNodes = getNodesByRAM(resultNodes, instanceIdent, serviceConfig)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("no nodes with available RAM")
	}

	resultNodes = getTopPriorityNodes(resultNodes)
	if len(resultNodes) == 0 {
		return nil, aoserrors.Errorf("can't get top priority nodes")
	}

	slices.SortStableFunc(resultNodes, func(node1, node2 *nodeHandler) bool {
		if node1.availableCPU < node2.availableCPU {
			return false
		}

		if node1.availableCPU > node2.availableCPU {
			return true
		}

		return false
	})

	return resultNodes[0], nil
}
