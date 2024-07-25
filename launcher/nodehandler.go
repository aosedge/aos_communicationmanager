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
	"reflect"
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

func (node *nodeHandler) addRunRequest(
	instance aostypes.InstanceInfo, service imagemanager.ServiceInfo, layers []imagemanager.LayerInfo,
) {
	log.WithFields(instanceIdentLogFields(
		instance.InstanceIdent, log.Fields{"node": node.nodeInfo.NodeID})).Debug("Schedule instance on node")

	node.runRequest.Instances = append(node.runRequest.Instances, instance)

	serviceInfo := service.ServiceInfo

	if !node.isLocalNode {
		serviceInfo.URL = service.RemoteURL
	}

	isNewService := true

	for _, oldService := range node.runRequest.Services {
		if reflect.DeepEqual(oldService, serviceInfo) {
			isNewService = false
			break
		}
	}

	if isNewService {
		log.WithFields(log.Fields{
			"serviceID": serviceInfo.ServiceID, "node": node.nodeInfo.NodeID,
		}).Debug("Schedule service on node")

		node.runRequest.Services = append(node.runRequest.Services, serviceInfo)
	}

layerLoopLabel:
	for _, layer := range layers {
		newLayer := layer.LayerInfo

		if !node.isLocalNode {
			newLayer.URL = layer.RemoteURL
		}

		for _, oldLayer := range node.runRequest.Layers {
			if reflect.DeepEqual(newLayer, oldLayer) {
				continue layerLoopLabel
			}
		}

		log.WithFields(log.Fields{
			"digest": newLayer.Digest, "node": node.nodeInfo.NodeID,
		}).Debug("Schedule layer on node")

		node.runRequest.Layers = append(node.runRequest.Layers, newLayer)
	}
}

func getNodesByStaticResources(allNodes []*nodeHandler,
	serviceInfo imagemanager.ServiceInfo, instanceInfo cloudprotocol.InstanceInfo,
) ([]*nodeHandler, error) {
	nodes := getNodeByRunners(allNodes, serviceInfo.Config.Runners)
	if len(nodes) == 0 {
		return nodes, aoserrors.Errorf("no node with runner: %s", serviceInfo.Config.Runners)
	}

	nodes = getNodesByLabels(nodes, instanceInfo.Labels)
	if len(nodes) == 0 {
		return nodes, aoserrors.Errorf("no node with labels %v", instanceInfo.Labels)
	}

	nodes = getNodesByResources(nodes, serviceInfo.Config.Resources)
	if len(nodes) == 0 {
		return nodes, aoserrors.Errorf("no node with resources %v", serviceInfo.Config.Resources)
	}

	return nodes, nil
}

func getNodesByDevices(availableNodes []*nodeHandler, desiredDevices []aostypes.ServiceDevice) ([]*nodeHandler, error) {
	if len(desiredDevices) == 0 {
		return availableNodes, nil
	}

	nodes := make([]*nodeHandler, 0)

	for _, node := range availableNodes {
		if !node.nodeHasDesiredDevices(desiredDevices) {
			continue
		}

		nodes = append(nodes, node)
	}

	if len(nodes) == 0 {
		return nodes, aoserrors.New("no available device found")
	}

	return nodes, nil
}

func getNodesByResources(nodes []*nodeHandler, desiredResources []string) (newNodes []*nodeHandler) {
	if len(desiredResources) == 0 {
		return nodes
	}

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

		newNodes = append(newNodes, node)
	}

	return newNodes
}

func getNodesByLabels(nodes []*nodeHandler, desiredLabels []string) (newNodes []*nodeHandler) {
	if len(desiredLabels) == 0 {
		return nodes
	}

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

		newNodes = append(newNodes, node)
	}

	return newNodes
}

func getNodeByRunners(allNodes []*nodeHandler, runners []string) (nodes []*nodeHandler) {
	if len(runners) == 0 {
		runners = defaultRunners
	}

	for _, runner := range runners {
		for _, node := range allNodes {
			nodeRunners, err := node.nodeInfo.GetNodeRunners()
			if err != nil {
				log.WithField("nodeID", node.nodeInfo.NodeID).Errorf("Can't get node runners: %v", err)

				continue
			}

			if (len(nodeRunners) == 0 && slices.Contains(defaultRunners, runner)) ||
				slices.Contains(nodeRunners, runner) {
				nodes = append(nodes, node)
			}
		}
	}

	return nodes
}

func getInstanceNode(service imagemanager.ServiceInfo, nodes []*nodeHandler) (*nodeHandler, error) {
	nodes, err := getNodesByDevices(nodes, service.Config.Devices)
	if err != nil {
		return nil, err
	}

	return nodes[0], nil
}
