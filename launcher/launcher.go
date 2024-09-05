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
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

var ErrNotExist = errors.New("entry not exist")

const defaultResourceRation = 50.0

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// NodeRunInstanceStatus instance run status for the node.
type NodeRunInstanceStatus struct {
	NodeID    string
	NodeType  string
	Instances []cloudprotocol.InstanceStatus
}

// Launcher service instances launcher.
type Launcher struct {
	sync.Mutex

	config           *config.Config
	nodeInfoProvider NodeInfoProvider
	nodeManager      NodeManager
	imageProvider    ImageProvider
	resourceManager  ResourceManager
	networkManager   NetworkManager

	runStatusChannel chan []cloudprotocol.InstanceStatus
	nodes            map[string]*nodeHandler

	cancelFunc      context.CancelFunc
	connectionTimer *time.Timer

	instanceManager *instanceManager
}

// NetworkManager network manager interface.
type NetworkManager interface {
	PrepareInstanceNetworkParameters(
		instanceIdent aostypes.InstanceIdent, networkID string,
		params networkmanager.NetworkParameters) (aostypes.NetworkParameters, error)
	RemoveInstanceNetworkParameters(instanceIdent aostypes.InstanceIdent, networkID string)
	RestartDNSServer() error
	GetInstances() []aostypes.InstanceIdent
	UpdateProviderNetwork(providers []string, nodeID string) error
}

// ImageProvider provides image information.
type ImageProvider interface {
	GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error)
	GetLayerInfo(digest string) (imagemanager.LayerInfo, error)
	GetRemoveServiceChannel() (channel <-chan string)
}

// NodeManager nodes controller.
type NodeManager interface {
	RunInstances(
		nodeID string, services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo,
		forceRestart bool,
	) error
	GetRunInstancesStatusChannel() <-chan NodeRunInstanceStatus
	GetAverageMonitoring(nodeID string) (aostypes.NodeMonitoring, error)
}

// NodeInfoProvider node information.
type NodeInfoProvider interface {
	GetNodeID() string
	GetNodeInfo(nodeID string) (cloudprotocol.NodeInfo, error)
	GetAllNodeIDs() (nodeIDs []string, err error)
}

// ResourceManager provides node resources.
type ResourceManager interface {
	GetNodeConfig(nodeID, nodeType string) (cloudprotocol.NodeConfig, error)
}

// StorageStateProvider instances storage state provider.
type StorageStateProvider interface {
	Setup(params storagestate.SetupParams) (storagePath string, statePath string, err error)
	Cleanup(instanceIdent aostypes.InstanceIdent) error
	RemoveServiceInstance(instanceIdent aostypes.InstanceIdent) error
	GetInstanceCheckSum(instance aostypes.InstanceIdent) string
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates launcher instance.
func New(
	config *config.Config, storage Storage, nodeInfoProvider NodeInfoProvider, nodeManager NodeManager,
	imageProvider ImageProvider, resourceManager ResourceManager, storageStateProvider StorageStateProvider,
	networkManager NetworkManager,
) (launcher *Launcher, err error) {
	log.Debug("Create launcher")

	launcher = &Launcher{
		config: config, nodeInfoProvider: nodeInfoProvider, nodeManager: nodeManager, imageProvider: imageProvider,
		resourceManager: resourceManager, networkManager: networkManager,
		runStatusChannel: make(chan []cloudprotocol.InstanceStatus, 10),
	}

	if launcher.instanceManager, err = newInstanceManager(config, imageProvider, storageStateProvider, storage,
		launcher.imageProvider.GetRemoveServiceChannel()); err != nil {
		return nil, err
	}

	if err := launcher.initNodes(false); err != nil {
		return nil, err
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	launcher.cancelFunc = cancelFunction
	launcher.connectionTimer = time.AfterFunc(
		config.SMController.NodesConnectionTimeout.Duration, launcher.sendCurrentStatus)

	go launcher.processChannels(ctx)

	return launcher, nil
}

// Close closes launcher.
func (launcher *Launcher) Close() {
	log.Debug("Close launcher")

	if launcher.cancelFunc != nil {
		launcher.cancelFunc()
	}

	launcher.instanceManager.close()
}

// RunInstances performs run service instances.
func (launcher *Launcher) RunInstances(instances []cloudprotocol.InstanceInfo, rebalancing bool) error {
	launcher.Lock()
	defer launcher.Unlock()

	log.WithField("rebalancing", rebalancing).Debug("Run instances")

	sort.Slice(instances, func(i, j int) bool {
		if instances[i].Priority == instances[j].Priority {
			return instances[i].ServiceID < instances[j].ServiceID
		}

		return instances[i].Priority > instances[j].Priority
	})

	launcher.prepareBalancing(rebalancing)

	if err := launcher.processRemovedInstances(instances); err != nil {
		log.Errorf("Can't process removed instances: %v", err)
	}

	if err := launcher.updateNetworks(instances); err != nil {
		log.Errorf("Can't update networks: %v", err)
	}

	if rebalancing {
		launcher.performPolicyBalancing(instances)
	}

	launcher.performNodeBalancing(instances, rebalancing)

	// first prepare network for instance which have exposed ports
	launcher.prepareNetworkForInstances(true)

	// then prepare network for rest of instances
	launcher.prepareNetworkForInstances(false)

	if err := launcher.networkManager.RestartDNSServer(); err != nil {
		log.Errorf("Can't restart DNS server: %v", err)
	}

	return launcher.sendRunInstances(false)
}

// GetRunStatusesChannel gets channel with run status instances status.
func (launcher *Launcher) GetRunStatusesChannel() <-chan []cloudprotocol.InstanceStatus {
	return launcher.runStatusChannel
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (launcher *Launcher) prepareBalancing(rebalancing bool) {
	if err := launcher.initNodes(rebalancing); err != nil {
		log.Errorf("Can't init nodes: %v", err)
	}

	launcher.instanceManager.initInstances()

	localNode := launcher.getLocalNode()

	if localNode != nil {
		launcher.instanceManager.resetStorageStateUsage(
			localNode.getPartitionSize(aostypes.StoragesPartition),
			localNode.getPartitionSize(aostypes.StatesPartition))
	} else {
		log.Errorf("Local node not found")
	}
}

func (launcher *Launcher) initNodes(rebalancing bool) error {
	launcher.nodes = make(map[string]*nodeHandler)

	nodes, err := launcher.nodeInfoProvider.GetAllNodeIDs()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, nodeID := range nodes {
		nodeInfo, err := launcher.nodeInfoProvider.GetNodeInfo(nodeID)
		if err != nil {
			log.WithField("nodeID", nodeID).Errorf("Can't get node info: %v", err)

			continue
		}

		if nodeInfo.Status == cloudprotocol.NodeStatusUnprovisioned {
			log.WithField("nodeID", nodeID).Debug("Skip not provisioned node")

			continue
		}

		nodeHandler, err := newNodeHandler(
			nodeInfo, launcher.nodeManager, launcher.resourceManager,
			nodeInfo.NodeID == launcher.nodeInfoProvider.GetNodeID(), rebalancing)
		if err != nil {
			log.WithField("nodeID", nodeID).Errorf("Can't create node handler: %v", err)

			continue
		}

		launcher.nodes[nodeID] = nodeHandler
	}

	return nil
}

func (launcher *Launcher) processChannels(ctx context.Context) {
	for {
		select {
		case instances := <-launcher.nodeManager.GetRunInstancesStatusChannel():
			launcher.processRunInstanceStatus(instances)

		case <-ctx.Done():
			return
		}
	}
}

func (launcher *Launcher) sendRunInstances(forceRestart bool) (err error) {
	launcher.connectionTimer = time.AfterFunc(
		launcher.config.SMController.NodesConnectionTimeout.Duration, launcher.sendCurrentStatus)

	for _, node := range launcher.getNodesByPriorities() {
		node.waitStatus = true

		if runErr := launcher.nodeManager.RunInstances(
			node.nodeInfo.NodeID, node.runRequest.Services, node.runRequest.Layers,
			node.runRequest.Instances, forceRestart); runErr != nil {
			log.WithField("nodeID", node.nodeInfo.NodeID).Errorf("Can't run instances: %v", runErr)

			if err == nil {
				err = runErr
			}
		}
	}

	return err
}

func (launcher *Launcher) processRunInstanceStatus(runStatus NodeRunInstanceStatus) {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debugf("Received run status from nodeID: %s", runStatus.NodeID)

	currentStatus := launcher.getNode(runStatus.NodeID)
	if currentStatus == nil {
		log.Errorf("Received status for unknown nodeID  %s", runStatus.NodeID)

		return
	}

	currentStatus.runStatus = runStatus.Instances
	currentStatus.waitStatus = false

	for _, node := range launcher.nodes {
		if node.waitStatus {
			return
		}
	}

	log.Info("All SM statuses received")

	launcher.connectionTimer.Stop()

	launcher.sendCurrentStatus()
}

func (launcher *Launcher) sendCurrentStatus() {
	instancesStatus := make([]cloudprotocol.InstanceStatus, 0)

	for _, node := range launcher.getNodesByPriorities() {
		if node.waitStatus {
			node.waitStatus = false

			for _, errInstance := range node.runRequest.Instances {
				instancesStatus = append(instancesStatus, cloudprotocol.InstanceStatus{
					InstanceIdent: errInstance.InstanceIdent,
					NodeID:        node.nodeInfo.NodeID, Status: cloudprotocol.InstanceStateFailed,
					ErrorInfo: &cloudprotocol.ErrorInfo{Message: "wait run status timeout"},
				})
			}

			continue
		}

		instancesStatus = append(instancesStatus, node.runStatus...)
	}

	for i := range instancesStatus {
		instancesStatus[i].StateChecksum = launcher.instanceManager.getInstanceCheckSum(
			instancesStatus[i].InstanceIdent)
	}

	instancesStatus = append(instancesStatus, launcher.instanceManager.getErrorInstanceStatuses()...)
	launcher.runStatusChannel <- instancesStatus
}

func (launcher *Launcher) processRemovedInstances(instances []cloudprotocol.InstanceInfo) error {
	launcher.removeInstanceNetworkParameters(instances)

	for _, curInstance := range launcher.instanceManager.getCurrentInstances() {
		if !slices.ContainsFunc(instances, func(info cloudprotocol.InstanceInfo) bool {
			return curInstance.ServiceID == info.ServiceID && curInstance.SubjectID == info.SubjectID &&
				curInstance.Instance < info.NumInstances
		}) {
			if err := launcher.instanceManager.cacheInstance(curInstance); err != nil {
				log.WithFields(instanceIdentLogFields(curInstance.InstanceIdent, nil)).Errorf(
					"Can't cache instance: %v", err)
			}
		}
	}

	return nil
}

func (launcher *Launcher) updateNetworks(instances []cloudprotocol.InstanceInfo) error {
	providers := make([]string, len(instances))

	for i, instance := range instances {
		serviceInfo, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		providers[i] = serviceInfo.ProviderID
	}

	for _, node := range launcher.nodes {
		if err := launcher.networkManager.UpdateProviderNetwork(providers, node.nodeInfo.NodeID); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (launcher *Launcher) performPolicyBalancing(instances []cloudprotocol.InstanceInfo) {
	for _, instance := range instances {
		log.WithFields(log.Fields{
			"serviceID":    instance.ServiceID,
			"subjectID":    instance.SubjectID,
			"numInstances": instance.NumInstances,
			"priority":     instance.Priority,
		}).Debug("Check balancing policy")

		service, layers, err := launcher.getServiceLayers(instance)
		if err != nil {
			launcher.instanceManager.setAllInstanceError(instance, service.Version, err)

			continue
		}

		if service.Config.BalancingPolicy != aostypes.BalancingDisabled {
			continue
		}

		for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
			curInstance, err := launcher.instanceManager.getCurrentInstance(
				createInstanceIdent(instance, instanceIndex))
			if err != nil {
				launcher.instanceManager.setInstanceError(
					createInstanceIdent(instance, instanceIndex),
					service.Version, err)

				continue
			}

			node := launcher.getNode(curInstance.NodeID)
			if node == nil {
				launcher.instanceManager.setInstanceError(
					createInstanceIdent(instance, instanceIndex),
					service.Version, aoserrors.Errorf("node not found"))

				continue
			}

			instanceInfo, err := launcher.instanceManager.setupInstance(
				instance, instanceIndex, node, service, true)
			if err != nil {
				launcher.instanceManager.setInstanceError(
					createInstanceIdent(instance, instanceIndex), service.Version, err)

				continue
			}

			if err = node.addRunRequest(instanceInfo, service, layers); err != nil {
				launcher.instanceManager.setInstanceError(
					createInstanceIdent(instance, instanceIndex), service.Version, err)

				continue
			}
		}
	}
}

func (launcher *Launcher) performNodeBalancing(instances []cloudprotocol.InstanceInfo, rebalancing bool) {
	for _, instance := range instances {
		log.WithFields(log.Fields{
			"serviceID":    instance.ServiceID,
			"subjectID":    instance.SubjectID,
			"numInstances": instance.NumInstances,
			"priority":     instance.Priority,
		}).Debug("Balance instances")

		service, layers, err := launcher.getServiceLayers(instance)
		if err != nil {
			launcher.instanceManager.setAllInstanceError(instance, service.Version, err)
			continue
		}

		nodes, err := getNodesByStaticResources(launcher.getNodesByPriorities(), service.Config, instance)
		if err != nil {
			launcher.instanceManager.setAllInstanceError(instance, service.Version, err)
			continue
		}

		for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
			instanceIdent := createInstanceIdent(instance, instanceIndex)

			if launcher.instanceManager.isInstanceScheduled(instanceIdent) {
				continue
			}

			if rebalancing {
				curInstance, err := launcher.instanceManager.getCurrentInstance(instanceIdent)
				if err != nil {
					launcher.instanceManager.setInstanceError(instanceIdent, service.Version, err)
					continue
				}

				if curInstance.PrevNodeID != "" && curInstance.PrevNodeID != curInstance.NodeID {
					nodes = excludeNodes(nodes, []string{curInstance.PrevNodeID})
					if len(nodes) == 0 {
						launcher.instanceManager.setInstanceError(instanceIdent, service.Version,
							aoserrors.Errorf("can't find node for rebalancing"))
						continue
					}
				}
			}

			node, err := getInstanceNode(nodes, instanceIdent, service.Config)
			if err != nil {
				launcher.instanceManager.setInstanceError(instanceIdent, service.Version, err)
				continue
			}

			instanceInfo, err := launcher.instanceManager.setupInstance(
				instance, instanceIndex, node, service, rebalancing)
			if err != nil {
				launcher.instanceManager.setInstanceError(instanceIdent, service.Version, err)
				continue
			}

			if err = node.addRunRequest(instanceInfo, service, layers); err != nil {
				launcher.instanceManager.setInstanceError(instanceIdent, service.Version, err)
				continue
			}
		}
	}
}

func (launcher *Launcher) getServiceLayers(instance cloudprotocol.InstanceInfo) (
	imagemanager.ServiceInfo, []imagemanager.LayerInfo, error,
) {
	service, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
	if err != nil {
		return service, nil, aoserrors.Wrap(err)
	}

	if service.Cached {
		return service, nil, aoserrors.New("service deleted")
	}

	layers, err := launcher.getLayersForService(service.Layers)
	if err != nil {
		return service, layers, err
	}

	return service, layers, nil
}

func (launcher *Launcher) prepareNetworkForInstances(onlyExposedPorts bool) {
	for _, node := range launcher.getNodesByPriorities() {
		for i, instance := range node.runRequest.Instances {
			serviceVersion := ""

			if err := func() error {
				serviceInfo, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
				if err != nil {
					return aoserrors.Wrap(err)
				}

				serviceVersion = serviceInfo.Version

				if onlyExposedPorts && len(serviceInfo.ExposedPorts) == 0 {
					return nil
				}

				if instance.NetworkParameters, err = launcher.networkManager.PrepareInstanceNetworkParameters(
					instance.InstanceIdent, serviceInfo.ProviderID,
					prepareNetworkParameters(serviceInfo)); err != nil {
					return aoserrors.Wrap(err)
				}

				node.runRequest.Instances[i] = instance

				return nil
			}(); err != nil {
				launcher.instanceManager.setInstanceError(instance.InstanceIdent, serviceVersion, err)
			}
		}
	}
}

func prepareNetworkParameters(serviceInfo imagemanager.ServiceInfo) networkmanager.NetworkParameters {
	var hosts []string

	if serviceInfo.Config.Hostname != nil {
		hosts = append(hosts, *serviceInfo.Config.Hostname)
	}

	params := networkmanager.NetworkParameters{
		Hosts:       hosts,
		ExposePorts: serviceInfo.ExposedPorts,
	}

	params.AllowConnections = make([]string, 0, len(serviceInfo.Config.AllowedConnections))

	for key := range serviceInfo.Config.AllowedConnections {
		params.AllowConnections = append(params.AllowConnections, key)
	}

	return params
}

func (launcher *Launcher) removeInstanceNetworkParameters(instances []cloudprotocol.InstanceInfo) {
	networkInstances := launcher.networkManager.GetInstances()

nextNetInstance:
	for _, netInstance := range networkInstances {
		for _, instance := range instances {
			for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
				instanceIdent := aostypes.InstanceIdent{
					ServiceID: instance.ServiceID, SubjectID: instance.SubjectID,
					Instance: instanceIndex,
				}

				if netInstance == instanceIdent {
					continue nextNetInstance
				}
			}
		}

		serviceInfo, err := launcher.imageProvider.GetServiceInfo(netInstance.ServiceID)
		if err != nil {
			log.WithField("serviceID", netInstance.ServiceID).Errorf("Can't get service info: %v", err)
			continue
		}

		launcher.networkManager.RemoveInstanceNetworkParameters(netInstance, serviceInfo.ProviderID)
	}
}

func (launcher *Launcher) getNodesByPriorities() []*nodeHandler {
	nodes := maps.Values(launcher.nodes)

	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].nodeConfig.Priority == nodes[j].nodeConfig.Priority {
			return nodes[i].nodeInfo.NodeID < nodes[j].nodeInfo.NodeID
		}

		return nodes[i].nodeConfig.Priority > nodes[j].nodeConfig.Priority
	})

	return nodes
}

func (launcher *Launcher) getNode(nodeID string) *nodeHandler {
	node, ok := launcher.nodes[nodeID]
	if !ok {
		return nil
	}

	return node
}

func (launcher *Launcher) getLocalNode() *nodeHandler {
	for _, node := range launcher.nodes {
		if node.isLocalNode {
			return node
		}
	}

	return nil
}

func (launcher *Launcher) getLayersForService(digests []string) ([]imagemanager.LayerInfo, error) {
	layers := make([]imagemanager.LayerInfo, len(digests))

	for i, digest := range digests {
		layer, err := launcher.imageProvider.GetLayerInfo(digest)
		if err != nil {
			return layers, aoserrors.Wrap(err)
		}

		layers[i] = layer
	}

	return layers, nil
}

func getReqDiskSize(serviceConfig aostypes.ServiceConfig, nodeRatios *aostypes.ResourceRatiosInfo,
) (stateSize, storageSize uint64) {
	stateQuota := serviceConfig.Quotas.StateLimit
	storageQuota := serviceConfig.Quotas.StorageLimit

	if serviceConfig.RequestedResources != nil && serviceConfig.RequestedResources.State != nil {
		stateSize = clampResource(*serviceConfig.RequestedResources.State, stateQuota)
	} else {
		stateSize = getReqStateFromNodeConf(stateQuota, nodeRatios)
	}

	if serviceConfig.RequestedResources != nil && serviceConfig.RequestedResources.Storage != nil {
		storageSize = clampResource(*serviceConfig.RequestedResources.Storage, storageQuota)
	} else {
		storageSize = getReqStorageFromNodeConf(storageQuota, nodeRatios)
	}

	return stateSize, storageSize
}

func getReqCPUFromNodeConf(cpuQuota *uint64, nodeRatios *aostypes.ResourceRatiosInfo) uint64 {
	ratio := defaultResourceRation / 100.0

	if nodeRatios != nil && nodeRatios.CPU != nil {
		ratio = float64(*nodeRatios.CPU) / 100.0
	}

	if ratio > 1.0 {
		ratio = 1.0
	}

	if cpuQuota != nil {
		return uint64(float64(*cpuQuota)*ratio + 0.5)
	}

	return 0
}

func getReqRAMFromNodeConf(ramQuota *uint64, nodeRatios *aostypes.ResourceRatiosInfo) uint64 {
	ratio := defaultResourceRation / 100.0

	if nodeRatios != nil && nodeRatios.RAM != nil {
		ratio = float64(*nodeRatios.RAM) / 100.0
	}

	if ratio > 1.0 {
		ratio = 1.0
	}

	if ramQuota != nil {
		return uint64(float64(*ramQuota)*ratio + 0.5)
	}

	return 0
}

func getReqStorageFromNodeConf(storageQuota *uint64, nodeRatios *aostypes.ResourceRatiosInfo) uint64 {
	ratio := defaultResourceRation / 100.0

	if nodeRatios != nil && nodeRatios.Storage != nil {
		ratio = float64(*nodeRatios.Storage) / 100.0
	}

	if ratio > 1.0 {
		ratio = 1.0
	}

	if storageQuota != nil {
		return uint64(float64(*storageQuota)*ratio + 0.5)
	}

	return 0
}

func getReqStateFromNodeConf(stateQuota *uint64, nodeRatios *aostypes.ResourceRatiosInfo) uint64 {
	ratio := defaultResourceRation / 100.0

	if nodeRatios != nil && nodeRatios.State != nil {
		ratio = float64(*nodeRatios.State) / 100.0
	}

	if ratio > 1.0 {
		ratio = 1.0
	}

	if stateQuota != nil {
		return uint64(float64(*stateQuota)*ratio + 0.5)
	}

	return 0
}

func clampResource(value uint64, quota *uint64) uint64 {
	if quota != nil && value > *quota {
		return *quota
	}

	return value
}

func instanceIdentLogFields(instance aostypes.InstanceIdent, extraFields log.Fields) log.Fields {
	logFields := log.Fields{
		"serviceID": instance.ServiceID,
		"subjectID": instance.SubjectID,
		"instance":  instance.Instance,
	}

	for k, v := range extraFields {
		logFields[k] = v
	}

	return logFields
}
