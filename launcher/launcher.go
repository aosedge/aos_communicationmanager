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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/slices"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/imagemanager"
	"github.com/aoscloud/aos_communicationmanager/storagestate"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
	"github.com/aoscloud/aos_communicationmanager/utils/uidgidpool"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

var ErrNotExist = errors.New("instance not exist")

const defaultRunner = "crun"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// NodeRunInstanceStatus instance run status for the node.
type NodeRunInstanceStatus struct {
	NodeID    string
	NodeType  string
	Instances []cloudprotocol.InstanceStatus
}

// NodeConfiguration node static configuration.
type NodeInfo struct {
	cloudprotocol.NodeInfo
	RemoteNode    bool
	RunnerFeature []string
}

// Launcher service instances launcher.
type Launcher struct {
	sync.Mutex

	config                  *config.Config
	storage                 Storage
	nodeManager             NodeManager
	imageProvider           ImageProvider
	resourceManager         ResourceManager
	storageStateProvider    StorageStateProvider
	networkManager          NetworkManager
	runStatusChannel        chan unitstatushandler.RunInstancesStatus
	nodes                   []*nodeStatus
	uidPool                 *uidgidpool.IdentifierPool
	currentDesiredInstances []cloudprotocol.InstanceInfo
	currentRunStatus        []cloudprotocol.InstanceStatus
	currentErrorStatus      []cloudprotocol.InstanceStatus
	pendingNewServices      []string

	cancelFunc      context.CancelFunc
	connectionTimer time.Timer
}

// Storage storage interface.
type Storage interface {
	AddInstance(instance aostypes.InstanceIdent, uid int) error
	GetInstanceUID(instance aostypes.InstanceIdent) (int, error)
	GetAllUIDs() ([]int, error)
	SetDesiredInstances(instances json.RawMessage) error
	GetDesiredInstances() (instances json.RawMessage, err error)
}

// NetworkManager network manager interface.
type NetworkManager interface {
	PrepareInstanceNetworkParameters(
		instanceIdent aostypes.InstanceIdent, networkID string) (aostypes.NetworkParameters, error)
	RemoveInstanceNetworkParameters(instanceIdent aostypes.InstanceIdent, networkID string)
	GetInstances() []aostypes.InstanceIdent
}

// ImageProvider provides image information.
type ImageProvider interface {
	GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error)
	GetLayerInfo(digest string) (imagemanager.LayerInfo, error)
	RevertService(serviceID string) error
}

// NodeManager nodes controller.
type NodeManager interface {
	GetNodeConfiguration(nodeID string) (NodeInfo, error)
	RunInstances(
		nodeID string, services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo,
		forceRestart bool,
	) error
	GetRunInstancesStatusChannel() <-chan NodeRunInstanceStatus
	GetSystemLimitAlertChannel() <-chan cloudprotocol.SystemQuotaAlert
	GetNodeMonitoringData(nodeID string) (data cloudprotocol.NodeMonitoringData, err error)
}

// ResourceManager provides node resources.
type ResourceManager interface {
	GetUnitConfiguration(nodeType string) aostypes.NodeUnitConfig
}

// StorageStateProvider instances storage state provider.
type StorageStateProvider interface {
	Setup(params storagestate.SetupParams) (storagePath string, statePath string, err error)
	Cleanup(instanceIdent aostypes.InstanceIdent) error
	GetInstanceCheckSum(instance aostypes.InstanceIdent) string
}

type nodeStatus struct {
	NodeInfo
	availableResources   []string
	availableLabels      []string
	availableDevices     []nodeDeviceResource
	priority             uint32
	receivedRunInstances []cloudprotocol.InstanceStatus
	currentRunRequest    *runRequestInfo
	waitStatus           bool
}

type nodeDeviceResource struct {
	name             string
	sharedCount      int
	allocatedCount   int
	preallocateCount int
}

type runRequestInfo struct {
	services  []aostypes.ServiceInfo
	layers    []aostypes.LayerInfo
	instances []aostypes.InstanceInfo
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates launcher instance.
func New(
	config *config.Config, storage Storage, nodeManager NodeManager, imageProvider ImageProvider,
	resourceManager ResourceManager, storageStateProvider StorageStateProvider, networkManager NetworkManager,
) (launcher *Launcher, err error) {
	launcher = &Launcher{
		config: config, storage: storage, nodeManager: nodeManager, imageProvider: imageProvider,
		resourceManager: resourceManager, storageStateProvider: storageStateProvider,
		networkManager:   networkManager,
		runStatusChannel: make(chan unitstatushandler.RunInstancesStatus, 10),
		nodes:            []*nodeStatus{},
		uidPool:          uidgidpool.NewUserIDPool(),
	}

	launcher.fillUIDPool()

	if rawDesiredInstances, err := launcher.storage.GetDesiredInstances(); err != nil {
		log.Errorf("Can't get desired instances: %v", err)
	} else {
		if err = json.Unmarshal(rawDesiredInstances, &launcher.currentDesiredInstances); err != nil {
			log.Debugf("Can't parse desire instances: %v", err)
		}
	}

	ctx, cancelFunction := context.WithCancel(context.Background())

	launcher.cancelFunc = cancelFunction
	launcher.connectionTimer = *time.AfterFunc(
		config.SMController.NodesConnectionTimeout.Duration, launcher.sendCurrentStatus)

	go launcher.processChannels(ctx)

	return launcher, nil
}

// Close closes launcher.
func (launcher *Launcher) Close() {
	if launcher.cancelFunc != nil {
		launcher.cancelFunc()
	}
}

// RunInstances performs run service instances.
func (launcher *Launcher) RunInstances(instances []cloudprotocol.InstanceInfo, newServices []string) error {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debug("Run instances")

	launcher.connectionTimer.Reset(launcher.config.SMController.NodesConnectionTimeout.Duration)

	launcher.resetDeviceAllocation()

	if rawDesiredInstances, err := json.Marshal(instances); err != nil {
		log.Errorf("Can't marshall desired instances: %v", err)
	} else {
		if err := launcher.storage.SetDesiredInstances(rawDesiredInstances); err != nil {
			log.Errorf("Can't store desired instances: %v", err)
		}
	}

	launcher.currentDesiredInstances = instances
	launcher.pendingNewServices = newServices
	launcher.currentErrorStatus = launcher.performNodeBalancing(instances)

	return launcher.sendRunInstances(false)
}

// RestartInstances performs restart service instances.
func (launcher *Launcher) RestartInstances() error {
	launcher.Lock()
	defer launcher.Unlock()

	launcher.connectionTimer.Reset(launcher.config.SMController.NodesConnectionTimeout.Duration)

	for _, node := range launcher.nodes {
		launcher.initNodeUnitConfiguration(node, node.NodeType)
	}

	launcher.currentErrorStatus = launcher.performNodeBalancing(launcher.currentDesiredInstances)

	return launcher.sendRunInstances(true)
}

// GetRunStatusesChannel gets channel with run status instances status.
func (launcher *Launcher) GetRunStatusesChannel() <-chan unitstatushandler.RunInstancesStatus {
	return launcher.runStatusChannel
}

// GetNodesConfiguration gets nodes configuration.
func (launcher *Launcher) GetNodesConfiguration() []cloudprotocol.NodeInfo {
	nodes := make([]cloudprotocol.NodeInfo, len(launcher.nodes))

	i := 0

	for _, v := range launcher.nodes {
		nodes[i] = v.NodeInfo.NodeInfo
		i++
	}

	return nodes
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (launcher *Launcher) fillUIDPool() {
	uids, err := launcher.storage.GetAllUIDs()
	if err != nil {
		log.Errorf("Can't fill UID pool: %v", err)
	}

	for _, uid := range uids {
		if err = launcher.uidPool.AddID(uid); err != nil {
			log.Errorf("Can't add UID to pool: %v", err)
		}
	}
}

func (launcher *Launcher) processChannels(ctx context.Context) {
	for {
		select {
		case instances := <-launcher.nodeManager.GetRunInstancesStatusChannel():
			launcher.processRunInstanceStatus(instances)

		case alert := <-launcher.nodeManager.GetSystemLimitAlertChannel():
			launcher.performRebalancing(alert)

		case <-ctx.Done():
			return
		}
	}
}

func (launcher *Launcher) sendRunInstances(forceRestart bool) (err error) {
	for _, node := range launcher.nodes {
		node.waitStatus = true

		if runErr := launcher.nodeManager.RunInstances(
			node.NodeID, node.currentRunRequest.services, node.currentRunRequest.layers,
			node.currentRunRequest.instances, forceRestart); runErr != nil {
			log.WithField("nodeID", node.NodeID).Errorf("Can't run instances %v", runErr)

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

	log.Debugf("Received runstatus from nodeID: %s", runStatus.NodeID)

	currentStatus := launcher.getNode(runStatus.NodeID)
	if currentStatus == nil {
		if !slices.Contains(launcher.config.SMController.NodeIDs, runStatus.NodeID) {
			log.Errorf("Received status for unknown nodeID  %s", runStatus.NodeID)

			return
		}

		var err error

		currentStatus, err = launcher.initNodeStatus(runStatus.NodeID, runStatus.NodeType)
		if err != nil {
			log.Errorf("Can't init node: %v", err)

			return
		}

		launcher.nodes = append(launcher.nodes, currentStatus)

		if len(launcher.nodes) == len(launcher.config.SMController.NodeIDs) {
			log.Debug("All clients connected")
		}

		slices.SortFunc(launcher.nodes, func(a, b *nodeStatus) bool {
			if a.priority == b.priority {
				return a.NodeID < b.NodeID
			}

			return a.priority < b.priority
		})
	}

	currentStatus.receivedRunInstances = runStatus.Instances
	currentStatus.waitStatus = false

	if len(launcher.nodes) != len(launcher.config.SMController.NodeIDs) {
		return
	}

	for _, node := range launcher.nodes {
		if node.waitStatus {
			return
		}
	}

	log.Info("All SM statuses received")

	launcher.connectionTimer.Stop()

	launcher.sendCurrentStatus()
}

func (launcher *Launcher) performRebalancing(alert cloudprotocol.SystemQuotaAlert) {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debug("Perform rebalaincing")

	nodes := launcher.getNodesForRebalancingByNodePriority(alert.NodeID)
	if len(nodes) == 0 {
		log.Error("No nodes with less priority for rebalancing")

		return
	}

	// init interna resource allocations perform initila balancing
	launcher.resetDeviceAllocation()
	launcher.currentErrorStatus = launcher.performNodeBalancing(launcher.currentDesiredInstances)

	nodeWithIssue := launcher.getNode(alert.NodeID)

	for i := len(nodeWithIssue.currentRunRequest.instances) - 1; i >= 0; i++ {
		currentServideID := nodeWithIssue.currentRunRequest.instances[i].ServiceID

		serviceInfo, err := launcher.imageProvider.GetServiceInfo(currentServideID)
		if err != nil {
			log.Errorf("Can't get service: %v", err)
			continue
		}

		labels, err := launcher.getLabeslForInstance(nodeWithIssue.currentRunRequest.instances[i].InstanceIdent)
		if err != nil {
			log.Errorf("Can't get labesl for instacne %v", err)
		}

		nodesToRebalance, _ := launcher.getNodesByStaticResources(launcher.nodes, serviceInfo, cloudprotocol.InstanceInfo{
			ServiceID: currentServideID,
			SubjectID: nodeWithIssue.currentRunRequest.instances[i].SubjectID, Labels: labels,
		}, true)
		if len(nodesToRebalance) == 0 {
			continue
		}

		nodesToRebalance = launcher.getNodesByDevices(nodesToRebalance, serviceInfo.Config.Devices)
		if len(nodesToRebalance) == 0 {
			continue
		}

		nodesToRebalance = launcher.getNodeByMonitoringData(nodesToRebalance, alert.Parameter)

		layersForService, err := launcher.getLayersForService(serviceInfo.Layers)
		if err != nil {
			log.Errorf("Can't get layer: %v", err)
			launcher.dealocateResources(nodesToRebalance, serviceInfo)

			continue
		}

		launcher.addRunRequest(
			serviceInfo, layersForService, nodeWithIssue.currentRunRequest.instances[i], nodesToRebalance[0])

		launcher.dealocateResources(append(nodesToRebalance[1:], nodeWithIssue), serviceInfo)

		nodeWithIssue.currentRunRequest.instances = append(nodeWithIssue.currentRunRequest.instances[:i],
			nodeWithIssue.currentRunRequest.instances[i+1:]...)

		launcher.connectionTimer.Reset(launcher.config.SMController.NodesConnectionTimeout.Duration)

		if err := launcher.sendRunInstances(false); err != nil {
			log.Errorf("Can't send run instance while rebalancing: %v", err)
		}

		return
	}

	log.Error("Can't perform rebalancing")
}

func (launcher *Launcher) initNodeStatus(nodeID, nodeType string) (*nodeStatus, error) {
	status := &nodeStatus{}

	config, err := launcher.nodeManager.GetNodeConfiguration(nodeID)
	if err != nil {
		return nil, aoserrors.Errorf("can't get node configuration fot nodeID %s: %v", nodeID, err)
	}

	status.NodeInfo = config

	launcher.initNodeUnitConfiguration(status, nodeType)

	return status, nil
}

func (launcher *Launcher) initNodeUnitConfiguration(nodeStatus *nodeStatus, nodeType string) {
	nodeUnitConfig := launcher.resourceManager.GetUnitConfiguration(nodeType)

	nodeStatus.priority = nodeUnitConfig.Priority
	nodeStatus.availableLabels = nodeUnitConfig.Labels
	nodeStatus.availableResources = make([]string, len(nodeUnitConfig.Resources))
	nodeStatus.availableDevices = make([]nodeDeviceResource, len(nodeUnitConfig.Devices))

	for i, resource := range nodeUnitConfig.Resources {
		nodeStatus.availableResources[i] = resource.Name
	}

	for i, device := range nodeUnitConfig.Devices {
		nodeStatus.availableDevices[i] = nodeDeviceResource{
			name: device.Name, sharedCount: device.SharedCount, allocatedCount: 0, preallocateCount: 0,
		}
	}
}

func (launcher *Launcher) resetDeviceAllocation() {
	for _, node := range launcher.nodes {
		for i, device := range node.availableDevices {
			if device.sharedCount == 0 {
				continue
			}

			node.availableDevices[i].allocatedCount = 0
			node.availableDevices[i].preallocateCount = 0
		}
	}
}

func (launcher *Launcher) sendCurrentStatus() {
	runStatusToSend := unitstatushandler.RunInstancesStatus{
		UnitSubjects: []string{}, Instances: []cloudprotocol.InstanceStatus{},
	}

	for _, node := range launcher.nodes {
		if node.waitStatus {
			node.waitStatus = false

			for _, errInstance := range node.currentRunRequest.instances {
				runStatusToSend.Instances = append(runStatusToSend.Instances, cloudprotocol.InstanceStatus{
					InstanceIdent: errInstance.InstanceIdent,
					NodeID:        node.NodeID, RunState: cloudprotocol.InstanceStateFailed,
					ErrorInfo: &cloudprotocol.ErrorInfo{Message: "wait run status timeout"},
				})
			}
		} else {
			runStatusToSend.Instances = append(runStatusToSend.Instances, node.receivedRunInstances...)
		}
	}

	errorInstances := []aostypes.InstanceIdent{}

	for i := range runStatusToSend.Instances {
		if runStatusToSend.Instances[i].ErrorInfo != nil {
			errorInstances = append(errorInstances, runStatusToSend.Instances[i].InstanceIdent)

			continue
		}

		runStatusToSend.Instances[i].StateChecksum = launcher.storageStateProvider.GetInstanceCheckSum(
			runStatusToSend.Instances[i].InstanceIdent)
	}

newServicesLoop:
	for _, newService := range launcher.pendingNewServices {
		for _, instance := range runStatusToSend.Instances {
			if instance.ServiceID == newService {
				if instance.ErrorInfo == nil {
					continue newServicesLoop
				}
			}
		}

		errorService := cloudprotocol.ServiceStatus{
			ID: newService, Status: cloudprotocol.ErrorStatus, ErrorInfo: &cloudprotocol.ErrorInfo{},
		}

		service, err := launcher.imageProvider.GetServiceInfo(newService)
		if err != nil {
			errorService.ErrorInfo.Message = err.Error()
		} else {
			errorService.AosVersion = service.AosVersion
			errorService.ErrorInfo.Message = "can't run any instances"
		}

		runStatusToSend.ErrorServices = append(runStatusToSend.ErrorServices, errorService)

		if err := launcher.imageProvider.RevertService(newService); err != nil {
			log.WithField("serviceID:", newService).Errorf("Can't revert service: %v", err)
		}
	}

	launcher.pendingNewServices = []string{}

	launcher.processStoppedInstances(runStatusToSend.Instances, errorInstances)

	runStatusToSend.Instances = append(runStatusToSend.Instances, launcher.currentErrorStatus...)

	launcher.runStatusChannel <- runStatusToSend

	launcher.currentRunStatus = runStatusToSend.Instances
	launcher.currentErrorStatus = []cloudprotocol.InstanceStatus{}
}

func (launcher *Launcher) processStoppedInstances(
	newStatus []cloudprotocol.InstanceStatus, errorInstances []aostypes.InstanceIdent,
) {
	stopedInstances := errorInstances

currentInstancesloop:
	for _, currentStaus := range launcher.currentRunStatus {
		for _, newStatus := range newStatus {
			if currentStaus.InstanceIdent != newStatus.InstanceIdent {
				continue
			}

			if newStatus.ErrorInfo != nil && currentStaus.ErrorInfo == nil {
				stopedInstances = append(stopedInstances, currentStaus.InstanceIdent)
			}

			continue currentInstancesloop
		}

		if currentStaus.ErrorInfo == nil {
			stopedInstances = append(stopedInstances, currentStaus.InstanceIdent)
		}
	}

	for _, stopIdent := range stopedInstances {
		if err := launcher.storageStateProvider.Cleanup(stopIdent); err != nil {
			log.Errorf("Can't cleanup state storage for instance: %v", err)
		}
	}
}

func (launcher *Launcher) performNodeBalancing(instances []cloudprotocol.InstanceInfo,
) (errStatus []cloudprotocol.InstanceStatus) {
	for _, node := range launcher.nodes {
		node.currentRunRequest = &runRequestInfo{}
	}

	sort.Slice(instances, func(i, j int) bool { return instances[i].Priority > instances[j].Priority })

	launcher.removeInstanceNetworkParameters(instances)

instancesLoop:
	for _, instance := range instances {
		serviceInfo, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
		if err != nil {
			log.WithField("serviceID", instance.ServiceID).Errorf("Can't get service info: %v", err)
			errStatus = append(errStatus, createInstanceStatusFromInfo(instance, 0, 0,
				cloudprotocol.InstanceStateFailed, err.Error()))

			continue
		}

		layers, err := launcher.getLayersForService(serviceInfo.Layers)
		if err != nil {
			log.Errorf("Can't get layer: %v", err)
			errStatus = append(errStatus, createInstanceStatusFromInfo(instance, 0, serviceInfo.AosVersion,
				cloudprotocol.InstanceStateFailed, err.Error()))

			continue instancesLoop
		}

		if serviceInfo.Config.Runner == "" {
			serviceInfo.Config.Runner = defaultRunner
		}

		nodes, status := launcher.getNodesByStaticResources(launcher.nodes, serviceInfo, instance, false)
		if len(nodes) == 0 {
			errStatus = append(errStatus, status)
			continue
		}

		for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
			instanceInfo, err := launcher.prepareInstanceStartInfo(serviceInfo, instance, instanceIndex)
			if err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance, instanceIndex,
					serviceInfo.AosVersion, cloudprotocol.InstanceStateFailed, err.Error()))
			}

			nodeForInstance := launcher.getNodesByDevices(nodes, serviceInfo.Config.Devices)
			if len(nodeForInstance) == 0 {
				log.WithFields(instanceIdentLogFields(aostypes.InstanceIdent{
					ServiceID: instance.ServiceID, SubjectID: instance.SubjectID, Instance: instanceIndex,
				}, nil)).Error("No devices for instance")

				errStatus = append(errStatus, createInstanceStatusFromInfo(instance, instanceIndex,
					serviceInfo.AosVersion, cloudprotocol.InstanceStateFailed, "no devices for instance"))

				continue
			}

			node := launcher.getMostPriorityNode(nodeForInstance, serviceInfo)

			launcher.addRunRequest(serviceInfo, layers, instanceInfo, node)
		}
	}

	return errStatus
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

func (launcher *Launcher) prepareInstanceStartInfo(service imagemanager.ServiceInfo,
	instance cloudprotocol.InstanceInfo, index uint64,
) (aostypes.InstanceInfo, error) {
	instanceInfo := aostypes.InstanceInfo{InstanceIdent: aostypes.InstanceIdent{
		ServiceID: instance.ServiceID, SubjectID: instance.SubjectID,
		Instance: index,
	}, Priority: instance.Priority}

	uid, err := launcher.storage.GetInstanceUID(instanceInfo.InstanceIdent)
	if err != nil {
		if !errors.Is(err, ErrNotExist) {
			log.Errorf("Can't get instance UID: %v", err)
			return instanceInfo, aoserrors.Errorf("Can't get instance UID: %v", err)
		}

		uid, err = launcher.uidPool.GetFreeID()
		if err != nil {
			log.Errorf("Can't get free UID: %v", err)
			return instanceInfo, aoserrors.Errorf("Can't get free UID: %v", err)
		}

		if err := launcher.storage.AddInstance(instanceInfo.InstanceIdent, uid); err != nil {
			log.Errorf("Can't store uid: %v", err)
		}
	}

	instanceInfo.UID = uint32(uid)

	stateStrageParams := storagestate.SetupParams{
		InstanceIdent: instanceInfo.InstanceIdent,
		UID:           uid, GID: int(service.GID),
	}

	if service.Config.Quotas.StateLimit != nil {
		stateStrageParams.StateQuota = *service.Config.Quotas.StateLimit
	}

	if service.Config.Quotas.StorageLimit != nil {
		stateStrageParams.StorageQuota = *service.Config.Quotas.StorageLimit
	}

	instanceInfo.StoragePath, instanceInfo.StatePath, err = launcher.storageStateProvider.Setup(stateStrageParams)
	if err != nil {
		log.Errorf("Can't setup storage and state for instance: %v", err)

		_ = launcher.uidPool.RemoveID(uid)

		return instanceInfo, aoserrors.Errorf("can't setup storage and state for instance: %v", err)
	}

	if instanceInfo.NetworkParameters, err = launcher.networkManager.PrepareInstanceNetworkParameters(
		instanceInfo.InstanceIdent, service.ProviderID); err != nil {
		return instanceInfo, aoserrors.Wrap(err)
	}

	return instanceInfo, nil
}

func (launcher *Launcher) getNodesByStaticResources(allNodes []*nodeStatus,
	serviceInfo imagemanager.ServiceInfo, instanceInfo cloudprotocol.InstanceInfo, rebalancing bool,
) (nodes []*nodeStatus, errStatus cloudprotocol.InstanceStatus) {
	nodes = launcher.getNodeByRunner(allNodes, serviceInfo.Config.Runner)
	if len(nodes) == 0 {
		if rebalancing {
			return nodes, errStatus
		}

		log.Errorf("No appropriate node with runner: %s", serviceInfo.Config.Runner)

		return nodes, createInstanceStatusFromInfo(instanceInfo, 0, serviceInfo.AosVersion,
			cloudprotocol.InstanceStateFailed, fmt.Sprintf(
				"no appropriate node with runner: %s", serviceInfo.Config.Runner))
	}

	nodes = launcher.getNodesByResources(nodes, serviceInfo.Config.Resources)
	if len(nodes) == 0 {
		if rebalancing {
			return nodes, errStatus
		}

		log.Errorf("No appropriate node with resources: %v", serviceInfo.Config.Resources)

		return nodes, createInstanceStatusFromInfo(instanceInfo, 0, serviceInfo.AosVersion,
			cloudprotocol.InstanceStateFailed, fmt.Sprintf(
				"no appropriate node with resourceser: %v", serviceInfo.Config.Resources))
	}

	nodes = launcher.getNodesByLabels(nodes, instanceInfo.Labels)
	if len(nodes) == 0 {
		if rebalancing {
			return nodes, errStatus
		}

		log.Errorf("No appropriate node with labels %v", instanceInfo.Labels)

		return nodes, createInstanceStatusFromInfo(instanceInfo, 0, serviceInfo.AosVersion,
			cloudprotocol.InstanceStateFailed, fmt.Sprintf(
				"no appropriate node with labels: %v", instanceInfo.Labels))
	}

	return nodes, errStatus
}

func (launcher *Launcher) getNodesByDevices(
	nodes []*nodeStatus, desiredDevices []aostypes.ServiceDevice,
) (newNodes []*nodeStatus) {
	if len(desiredDevices) == 0 {
		return slices.Clone(nodes)
	}

	for _, node := range nodes {
		if len(node.availableDevices) == 0 {
			continue
		}

		nodeAdded := true

		for _, desiredDevice := range desiredDevices {
			if !launcher.preallocateDevicesForNode(node, desiredDevice.Name) {
				nodeAdded = false
				break
			}
		}

		if nodeAdded {
			newNodes = append(newNodes, node)
		}

		for i := range node.availableDevices {
			if nodeAdded {
				node.availableDevices[i].allocatedCount = node.availableDevices[i].preallocateCount
			} else {
				node.availableDevices[i].preallocateCount = node.availableDevices[i].allocatedCount
			}
		}
	}

	return newNodes
}

func (launcher *Launcher) getNodeByMonitoringData(nodes []*nodeStatus, alertType string) (newNodes []*nodeStatus) {
	if len(nodes) == 1 {
		return nodes
	}

	type freeNodeResources struct {
		node    *nodeStatus
		freeRAM uint64
		freeCPU uint64
	}

	nodesResources := []freeNodeResources{}

	for _, node := range nodes {
		monitoringData, err := launcher.nodeManager.GetNodeMonitoringData(node.NodeID)
		if err != nil {
			log.Errorf("Can't get node monitoringdata: %v", err)
		}

		nodesResources = append(nodesResources, freeNodeResources{
			node:    node,
			freeRAM: node.TotalRAM - monitoringData.RAM,
			freeCPU: node.NumCPUs*100 - monitoringData.CPU,
		})
	}

	if alertType == "cpu" {
		slices.SortFunc(nodesResources, func(a, b freeNodeResources) bool {
			return a.freeCPU > b.freeCPU
		})
	} else {
		slices.SortFunc(nodesResources, func(a, b freeNodeResources) bool {
			return a.freeRAM > b.freeRAM
		})
	}

	for _, sortedResources := range nodesResources {
		newNodes = append(newNodes, sortedResources.node)
	}

	return newNodes
}

func (launcher *Launcher) preallocateDevicesForNode(node *nodeStatus, deviceName string) bool {
	for i, nodeDevice := range node.availableDevices {
		if nodeDevice.name != deviceName {
			continue
		}

		if nodeDevice.sharedCount == 0 {
			return true
		}

		if nodeDevice.preallocateCount < nodeDevice.sharedCount {
			node.availableDevices[i].preallocateCount++

			return true
		}

		return false
	}

	return false
}

func (launcher *Launcher) getNodesByResources(
	nodes []*nodeStatus, desiredResources []string,
) (newNodes []*nodeStatus) {
	if len(desiredResources) == 0 {
		return nodes
	}

nodeLoop:
	for _, node := range nodes {
		if len(node.availableResources) == 0 {
			continue
		}

		for _, resource := range desiredResources {
			if !slices.Contains(node.availableResources, resource) {
				continue nodeLoop
			}
		}

		newNodes = append(newNodes, node)
	}

	return newNodes
}

func (launcher *Launcher) getNodesByLabels(nodes []*nodeStatus, desiredLabels []string) (newNodes []*nodeStatus) {
	if len(desiredLabels) == 0 {
		return nodes
	}

nodeLoop:
	for _, node := range nodes {
		if len(node.availableLabels) == 0 {
			continue
		}

		for _, label := range desiredLabels {
			if !slices.Contains(node.availableLabels, label) {
				continue nodeLoop
			}
		}

		newNodes = append(newNodes, node)
	}

	return newNodes
}

func (launcher *Launcher) getNodeByRunner(allNodes []*nodeStatus, runner string) (nodes []*nodeStatus) {
	for _, node := range allNodes {
		if slices.Contains(node.RunnerFeature, runner) {
			nodes = append(nodes, node)
		}
	}

	return nodes
}

func (launcher *Launcher) getMostPriorityNode(
	nodes []*nodeStatus, serviceInfo imagemanager.ServiceInfo,
) (node *nodeStatus) {
	if len(nodes) == 1 {
		return nodes[0]
	}

	maxNodePriorityIndex := 0

	for i := 1; i < len(nodes); i++ {
		if nodes[maxNodePriorityIndex].priority < nodes[i].priority {
			maxNodePriorityIndex = i
		}
	}

	node = nodes[maxNodePriorityIndex]

	nodesForCleanUp := slices.Delete(slices.Clone(nodes), maxNodePriorityIndex, maxNodePriorityIndex+1)

	launcher.dealocateResources(nodesForCleanUp, serviceInfo)

	return node
}

func (launcher *Launcher) dealocateResources(nodes []*nodeStatus, serviceInfo imagemanager.ServiceInfo) {
	if len(serviceInfo.Config.Devices) == 0 {
		return
	}

	for _, device := range serviceInfo.Config.Devices {
		for _, node := range nodes {
			for i, nodeDeviceStatus := range node.availableDevices {
				if nodeDeviceStatus.name != device.Name {
					continue
				}

				if nodeDeviceStatus.sharedCount != 0 {
					node.availableDevices[i].allocatedCount--
					node.availableDevices[i].preallocateCount--
				}

				break
			}
		}
	}
}

func (launcher *Launcher) addRunRequest(service imagemanager.ServiceInfo, layers []imagemanager.LayerInfo,
	instance aostypes.InstanceInfo, node *nodeStatus,
) {
	node.currentRunRequest.instances = append(node.currentRunRequest.instances, instance)

	serviceInfo := service.ServiceInfo

	if node.RemoteNode {
		serviceInfo.URL = service.RemoteURL
	}

	isNewService := true

	for _, oldService := range node.currentRunRequest.services {
		if reflect.DeepEqual(oldService, serviceInfo) {
			isNewService = false
			break
		}
	}

	if isNewService {
		node.currentRunRequest.services = append(node.currentRunRequest.services, serviceInfo)
	}

layerLoopLabel:
	for _, layer := range layers {
		newLayer := layer.LayerInfo

		if node.RemoteNode {
			newLayer.URL = layer.RemoteURL
		}

		for _, oldLayer := range node.currentRunRequest.layers {
			if reflect.DeepEqual(newLayer, oldLayer) {
				continue layerLoopLabel
			}
		}

		node.currentRunRequest.layers = append(node.currentRunRequest.layers, newLayer)
	}
}

func createInstanceStatusFromInfo(
	info cloudprotocol.InstanceInfo, instanceIndex, serviceVersion uint64, runState, errorMsg string,
) cloudprotocol.InstanceStatus {
	instanceStatus := cloudprotocol.InstanceStatus{
		InstanceIdent: aostypes.InstanceIdent{
			ServiceID: info.ServiceID, SubjectID: info.SubjectID, Instance: instanceIndex,
		},
		AosVersion: serviceVersion, RunState: runState,
	}

	if errorMsg != "" {
		instanceStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: errorMsg}
	}

	return instanceStatus
}

func (launcher *Launcher) getNode(nodeID string) *nodeStatus {
	for _, node := range launcher.nodes {
		if node.NodeID == nodeID {
			return node
		}
	}

	return nil
}

func (launcher *Launcher) getNodesForRebalancingByNodePriority(nodeID string) (nodes []*nodeStatus) {
	nodeWithIssue := launcher.getNode(nodeID)
	if nodeWithIssue == nil {
		return nodes
	}

	for _, node := range launcher.nodes {
		if nodeWithIssue.priority < node.priority {
			continue
		}

		if node.NodeID != nodeWithIssue.NodeID {
			nodes = append(nodes, node)
		}
	}

	slices.SortFunc(launcher.nodes, func(a, b *nodeStatus) bool {
		if a.priority == b.priority {
			return a.NodeID < b.NodeID
		}

		return a.priority < b.priority
	})

	return nodes
}

func (launcher *Launcher) getLabeslForInstance(ident aostypes.InstanceIdent) ([]string, error) {
	for _, instance := range launcher.currentDesiredInstances {
		if instance.ServiceID == ident.ServiceID && instance.SubjectID == ident.SubjectID {
			return instance.Labels, nil
		}
	}

	return nil, aoserrors.New("no labels for instance")
}

func (launcher *Launcher) getLayersForService(digests []string) ([]imagemanager.LayerInfo, error) {
	layers := make([]imagemanager.LayerInfo, len(digests))

	for i, digest := range digests {
		layer, err := launcher.imageProvider.GetLayerInfo(digest)
		if err != nil {
			return layers, err // nolint
		}

		layers[i] = layer
	}

	return layers, nil
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
