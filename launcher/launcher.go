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

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/imagemanager"
	"github.com/aosedge/aos_communicationmanager/networkmanager"
	"github.com/aosedge/aos_communicationmanager/storagestate"
	"github.com/aosedge/aos_communicationmanager/unitstatushandler"
)

/**********************************************************************************************************************
* Consts
**********************************************************************************************************************/

var ErrNotExist = errors.New("entry not exist")

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

	config               *config.Config
	storage              Storage
	nodeManager          NodeManager
	imageProvider        ImageProvider
	resourceManager      ResourceManager
	storageStateProvider StorageStateProvider
	networkManager       NetworkManager

	runStatusChannel   chan unitstatushandler.RunInstancesStatus
	nodes              map[string]*nodeHandler
	currentRunStatus   []cloudprotocol.InstanceStatus
	currentErrorStatus []cloudprotocol.InstanceStatus
	pendingNewServices []string

	cancelFunc      context.CancelFunc
	connectionTimer *time.Timer

	instanceManager *instanceManager
}

type InstanceInfo struct {
	aostypes.InstanceIdent
	UID       int
	Timestamp time.Time
	Cached    bool
}

// Storage storage interface.
type Storage interface {
	AddInstance(instanceInfo InstanceInfo) error
	RemoveInstance(instanceIdent aostypes.InstanceIdent) error
	SetInstanceCached(instance aostypes.InstanceIdent, cached bool) error
	GetInstanceUID(instance aostypes.InstanceIdent) (int, error)
	GetInstances() ([]InstanceInfo, error)
	GetServiceInfo(serviceID string) (imagemanager.ServiceInfo, error)
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
	RevertService(serviceID string) error
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
		config: config, storage: storage, nodeManager: nodeManager, imageProvider: imageProvider,
		resourceManager: resourceManager, storageStateProvider: storageStateProvider, networkManager: networkManager,
		runStatusChannel: make(chan unitstatushandler.RunInstancesStatus, 10),
		nodes:            make(map[string]*nodeHandler),
	}

	if launcher.instanceManager, err = newInstanceManager(config, storage, storageStateProvider,
		launcher.imageProvider.GetRemoveServiceChannel()); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	nodes, err := nodeInfoProvider.GetAllNodeIDs()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, nodeID := range nodes {
		nodeInfo, err := nodeInfoProvider.GetNodeInfo(nodeID)
		if err != nil {
			log.WithField("nodeID", nodeID).Errorf("Can't get node info: %v", err)

			continue
		}

		if nodeInfo.Status == cloudprotocol.NodeStatusProvisioned {
			log.WithField("nodeID", nodeID).Debug("Skip not provisioned node")

			continue
		}

		nodeHandler, err := newNodeHandler(
			nodeInfo, resourceManager, nodeInfo.NodeID == nodeInfoProvider.GetNodeID())
		if err != nil {
			log.WithField("nodeID", nodeID).Errorf("Can't create node handler: %v", err)

			continue
		}

		launcher.nodes[nodeID] = nodeHandler
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
func (launcher *Launcher) RunInstances(instances []cloudprotocol.InstanceInfo, newServices []string) error {
	launcher.Lock()
	defer launcher.Unlock()

	log.Debug("Run instances")

	launcher.connectionTimer = time.AfterFunc(
		launcher.config.SMController.NodesConnectionTimeout.Duration, launcher.sendCurrentStatus)

	if err := launcher.updateNetworks(instances); err != nil {
		log.Errorf("Can't update networks: %v", err)
	}

	launcher.pendingNewServices = newServices
	launcher.currentErrorStatus = launcher.performNodeBalancing(instances)

	if err := launcher.networkManager.RestartDNSServer(); err != nil {
		log.Errorf("Can't restart DNS server: %v", err)
	}

	return launcher.sendRunInstances(false)
}

// GetRunStatusesChannel gets channel with run status instances status.
func (launcher *Launcher) GetRunStatusesChannel() <-chan unitstatushandler.RunInstancesStatus {
	return launcher.runStatusChannel
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

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
	for _, node := range launcher.getNodesByPriorities() {
		node.waitStatus = true

		if runErr := launcher.nodeManager.RunInstances(
			node.nodeInfo.NodeID, node.currentRunRequest.Services, node.currentRunRequest.Layers,
			node.currentRunRequest.Instances, forceRestart); runErr != nil {
			log.WithField("nodeID", node.nodeInfo.NodeID).Errorf("Can't run instances %v", runErr)

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

	currentStatus.receivedRunInstances = runStatus.Instances
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

func (launcher *Launcher) resetDeviceAllocation() {
	for _, node := range launcher.nodes {
		for i := range node.availableDevices {
			node.availableDevices[i].allocatedCount = 0
		}
	}
}

func (launcher *Launcher) sendCurrentStatus() {
	runStatusToSend := unitstatushandler.RunInstancesStatus{
		UnitSubjects: []string{}, Instances: []cloudprotocol.InstanceStatus{},
	}

	for _, node := range launcher.getNodesByPriorities() {
		if node.waitStatus {
			node.waitStatus = false

			for _, errInstance := range node.currentRunRequest.Instances {
				runStatusToSend.Instances = append(runStatusToSend.Instances, cloudprotocol.InstanceStatus{
					InstanceIdent: errInstance.InstanceIdent,
					NodeID:        node.nodeInfo.NodeID, Status: cloudprotocol.InstanceStateFailed,
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
			if instance.ServiceID == newService && instance.ErrorInfo == nil {
				continue newServicesLoop
			}
		}

		errorService := cloudprotocol.ServiceStatus{
			ServiceID: newService, Status: cloudprotocol.ErrorStatus, ErrorInfo: &cloudprotocol.ErrorInfo{},
		}

		service, err := launcher.imageProvider.GetServiceInfo(newService)
		if err != nil {
			errorService.ErrorInfo.Message = err.Error()
		} else {
			errorService.Version = service.Version
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
	stoppedInstances := errorInstances

currentInstancesLoop:
	for _, currentStatus := range launcher.currentRunStatus {
		for _, newStatus := range newStatus {
			if currentStatus.InstanceIdent != newStatus.InstanceIdent {
				continue
			}

			if newStatus.ErrorInfo != nil && currentStatus.ErrorInfo == nil {
				stoppedInstances = append(stoppedInstances, currentStatus.InstanceIdent)
			}

			continue currentInstancesLoop
		}

		if currentStatus.ErrorInfo == nil {
			stoppedInstances = append(stoppedInstances, currentStatus.InstanceIdent)
		}
	}

	for _, stopIdent := range stoppedInstances {
		if err := launcher.storageStateProvider.Cleanup(stopIdent); err != nil {
			log.Errorf("Can't cleanup state storage for instance: %v", err)
		}
	}
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

//nolint:funlen,gocognit
func (launcher *Launcher) performNodeBalancing(instances []cloudprotocol.InstanceInfo,
) (errStatus []cloudprotocol.InstanceStatus) {
	for _, node := range launcher.nodes {
		node.currentRunRequest = &runRequestInfo{}
	}

	launcher.resetDeviceAllocation()

	sort.Slice(instances, func(i, j int) bool {
		if instances[i].Priority == instances[j].Priority {
			return instances[i].ServiceID < instances[j].ServiceID
		}

		return instances[i].Priority > instances[j].Priority
	})

	launcher.cacheInstances(instances)
	launcher.removeInstanceNetworkParameters(instances)

	for _, instance := range instances {
		log.WithFields(log.Fields{
			"serviceID":    instance.ServiceID,
			"subjectID":    instance.SubjectID,
			"numInstances": instance.NumInstances,
			"priority":     instance.Priority,
		}).Debug("Balance instances")

		serviceInfo, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
		if err != nil {
			errStatus = append(errStatus, createInstanceStatusFromInfo(
				instance.ServiceID, instance.SubjectID, 0, "0.0.0",
				cloudprotocol.InstanceStateFailed, err.Error()))

			continue
		}

		if serviceInfo.Cached {
			errStatus = append(errStatus, createInstanceStatusFromInfo(
				instance.ServiceID, instance.SubjectID, 0, "0.0.0",
				cloudprotocol.InstanceStateFailed, "service deleted"))

			continue
		}

		layers, err := launcher.getLayersForService(serviceInfo.Layers)
		if err != nil {
			for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instanceIndex, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))
			}

			continue
		}

		nodes, err := getNodesByStaticResources(launcher.getNodesByPriorities(), serviceInfo, instance)
		if err != nil {
			for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instanceIndex, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))
			}

			continue
		}

		// createInstanceStatusFromInfo

		for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
			nodeForInstance, err := getNodesByDevices(nodes, serviceInfo.Config.Devices)
			if err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instanceIndex, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))

				continue
			}

			instanceInfo, err := launcher.prepareInstanceStartInfo(serviceInfo, instance, instanceIndex)
			if err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instanceIndex, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))
			}

			node := getMostPriorityNode(nodeForInstance)

			if err = node.allocateDevices(serviceInfo.Config.Devices); err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instanceIndex, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))

				continue
			}

			node.addRunRequest(instanceInfo, serviceInfo, layers)
		}
	}

	// first prepare network for instance which have exposed ports
	errNetworkStatus := launcher.prepareNetworkForInstances(true)
	errStatus = append(errStatus, errNetworkStatus...)

	// then prepare network for rest of instances
	errNetworkStatus = launcher.prepareNetworkForInstances(false)
	errStatus = append(errStatus, errNetworkStatus...)

	return errStatus
}

func (launcher *Launcher) prepareNetworkForInstances(onlyExposedPorts bool) (errStatus []cloudprotocol.InstanceStatus) {
	for _, node := range launcher.getNodesByPriorities() {
		for i, instance := range node.currentRunRequest.Instances {
			serviceInfo, err := launcher.imageProvider.GetServiceInfo(instance.ServiceID)
			if err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID, 0, "0.0",
					cloudprotocol.InstanceStateFailed, err.Error()))

				continue
			}

			if onlyExposedPorts && len(serviceInfo.ExposedPorts) == 0 {
				continue
			}

			if instance.NetworkParameters, err = launcher.networkManager.PrepareInstanceNetworkParameters(
				instance.InstanceIdent, serviceInfo.ProviderID,
				prepareNetworkParameters(serviceInfo)); err != nil {
				errStatus = append(errStatus, createInstanceStatusFromInfo(instance.ServiceID, instance.SubjectID,
					instance.Instance, serviceInfo.Version, cloudprotocol.InstanceStateFailed, err.Error()))
			}

			node.currentRunRequest.Instances[i] = instance
		}
	}

	return errStatus
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

func (launcher *Launcher) cacheInstances(instances []cloudprotocol.InstanceInfo) {
	storeInstances, err := launcher.storage.GetInstances()
	if err != nil {
		log.Errorf("Can't get instances from storage: %v", err)
		return
	}

nextStoreInstance:
	for _, storeInstance := range storeInstances {
		for _, instance := range instances {
			for instanceIndex := uint64(0); instanceIndex < instance.NumInstances; instanceIndex++ {
				instanceIdent := aostypes.InstanceIdent{
					ServiceID: instance.ServiceID, SubjectID: instance.SubjectID,
					Instance: instanceIndex,
				}

				if storeInstance.InstanceIdent == instanceIdent {
					continue nextStoreInstance
				}
			}
		}

		log.WithFields(instanceIdentLogFields(storeInstance.InstanceIdent, nil)).Debug("Cache instance")

		if err := launcher.storage.SetInstanceCached(storeInstance.InstanceIdent, true); err != nil {
			log.WithFields(
				instanceIdentLogFields(storeInstance.InstanceIdent, nil)).Errorf("Can't mark instance as cached: %v", err)
		}
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
			return instanceInfo, aoserrors.Wrap(err)
		}

		uid, err = launcher.instanceManager.acquireUID()
		if err != nil {
			return instanceInfo, aoserrors.Wrap(err)
		}

		if err := launcher.storage.AddInstance(InstanceInfo{
			InstanceIdent: instanceInfo.InstanceIdent,
			UID:           uid,
			Timestamp:     time.Now(),
		}); err != nil {
			log.Errorf("Can't store uid: %v", err)
		}
	}

	instanceInfo.UID = uint32(uid)

	stateStorageParams := storagestate.SetupParams{
		InstanceIdent: instanceInfo.InstanceIdent,
		UID:           uid, GID: int(service.GID),
	}

	if service.Config.Quotas.StateLimit != nil {
		stateStorageParams.StateQuota = *service.Config.Quotas.StateLimit
	}

	if service.Config.Quotas.StorageLimit != nil {
		stateStorageParams.StorageQuota = *service.Config.Quotas.StorageLimit
	}

	instanceInfo.StoragePath, instanceInfo.StatePath, err = launcher.storageStateProvider.Setup(stateStorageParams)
	if err != nil {
		_ = launcher.instanceManager.releaseUID(uid)

		return instanceInfo, aoserrors.Wrap(err)
	}

	// make sure that instance is not cached
	if err := launcher.storage.SetInstanceCached(instanceInfo.InstanceIdent, false); err != nil {
		log.WithFields(instanceIdentLogFields(instanceInfo.InstanceIdent,
			nil)).Errorf("Can't mark instance as not cached: %v", err)

		return instanceInfo, aoserrors.Wrap(err)
	}

	return instanceInfo, nil
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

func createInstanceStatusFromInfo(
	serviceID, subjectID string, instanceIndex uint64, serviceVersion string, status, errorMsg string,
) cloudprotocol.InstanceStatus {
	ident := aostypes.InstanceIdent{
		ServiceID: serviceID, SubjectID: subjectID, Instance: instanceIndex,
	}

	instanceStatus := cloudprotocol.InstanceStatus{
		InstanceIdent: ident, ServiceVersion: serviceVersion, Status: status,
	}

	if errorMsg != "" {
		log.WithFields(instanceIdentLogFields(ident, nil)).Errorf("Can't schedule instance: %s", errorMsg)

		instanceStatus.ErrorInfo = &cloudprotocol.ErrorInfo{Message: errorMsg}
	}

	return instanceStatus
}

func (launcher *Launcher) getNode(nodeID string) *nodeHandler {
	node, ok := launcher.nodes[nodeID]
	if !ok {
		return nil
	}

	return node
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
