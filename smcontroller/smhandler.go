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

package smcontroller

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/api/common"
	pb "github.com/aosedge/aos_common/api/servicemanager"
	"github.com/aosedge/aos_common/utils/pbconvert"
	"github.com/aosedge/aos_common/utils/syncstream"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aosedge/aos_communicationmanager/launcher"
	"github.com/aosedge/aos_communicationmanager/unitconfig"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const waitMessageTimeout = 5 * time.Second

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type smHandler struct {
	stream                 pb.SMService_RegisterSMServer
	messageSender          MessageSender
	alertSender            AlertSender
	monitoringSender       MonitoringSender
	syncstream             *syncstream.SyncStream
	nodeConfigStatus       unitconfig.NodeConfigStatus
	runStatusCh            chan<- launcher.NodeRunInstanceStatus
	updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus
	systemLimitAlertCh     chan<- cloudprotocol.SystemQuotaAlert
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newSMHandler(
	stream pb.SMService_RegisterSMServer, messageSender MessageSender, alertSender AlertSender,
	monitoringSender MonitoringSender, runStatusCh chan<- launcher.NodeRunInstanceStatus,
	updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus,
	systemLimitAlertCh chan<- cloudprotocol.SystemQuotaAlert,
) (*smHandler, error) {
	handler := smHandler{
		stream:                 stream,
		messageSender:          messageSender,
		alertSender:            alertSender,
		monitoringSender:       monitoringSender,
		syncstream:             syncstream.New(),
		runStatusCh:            runStatusCh,
		updateInstanceStatusCh: updateInstanceStatusCh,
		systemLimitAlertCh:     systemLimitAlertCh,
	}

	return &handler, nil
}

func (handler *smHandler) getNodeConfigStatus() (unitconfig.NodeConfigStatus, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(
		ctx, handler.sendGetNodeConfigStatus, reflect.TypeOf(&pb.SMOutgoingMessages_NodeConfigStatus{}))
	if err != nil {
		return unitconfig.NodeConfigStatus{}, aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_NodeConfigStatus)
	if !ok {
		return unitconfig.NodeConfigStatus{}, aoserrors.New("incorrect type")
	}

	if pbStatus.NodeConfigStatus.GetError().GetMessage() != "" {
		return unitconfig.NodeConfigStatus{}, aoserrors.New(pbStatus.NodeConfigStatus.GetError().GetMessage())
	}

	return nodeConfigStatusFromPB(pbStatus.NodeConfigStatus), nil
}

func (handler *smHandler) checkNodeConfig(version string, unitConfig cloudprotocol.NodeConfig) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(ctx, func() error {
		return handler.sendCheckNodeConfig(unitConfig, version)
	}, reflect.TypeOf(&pb.SMOutgoingMessages_NodeConfigStatus{}))
	if err != nil {
		return aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_NodeConfigStatus)
	if !ok {
		return aoserrors.New("incorrect type")
	}

	if pbStatus.NodeConfigStatus.GetError() != nil {
		return aoserrors.New(pbStatus.NodeConfigStatus.GetError().GetMessage())
	}

	return nil
}

func (handler *smHandler) setNodeConfig(version string, cfg cloudprotocol.NodeConfig) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(ctx, func() error {
		return handler.sendSetNodeConfig(cfg, version)
	}, reflect.TypeOf(&pb.SMOutgoingMessages_NodeConfigStatus{}))
	if err != nil {
		return aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_NodeConfigStatus)
	if !ok {
		return aoserrors.New("incorrect type")
	}

	if pbStatus.NodeConfigStatus.GetError() != nil {
		return aoserrors.New(pbStatus.NodeConfigStatus.GetError().GetMessage())
	}

	return nil
}

func (handler *smHandler) updateNetworks(networkParameters []aostypes.NetworkParameters) error {
	log.WithFields(log.Fields{
		"nodeID": handler.nodeConfigStatus.NodeID,
	}).Debug("CM update networks")

	pbNetworkParameters := make([]*pb.NetworkParameters, len(networkParameters))

	for i, networkParameter := range networkParameters {
		pbNetworkParameters[i] = &pb.NetworkParameters{
			NetworkId: networkParameter.NetworkID,
			Ip:        networkParameter.IP,
			Subnet:    networkParameter.Subnet,
			VlanId:    networkParameter.VlanID,
		}
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_UpdateNetworks{
		UpdateNetworks: &pb.UpdateNetworks{
			Networks: pbNetworkParameters,
		},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) runInstances(
	services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo, forceRestart bool,
) error {
	log.WithFields(log.Fields{
		"nodeID": handler.nodeConfigStatus.NodeID,
	}).Debug("SM run instances")

	pbRunInstances := &pb.RunInstances{
		Services:     make([]*pb.ServiceInfo, len(services)),
		Layers:       make([]*pb.LayerInfo, len(layers)),
		Instances:    make([]*pb.InstanceInfo, len(instances)),
		ForceRestart: forceRestart,
	}

	for i, serviceInfo := range services {
		pbRunInstances.Services[i] = &pb.ServiceInfo{
			Version:    serviceInfo.Version,
			Url:        serviceInfo.URL,
			ServiceId:  serviceInfo.ServiceID,
			ProviderId: serviceInfo.ProviderID,
			Gid:        serviceInfo.GID,
			Sha256:     serviceInfo.Sha256,
			Size:       serviceInfo.Size,
		}
	}

	for i, layerInfo := range layers {
		pbRunInstances.Layers[i] = &pb.LayerInfo{
			Version: layerInfo.Version,
			Url:     layerInfo.URL,
			LayerId: layerInfo.LayerID,
			Digest:  layerInfo.Digest,
			Sha256:  layerInfo.Sha256,
			Size:    layerInfo.Size,
		}
	}

	for i, instanceInfo := range instances {
		pbRunInstances.Instances[i] = &pb.InstanceInfo{
			Instance:          pbconvert.InstanceIdentToPB(instanceInfo.InstanceIdent),
			Uid:               instanceInfo.UID,
			Priority:          instanceInfo.Priority,
			StoragePath:       instanceInfo.StoragePath,
			StatePath:         instanceInfo.StatePath,
			NetworkParameters: pbconvert.NetworkParametersToPB(instanceInfo.NetworkParameters),
		}
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_RunInstances{
		RunInstances: pbRunInstances,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) getSystemLog(logRequest cloudprotocol.RequestLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID": handler.nodeConfigStatus.NodeID,
		"logID":  logRequest.LogID,
		"from":   logRequest.Filter.From,
		"till":   logRequest.Filter.Till,
	}).Debug("Get SM system log")

	request := &pb.SystemLogRequest{LogId: logRequest.LogID}

	if logRequest.Filter.From != nil {
		request.From = timestamppb.New(*logRequest.Filter.From)
	}

	if logRequest.Filter.Till != nil {
		request.Till = timestamppb.New(*logRequest.Filter.Till)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SystemLogRequest{
		SystemLogRequest: request,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

//nolint:dupl
func (handler *smHandler) getInstanceLog(logRequest cloudprotocol.RequestLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID":    handler.nodeConfigStatus.NodeID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.Filter.ServiceID,
		"from":      logRequest.Filter.From,
		"till":      logRequest.Filter.Till,
	}).Debug("Get instance log")

	request := &pb.InstanceLogRequest{
		LogId: logRequest.LogID, InstanceFilter: pbconvert.InstanceFilterToPB(logRequest.Filter.InstanceFilter),
	}

	if logRequest.Filter.From != nil {
		request.From = timestamppb.New(*logRequest.Filter.From)
	}

	if logRequest.Filter.Till != nil {
		request.Till = timestamppb.New(*logRequest.Filter.Till)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceLogRequest{
		InstanceLogRequest: request,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

//nolint:dupl
func (handler *smHandler) getInstanceCrashLog(logRequest cloudprotocol.RequestLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID":    handler.nodeConfigStatus.NodeID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.Filter.ServiceID,
		"from":      logRequest.Filter.From,
		"till":      logRequest.Filter.Till,
	}).Debug("Get instance crash log")

	request := &pb.InstanceCrashLogRequest{
		LogId: logRequest.LogID, InstanceFilter: pbconvert.InstanceFilterToPB(logRequest.Filter.InstanceFilter),
	}

	if logRequest.Filter.From != nil {
		request.From = timestamppb.New(*logRequest.Filter.From)
	}

	if logRequest.Filter.Till != nil {
		request.Till = timestamppb.New(*logRequest.Filter.Till)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{
		SMIncomingMessage: &pb.SMIncomingMessages_InstanceCrashLogRequest{
			InstanceCrashLogRequest: request,
		},
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) overrideEnvVars(envVars cloudprotocol.OverrideEnvVars) (err error) {
	log.WithFields(log.Fields{"nodeID": handler.nodeConfigStatus.NodeID}).Debug("Override env vars SM ")

	request := &pb.OverrideEnvVars{EnvVars: make([]*pb.OverrideInstanceEnvVar, len(envVars.Items))}

	for i, item := range envVars.Items {
		requestItem := &pb.OverrideInstanceEnvVar{
			InstanceFilter: pbconvert.InstanceFilterToPB(item.InstanceFilter),
			Variables:      make([]*pb.EnvVarInfo, len(item.Variables)),
		}

		for j, envVar := range item.Variables {
			requestVar := &pb.EnvVarInfo{Name: envVar.Name, Value: envVar.Value}

			if envVar.TTL != nil {
				requestVar.Ttl = timestamppb.New(*envVar.TTL)
			}

			requestItem.Variables[j] = requestVar
		}

		request.EnvVars[i] = requestItem
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_OverrideEnvVars{
		OverrideEnvVars: request,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) getAverageMonitoring() (monitoring aostypes.NodeMonitoring, err error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(
		ctx, handler.sendGetAverageMonitoring, reflect.TypeOf(&pb.SMOutgoingMessages_AverageMonitoring{}))
	if err != nil {
		return monitoring, aoserrors.Wrap(err)
	}

	pbMonitoring, ok := status.(*pb.SMOutgoingMessages_AverageMonitoring)
	if !ok {
		return monitoring, aoserrors.New("incorrect type")
	}

	monitoring = averageMonitoringFromPB(pbMonitoring.AverageMonitoring)
	monitoring.NodeID = handler.nodeConfigStatus.NodeID

	return monitoring, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (handler *smHandler) processSMMessages(message *pb.SMOutgoingMessages) {
	if handler.syncstream.ProcessMessages(message.GetSMOutgoingMessage()) {
		return
	}

	switch data := message.GetSMOutgoingMessage().(type) {
	case *pb.SMOutgoingMessages_InstantMonitoring:
		handler.processInstantMonitoring(data.InstantMonitoring)

	case *pb.SMOutgoingMessages_Alert:
		handler.processAlert(data.Alert)

	case *pb.SMOutgoingMessages_RunInstancesStatus:
		handler.processRunInstanceStatus(data.RunInstancesStatus)

	case *pb.SMOutgoingMessages_UpdateInstancesStatus:
		handler.processUpdateInstancesStatus(data.UpdateInstancesStatus)

	case *pb.SMOutgoingMessages_Log:
		handler.processLogMessage(data.Log)

	case *pb.SMOutgoingMessages_OverrideEnvVarStatus:
		handler.processOverrideEnvVarsStatus(data.OverrideEnvVarStatus)

	case *pb.SMOutgoingMessages_ClockSyncRequest:
		handler.sendClockSyncResponse()

	default:
		log.Warnf("Received unprocessed message: %v", data)
	}
}

func (handler *smHandler) processRunInstanceStatus(status *pb.RunInstancesStatus) {
	runStatus := launcher.NodeRunInstanceStatus{
		NodeID:   handler.nodeConfigStatus.NodeID,
		NodeType: handler.nodeConfigStatus.NodeType,
		Instances: instancesStatusFromPB(status.GetInstances(),
			handler.nodeConfigStatus.NodeID),
	}

	handler.runStatusCh <- runStatus
}

func (handler *smHandler) processUpdateInstancesStatus(data *pb.UpdateInstancesStatus) {
	log.WithFields(log.Fields{"nodeID": handler.nodeConfigStatus.NodeID}).Debug("Receive SM update instances status")

	handler.updateInstanceStatusCh <- instancesStatusFromPB(data.GetInstances(), handler.nodeConfigStatus.NodeID)
}

func (handler *smHandler) processAlert(alert *pb.Alert) {
	log.WithFields(log.Fields{
		"nodeID": handler.nodeConfigStatus.NodeID,
		"tag":    alert.GetTag(),
	}).Debug("Receive SM alert")

	alertItem := cloudprotocol.AlertItem{
		Timestamp: alert.GetTimestamp().AsTime(),
		Tag:       alert.GetTag(),
	}

	switch data := alert.GetPayload().(type) {
	case *pb.Alert_SystemAlert:
		alertItem.Payload = cloudprotocol.SystemAlert{
			Message: data.SystemAlert.GetMessage(),
			NodeID:  handler.nodeConfigStatus.NodeID,
		}

	case *pb.Alert_CoreAlert:
		alertItem.Payload = cloudprotocol.CoreAlert{
			CoreComponent: data.CoreAlert.GetCoreComponent(),
			Message:       data.CoreAlert.GetMessage(),
			NodeID:        handler.nodeConfigStatus.NodeID,
		}

	case *pb.Alert_ResourceValidateAlert:
		resourceValidate := cloudprotocol.ResourceValidateAlert{
			Errors: make([]cloudprotocol.ErrorInfo, len(data.ResourceValidateAlert.GetErrors())),
			NodeID: handler.nodeConfigStatus.NodeID,
			Name:   data.ResourceValidateAlert.Name,
		}

		for i, error := range data.ResourceValidateAlert.GetErrors() {
			resourceValidate.Errors[i] = *pbconvert.ErrorInfoFromPB(error)
		}

		alertItem.Payload = resourceValidate

	case *pb.Alert_DeviceAllocateAlert:
		alertItem.Payload = cloudprotocol.DeviceAllocateAlert{
			InstanceIdent: pbconvert.InstanceIdentFromPB(data.DeviceAllocateAlert.GetInstance()),
			Device:        data.DeviceAllocateAlert.GetDevice(),
			Message:       data.DeviceAllocateAlert.GetMessage(),
			NodeID:        handler.nodeConfigStatus.NodeID,
		}

	case *pb.Alert_SystemQuotaAlert:
		alertPayload := cloudprotocol.SystemQuotaAlert{
			Parameter: data.SystemQuotaAlert.GetParameter(),
			Value:     data.SystemQuotaAlert.GetValue(),
			NodeID:    handler.nodeConfigStatus.NodeID,
		}

		if alertPayload.Parameter == "cpu" || alertPayload.Parameter == "ram" {
			handler.systemLimitAlertCh <- alertPayload
		}

		alertItem.Payload = alertPayload

	case *pb.Alert_InstanceQuotaAlert:
		alertItem.Payload = cloudprotocol.InstanceQuotaAlert{
			InstanceIdent: pbconvert.InstanceIdentFromPB(data.InstanceQuotaAlert.GetInstance()),
			Parameter:     data.InstanceQuotaAlert.GetParameter(),
			Value:         data.InstanceQuotaAlert.GetValue(),
		}

	case *pb.Alert_InstanceAlert:
		alertItem.Payload = cloudprotocol.ServiceInstanceAlert{
			InstanceIdent:  pbconvert.InstanceIdentFromPB(data.InstanceAlert.GetInstance()),
			ServiceVersion: data.InstanceAlert.GetServiceVersion(),
			Message:        data.InstanceAlert.GetMessage(),
		}

	default:
		log.Warn("Unsupported alert notification")
	}

	handler.alertSender.SendAlert(alertItem)
}

func (handler *smHandler) processLogMessage(data *pb.LogData) {
	log.WithFields(log.Fields{
		"nodeID":    handler.nodeConfigStatus.NodeID,
		"logID":     data.GetLogId(),
		"part":      data.GetPart(),
		"partCount": data.GetPartCount(),
	}).Debug("Receive SM push log")

	if err := handler.messageSender.SendLog(cloudprotocol.PushLog{
		NodeID:     handler.nodeConfigStatus.NodeID,
		LogID:      data.GetLogId(),
		PartsCount: data.GetPartCount(),
		Part:       data.GetPart(),
		Content:    data.GetData(),
		ErrorInfo: &cloudprotocol.ErrorInfo{
			AosCode:  int(data.GetError().AosCode),
			ExitCode: int(data.GetError().ExitCode),
			Message:  data.GetError().Message,
		},
	}); err != nil {
		log.Errorf("Can't send log: %v", err)
	}
}

func (handler *smHandler) processInstantMonitoring(instantMonitoring *pb.InstantMonitoring) {
	log.WithFields(log.Fields{"nodeID": handler.nodeConfigStatus.NodeID}).Debug("Receive SM monitoring")

	nodeMonitoring := instantMonitoringFromPB(instantMonitoring)

	nodeMonitoring.NodeID = handler.nodeConfigStatus.NodeID

	handler.monitoringSender.SendNodeMonitoring(nodeMonitoring)
}

func (handler *smHandler) processOverrideEnvVarsStatus(envVarStatus *pb.OverrideEnvVarStatus) {
	statuses := make([]cloudprotocol.EnvVarsInstanceStatus, len(envVarStatus.GetEnvVarsStatus()))

	for i, item := range envVarStatus.GetEnvVarsStatus() {
		statusItem := cloudprotocol.EnvVarsInstanceStatus{
			InstanceFilter: cloudprotocol.NewInstanceFilter(item.GetInstanceFilter().GetServiceId(),
				item.GetInstanceFilter().GetSubjectId(), item.GetInstanceFilter().GetInstance()),
			Statuses: make([]cloudprotocol.EnvVarStatus, len(item.GetStatuses())),
		}

		for j, varStatus := range item.GetStatuses() {
			statusItem.Statuses[j] = cloudprotocol.EnvVarStatus{
				Name:      varStatus.GetName(),
				ErrorInfo: pbconvert.ErrorInfoFromPB(varStatus.GetError()),
			}
		}

		statuses[i] = statusItem
	}

	if err := handler.messageSender.SendOverrideEnvVarsStatus(
		cloudprotocol.OverrideEnvVarsStatus{Statuses: statuses}); err != nil {
		log.Errorf("Can't send override env ears status: %v", err.Error())
	}
}

func (handler *smHandler) sendClockSyncResponse() {
	tm := time.Now()

	log.Debugf("Send clock sync response: %v", tm)

	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_ClockSync{
			ClockSync: &pb.ClockSync{CurrentTime: timestamppb.New(tm)},
		}}); err != nil {
		log.Errorf("Can't send clock sync response: %v", err.Error())
	}
}

func (handler *smHandler) sendGetNodeConfigStatus() error {
	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetNodeConfigStatus{}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendCheckNodeConfig(nodeConfig cloudprotocol.NodeConfig, version string) error {
	configJSON, err := json.Marshal(nodeConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_CheckNodeConfig{
		CheckNodeConfig: &pb.CheckNodeConfig{NodeConfig: string(configJSON), Version: version},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendSetNodeConfig(nodeConfig cloudprotocol.NodeConfig, version string) error {
	configJSON, err := json.Marshal(nodeConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SetNodeConfig{
		SetNodeConfig: &pb.SetNodeConfig{NodeConfig: string(configJSON), Version: version},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendGetAverageMonitoring() error {
	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetAverageMonitoring{}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendConnectionStatus(cloudConnected bool) error {
	cloudStatus := pb.ConnectionEnum_DISCONNECTED

	if cloudConnected {
		cloudStatus = pb.ConnectionEnum_CONNECTED
	}

	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_ConnectionStatus{
			ConnectionStatus: &pb.ConnectionStatus{CloudStatus: cloudStatus},
		}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func instancesStatusFromPB(pbStatuses []*pb.InstanceStatus, nodeID string) []cloudprotocol.InstanceStatus {
	instancesStatus := make([]cloudprotocol.InstanceStatus, len(pbStatuses))

	for i, status := range pbStatuses {
		instancesStatus[i] = cloudprotocol.InstanceStatus{
			InstanceIdent:  pbconvert.InstanceIdentFromPB(status.GetInstance()),
			NodeID:         nodeID,
			ServiceVersion: status.GetServiceVersion(),
			RunState:       status.GetRunState(),
			ErrorInfo:      errorInfoFromPB(status.GetErrorInfo()),
		}
	}

	return instancesStatus
}

func errorInfoFromPB(pbError *common.ErrorInfo) *cloudprotocol.ErrorInfo {
	if pbError == nil {
		return nil
	}

	return &cloudprotocol.ErrorInfo{
		AosCode: int(pbError.GetAosCode()), ExitCode: int(pbError.GetExitCode()), Message: pbError.GetMessage(),
	}
}

func averageMonitoringFromPB(averageMonitoring *pb.AverageMonitoring) aostypes.NodeMonitoring {
	nodeMonitoringData := aostypes.NodeMonitoring{
		NodeData:      monitoringDataFromPB(averageMonitoring.GetNodeMonitoring()),
		InstancesData: make([]aostypes.InstanceMonitoring, len(averageMonitoring.GetInstancesMonitoring())),
	}

	for i, pbInstanceMonitoring := range averageMonitoring.GetInstancesMonitoring() {
		nodeMonitoringData.InstancesData[i] = aostypes.InstanceMonitoring{
			InstanceIdent:  pbconvert.InstanceIdentFromPB(pbInstanceMonitoring.GetInstance()),
			MonitoringData: monitoringDataFromPB(pbInstanceMonitoring.GetMonitoringData()),
		}
	}

	return nodeMonitoringData
}

func instantMonitoringFromPB(instantMonitoring *pb.InstantMonitoring) aostypes.NodeMonitoring {
	nodeMonitoringData := aostypes.NodeMonitoring{
		NodeData:      monitoringDataFromPB(instantMonitoring.GetNodeMonitoring()),
		InstancesData: make([]aostypes.InstanceMonitoring, len(instantMonitoring.GetInstancesMonitoring())),
	}

	for i, pbInstanceMonitoring := range instantMonitoring.GetInstancesMonitoring() {
		nodeMonitoringData.InstancesData[i] = aostypes.InstanceMonitoring{
			InstanceIdent:  pbconvert.InstanceIdentFromPB(pbInstanceMonitoring.GetInstance()),
			MonitoringData: monitoringDataFromPB(pbInstanceMonitoring.GetMonitoringData()),
		}
	}

	return nodeMonitoringData
}

func monitoringDataFromPB(pbMonitoring *pb.MonitoringData) aostypes.MonitoringData {
	monitoringData := aostypes.MonitoringData{
		Timestamp:  pbMonitoring.GetTimestamp().AsTime(),
		RAM:        pbMonitoring.GetRam(),
		CPU:        pbMonitoring.GetCpu(),
		InTraffic:  pbMonitoring.GetInTraffic(),
		OutTraffic: pbMonitoring.GetOutTraffic(),
		Disk:       make([]aostypes.PartitionUsage, len(pbMonitoring.GetDisk())),
	}

	for i, pbData := range pbMonitoring.GetDisk() {
		monitoringData.Disk[i] = aostypes.PartitionUsage{Name: pbData.GetName(), UsedSize: pbData.GetUsedSize()}
	}

	return monitoringData
}

func nodeConfigStatusFromPB(pbStatus *pb.NodeConfigStatus) unitconfig.NodeConfigStatus {
	return unitconfig.NodeConfigStatus{
		NodeID:   pbStatus.GetNodeId(),
		NodeType: pbStatus.GetNodeType(),
		Version:  pbStatus.GetVersion(),
		Error:    pbconvert.ErrorInfoFromPB(pbStatus.GetError()),
	}
}
