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
	"errors"
	"io"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aosedge/aos_communicationmanager/launcher"
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
	config                 launcher.NodeInfo
	runStatusCh            chan<- launcher.NodeRunInstanceStatus
	updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus
	systemLimitAlertCh     chan<- cloudprotocol.SystemQuotaAlert
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newSMHandler(
	stream pb.SMService_RegisterSMServer, messageSender MessageSender, alertSender AlertSender,
	monitoringSender MonitoringSender, config launcher.NodeInfo,
	runStatusCh chan<- launcher.NodeRunInstanceStatus, updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus,
	systemLimitAlertCh chan<- cloudprotocol.SystemQuotaAlert,
) (*smHandler, error) {
	handler := smHandler{
		stream:                 stream,
		messageSender:          messageSender,
		alertSender:            alertSender,
		monitoringSender:       monitoringSender,
		syncstream:             syncstream.New(),
		config:                 config,
		runStatusCh:            runStatusCh,
		updateInstanceStatusCh: updateInstanceStatusCh,
		systemLimitAlertCh:     systemLimitAlertCh,
	}

	return &handler, nil
}

func (handler *smHandler) getUnitConfigState() (vendorVersion string, err error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(
		ctx, handler.sendGetUnitConfigStatus, reflect.TypeOf(&pb.SMOutgoingMessages_UnitConfigStatus{}))
	if err != nil {
		return vendorVersion, aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_UnitConfigStatus)
	if !ok {
		return "", aoserrors.New("incorrect type")
	}

	if pbStatus.UnitConfigStatus.GetError().GetMessage() != "" {
		return vendorVersion, aoserrors.New(pbStatus.UnitConfigStatus.GetError().GetMessage())
	}

	return pbStatus.UnitConfigStatus.GetVersion(), nil
}

func (handler *smHandler) checkUnitConfigState(cfg cloudprotocol.NodeConfig, vendorVersion string) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(ctx, func() error {
		return handler.sendCheckUnitConfig(cfg, vendorVersion)
	}, reflect.TypeOf(&pb.SMOutgoingMessages_UnitConfigStatus{}))
	if err != nil {
		return aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_UnitConfigStatus)
	if !ok {
		return aoserrors.New("incorrect type")
	}

	if pbStatus.UnitConfigStatus.GetError().GetMessage() != "" {
		return aoserrors.New(pbStatus.UnitConfigStatus.GetError().GetMessage())
	}

	return nil
}

func (handler *smHandler) setUnitConfig(cfg cloudprotocol.NodeConfig, vendorVersion string) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(ctx, func() error {
		return handler.sendSetUnitConfig(cfg, vendorVersion)
	}, reflect.TypeOf(&pb.SMOutgoingMessages_UnitConfigStatus{}))
	if err != nil {
		return aoserrors.Wrap(err)
	}

	pbStatus, ok := status.(*pb.SMOutgoingMessages_UnitConfigStatus)
	if !ok {
		return aoserrors.New("incorrect type")
	}

	if pbStatus.UnitConfigStatus.GetError().GetMessage() != "" {
		return aoserrors.New(pbStatus.UnitConfigStatus.GetError().GetMessage())
	}

	return nil
}

func (handler *smHandler) updateNetworks(networkParameters []aostypes.NetworkParameters) error {
	log.WithFields(log.Fields{
		"nodeID": handler.config.NodeID,
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
		"nodeID": handler.config.NodeID,
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
		"nodeID": handler.config.NodeID,
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
		"nodeID":    handler.config.NodeID,
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
		"nodeID":    handler.config.NodeID,
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
	log.WithFields(log.Fields{"nodeID": handler.config.NodeID}).Debug("Override env vars SM ")

	request := &pb.OverrideEnvVars{EnvVars: make([]*pb.OverrideInstanceEnvVar, len(envVars.OverrideEnvVars))}

	for i, item := range envVars.OverrideEnvVars {
		requestItem := &pb.OverrideInstanceEnvVar{
			InstanceFilter: pbconvert.InstanceFilterToPB(item.InstanceFilter),
			Vars:           make([]*pb.EnvVarInfo, len(item.EnvVars)),
		}

		for j, envVar := range item.EnvVars {
			requestVar := &pb.EnvVarInfo{VarId: envVar.ID, Variable: envVar.Variable}

			if envVar.TTL != nil {
				requestVar.Ttl = timestamppb.New(*envVar.TTL)
			}

			requestItem.Vars[j] = requestVar
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

func (handler *smHandler) getNodeMonitoring() (data cloudprotocol.NodeMonitoringData, err error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitMessageTimeout)
	defer cancelFunc()

	status, err := handler.syncstream.Send(
		ctx, handler.sendGetNodeMonitoring, reflect.TypeOf(&pb.SMOutgoingMessages_NodeMonitoring{}))
	if err != nil {
		return data, aoserrors.Wrap(err)
	}

	pbMonitoring, ok := status.(*pb.SMOutgoingMessages_NodeMonitoring)
	if !ok {
		return data, aoserrors.New("incorrect type")
	}

	data = nodeMonitoringFromPb(pbMonitoring.NodeMonitoring)
	data.NodeID = handler.config.NodeID

	return data, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (handler *smHandler) processSMMessages() {
	for {
		message, err := handler.stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) && codes.Canceled != status.Code(err) {
				log.Errorf("Close SM client connection error: %v", err)
			}

			return
		}

		if handler.syncstream.ProcessMessages(message.GetSMOutgoingMessage()) {
			continue
		}

		switch data := message.GetSMOutgoingMessage().(type) {
		case *pb.SMOutgoingMessages_NodeMonitoring:
			handler.processMonitoring(data.NodeMonitoring)

		case *pb.SMOutgoingMessages_Alert:
			handler.processAlert(data.Alert)

		case *pb.SMOutgoingMessages_NodeConfiguration:
			log.Errorf("Unexpected node configuration msg from %s", data.NodeConfiguration.GetNodeId())

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
			log.Warn("Received unprocessed message")
		}
	}
}

func (handler *smHandler) processRunInstanceStatus(status *pb.RunInstancesStatus) {
	runStatus := launcher.NodeRunInstanceStatus{
		NodeID: handler.config.NodeID, NodeType: handler.config.NodeType,
		Instances: instancesStatusFromPB(status.GetInstances(), handler.config.NodeID),
	}

	handler.runStatusCh <- runStatus
}

func (handler *smHandler) processUpdateInstancesStatus(data *pb.UpdateInstancesStatus) {
	log.WithFields(log.Fields{"nodeID": handler.config.NodeID}).Debug("Receive SM update instances status")

	handler.updateInstanceStatusCh <- instancesStatusFromPB(data.GetInstances(), handler.config.NodeID)
}

func (handler *smHandler) processAlert(alert *pb.Alert) {
	log.WithFields(log.Fields{
		"nodeID": handler.config.NodeID,
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
			NodeID:  handler.config.NodeID,
		}

	case *pb.Alert_CoreAlert:
		alertItem.Payload = cloudprotocol.CoreAlert{
			CoreComponent: data.CoreAlert.GetCoreComponent(),
			Message:       data.CoreAlert.GetMessage(),
			NodeID:        handler.config.NodeID,
		}

	case *pb.Alert_ResourceValidateAlert:
		resourceValidate := cloudprotocol.ResourceValidateAlert{
			Errors: make([]cloudprotocol.ErrorInfo, len(data.ResourceValidateAlert.GetErrors())),
			NodeID: handler.config.NodeID,
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
			NodeID:        handler.config.NodeID,
		}

	case *pb.Alert_SystemQuotaAlert:
		alertPayload := cloudprotocol.SystemQuotaAlert{
			Parameter: data.SystemQuotaAlert.GetParameter(),
			Value:     data.SystemQuotaAlert.GetValue(),
			NodeID:    handler.config.NodeID,
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
		"nodeID":    handler.config.NodeID,
		"logID":     data.GetLogId(),
		"part":      data.GetPart(),
		"partCount": data.GetPartCount(),
	}).Debug("Receive SM push log")

	if err := handler.messageSender.SendLog(cloudprotocol.PushLog{
		NodeID:     handler.config.NodeID,
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

func (handler *smHandler) processMonitoring(data *pb.NodeMonitoring) {
	log.WithFields(log.Fields{"nodeID": handler.config.NodeID}).Debug("Receive SM monitoring")

	nodeMonitoring := nodeMonitoringFromPb(data)

	nodeMonitoring.NodeID = handler.config.NodeID

	handler.monitoringSender.SendMonitoringData(nodeMonitoring)
}

func (handler *smHandler) processOverrideEnvVarsStatus(envVarStatus *pb.OverrideEnvVarStatus) {
	response := make([]cloudprotocol.EnvVarsInstanceStatus, len(envVarStatus.GetEnvVarsStatus()))

	for i, item := range envVarStatus.GetEnvVarsStatus() {
		responseItem := cloudprotocol.EnvVarsInstanceStatus{
			InstanceFilter: cloudprotocol.NewInstanceFilter(item.GetInstanceFilter().GetServiceId(),
				item.GetInstanceFilter().GetSubjectId(), item.GetInstanceFilter().GetInstance()),
			Statuses: make([]cloudprotocol.EnvVarStatus, len(item.GetVarsStatus())),
		}

		for j, varStatus := range item.GetVarsStatus() {
			responseItem.Statuses[j] = cloudprotocol.EnvVarStatus{
				ID:        varStatus.GetVarId(),
				ErrorInfo: pbconvert.ErrorInfoFromPB(varStatus.GetError()),
			}
		}

		response[i] = responseItem
	}

	if err := handler.messageSender.SendOverrideEnvVarsStatus(
		cloudprotocol.OverrideEnvVarsStatus{OverrideEnvVarsStatus: response}); err != nil {
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

func (handler *smHandler) sendGetUnitConfigStatus() error {
	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetUnitConfigStatus{}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendCheckUnitConfig(cfg cloudprotocol.NodeConfig, vendorVersion string) error {
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_CheckUnitConfig{
		CheckUnitConfig: &pb.CheckUnitConfig{UnitConfig: string(configJSON), Version: vendorVersion},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendSetUnitConfig(cfg cloudprotocol.NodeConfig, vendorVersion string) error {
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SetUnitConfig{
		SetUnitConfig: &pb.SetUnitConfig{UnitConfig: string(configJSON), Version: vendorVersion},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendGetNodeMonitoring() error {
	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetNodeMonitoring{}}); err != nil {
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

func nodeMonitoringFromPb(pbNodeMonitoring *pb.NodeMonitoring) cloudprotocol.NodeMonitoringData {
	nodeMonitoringData := cloudprotocol.NodeMonitoringData{
		Timestamp:        pbNodeMonitoring.GetTimestamp().AsTime(),
		MonitoringData:   monitoringDataFromPb(pbNodeMonitoring.GetMonitoringData()),
		ServiceInstances: make([]cloudprotocol.InstanceMonitoringData, len(pbNodeMonitoring.GetInstanceMonitoring())),
	}

	for i, pbInstanceMonitoring := range pbNodeMonitoring.GetInstanceMonitoring() {
		nodeMonitoringData.ServiceInstances[i] = cloudprotocol.InstanceMonitoringData{
			InstanceIdent:  pbconvert.NewInstanceIdentFromPB(pbInstanceMonitoring.GetInstance()),
			MonitoringData: monitoringDataFromPb(pbInstanceMonitoring.GetMonitoringData()),
		}
	}

	return nodeMonitoringData
}

func monitoringDataFromPb(pbMonitoring *pb.MonitoringData) cloudprotocol.MonitoringData {
	monitoringData := cloudprotocol.MonitoringData{
		RAM:        pbMonitoring.GetRam(),
		CPU:        pbMonitoring.GetCpu(),
		InTraffic:  pbMonitoring.GetInTraffic(),
		OutTraffic: pbMonitoring.GetOutTraffic(),
		Disk:       make([]cloudprotocol.PartitionUsage, len(pbMonitoring.GetDisk())),
	}

	for i, pbData := range pbMonitoring.GetDisk() {
		monitoringData.Disk[i] = cloudprotocol.PartitionUsage{Name: pbData.GetName(), UsedSize: pbData.GetUsedSize()}
	}

	return monitoringData
}
