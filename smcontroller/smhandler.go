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

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v3"
	"github.com/aoscloud/aos_common/utils/pbconvert"
	"github.com/aoscloud/aos_common/utils/syncstream"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aoscloud/aos_communicationmanager/launcher"
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
	config                 launcher.NodeConfiguration
	runStatusCh            chan<- launcher.NodeRunInstanceStatus
	updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newSMHandler(
	stream pb.SMService_RegisterSMServer, messageSender MessageSender, alertSender AlertSender,
	monitoringSender MonitoringSender, config launcher.NodeConfiguration,
	runStatusCh chan<- launcher.NodeRunInstanceStatus, updateInstanceStatusCh chan<- []cloudprotocol.InstanceStatus,
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

	if pbStatus.UnitConfigStatus.Error != "" {
		return vendorVersion, aoserrors.New(pbStatus.UnitConfigStatus.Error)
	}

	return pbStatus.UnitConfigStatus.VendorVersion, nil
}

func (handler *smHandler) checkUnitConfigState(cfg aostypes.NodeUnitConfig, vendorVersion string) error {
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

	if pbStatus.UnitConfigStatus.Error != "" {
		return aoserrors.New(pbStatus.UnitConfigStatus.Error)
	}

	return nil
}

func (handler *smHandler) setUnitConfig(cfg aostypes.NodeUnitConfig, vendorVersion string) error {
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

	if pbStatus.UnitConfigStatus.Error != "" {
		return aoserrors.New(pbStatus.UnitConfigStatus.Error)
	}

	return nil
}

func (handler *smHandler) runInstances(
	services []aostypes.ServiceInfo, layers []aostypes.LayerInfo, instances []aostypes.InstanceInfo,
) error {
	log.WithFields(log.Fields{
		"nodeID": handler.config.NodeID,
	}).Debug("SM run instances")

	pbRunInstances := &pb.RunInstances{
		Services:  make([]*pb.ServiceInfo, len(services)),
		Layers:    make([]*pb.LayerInfo, len(layers)),
		Instances: make([]*pb.InstanceInfo, len(instances)),
	}

	for i, serviceInfo := range services {
		pbRunInstances.Services[i] = &pb.ServiceInfo{
			VersionInfo: &pb.VesionInfo{
				AosVersion:    serviceInfo.AosVersion,
				VendorVersion: serviceInfo.VendorVersion,
				Description:   serviceInfo.Description,
			},
			Url:        serviceInfo.URL,
			ServiceId:  serviceInfo.ID,
			ProviderId: serviceInfo.ProviderID,
			Gid:        serviceInfo.GID,
			Sha256:     serviceInfo.Sha256,
			Sha512:     serviceInfo.Sha512,
			Size:       serviceInfo.Size,
		}
	}

	for i, layerInfo := range layers {
		pbRunInstances.Layers[i] = &pb.LayerInfo{
			VersionInfo: &pb.VesionInfo{
				AosVersion:    layerInfo.AosVersion,
				VendorVersion: layerInfo.VendorVersion,
				Description:   layerInfo.Description,
			},
			Url:     layerInfo.URL,
			LayerId: layerInfo.ID,
			Digest:  layerInfo.Digest,
			Sha256:  layerInfo.Sha256,
			Sha512:  layerInfo.Sha512,
			Size:    layerInfo.Size,
		}
	}

	for i, instanceInfo := range instances {
		pbRunInstances.Instances[i] = &pb.InstanceInfo{
			Instance:    pbconvert.InstanceIdentToPB(instanceInfo.InstanceIdent),
			Uid:         instanceInfo.UID,
			Priority:    instanceInfo.Priority,
			StoragePath: instanceInfo.StoragePath,
			StatePath:   instanceInfo.StatePath,
		}
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_RunInstances{
		RunInstances: pbRunInstances,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) getSystemLog(logRequest cloudprotocol.RequestSystemLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID": handler.config.NodeID,
		"logID":  logRequest.LogID,
		"from":   logRequest.From,
		"till":   logRequest.Till,
	}).Debug("Get SM system log")

	request := &pb.SystemLogRequest{LogId: logRequest.LogID}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SystemLogRequest{
		SystemLogRequest: request,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// nolint:dupl
func (handler *smHandler) getInstanceLog(logRequest cloudprotocol.RequestServiceLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID":    handler.config.NodeID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get instance log")

	request := &pb.InstanceLogRequest{
		LogId: logRequest.LogID, Instance: pbconvert.InstanceFilterToPB(logRequest.InstanceFilter),
	}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_InstanceLogRequest{
		InstanceLogRequest: request,
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// nolint:dupl
func (handler *smHandler) getInstanceCrashLog(logRequest cloudprotocol.RequestServiceCrashLog) (err error) {
	log.WithFields(log.Fields{
		"nodeID":    handler.config.NodeID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get instance crash log")

	request := &pb.InstanceCrashLogRequest{
		LogId: logRequest.LogID, Instance: pbconvert.InstanceFilterToPB(logRequest.InstanceFilter),
	}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
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
			Instance: pbconvert.InstanceFilterToPB(item.InstanceFilter),
			Vars:     make([]*pb.EnvVarInfo, len(item.EnvVars)),
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

		if handler.syncstream.ProcessMessages(message.SMOutgoingMessage) {
			continue
		}

		switch data := message.SMOutgoingMessage.(type) {
		case *pb.SMOutgoingMessages_NodeMonitoring:
			handler.processMonitoring(data.NodeMonitoring)

		case *pb.SMOutgoingMessages_Alert:
			handler.processAlert(data.Alert)

		case *pb.SMOutgoingMessages_NodeConfiguration:
			log.Errorf("Unexpected node configuration msg from %s", data.NodeConfiguration.NodeId)

		case *pb.SMOutgoingMessages_RunInstancesStatus:
			handler.processRunInstanceStatus(data.RunInstancesStatus)

		case *pb.SMOutgoingMessages_UpdateInstancesStatus:
			handler.processUpdateInstancesStatus(data.UpdateInstancesStatus)

		case *pb.SMOutgoingMessages_Log:
			handler.proccLogMessage(data.Log)

		case *pb.SMOutgoingMessages_OverrideEnvVarStatus:
			handler.processOverrideEnvVarsStatus(data.OverrideEnvVarStatus)

		default:
			log.Warn("Received unprocessed message")
		}
	}
}

func (handler *smHandler) processRunInstanceStatus(status *pb.RunInstancesStatus) {
	runStatus := launcher.NodeRunInstanceStatus{
		NodeID: handler.config.NodeID, NodeType: handler.config.NodeType,
		Instances: instancesStatusFromPB(status.Instances),
	}

	handler.runStatusCh <- runStatus
}

func (handler *smHandler) processUpdateInstancesStatus(data *pb.UpdateInstancesStatus) {
	log.WithFields(log.Fields{"nodeID": handler.config.NodeID}).Debug("Receive SM update instances status")

	handler.updateInstanceStatusCh <- instancesStatusFromPB(data.Instances)
}

func (handler *smHandler) processAlert(alert *pb.Alert) {
	log.WithFields(log.Fields{
		"nodeID": handler.config.NodeID,
		"tag":    alert.Tag,
	}).Debug("Receive SM alert")

	alertItem := cloudprotocol.AlertItem{
		Timestamp: alert.Timestamp.AsTime(),
		Tag:       alert.Tag,
	}

	switch data := alert.Payload.(type) {
	case *pb.Alert_SystemAlert:
		alertItem.Payload = cloudprotocol.SystemAlert{
			Message: data.SystemAlert.Message,
			NodeID:  handler.config.NodeID,
		}

	case *pb.Alert_CoreAlert:
		alertItem.Payload = cloudprotocol.CoreAlert{
			CoreComponent: data.CoreAlert.CoreComponent,
			Message:       data.CoreAlert.Message,
			NodeID:        handler.config.NodeID,
		}

	case *pb.Alert_ResourceValidateAlert:
		resourceValidate := cloudprotocol.ResourceValidateAlert{
			ResourcesErrors: make([]cloudprotocol.ResourceValidateError, len(data.ResourceValidateAlert.Errors)),
			NodeID:          handler.config.NodeID,
		}

		for i, resourceError := range data.ResourceValidateAlert.Errors {
			resourceValidate.ResourcesErrors[i] = cloudprotocol.ResourceValidateError{
				Name:   resourceError.Name,
				Errors: resourceError.ErrorMsg,
			}
		}

		alertItem.Payload = resourceValidate

	case *pb.Alert_DeviceAllocateAlert:
		alertItem.Payload = cloudprotocol.DeviceAllocateAlert{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(data.DeviceAllocateAlert.Instance),
			Device:        data.DeviceAllocateAlert.Device,
			Message:       data.DeviceAllocateAlert.Message,
			NodeID:        handler.config.NodeID,
		}

	case *pb.Alert_SystemQuotaAlert:
		alertItem.Payload = cloudprotocol.SystemQuotaAlert{
			Parameter: data.SystemQuotaAlert.Parameter,
			Value:     data.SystemQuotaAlert.Value,
			NodeID:    handler.config.NodeID,
		}

	case *pb.Alert_InstanceQuotaAlert:
		alertItem.Payload = cloudprotocol.InstanceQuotaAlert{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(data.InstanceQuotaAlert.Instance),
			Parameter:     data.InstanceQuotaAlert.Parameter,
			Value:         data.InstanceQuotaAlert.Value,
		}

	case *pb.Alert_InstanceAlert:
		alertItem.Payload = cloudprotocol.ServiceInstanceAlert{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(data.InstanceAlert.Instance),
			AosVersion:    data.InstanceAlert.AosVersion,
			Message:       data.InstanceAlert.Message,
		}

	default:
		log.Warn("Unsupported alert notification")
	}

	handler.alertSender.SendAlert(alertItem)
}

func (handler *smHandler) proccLogMessage(data *pb.LogData) {
	log.WithFields(log.Fields{
		"nodeID":    handler.config.NodeID,
		"logID":     data.LogId,
		"part":      data.Part,
		"partCount": data.PartCount,
	}).Debug("Receive SM push log")

	if err := handler.messageSender.SendLog(cloudprotocol.PushLog{
		LogID:     data.LogId,
		PartCount: data.PartCount,
		Part:      data.Part,
		Data:      data.Data,
		Error:     data.Error,
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
	response := make([]cloudprotocol.EnvVarsInstanceStatus, len(envVarStatus.EnvVarsStatus))

	for i, item := range envVarStatus.EnvVarsStatus {
		responseItem := cloudprotocol.EnvVarsInstanceStatus{
			InstanceFilter: cloudprotocol.NewInstanceFilter(item.Instance.ServiceId,
				item.Instance.SubjectId, item.Instance.Instance),
			Statuses: make([]cloudprotocol.EnvVarStatus, len(item.VarsStatus)),
		}

		for j, varStatus := range item.VarsStatus {
			responseItem.Statuses[j] = cloudprotocol.EnvVarStatus{ID: varStatus.VarId, Error: varStatus.Error}
		}

		response[i] = responseItem
	}

	if err := handler.messageSender.SendOverrideEnvVarsStatus(
		cloudprotocol.OverrideEnvVarsStatus{OverrideEnvVarsStatus: response}); err != nil {
		log.Errorf("Can't send override env ears status: %v", err.Error())
	}
}

func (handler *smHandler) sendGetUnitConfigStatus() error {
	if err := handler.stream.Send(
		&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_GetUnitConfigStatus{}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendCheckUnitConfig(cfg aostypes.NodeUnitConfig, vendorVersion string) error {
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_CheckUnitConfig{
		CheckUnitConfig: &pb.CheckUnitConfig{UnitConfig: string(configJSON), VendorVersion: vendorVersion},
	}}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (handler *smHandler) sendSetUnitConfig(cfg aostypes.NodeUnitConfig, vendorVersion string) error {
	configJSON, err := json.Marshal(cfg)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := handler.stream.Send(&pb.SMIncomingMessages{SMIncomingMessage: &pb.SMIncomingMessages_SetUnitConfig{
		SetUnitConfig: &pb.SetUnitConfig{UnitConfig: string(configJSON), VendorVersion: vendorVersion},
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

func instancesStatusFromPB(pbStatuses []*pb.InstanceStatus) []cloudprotocol.InstanceStatus {
	instancesStaus := make([]cloudprotocol.InstanceStatus, len(pbStatuses))

	for i, status := range pbStatuses {
		instancesStaus[i] = cloudprotocol.InstanceStatus{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(status.Instance),
			AosVersion:    status.AosVersion,
			RunState:      status.RunState,
			ErrorInfo:     errorInfoFromPB(status.ErrorInfo),
		}
	}

	return instancesStaus
}

func errorInfoFromPB(pbError *pb.ErrorInfo) *cloudprotocol.ErrorInfo {
	if pbError == nil {
		return nil
	}

	return &cloudprotocol.ErrorInfo{
		AosCode:  int(pbError.AosCode),
		ExitCode: int(pbError.ExitCode), Message: pbError.Message,
	}
}

func nodeMonitoringFromPb(pbNodeMonitoring *pb.NodeMonitoring) cloudprotocol.NodeMonitoringData {
	nodeMonitoringData := cloudprotocol.NodeMonitoringData{
		Timestamp:        pbNodeMonitoring.Timestamp.AsTime(),
		MonitoringData:   monitoringDataFromPb(pbNodeMonitoring.MonitoringData),
		ServiceInstances: make([]cloudprotocol.InstanceMonitoringData, len(pbNodeMonitoring.InstanceMonitoring)),
	}

	for i, pbInstanceMonitoring := range pbNodeMonitoring.InstanceMonitoring {
		nodeMonitoringData.ServiceInstances[i] = cloudprotocol.InstanceMonitoringData{
			InstanceIdent:  pbconvert.NewInstanceIdentFromPB(pbInstanceMonitoring.Instance),
			MonitoringData: monitoringDataFromPb(pbInstanceMonitoring.MonitoringData),
		}
	}

	return nodeMonitoringData
}

func monitoringDataFromPb(pbMonitoring *pb.MonitoringData) cloudprotocol.MonitoringData {
	monitoringData := cloudprotocol.MonitoringData{
		RAM:        pbMonitoring.Ram,
		CPU:        pbMonitoring.Cpu,
		InTraffic:  pbMonitoring.InTraffic,
		OutTraffic: pbMonitoring.OutTraffic,
		Disk:       make([]cloudprotocol.PartitionUsage, len(pbMonitoring.Disk)),
	}

	for i, pbData := range pbMonitoring.Disk {
		monitoringData.Disk[i] = cloudprotocol.PartitionUsage{Name: pbData.Name, UsedSize: pbData.UsedSize}
	}

	return monitoringData
}
