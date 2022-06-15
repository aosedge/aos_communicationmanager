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

package smcontroller

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v2"
	"github.com/aoscloud/aos_common/utils/pbconvert"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	smRequestTimeout   = 1 * time.Minute
	smInstallTimeout   = 10 * time.Minute
	smReconnectTimeout = 10 * time.Second
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type smClient struct {
	messageSender            MessageSender
	alertSender              AlertSender
	monitoringSender         MonitoringSender
	updateInstanceStatusChan chan<- []cloudprotocol.InstanceStatus
	runStatusChan            chan<- unitstatushandler.RunInstancesStatus
	cfg                      config.SMConfig
	connection               *grpc.ClientConn
	pbClient                 pb.SMServiceClient
	ctx                      context.Context // nolint:containedctx
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newSMClient(ctx context.Context, cfg config.SMConfig,
	messageSender MessageSender, alertSender AlertSender, monitoringSender MonitoringSender,
	updateInstanceStatusChan chan<- []cloudprotocol.InstanceStatus,
	runStatus chan<- unitstatushandler.RunInstancesStatus, secureOpt grpc.DialOption,
) (client *smClient, err error) {
	client = &smClient{
		ctx:                      ctx,
		cfg:                      cfg,
		messageSender:            messageSender,
		alertSender:              alertSender,
		monitoringSender:         monitoringSender,
		updateInstanceStatusChan: updateInstanceStatusChan,
		runStatusChan:            runStatus,
	}

	log.WithFields(log.Fields{"url": client.cfg.ServerURL, "id": client.cfg.SMID}).Debugf("Connecting to SM...")

	defer func() {
		if err != nil {
			client.close()
			client = nil
		}
	}()

	if client.connection, err = grpc.DialContext(ctx, client.cfg.ServerURL, secureOpt, grpc.WithBlock()); err != nil {
		return client, aoserrors.Wrap(err)
	}

	client.pbClient = pb.NewSMServiceClient(client.connection)

	log.WithFields(log.Fields{"url": client.cfg.ServerURL, "id": client.cfg.SMID}).Debugf("Connected to SM")

	go client.handleSMNotifications()

	return client, nil
}

func (client *smClient) close() (err error) {
	log.WithField("id", client.cfg.SMID).Debugf("Disconnect from SM")

	if client.connection != nil {
		client.connection.Close()
	}

	return nil
}

func (client *smClient) getServicesStatus() ([]unitstatushandler.ServiceStatus, error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Get SM all services")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	status, err := client.pbClient.GetServicesStatus(ctx, &empty.Empty{})
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	servicesStatus := make([]unitstatushandler.ServiceStatus, len(status.Services))

	for i, service := range status.Services {
		servicesStatus[i] = unitstatushandler.ServiceStatus{
			ServiceStatus: cloudprotocol.ServiceStatus{
				ID:         service.ServiceId,
				AosVersion: service.AosVersion,
				Status:     cloudprotocol.InstalledStatus,
			}, Cached: service.Cached,
		}
	}

	return servicesStatus, nil
}

func (client *smClient) getLayersStatus() ([]unitstatushandler.LayerStatus, error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Get SM layer status")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	status, err := client.pbClient.GetLayersStatus(ctx, &empty.Empty{})
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	layersStatus := make([]unitstatushandler.LayerStatus, len(status.Layers))

	for i, layer := range status.Layers {
		layersStatus[i] = unitstatushandler.LayerStatus{
			LayerStatus: cloudprotocol.LayerStatus{
				ID:         layer.LayerId,
				AosVersion: layer.AosVersion,
				Digest:     layer.Digest,
				Status:     cloudprotocol.InstalledStatus,
			}, Cached: layer.Cached,
		}
	}

	return layersStatus, nil
}

func (client *smClient) getBoardConfigStatus() (vendorVersion string, err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Get SM board config status")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	boardConfigStatus, err := client.pbClient.GetBoardConfigStatus(ctx, &empty.Empty{})
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return boardConfigStatus.VendorVersion, nil
}

func (client *smClient) checkBoardConfig(boardConfig aostypes.BoardConfig) error {
	log.WithFields(log.Fields{
		"id":            client.cfg.SMID,
		"vendorVersion": boardConfig.VendorVersion,
	}).Debug("Check SM board config")

	configJSON, err := json.Marshal(boardConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	if _, err := client.pbClient.CheckBoardConfig(ctx, &pb.BoardConfig{BoardConfig: string(configJSON)}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) setBoardConfig(boardConfig aostypes.BoardConfig) error {
	log.WithFields(log.Fields{
		"id":            client.cfg.SMID,
		"vendorVersion": boardConfig.VendorVersion,
	}).Debug("Set SM board config")

	configJSON, err := json.Marshal(boardConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.SetBoardConfig(ctx, &pb.BoardConfig{BoardConfig: string(configJSON)}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) runInstances(instances []cloudprotocol.InstanceInfo) error {
	log.WithFields(log.Fields{
		"id": client.cfg.SMID,
	}).Debug("Run SM instances")

	ctx, cancel := context.WithTimeout(client.ctx, smInstallTimeout)
	defer cancel()

	runRequest := &pb.RunInstancesRequest{Instances: make([]*pb.RunInstanceRequest, len(instances))}

	for i, instance := range instances {
		runRequest.Instances[i] = &pb.RunInstanceRequest{
			ServiceId: instance.ServiceID, SubjectId: instance.SubjectID,
			NumInstances: instance.NumInstances,
		}
	}

	if _, err := client.pbClient.RunInstances(ctx, runRequest); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) installService(serviceInfo cloudprotocol.ServiceInfo) error {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": serviceInfo.ID,
	}).Debug("Install SM service")

	ctx, cancel := context.WithTimeout(client.ctx, smInstallTimeout)
	defer cancel()

	if _, err := client.pbClient.InstallService(ctx, &pb.InstallServiceRequest{
		Url:           serviceInfo.URLs[0],
		ServiceId:     serviceInfo.ID,
		ProviderId:    serviceInfo.ProviderID,
		AosVersion:    serviceInfo.AosVersion,
		VendorVersion: serviceInfo.VendorVersion,
		Description:   serviceInfo.Description,
		Sha256:        serviceInfo.Sha256,
		Sha512:        serviceInfo.Sha512,
		Size:          serviceInfo.Size,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) restoreService(serviceID string) error {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": serviceID,
	}).Debug("Restore SM service")

	ctx, cancel := context.WithTimeout(client.ctx, smInstallTimeout)
	defer cancel()

	if _, err := client.pbClient.RestoreService(ctx, &pb.RestoreServiceRequest{ServiceId: serviceID}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) removeService(serviceID string) error {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": serviceID,
	}).Debug("Remove SM service")

	ctx, cancel := context.WithTimeout(client.ctx, smInstallTimeout)
	defer cancel()

	if _, err := client.pbClient.RemoveService(ctx, &pb.RemoveServiceRequest{ServiceId: serviceID}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) installLayer(layerInfo cloudprotocol.LayerInfo) (err error) {
	log.WithFields(log.Fields{
		"id":      client.cfg.SMID,
		"layerID": layerInfo.ID,
		"digest":  layerInfo.Digest,
	}).Debug("Install SM layer")

	ctx, cancel := context.WithTimeout(client.ctx, smInstallTimeout)
	defer cancel()

	if _, err = client.pbClient.InstallLayer(ctx, &pb.InstallLayerRequest{
		Url:           layerInfo.URLs[0],
		LayerId:       layerInfo.ID,
		AosVersion:    layerInfo.AosVersion,
		VendorVersion: layerInfo.VendorVersion,
		Digest:        layerInfo.Digest,
		Description:   layerInfo.Description,
		Sha256:        layerInfo.Sha256,
		Sha512:        layerInfo.Sha512,
		Size:          layerInfo.Size,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) instanceStateAcceptance(stateAcceptance cloudprotocol.StateAcceptance) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": stateAcceptance.ServiceID,
		"checksum":  stateAcceptance.Checksum,
		"result":    stateAcceptance.Result,
		"reason":    stateAcceptance.Reason,
	}).Debug("SM service state acceptance")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.InstanceStateAcceptance(ctx, &pb.StateAcceptance{
		Instance:      pbconvert.InstanceIdentToPB(stateAcceptance.InstanceIdent),
		StateChecksum: stateAcceptance.Checksum,
		Result:        stateAcceptance.Result,
		Reason:        stateAcceptance.Reason,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) setInstanceState(state cloudprotocol.UpdateState) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": state.ServiceID,
		"checksum":  state.Checksum,
	}).Debug("SM set instance state")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.SetInstanceState(ctx, &pb.InstanceState{
		Instance:      pbconvert.InstanceIdentToPB(state.InstanceIdent),
		State:         []byte(state.State),
		StateChecksum: state.Checksum,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) overrideEnvVars(envVars cloudprotocol.DecodedOverrideEnvVars) (err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("override env vars SM ")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	request := &pb.OverrideEnvVarsRequest{EnvVars: make([]*pb.OverrideInstanceEnvVar, len(envVars.OverrideEnvVars))}

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

	envVarStatus, err := client.pbClient.OverrideEnvVars(ctx, request)
	if err != nil {
		return aoserrors.Wrap(err)
	}

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

	if err = client.messageSender.SendOverrideEnvVarsStatus(
		cloudprotocol.OverrideEnvVarsStatus{OverrideEnvVarsStatus: response}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) getSystemLog(logRequest cloudprotocol.RequestSystemLog) (err error) {
	log.WithFields(log.Fields{
		"id":    client.cfg.SMID,
		"logID": logRequest.LogID,
		"from":  logRequest.From,
		"till":  logRequest.Till,
	}).Debug("Get SM system log")

	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	request := &pb.SystemLogRequest{LogId: logRequest.LogID}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if _, err = client.pbClient.GetSystemLog(ctx, request); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) getInstanceLog(logRequest cloudprotocol.RequestServiceLog) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get instance log")

	return aoserrors.Wrap(client.sendGetLogRequest(logRequest, client.pbClient.GetInstanceLog))
}

func (client *smClient) getInstanceCrashLog(logRequest cloudprotocol.RequestServiceCrashLog) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get instance crash log")

	return aoserrors.Wrap(
		client.sendGetLogRequest((cloudprotocol.RequestServiceLog)(logRequest), client.pbClient.GetInstanceCrashLog))
}

func (client *smClient) handleSMNotifications() {
	for {
		if err := client.subscribeSMNotifications(); err != nil {
			if client.ctx.Err() == nil {
				log.Errorf("Error subscribe to SM notifications: %s", err)
				log.Debugf("Reconnect to SM in %v...", smReconnectTimeout)
			}
		}

		select {
		case <-client.ctx.Done():
			return

		case <-time.After(smReconnectTimeout):
		}
	}
}

func (client *smClient) sendGetLogRequest(logRequest cloudprotocol.RequestServiceLog,
	pbCall func(context.Context, *pb.InstanceLogRequest, ...grpc.CallOption) (*empty.Empty, error),
) (err error) {
	ctx, cancel := context.WithTimeout(client.ctx, smRequestTimeout)
	defer cancel()

	request := &pb.InstanceLogRequest{
		LogId: logRequest.LogID, Instance: pbconvert.InstanceFilterToPB(logRequest.InstanceFilter),
	}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if _, err = pbCall(ctx, request); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) subscribeSMNotifications() (err error) {
	log.Debug("Subscribe to SM notifications")

	stream, err := client.pbClient.SubscribeSMNotifications(client.ctx, &empty.Empty{})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for {
		notification, err := stream.Recv()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		switch data := notification.SMNotification.(type) {
		case *pb.SMNotifications_Alert:
			client.processAlertNotification(data)

		case *pb.SMNotifications_Monitoring:
			client.processMonitoringNotification(data)

		case *pb.SMNotifications_NewInstanceState:
			client.processNewInstanceSateNotification(data)

		case *pb.SMNotifications_InstanceStateRequest:
			client.processInstanceSateRequestNotification(data)

		case *pb.SMNotifications_Log:
			client.processSMLogNotification(data)

		case *pb.SMNotifications_RunInstancesStatus:
			client.processRunInstancesStatus(data)

		case *pb.SMNotifications_UpdateInstancesStatus:
			client.processUpdateInstancesStatus(data)

		default:
			log.Warnf("Receive unsupported SM notification: %s", reflect.TypeOf(data))
		}
	}
}

func (client *smClient) processAlertNotification(data *pb.SMNotifications_Alert) {
	log.WithFields(log.Fields{
		"id":  client.cfg.SMID,
		"tag": data.Alert.Tag,
	}).Debug("Receive SM alert")

	alertItem := cloudprotocol.AlertItem{
		Timestamp: data.Alert.Timestamp.AsTime(),
		Tag:       data.Alert.Tag,
	}

	switch data := data.Alert.Payload.(type) {
	case *pb.Alert_SystemAlert:
		alertItem.Payload = cloudprotocol.SystemAlert{Message: data.SystemAlert.Message}

	case *pb.Alert_CoreAlert:
		alertItem.Payload = cloudprotocol.CoreAlert{
			CoreComponent: data.CoreAlert.CoreComponent,
			Message:       data.CoreAlert.Message,
		}

	case *pb.Alert_ResourceValidateAlert:
		resourceValidate := cloudprotocol.ResourceValidateAlert{
			ResourcesErrors: make([]cloudprotocol.ResourceValidateError, len(data.ResourceValidateAlert.Errors)),
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
		}

	case *pb.Alert_SystemQuotaAlert:
		alertItem.Payload = cloudprotocol.SystemQuotaAlert{
			Parameter: data.SystemQuotaAlert.Parameter,
			Value:     data.SystemQuotaAlert.Value,
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
		log.Warn("Unsupported alert alert notification")
	}

	client.alertSender.SendAlert(alertItem)
}

func (client *smClient) processMonitoringNotification(data *pb.SMNotifications_Monitoring) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Receive SM monitoring")

	monitoringData := cloudprotocol.MonitoringData{
		Timestamp: data.Monitoring.Timestamp.AsTime(),
		Global: cloudprotocol.GlobalMonitoringData{
			RAM:        data.Monitoring.SystemMonitoring.Ram,
			CPU:        data.Monitoring.SystemMonitoring.Cpu,
			UsedDisk:   data.Monitoring.SystemMonitoring.UsedDisk,
			InTraffic:  data.Monitoring.SystemMonitoring.InTraffic,
			OutTraffic: data.Monitoring.SystemMonitoring.OutTraffic,
		},
		ServiceInstances: make([]cloudprotocol.InstanceMonitoringData, len(data.Monitoring.InstanceMonitoring)),
	}

	for i, instanceMonitoring := range data.Monitoring.InstanceMonitoring {
		monitoringData.ServiceInstances[i] = cloudprotocol.InstanceMonitoringData{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(instanceMonitoring.Instance),
			RAM:           instanceMonitoring.Ram,
			CPU:           instanceMonitoring.Cpu,
			UsedDisk:      instanceMonitoring.UsedDisk,
			InTraffic:     instanceMonitoring.InTraffic,
			OutTraffic:    instanceMonitoring.OutTraffic,
		}
	}

	client.monitoringSender.SendMonitoringData(monitoringData)
}

func (client *smClient) processNewInstanceSateNotification(data *pb.SMNotifications_NewInstanceState) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": data.NewInstanceState.State.Instance.ServiceId,
	}).Debug("Receive SM new instance state")

	if err := client.messageSender.SendInstanceNewState(cloudprotocol.NewState{
		InstanceIdent: pbconvert.NewInstanceIdentFromPB(data.NewInstanceState.State.Instance),
		Checksum:      data.NewInstanceState.State.StateChecksum,
		State:         string(data.NewInstanceState.State.State),
	}); err != nil {
		log.Errorf("Can't send service new state: %s", err)
	}
}

func (client *smClient) processInstanceSateRequestNotification(data *pb.SMNotifications_InstanceStateRequest) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": data.InstanceStateRequest.Instance.ServiceId,
		"default":   data.InstanceStateRequest.Default,
	}).Debug("Receive SM instance state request")

	if err := client.messageSender.SendInstanceStateRequest(cloudprotocol.StateRequest{
		InstanceIdent: pbconvert.NewInstanceIdentFromPB(data.InstanceStateRequest.Instance),
		Default:       data.InstanceStateRequest.Default,
	}); err != nil {
		log.Errorf("Can't send instance state request: %s", err)
	}
}

func (client *smClient) processSMLogNotification(data *pb.SMNotifications_Log) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"logID":     data.Log.LogId,
		"part":      data.Log.Part,
		"partCount": data.Log.PartCount,
	}).Debug("Receive SM push log")

	if err := client.messageSender.SendLog(cloudprotocol.PushLog{
		LogID:     data.Log.LogId,
		PartCount: data.Log.PartCount,
		Part:      data.Log.Part,
		Data:      data.Log.Data,
		Error:     data.Log.Error,
	}); err != nil {
		log.Errorf("Can't send log: %s", err)
	}
}

func (client *smClient) processRunInstancesStatus(data *pb.SMNotifications_RunInstancesStatus) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Receive SM run instances status")

	if data.RunInstancesStatus.UnitSubjects == nil {
		data.RunInstancesStatus.UnitSubjects = make([]string, 0)
	}

	runStatus := unitstatushandler.RunInstancesStatus{
		UnitSubjects:  data.RunInstancesStatus.UnitSubjects,
		Instances:     instancesStatusFromPB(data.RunInstancesStatus.Instances),
		ErrorServices: make([]cloudprotocol.ServiceStatus, len(data.RunInstancesStatus.ErrorServices)),
	}

	for i, serviceStatus := range data.RunInstancesStatus.ErrorServices {
		runStatus.ErrorServices[i] = cloudprotocol.ServiceStatus{
			ID: serviceStatus.ServiceId, AosVersion: serviceStatus.AosVersion,
			Status:    cloudprotocol.ErrorStatus,
			ErrorInfo: errorInfoFromPB(serviceStatus.ErrorInfo),
		}
	}

	client.runStatusChan <- runStatus
}

func (client *smClient) processUpdateInstancesStatus(data *pb.SMNotifications_UpdateInstancesStatus) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Receive SM update instances status")

	client.updateInstanceStatusChan <- instancesStatusFromPB(data.UpdateInstancesStatus.Instances)
}

func instancesStatusFromPB(pbStatuses []*pb.InstanceStatus) []cloudprotocol.InstanceStatus {
	instancesStaus := make([]cloudprotocol.InstanceStatus, len(pbStatuses))

	for i, status := range pbStatuses {
		instancesStaus[i] = cloudprotocol.InstanceStatus{
			InstanceIdent: pbconvert.NewInstanceIdentFromPB(status.Instance),
			AosVersion:    status.AosVersion,
			StateChecksum: status.StateChecksum,
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
