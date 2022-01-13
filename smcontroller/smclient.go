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
	pb "github.com/aoscloud/aos_common/api/servicemanager/v1"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/aoscloud/aos_communicationmanager/boardconfig"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
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
	messageSender    MessageSender
	alertSender      AlertSender
	monitoringSender MonitoringSender
	cfg              config.SMConfig
	connection       *grpc.ClientConn
	pbClient         pb.SMServiceClient
	context          context.Context
}

type clientBoardConfig struct {
	FormatVersion uint64                       `json:"formatVersion"`
	VendorVersion string                       `json:"vendorVersion"`
	Devices       []boardconfig.DeviceResource `json:"devices"`
	Resources     []boardconfig.BoardResource  `json:"resources"`
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newSMClient(ctx context.Context, cfg config.SMConfig,
	messageSender MessageSender, alertSender AlertSender, monitoringSender MonitoringSender,
	secureOpt grpc.DialOption) (client *smClient, err error) {
	client = &smClient{
		context:          ctx,
		cfg:              cfg,
		messageSender:    messageSender,
		alertSender:      alertSender,
		monitoringSender: monitoringSender,
	}

	log.WithFields(log.Fields{"url": client.cfg.ServerURL, "id": client.cfg.SMID}).Debugf("Connecting to SM...")

	defer func() {
		if err != nil {
			client.close()
			client = nil
		}
	}()

	if client.connection, err = grpc.DialContext(
		client.context, client.cfg.ServerURL, secureOpt, grpc.WithBlock()); err != nil {
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

func (client *smClient) getUsersStatus(users []string) (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID, "users": users}).Debug("Get SM users status")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	status, err := client.pbClient.GetUsersStatus(ctx, &pb.Users{Users: users})
	if err != nil {
		return nil, nil, aoserrors.Wrap(err)
	}

	for _, service := range status.Services {
		servicesInfo = append(servicesInfo, cloudprotocol.ServiceInfo{
			ID:            service.ServiceId,
			AosVersion:    service.AosVersion,
			Status:        cloudprotocol.InstalledStatus,
			StateChecksum: service.StateChecksum,
		})
	}

	for _, layer := range status.Layers {
		layersInfo = append(layersInfo, cloudprotocol.LayerInfo{
			ID:         layer.LayerId,
			AosVersion: layer.AosVersion,
			Digest:     layer.Digest,
			Status:     cloudprotocol.InstalledStatus,
		})
	}

	return servicesInfo, layersInfo, nil
}

func (client *smClient) getAllStatus() (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Get SM all status")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	status, err := client.pbClient.GetAllStatus(ctx, &empty.Empty{})
	if err != nil {
		return nil, nil, aoserrors.Wrap(err)
	}

	for _, service := range status.Services {
		servicesInfo = append(servicesInfo, cloudprotocol.ServiceInfo{
			ID:            service.ServiceId,
			AosVersion:    service.AosVersion,
			Status:        cloudprotocol.InstalledStatus,
			StateChecksum: service.StateChecksum,
		})
	}

	for _, layer := range status.Layers {
		layersInfo = append(layersInfo, cloudprotocol.LayerInfo{
			ID:         layer.LayerId,
			AosVersion: layer.AosVersion,
			Digest:     layer.Digest,
			Status:     cloudprotocol.InstalledStatus,
		})
	}

	return servicesInfo, layersInfo, nil
}

func (client *smClient) getBoardConfigStatus() (vendorVersion string, err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("Get SM board config status")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	boardConfigStatus, err := client.pbClient.GetBoardConfigStatus(ctx, &empty.Empty{})
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return boardConfigStatus.VendorVersion, nil
}

func (client *smClient) checkBoardConfig(boardConfig clientBoardConfig) (vendorVersion string, err error) {
	log.WithFields(log.Fields{
		"id":            client.cfg.SMID,
		"vendorVersion": boardConfig.VendorVersion,
	}).Debug("Check SM board config")

	configJSON, err := json.Marshal(boardConfig)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	boardConfigStatus, err := client.pbClient.CheckBoardConfig(ctx, &pb.BoardConfig{BoardConfig: string(configJSON)})
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return boardConfigStatus.VendorVersion, nil
}

func (client *smClient) setBoardConfig(boardConfig clientBoardConfig) (err error) {
	log.WithFields(log.Fields{
		"id":            client.cfg.SMID,
		"vendorVersion": boardConfig.VendorVersion,
	}).Debug("Set SM board config")

	configJSON, err := json.Marshal(boardConfig)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.SetBoardConfig(ctx, &pb.BoardConfig{BoardConfig: string(configJSON)}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) installService(users []string,
	serviceInfo cloudprotocol.ServiceInfoFromCloud) (stateCheckSum string, err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": serviceInfo.ID,
	}).Debug("Install SM service")

	ctx, cancel := context.WithTimeout(client.context, smInstallTimeout)
	defer cancel()

	alertRulesJSON, err := json.Marshal(serviceInfo.AlertRules)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	serviceStatus, err := client.pbClient.InstallService(ctx, &pb.InstallServiceRequest{
		Users:         &pb.Users{Users: users},
		Url:           serviceInfo.URLs[0],
		ServiceId:     serviceInfo.ID,
		ProviderId:    serviceInfo.ProviderID,
		AosVersion:    serviceInfo.AosVersion,
		VendorVersion: serviceInfo.VendorVersion,
		AlertRules:    string(alertRulesJSON),
		Description:   serviceInfo.Description,
		Sha256:        serviceInfo.Sha256,
		Sha512:        serviceInfo.Sha512,
		Size:          serviceInfo.Size,
	})
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	if serviceStatus.AosVersion != serviceInfo.AosVersion || serviceStatus.VendorVersion != serviceInfo.VendorVersion {
		return "", aoserrors.New("result version mismatch")
	}

	return serviceStatus.StateChecksum, nil
}

func (client *smClient) removeService(users []string, serviceInfo cloudprotocol.ServiceInfo) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": serviceInfo.ID,
	}).Debug("Remove SM service")

	ctx, cancel := context.WithTimeout(client.context, smInstallTimeout)
	defer cancel()

	if _, err = client.pbClient.RemoveService(ctx, &pb.RemoveServiceRequest{
		Users:      &pb.Users{Users: users},
		ServiceId:  serviceInfo.ID,
		AosVersion: serviceInfo.AosVersion,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) installLayer(layerInfo cloudprotocol.LayerInfoFromCloud) (err error) {
	log.WithFields(log.Fields{
		"id":      client.cfg.SMID,
		"layerID": layerInfo.ID,
		"digest":  layerInfo.Digest,
	}).Debug("Install SM layer")

	ctx, cancel := context.WithTimeout(client.context, smInstallTimeout)
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

func (client *smClient) serviceStateAcceptance(
	correlationID string, stateAcceptance cloudprotocol.StateAcceptance) (err error) {
	log.WithFields(log.Fields{
		"id":            client.cfg.SMID,
		"serviceID":     stateAcceptance.ServiceID,
		"correlationID": correlationID,
		"checksum":      stateAcceptance.Checksum,
		"result":        stateAcceptance.Result,
		"reason":        stateAcceptance.Reason,
	}).Debug("SM service state acceptance")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.ServiceStateAcceptance(ctx, &pb.StateAcceptance{
		CorrelationId: correlationID,
		ServiceId:     stateAcceptance.ServiceID,
		StateChecksum: stateAcceptance.Checksum,
		Result:        stateAcceptance.Result,
		Reason:        stateAcceptance.Reason,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) setServiceState(users []string, state cloudprotocol.UpdateState) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"serviceID": state.ServiceID,
		"checksum":  state.Checksum,
	}).Debug("SM set service state")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	if _, err = client.pbClient.SetServiceState(ctx, &pb.ServiceState{
		Users:         &pb.Users{Users: users},
		ServiceId:     state.ServiceID,
		State:         []byte(state.State),
		StateChecksum: state.Checksum,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) overrideEnvVars(envVars cloudprotocol.DecodedOverrideEnvVars) (err error) {
	log.WithFields(log.Fields{"id": client.cfg.SMID}).Debug("SM set service state")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	request := &pb.OverrideEnvVarsRequest{}

	for _, item := range envVars.OverrideEnvVars {
		requestItem := &pb.OverrideEnvVar{ServiceId: item.ServiceID, SubjectId: item.SubjectID}

		for _, envVar := range item.EnvVars {
			requestVar := &pb.EnvVarInfo{VarId: envVar.ID, Variable: envVar.Variable}

			if envVar.TTL != nil {
				requestVar.Ttl = timestamppb.New(*envVar.TTL)
			}

			requestItem.Vars = append(requestItem.Vars, requestVar)
		}

		request.EnvVars = append(request.EnvVars, requestItem)
	}

	envVarStatus, err := client.pbClient.OverrideEnvVars(ctx, request)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	response := []cloudprotocol.EnvVarInfoStatus{}
	for _, item := range envVarStatus.EnvVarStatus {
		responseItem := cloudprotocol.EnvVarInfoStatus{ServiceID: item.ServiceId, SubjectID: item.SubjectId}

		for _, varStatus := range item.VarStatus {
			responseItem.Statuses = append(responseItem.Statuses, cloudprotocol.EnvVarStatus{
				ID:    varStatus.VarId,
				Error: varStatus.Error,
			})
		}

		response = append(response, responseItem)
	}

	if err = client.messageSender.SendOverrideEnvVarsStatus(response); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) GetSystemLog(logRequest cloudprotocol.RequestSystemLog) (err error) {
	log.WithFields(log.Fields{
		"id":    client.cfg.SMID,
		"logID": logRequest.LogID,
		"from":  logRequest.From,
		"till":  logRequest.Till,
	}).Debug("Get SM system log")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
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

func (client *smClient) GetServiceLog(logRequest cloudprotocol.RequestServiceLog) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get SM system log")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	request := &pb.ServiceLogRequest{LogId: logRequest.LogID, ServiceId: logRequest.ServiceID}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if _, err = client.pbClient.GetServiceLog(ctx, request); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) GetServiceCrashLog(logRequest cloudprotocol.RequestServiceCrashLog) (err error) {
	log.WithFields(log.Fields{
		"id":        client.cfg.SMID,
		"logID":     logRequest.LogID,
		"serviceID": logRequest.ServiceID,
		"from":      logRequest.From,
		"till":      logRequest.Till,
	}).Debug("Get SM system log")

	ctx, cancel := context.WithTimeout(client.context, smRequestTimeout)
	defer cancel()

	request := &pb.ServiceLogRequest{LogId: logRequest.LogID, ServiceId: logRequest.ServiceID}

	if logRequest.From != nil {
		request.From = timestamppb.New(*logRequest.From)
	}

	if logRequest.Till != nil {
		request.Till = timestamppb.New(*logRequest.Till)
	}

	if _, err = client.pbClient.GetServiceCrashLog(ctx, request); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (client *smClient) handleSMNotifications() {
	for {
		if err := client.subscribeSMNotifications(); err != nil {
			if client.context.Err() == nil {
				log.Errorf("Error subscribe to SM notifications: %s", err)
				log.Debugf("Reconnect to SM in %v...", smReconnectTimeout)
			}
		}

		select {
		case <-client.context.Done():
			return

		case <-time.After(smReconnectTimeout):
		}
	}
}

func (client *smClient) subscribeSMNotifications() (err error) {
	log.Debug("Subscribe to SM notifications")

	stream, err := client.pbClient.SubscribeSMNotifications(client.context, &empty.Empty{})
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
			log.WithFields(log.Fields{
				"id":     client.cfg.SMID,
				"tag":    data.Alert.Tag,
				"source": data.Alert.Source,
			}).Debug("Receive SM alert")

			alertItem := cloudprotocol.AlertItem{
				Timestamp:  data.Alert.Timestamp.AsTime(),
				Tag:        data.Alert.Tag,
				Source:     data.Alert.Source,
				AosVersion: data.Alert.AosVersion,
			}

			switch data := data.Alert.Payload.(type) {
			case *pb.Alert_SystemAlert:
				alertItem.Payload = &cloudprotocol.SystemAlert{Message: data.SystemAlert.Message}

			case *pb.Alert_ResourceAlert:
				alertItem.Payload = &cloudprotocol.ResourceAlert{
					Parameter: data.ResourceAlert.Parameter,
					Value:     data.ResourceAlert.Value,
				}

			case *pb.Alert_ResourceValidateAlert:
				resourceValidate := &cloudprotocol.ResourceValidateAlert{
					Type: data.ResourceValidateAlert.Type,
				}

				for _, resourceError := range data.ResourceValidateAlert.Errors {
					resourceValidate.Message = append(resourceValidate.Message, cloudprotocol.ResourceValidateError{
						Name:   resourceError.Name,
						Errors: resourceError.ErrorMsg,
					})
				}

				alertItem.Payload = resourceValidate
			}

			if err = client.alertSender.SendAlert(alertItem); err != nil {
				log.Errorf("Can't send alert: %s", err)
			}

		case *pb.SMNotifications_Monitoring:
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
			}

			for _, serviceMonitoring := range data.Monitoring.ServiceMonitoring {
				monitoringData.ServicesData = append(monitoringData.ServicesData,
					cloudprotocol.ServiceMonitoringData{
						ServiceID:  serviceMonitoring.ServiceId,
						RAM:        serviceMonitoring.Ram,
						CPU:        serviceMonitoring.Cpu,
						UsedDisk:   serviceMonitoring.UsedDisk,
						InTraffic:  serviceMonitoring.InTraffic,
						OutTraffic: serviceMonitoring.OutTraffic,
					})
			}

			if err = client.monitoringSender.SendMonitoringData(monitoringData); err != nil {
				log.Errorf("Can't send monitoring data: %s", err)
			}

		case *pb.SMNotifications_NewServiceState:
			log.WithFields(log.Fields{
				"id":            client.cfg.SMID,
				"correlationID": data.NewServiceState.CorrelationId,
				"serviceID":     data.NewServiceState.ServiceState.ServiceId,
			}).Debug("Receive SM new service state")

			if err = client.messageSender.SendServiceNewState(
				data.NewServiceState.CorrelationId,
				data.NewServiceState.ServiceState.ServiceId,
				string(data.NewServiceState.ServiceState.State),
				data.NewServiceState.ServiceState.StateChecksum,
			); err != nil {
				log.Errorf("Can't send service new state: %s", err)
			}

		case *pb.SMNotifications_ServiceStateRequest:
			log.WithFields(log.Fields{
				"id":        client.cfg.SMID,
				"serviceID": data.ServiceStateRequest.ServiceId,
				"default":   data.ServiceStateRequest.Default,
			}).Debug("Receive SM service state request")

			if err = client.messageSender.SendServiceStateRequest(
				data.ServiceStateRequest.ServiceId, data.ServiceStateRequest.Default); err != nil {
				log.Errorf("Can't send service state request: %s", err)
			}

		case *pb.SMNotifications_Log:
			log.WithFields(log.Fields{
				"id":        client.cfg.SMID,
				"logID":     data.Log.LogId,
				"part":      data.Log.Part,
				"partCount": data.Log.PartCount,
			}).Debug("Receive SM push log")

			if err = client.messageSender.SendLog(cloudprotocol.PushLog{
				LogID:     data.Log.LogId,
				PartCount: data.Log.PartCount,
				Part:      data.Log.Part,
				Data:      data.Log.Data,
				Error:     data.Log.Error,
			}); err != nil {
				log.Errorf("Can't send log: %s", err)
			}

		default:
			log.Warnf("Receive unsupported SM notification: %s", reflect.TypeOf(data))
		}
	}
}
