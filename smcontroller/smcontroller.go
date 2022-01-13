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
	"strings"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/aoscloud/aos_communicationmanager/boardconfig"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const connectClientTimeout = 1 * time.Minute

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller SM controller instance
type Controller struct {
	sync.Mutex

	messageSender    MessageSender
	alertSender      AlertSender
	monitoringSender MonitoringSender
	urlTranslator    URLTranslator
	clientsWG        sync.WaitGroup
	readyWG          sync.WaitGroup
	clients          map[string]*smClient
	context          context.Context
	cancelFunction   context.CancelFunc
}

// URLTranslator translates URL from local to remote if required
type URLTranslator interface {
	TranslateURL(isLocal bool, inURL string) (outURL string, err error)
}

// AlertSender sends alert
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem) (err error)
}

// MonitoringSender sends monitoring data
type MonitoringSender interface {
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error)
}

// MessageSender sends messages to the cloud
type MessageSender interface {
	SendServiceNewState(correlationID, serviceID, state, checksum string) (err error)
	SendServiceStateRequest(serviceID string, defaultState bool) (err error)
	SendOverrideEnvVarsStatus(envs []cloudprotocol.EnvVarInfoStatus) (err error)
	SendLog(serviceLog cloudprotocol.PushLog) (err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new SM controller
func New(
	cfg *config.Config, messageSender MessageSender, alertSender AlertSender, monitoringSender MonitoringSender,
	urlTranslator URLTranslator, insecure bool) (controller *Controller, err error) {
	log.Debug("Create SM controller")

	controller = &Controller{
		messageSender:    messageSender,
		alertSender:      alertSender,
		monitoringSender: monitoringSender,
		urlTranslator:    urlTranslator,
		clients:          make(map[string]*smClient),
	}
	controller.context, controller.cancelFunction = context.WithCancel(context.Background())

	defer func() {
		if err != nil {
			controller.Close()
			controller = nil
		}
	}()

	var secureOpt grpc.DialOption

	if insecure {
		secureOpt = grpc.WithInsecure()
	} else {
		tlsConfig, err := cryptutils.GetClientMutualTLSConfig(cfg.Crypt.CACert, cfg.CertStorage)
		if err != nil {
			return controller, aoserrors.Wrap(err)
		}

		secureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	for _, smConfig := range cfg.SMController.SMList {
		controller.readyWG.Add(1)
		go controller.connectClient(smConfig, secureOpt)
	}

	return controller, nil
}

// Close closes SM controller
func (controller *Controller) Close() (err error) {
	log.Debug("Close SM controller")

	controller.cancelFunction()
	controller.clientsWG.Wait()

	controller.Lock()
	defer controller.Unlock()

	for _, client := range controller.clients {
		client.close()
	}

	return nil
}

// GetUsersStatus returns SM users status
func (controller *Controller) GetUsersStatus(users []string) (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	for _, client := range clients {
		clientServices, clientLayers, err := client.getUsersStatus(users)
		if err != nil {
			return nil, nil, aoserrors.Wrap(err)
		}

		servicesInfo = append(servicesInfo, clientServices...)
		layersInfo = append(layersInfo, clientLayers...)
	}

	return servicesInfo, layersInfo, nil
}

// GetAllStatus returns SM all existing layers and services status
func (controller *Controller) GetAllStatus() (
	servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	for _, client := range clients {
		clientServices, clientLayers, err := client.getAllStatus()
		if err != nil {
			return nil, nil, aoserrors.Wrap(err)
		}

		servicesInfo = append(servicesInfo, clientServices...)
		layersInfo = append(layersInfo, clientLayers...)
	}

	return servicesInfo, layersInfo, nil
}

// CheckBoardConfig checks board config
func (controller *Controller) CheckBoardConfig(boardConfig boardconfig.BoardConfig) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, check the same config for all SM's

	clientBoardConfig := clientBoardConfig{
		FormatVersion: boardConfig.FormatVersion,
		VendorVersion: boardConfig.VendorVersion,
		Devices:       boardConfig.Devices,
		Resources:     boardConfig.Resources,
	}

	for _, client := range clients {
		vendorVersion, err := client.checkBoardConfig(clientBoardConfig)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if vendorVersion != boardConfig.VendorVersion {
			return aoserrors.New("wrong vendor version")
		}
	}

	return nil
}

// SetBoardConfig sets board config
func (controller *Controller) SetBoardConfig(boardConfig boardconfig.BoardConfig) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, set the same config for all SM's

	clientBoardConfig := clientBoardConfig{
		FormatVersion: boardConfig.FormatVersion,
		VendorVersion: boardConfig.VendorVersion,
		Devices:       boardConfig.Devices,
		Resources:     boardConfig.Resources,
	}

	for _, client := range clients {
		// Check board config version and set if it is different

		currentVendorVersion, err := client.getBoardConfigStatus()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if currentVendorVersion == clientBoardConfig.VendorVersion {
			continue
		}

		if err = client.setBoardConfig(clientBoardConfig); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// InstallService requests to install servie
func (controller *Controller) InstallService(users []string,
	serviceInfo cloudprotocol.ServiceInfoFromCloud) (stateChecksum string, err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	if len(serviceInfo.URLs) == 0 {
		return "", aoserrors.New("no service URL")
	}

	// TODO: we do not support multiple SM right now, install same service for all SM's

	for _, client := range clients {
		if serviceInfo.URLs[0], err = controller.urlTranslator.TranslateURL(
			client.cfg.IsLocal, serviceInfo.URLs[0]); err != nil {
			return "", aoserrors.Wrap(err)
		}

		if stateChecksum, err = client.installService(users, serviceInfo); err != nil {
			return "", aoserrors.Wrap(err)
		}
	}

	return stateChecksum, nil
}

// RemoveService remove service request
func (controller *Controller) RemoveService(users []string, serviceInfo cloudprotocol.ServiceInfo) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, remove service for all SM's

	for _, client := range clients {
		if err = client.removeService(users, serviceInfo); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// InstallLayer install layer request
func (controller *Controller) InstallLayer(layerInfo cloudprotocol.LayerInfoFromCloud) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	if len(layerInfo.URLs) == 0 {
		return aoserrors.New("no layer URL")
	}

	// TODO: we do not support multiple SM right now, install same layer for all SM's

	for _, client := range clients {
		if layerInfo.URLs[0], err = controller.urlTranslator.TranslateURL(
			client.cfg.IsLocal, layerInfo.URLs[0]); err != nil {
			return aoserrors.Wrap(err)
		}

		if err = client.installLayer(layerInfo); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// ServiceStateAcceptance handles service state acceptance
func (controller *Controller) ServiceStateAcceptance(
	correlationID string, stateAcceptance cloudprotocol.StateAcceptance) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, send service state acceptance for all SM's

	for _, client := range clients {
		if err = client.serviceStateAcceptance(correlationID, stateAcceptance); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// SetServiceState sets service state
func (controller *Controller) SetServiceState(users []string, state cloudprotocol.UpdateState) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, set service state for all SM's

	for _, client := range clients {
		if err = client.setServiceState(users, state); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// OverrideEnvVars overrides service env vars
func (controller *Controller) OverrideEnvVars(envVars cloudprotocol.DecodedOverrideEnvVars) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, override service env vars for all SM's

	for _, client := range clients {
		if err = client.overrideEnvVars(envVars); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetSystemLog requests system log from SM
func (controller *Controller) GetSystemLog(logRequest cloudprotocol.RequestSystemLog) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, get system log from all SM's

	for _, client := range clients {
		if err = client.GetSystemLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetServiceLog requests service log from SM
func (controller *Controller) GetServiceLog(logRequest cloudprotocol.RequestServiceLog) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, get service log from all SM's

	for _, client := range clients {
		if err = client.GetServiceLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetServiceCrashLog requests service crash log from SM
func (controller *Controller) GetServiceCrashLog(logRequest cloudprotocol.RequestServiceCrashLog) (err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// TODO: we do not support multiple SM right now, get service crash log from all SM's

	for _, client := range clients {
		if err = client.GetServiceCrashLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (controller *Controller) waitAndLock() {
	controller.readyWG.Wait()
	controller.Lock()
}

func (controller *Controller) connectClient(smConfig config.SMConfig, secureOpt grpc.DialOption) {
	defer controller.readyWG.Done()

	connectChannel := make(chan struct{}, 1)

	controller.clientsWG.Add(1)

	go func() {
		defer controller.clientsWG.Done()

		client, err := newSMClient(controller.context, smConfig,
			controller.messageSender, controller.alertSender, controller.monitoringSender, secureOpt)

		connectChannel <- struct{}{}

		if err != nil {
			if !strings.Contains(err.Error(), context.Canceled.Error()) {
				log.WithField("id", smConfig.SMID).Errorf("Can't connect to SM: %s", err)
			}

			return
		}

		controller.Lock()
		defer controller.Unlock()

		controller.clients[smConfig.SMID] = client
	}()

	select {
	case <-connectChannel:

	case <-time.After(connectClientTimeout):
		log.WithField("id", smConfig.SMID).Error("SM connection timeout")
	}
}
