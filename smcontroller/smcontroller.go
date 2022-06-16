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
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/unitstatushandler"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const connectClientTimeout = 1 * time.Minute

const statusChanSize = 10

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Controller SM controller instance.
type Controller struct {
	sync.Mutex

	messageSender             MessageSender
	alertSender               AlertSender
	monitoringSender          MonitoringSender
	urlTranslator             URLTranslator
	clientsWG                 sync.WaitGroup
	readyWG                   sync.WaitGroup
	updateInstancesStatusChan chan []cloudprotocol.InstanceStatus
	runInstancesStatusChan    chan unitstatushandler.RunInstancesStatus
	clients                   map[string]*smClient
	cancelFunction            context.CancelFunc
}

// URLTranslator translates URL from local to remote if required.
type URLTranslator interface {
	TranslateURL(isLocal bool, inURL string) (outURL string, err error)
}

// AlertSender sends alert.
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem)
}

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData)
}

// MessageSender sends messages to the cloud.
type MessageSender interface {
	SendInstanceNewState(newState cloudprotocol.NewState) error
	SendInstanceStateRequest(request cloudprotocol.StateRequest) error
	SendOverrideEnvVarsStatus(envs cloudprotocol.OverrideEnvVarsStatus) error
	SendLog(serviceLog cloudprotocol.PushLog) error
}

// CertificateProvider certificate and key provider interface.
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, keyURL string, err error)
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new SM controller.
func New(
	cfg *config.Config, messageSender MessageSender, alertSender AlertSender, monitoringSender MonitoringSender,
	urlTranslator URLTranslator, certProvider CertificateProvider, cryptcoxontext *cryptutils.CryptoContext,
	insecureConn bool,
) (controller *Controller, err error) {
	log.Debug("Create SM controller")

	controller = &Controller{
		messageSender:             messageSender,
		alertSender:               alertSender,
		monitoringSender:          monitoringSender,
		urlTranslator:             urlTranslator,
		updateInstancesStatusChan: make(chan []cloudprotocol.InstanceStatus, statusChanSize),
		runInstancesStatusChan:    make(chan unitstatushandler.RunInstancesStatus, statusChanSize),
		clients:                   make(map[string]*smClient),
	}

	ctx, cancelFunction := context.WithCancel(context.Background())
	controller.cancelFunction = cancelFunction

	defer func() {
		if err != nil {
			controller.Close()
			controller = nil
		}
	}()

	var secureOpt grpc.DialOption

	if insecureConn {
		secureOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	} else {
		certURL, keyURL, err := certProvider.GetCertificate(cfg.CertStorage, nil, "")
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		tlsConfig, err := cryptcoxontext.GetClientMutualTLSConfig(certURL, keyURL)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		secureOpt = grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig))
	}

	for _, smConfig := range cfg.SMController.SMList {
		controller.readyWG.Add(1)

		go controller.connectClient(ctx, smConfig, secureOpt)
	}

	return controller, nil
}

// Close closes SM controller.
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

// GetServicesStatus returns SM all existing services status.
func (controller *Controller) GetServicesStatus() (servicesStatus []unitstatushandler.ServiceStatus, err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	for _, client := range clients {
		clientServices, err := client.getServicesStatus()
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		servicesStatus = append(servicesStatus, clientServices...)
	}

	return servicesStatus, nil
}

// GetUsersStatus returns SM users status.
func (controller *Controller) GetLayersStatus() (layersStatus []unitstatushandler.LayerStatus, err error) {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	for _, client := range clients {
		clientLayers, err := client.getLayersStatus()
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		layersStatus = append(layersStatus, clientLayers...)
	}

	return layersStatus, nil
}

// CheckBoardConfig checks board config.
func (controller *Controller) CheckBoardConfig(boardConfig aostypes.BoardConfig) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, check the same config for all SM's

	for _, client := range clients {
		if err := client.checkBoardConfig(boardConfig); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// SetBoardConfig sets board config.
func (controller *Controller) SetBoardConfig(boardConfig aostypes.BoardConfig) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, set the same config for all SM's

	// Check board config version and set if it is different
	for _, client := range clients {
		currentVendorVersion, err := client.getBoardConfigStatus()
		if err != nil {
			return aoserrors.Wrap(err)
		}

		if currentVendorVersion == boardConfig.VendorVersion {
			continue
		}

		if err = client.setBoardConfig(boardConfig); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// RunInstances runs desired services instances.
func (controller *Controller) RunInstances(instances []cloudprotocol.InstanceInfo) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, ran same instances for all SM's

	for _, client := range clients {
		if err := client.runInstances(instances); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// InstallService requests to install servie.
func (controller *Controller) InstallService(serviceInfo cloudprotocol.ServiceInfo) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	if len(serviceInfo.URLs) == 0 {
		return aoserrors.New("no service URL")
	}

	var err error

	// we do not support multiple SM right now, install same service for all SM's

	for _, client := range clients {
		if serviceInfo.URLs[0], err = controller.urlTranslator.TranslateURL(
			client.cfg.IsLocal, serviceInfo.URLs[0]); err != nil {
			return aoserrors.Wrap(err)
		}

		if err = client.installService(serviceInfo); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// RestoreService restores service.
func (controller *Controller) RestoreService(serviceID string) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, restore same service for all SM's

	for _, client := range clients {
		if err := client.restoreService(serviceID); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// RemoveService requests to install servie.
func (controller *Controller) RemoveService(serviceID string) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, remove same service for all SM's

	for _, client := range clients {
		if err := client.removeService(serviceID); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// InstallLayer install layer request.
func (controller *Controller) InstallLayer(layerInfo cloudprotocol.LayerInfo) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	if len(layerInfo.URLs) == 0 {
		return aoserrors.New("no layer URL")
	}

	var err error

	// we do not support multiple SM right now, install same layer for all SM's

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

// RemoveLayer remove layer request.
func (controller *Controller) RemoveLayer(digest string) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, remove same layer for all SM's

	for _, client := range clients {
		if err := client.removeLayer(digest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// RestoreLayer restore layer request.
func (controller *Controller) RestoreLayer(digest string) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, restore same layer for all SM's

	for _, client := range clients {
		if err := client.restoreLayer(digest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// InstanceStateAcceptance handles instance state acceptance.
func (controller *Controller) InstanceStateAcceptance(stateAcceptance cloudprotocol.StateAcceptance) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, send service state acceptance for all SM's

	for _, client := range clients {
		if err := client.instanceStateAcceptance(stateAcceptance); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// SetServiceState sets instance state.
func (controller *Controller) SetInstanceState(state cloudprotocol.UpdateState) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, set service state for all SM's

	for _, client := range clients {
		if err := client.setInstanceState(state); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// OverrideEnvVars overrides instance env vars.
func (controller *Controller) OverrideEnvVars(envVars cloudprotocol.DecodedOverrideEnvVars) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, override service env vars for all SM's

	for _, client := range clients {
		if err := client.overrideEnvVars(envVars); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetSystemLog requests system log from SM.
func (controller *Controller) GetSystemLog(logRequest cloudprotocol.RequestSystemLog) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, get system log from all SM's

	for _, client := range clients {
		if err := client.getSystemLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetInstanceLog requests service instance log from SM.
func (controller *Controller) GetInstanceLog(logRequest cloudprotocol.RequestServiceLog) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, get service log from all SM's

	for _, client := range clients {
		if err := client.getInstanceLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetInstanceCrashLog requests service instance crash log from SM.
func (controller *Controller) GetInstanceCrashLog(logRequest cloudprotocol.RequestServiceCrashLog) error {
	controller.waitAndLock()
	clients := controller.clients
	controller.Unlock()

	// we do not support multiple SM right now, get service crash log from all SM's

	for _, client := range clients {
		if err := client.getInstanceCrashLog(logRequest); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

// GetUpdateInstancesStatusChannel returns channel with update instances status.
func (controller *Controller) GetUpdateInstancesStatusChannel() <-chan []cloudprotocol.InstanceStatus {
	return controller.updateInstancesStatusChan
}

// GetRunInstancesStatusChannel returns channel with run instances status.
func (controller *Controller) GetRunInstancesStatusChannel() <-chan unitstatushandler.RunInstancesStatus {
	return controller.runInstancesStatusChan
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (controller *Controller) waitAndLock() {
	controller.readyWG.Wait()
	controller.Lock()
}

func (controller *Controller) connectClient(ctx context.Context, smConfig config.SMConfig, secureOpt grpc.DialOption) {
	defer controller.readyWG.Done()

	connectChannel := make(chan struct{}, 1)

	controller.clientsWG.Add(1)

	go func() {
		defer controller.clientsWG.Done()

		client, err := newSMClient(ctx, smConfig, controller.messageSender, controller.alertSender,
			controller.monitoringSender, controller.updateInstancesStatusChan, controller.runInstancesStatusChan, secureOpt)

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
