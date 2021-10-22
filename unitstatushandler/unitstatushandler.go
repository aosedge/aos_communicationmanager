// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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

package unitstatushandler

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/image"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/action"

	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
	"aos_communicationmanager/downloader"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Downloader downloads packages
type Downloader interface {
	DownloadAndDecrypt(
		ctx context.Context, packageInfo cloudprotocol.DecryptDataStruct,
		chains []cloudprotocol.CertificateChain,
		certs []cloudprotocol.Certificate) (result downloader.Result, err error)
}

// StatusSender sends unit status to cloud
type StatusSender interface {
	SendUnitStatus(unitStatus cloudprotocol.UnitStatus) (err error)
}

// BoardConfigUpdater updates board configuration
type BoardConfigUpdater interface {
	GetStatus() (boardConfigInfo cloudprotocol.BoardConfigInfo, err error)
	GetBoardConfigVersion(configJSON json.RawMessage) (vendorVersion string, err error)
	CheckBoardConfig(configJSON json.RawMessage) (vendorVersion string, err error)
	UpdateBoardConfig(configJSON json.RawMessage) (err error)
}

// FirmwareUpdater updates system components
type FirmwareUpdater interface {
	GetStatus() (componentsInfo []cloudprotocol.ComponentInfo, err error)
	UpdateComponents(components []cloudprotocol.ComponentInfoFromCloud) (
		status []cloudprotocol.ComponentInfo, err error)
}

// SoftwareUpdater updates services, layers
type SoftwareUpdater interface {
	GetStatus() (servicesInfo []cloudprotocol.ServiceInfo, layersInfo []cloudprotocol.LayerInfo, err error)
	InstallService(serviceInfo cloudprotocol.ServiceInfoFromCloud) (stateChecksum string, err error)
	RemoveService(serviceInfo cloudprotocol.ServiceInfo) (err error)
	InstallLayer(layerInfo cloudprotocol.LayerInfoFromCloud) (err error)
	RemoveLayer(layerInfo cloudprotocol.LayerInfo) (err error)
}

// Storage used to store unit status handler states
type Storage interface {
	SetFirmwareUpdateState(state json.RawMessage) (err error)
	GetFirmwareUpdateState() (state json.RawMessage, err error)
	SetSoftwareUpdateState(state json.RawMessage) (err error)
	GetSoftwareUpdateState() (state json.RawMessage, err error)
}

// Instance instance of unit status handler
type Instance struct {
	sync.Mutex

	boardConfigUpdater BoardConfigUpdater
	firmwareUpdater    FirmwareUpdater
	softwareUpdater    SoftwareUpdater
	downloader         Downloader
	statusSender       StatusSender

	action *action.Handler

	isInitialized             bool
	isDesiredStatusProcessing bool
	pendindDesiredStatus      *cloudprotocol.DecodedDesiredStatus

	ctx    context.Context
	cancel context.CancelFunc

	statusTimer       *time.Timer
	boardConfigStatus itemStatus
	componentStatuses map[string]*itemStatus
	layerStatuses     map[string]*itemStatus
	serviceStatuses   map[string]*itemStatus

	sendStatusPeriod time.Duration

	// Used to wait when software update finished on switch user
	softwareWG           sync.WaitGroup
	cancelSoftwareUpdate context.CancelFunc
}

type statusDescriptor struct {
	amqpStatus interface{}
}

type itemStatus []statusDescriptor

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new unit status handler instance
func New(
	cfg *config.Config,
	boardConfigUpdater BoardConfigUpdater,
	firmwareUpdater FirmwareUpdater,
	softwareUpdater SoftwareUpdater,
	downloader Downloader,
	statusSender StatusSender) (instance *Instance, err error) {
	log.Debug("Create unit status handler")

	instance = &Instance{
		boardConfigUpdater: boardConfigUpdater,
		firmwareUpdater:    firmwareUpdater,
		softwareUpdater:    softwareUpdater,
		statusSender:       statusSender,
		downloader:         downloader,
		sendStatusPeriod:   cfg.UnitStatusSendTimeout.Duration,
		action:             action.New(maxConcurrentActions),
	}

	instance.ctx, instance.cancel = context.WithCancel(context.Background())

	// Initialize maps of statuses for avoiding situation of adding values to uninitialized map on go routine
	instance.componentStatuses = make(map[string]*itemStatus)
	instance.layerStatuses = make(map[string]*itemStatus)
	instance.serviceStatuses = make(map[string]*itemStatus)

	return instance, nil
}

// Close closes unit status handler
func (instance *Instance) Close() (err error) {
	instance.Lock()
	defer instance.Unlock()

	log.Debug("Close unit status handler")

	if instance.statusTimer != nil {
		instance.statusTimer.Stop()
	}

	instance.cancel()

	return nil
}

// ProcessDesiredStatus processes desired status
func (instance *Instance) ProcessDesiredStatus(desiredStatus cloudprotocol.DecodedDesiredStatus) {
	instance.Lock()
	defer instance.Unlock()

	if instance.isDesiredStatusProcessing {
		instance.pendindDesiredStatus = &desiredStatus
		return
	}

	instance.isDesiredStatusProcessing = true

	ctx, cancelFunction := context.WithCancel(instance.ctx)
	instance.cancelSoftwareUpdate = cancelFunction

	go instance.processDesiredStatus(ctx, desiredStatus)
}

// SendUnitStatus sends unit status
func (instance *Instance) SendUnitStatus() (err error) {
	instance.Lock()

	if instance.isDesiredStatusProcessing {
		instance.cancelSoftwareUpdate()
		instance.waitSoftwareUpdate()
		instance.pendindDesiredStatus = nil
	}

	instance.Unlock()

	// Init statuses may take time and should not lock mutex for long time
	if err = instance.initStatuses(); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.Lock()
	defer instance.Unlock()

	instance.isInitialized = true

	instance.statusChanged(true)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (instance *Instance) initStatuses() (err error) {
	log.Debug("Get initial firmware and software statuses")

	instance.Lock()

	instance.boardConfigStatus = nil
	instance.componentStatuses = make(map[string]*itemStatus)
	instance.serviceStatuses = make(map[string]*itemStatus)
	instance.layerStatuses = make(map[string]*itemStatus)

	instance.Unlock()

	// Get initial board config info

	boardConfigInfo, err := instance.boardConfigUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	instance.updateBoardConfigStatus(boardConfigInfo)

	// Get initial components info

	componentsInfo, err := instance.firmwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, componentInfo := range componentsInfo {
		instance.updateComponentStatus(componentInfo)
	}

	// Get initial services and layers info

	servicesInfo, layersInfo, err := instance.softwareUpdater.GetStatus()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, layerInfo := range layersInfo {
		instance.updateLayerStatus(layerInfo)
	}

	for _, serviceInfo := range servicesInfo {
		instance.updateServiceStatus(serviceInfo)
	}

	return nil
}

func (descriptor *statusDescriptor) getStatus() (status string) {
	switch amqpStatus := descriptor.amqpStatus.(type) {
	case *cloudprotocol.BoardConfigInfo:
		return amqpStatus.Status

	case *cloudprotocol.ComponentInfo:
		return amqpStatus.Status

	case *cloudprotocol.LayerInfo:
		return amqpStatus.Status

	case *cloudprotocol.ServiceInfo:
		return amqpStatus.Status

	default:
		return cloudprotocol.UnknownStatus
	}
}

func (descriptor *statusDescriptor) getVersion() (version string) {
	switch amqpStatus := descriptor.amqpStatus.(type) {
	case *cloudprotocol.BoardConfigInfo:
		return amqpStatus.VendorVersion

	case *cloudprotocol.ComponentInfo:
		return amqpStatus.VendorVersion

	case *cloudprotocol.LayerInfo:
		return strconv.FormatUint(amqpStatus.AosVersion, 10)

	case *cloudprotocol.ServiceInfo:
		return strconv.FormatUint(amqpStatus.AosVersion, 10)

	default:
		return ""
	}
}

func (status *itemStatus) isInstalled() (installed bool, descriptor statusDescriptor) {
	for _, element := range *status {
		if element.getStatus() == cloudprotocol.InstalledStatus {
			return true, element
		}
	}

	return false, statusDescriptor{}
}

func (instance *Instance) finishProcessDesiredStatus() {
	instance.Lock()
	defer instance.Unlock()

	if instance.pendindDesiredStatus != nil {
		ctx, cancelFunction := context.WithCancel(instance.ctx)
		instance.cancelSoftwareUpdate = cancelFunction

		go instance.processDesiredStatus(ctx, *instance.pendindDesiredStatus)
		instance.pendindDesiredStatus = nil
	} else {
		instance.isDesiredStatusProcessing = false
	}
}

func (instance *Instance) processDesiredStatus(ctx context.Context, desiredStatus cloudprotocol.DecodedDesiredStatus) {
	defer instance.finishProcessDesiredStatus()

	if desiredStatus.BoardConfig != nil {
		if err := instance.updateBoardConfig(desiredStatus.BoardConfig); err != nil {
			log.Errorf("Can't update board config: %s", err)
		}
	}

	// Firmware update uses instance context, it should not be canceled on set users
	if err := instance.updateComponents(instance.ctx, desiredStatus); err != nil {
		log.Errorf("Can't update components: %s", err)
	}

	// Software update uses local context which is canceled on set users
	if err := instance.updateSoftware(ctx, desiredStatus); err != nil {
		log.Errorf("Can't update software: %s", err)
	}
}

func (instance *Instance) updateSoftware(
	ctx context.Context, desiredStatus cloudprotocol.DecodedDesiredStatus) (err error) {
	if err = instance.startSoftwareUpdate(ctx); err != nil {
		return aoserrors.Wrap(err)
	}
	defer instance.finishSoftwareUpdate()

	if err := instance.installLayers(
		ctx, desiredStatus.Layers, desiredStatus.CertificateChains, desiredStatus.Certificates); err != nil {
		log.Errorf("Can't install layers: %s", err)
	}

	if err := instance.updateServices(
		ctx, desiredStatus.Services, desiredStatus.CertificateChains, desiredStatus.Certificates); err != nil {
		log.Errorf("Can't update services: %s", err)
	}

	if err := instance.removeLayers(ctx, desiredStatus.Layers); err != nil {
		log.Errorf("Can't remove layers: %s", err)
	}

	return nil
}

func (instance *Instance) startSoftwareUpdate(ctx context.Context) (err error) {
	instance.Lock()
	defer instance.Unlock()

	if err := ctx.Err(); err != nil {
		return aoserrors.Wrap(err)
	}

	instance.softwareWG.Add(1)

	return nil
}

func (instance *Instance) finishSoftwareUpdate() {
	instance.softwareWG.Done()
}

func (instance *Instance) waitSoftwareUpdate() {
	instance.softwareWG.Wait()
}

func (instance *Instance) sendCurrentStatus() {
	instance.Lock()
	defer instance.Unlock()

	unitStatus := cloudprotocol.UnitStatus{
		BoardConfig: make([]cloudprotocol.BoardConfigInfo, 0, len(instance.boardConfigStatus)),
		Components:  make([]cloudprotocol.ComponentInfo, 0, len(instance.componentStatuses)),
		Layers:      make([]cloudprotocol.LayerInfo, 0, len(instance.layerStatuses)),
		Services:    make([]cloudprotocol.ServiceInfo, 0, len(instance.serviceStatuses)),
	}

	for _, status := range instance.boardConfigStatus {
		unitStatus.BoardConfig = append(unitStatus.BoardConfig, *status.amqpStatus.(*cloudprotocol.BoardConfigInfo))
	}

	for _, componentStatus := range instance.componentStatuses {
		for _, status := range *componentStatus {
			unitStatus.Components = append(unitStatus.Components, *status.amqpStatus.(*cloudprotocol.ComponentInfo))
		}
	}

	for _, layerStatus := range instance.layerStatuses {
		for _, status := range *layerStatus {
			unitStatus.Layers = append(unitStatus.Layers, *status.amqpStatus.(*cloudprotocol.LayerInfo))
		}
	}

	for _, serviceStatus := range instance.serviceStatuses {
		for _, status := range *serviceStatus {
			unitStatus.Services = append(unitStatus.Services, *status.amqpStatus.(*cloudprotocol.ServiceInfo))
		}
	}

	if err := instance.statusSender.SendUnitStatus(unitStatus); err != nil {
		log.Errorf("Can't send unit status: %s", err)
	}

	if instance.statusTimer != nil {
		instance.statusTimer.Stop()
		instance.statusTimer = nil
	}
}

func (instance *Instance) updateBoardConfigStatus(boardConfigInfo cloudprotocol.BoardConfigInfo) {
	instance.Lock()
	defer instance.Unlock()

	log.WithFields(log.Fields{
		"status":        boardConfigInfo.Status,
		"vendorVersion": boardConfigInfo.VendorVersion,
		"error":         boardConfigInfo.Error}).Debug("Update board config status")

	instance.updateStatus(&instance.boardConfigStatus, statusDescriptor{&boardConfigInfo})
}

func (instance *Instance) updateComponentStatus(componentInfo cloudprotocol.ComponentInfo) {
	instance.Lock()
	defer instance.Unlock()

	log.WithFields(log.Fields{
		"id":            componentInfo.ID,
		"status":        componentInfo.Status,
		"vendorVersion": componentInfo.VendorVersion,
		"error":         componentInfo.Error}).Debug("Update component status")

	componentStatus, ok := instance.componentStatuses[componentInfo.ID]
	if !ok {
		componentStatus = &itemStatus{}
		instance.componentStatuses[componentInfo.ID] = componentStatus
	}

	instance.updateStatus(componentStatus, statusDescriptor{&componentInfo})
}

func (instance *Instance) updateLayerStatus(layerInfo cloudprotocol.LayerInfo) {
	instance.Lock()
	defer instance.Unlock()

	log.WithFields(log.Fields{
		"id":         layerInfo.ID,
		"digest":     layerInfo.Digest,
		"status":     layerInfo.Status,
		"aosVersion": layerInfo.AosVersion,
		"error":      layerInfo.Error}).Debug("Update layer status")

	layerStatus, ok := instance.layerStatuses[layerInfo.Digest]
	if !ok {
		layerStatus = &itemStatus{}
		instance.layerStatuses[layerInfo.Digest] = layerStatus
	}

	instance.updateStatus(layerStatus, statusDescriptor{&layerInfo})
}

func (instance *Instance) updateServiceStatus(serviceInfo cloudprotocol.ServiceInfo) {
	instance.Lock()
	defer instance.Unlock()

	log.WithFields(log.Fields{
		"id":         serviceInfo.ID,
		"status":     serviceInfo.Status,
		"aosVersion": serviceInfo.AosVersion,
		"error":      serviceInfo.Error}).Debug("Update service status")

	serviceStatus, ok := instance.serviceStatuses[serviceInfo.ID]
	if !ok {
		serviceStatus = &itemStatus{}
		instance.serviceStatuses[serviceInfo.ID] = serviceStatus
	}

	instance.updateStatus(serviceStatus, statusDescriptor{&serviceInfo})
}

func (instance *Instance) statusChanged(force bool) {
	if !instance.isInitialized {
		return
	}

	if instance.statusTimer != nil {
		if !force {
			return
		}

		instance.statusTimer.Stop()
	}

	sendIn := instance.sendStatusPeriod

	if force {
		sendIn = time.Duration(0)
	}

	instance.statusTimer = time.AfterFunc(sendIn, func() {
		instance.sendCurrentStatus()
	})
}

func (instance *Instance) updateStatus(status *itemStatus, descriptor statusDescriptor) {
	defer instance.statusChanged(false)

	if descriptor.getStatus() == cloudprotocol.InstalledStatus {
		*status = itemStatus{descriptor}

		return
	}

	for i, element := range *status {
		if element.getVersion() == descriptor.getVersion() {
			(*status)[i] = descriptor

			return
		}
	}

	*status = append(*status, descriptor)
}

func (instance *Instance) updateBoardConfig(config json.RawMessage) (err error) {
	var boardConfigInfo cloudprotocol.BoardConfigInfo

	log.Debug("Install new board config")

	defer func() {
		if err != nil {
			boardConfigInfo.Status = cloudprotocol.ErrorStatus
			boardConfigInfo.Error = err.Error()
		}

		instance.updateBoardConfigStatus(boardConfigInfo)
	}()

	if boardConfigInfo.VendorVersion, err = instance.boardConfigUpdater.CheckBoardConfig(config); err != nil {
		return aoserrors.Wrap(err)
	}

	boardConfigInfo.Status = cloudprotocol.InstallingStatus

	instance.updateBoardConfigStatus(boardConfigInfo)

	if err = instance.boardConfigUpdater.UpdateBoardConfig(config); err != nil {
		return aoserrors.Wrap(err)
	}

	boardConfigInfo.Status = cloudprotocol.InstalledStatus

	return nil
}

func (instance *Instance) updateComponents(
	ctx context.Context, desiredStatus cloudprotocol.DecodedDesiredStatus) (err error) {
	var (
		wg               sync.WaitGroup
		updateComponents []cloudprotocol.ComponentInfoFromCloud
	)

desiredLoop:
	for _, desiredComponent := range desiredStatus.Components {
		if itemStatus, ok := instance.componentStatuses[desiredComponent.ID]; ok {
			if installed, descriptor := itemStatus.isInstalled(); installed &&
				descriptor.getVersion() == desiredComponent.VendorVersion {
				continue desiredLoop
			}
		}

		componentStatus := cloudprotocol.ComponentInfo{
			ID:            desiredComponent.ID,
			AosVersion:    desiredComponent.AosVersion,
			VendorVersion: desiredComponent.VendorVersion,
		}

		log.WithFields(log.Fields{
			"id":            componentStatus.ID,
			"aosVersion":    componentStatus.AosVersion,
			"vendorVersion": componentStatus.VendorVersion,
		}).Debug("Install component")

		componentStatus.Status = cloudprotocol.DownloadingStatus
		instance.updateComponentStatus(componentStatus)

		result, err := instance.downloader.DownloadAndDecrypt(
			ctx, desiredComponent.DecryptDataStruct, desiredStatus.CertificateChains, desiredStatus.Certificates)
		if err != nil {
			return aoserrors.Wrap(err)
		}
		defer func() {
			if err := os.RemoveAll(result.GetFileName()); err != nil {
				log.Errorf("Can't remove decrypted file: %s", err)
			}
		}()

		wg.Add(1)

		go func(desiredComponent cloudprotocol.ComponentInfoFromCloud) {
			defer wg.Done()

			if componentErr := instance.waitComponentDownload(
				ctx, result, &desiredComponent, componentStatus); componentErr != nil {
				componentErr = aoserrors.Wrap(componentErr)

				if strings.Contains(componentErr.Error(), context.Canceled.Error()) {
					log.WithField("id", componentStatus.ID).Errorf("Download component aborted: %s", componentErr)
				} else {
					componentStatus.Status = cloudprotocol.ErrorStatus
					componentStatus.Error = componentErr.Error()
					instance.updateComponentStatus(componentStatus)
				}

				if err == nil {
					err = componentErr
				}
			}

			instance.Lock()
			defer instance.Unlock()

			updateComponents = append(updateComponents, desiredComponent)
		}(desiredComponent)
	}

	wg.Wait()

	if err != nil {
		return aoserrors.Wrap(err)
	}

	if len(updateComponents) != 0 {
		if _, err = instance.firmwareUpdater.UpdateComponents(updateComponents); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (instance *Instance) waitComponentDownload(ctx context.Context, result downloader.Result,
	component *cloudprotocol.ComponentInfoFromCloud, componentStatus cloudprotocol.ComponentInfo) (err error) {
	if err = result.Wait(); err != nil {
		if err != nil {
			return aoserrors.Wrap(err)
		}
	}

	url := url.URL{
		Scheme: "file",
		Path:   result.GetFileName(),
	}

	component.URLs = []string{url.String()}

	fileInfo, err := image.CreateFileInfo(ctx, result.GetFileName())
	if err != nil {
		return aoserrors.Wrap(err)
	}

	component.Size = fileInfo.Size
	component.Sha256 = fileInfo.Sha256
	component.Sha512 = fileInfo.Sha512

	componentStatus.Status = cloudprotocol.DownloadedStatus
	instance.updateComponentStatus(componentStatus)

	return nil
}

func (instance *Instance) installLayers(ctx context.Context, desiredLayers []cloudprotocol.LayerInfoFromCloud,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (err error) {
desiredLoop:
	for _, desiredLayer := range desiredLayers {
		if status, ok := instance.layerStatuses[desiredLayer.Digest]; ok {
			if installed, _ := status.isInstalled(); installed {
				continue desiredLoop
			}
		}

		layerInfo := desiredLayer
		layerStatus := cloudprotocol.LayerInfo{
			ID:         desiredLayer.ID,
			AosVersion: desiredLayer.AosVersion,
			Digest:     desiredLayer.Digest,
		}

		log.WithFields(log.Fields{
			"id":         layerStatus.ID,
			"aosVersion": layerStatus.AosVersion,
			"digest":     layerStatus.Digest,
		}).Debug("Install layer")

		layerStatus.Status = cloudprotocol.PendingStatus
		instance.updateLayerStatus(layerStatus)

		instance.action.PutInQueue(layerInfo.Digest, func(digest string) {
			if layerErr := instance.installLayer(ctx, layerInfo, chains, certs, layerStatus); layerErr != nil {
				layerErr = aoserrors.Wrap(layerErr)

				if strings.Contains(layerErr.Error(), context.Canceled.Error()) {
					log.WithFields(log.Fields{
						"id":     layerStatus.ID,
						"digest": layerStatus.Digest}).Errorf("Download layer aborted: %s", layerErr)
				} else {
					layerStatus.Status = cloudprotocol.ErrorStatus
					layerStatus.Error = layerErr.Error()
					instance.updateLayerStatus(layerStatus)
				}

				if err == nil {
					err = layerErr
				}
			}
		})
	}

	instance.action.Wait()

	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *Instance) removeLayers(
	ctx context.Context, desiredLayers []cloudprotocol.LayerInfoFromCloud) (err error) {
nextLayer:
	for digest, status := range instance.layerStatuses {
		installed, descriptor := status.isInstalled()
		if !installed {
			continue
		}

		for _, desiredLayer := range desiredLayers {
			if desiredLayer.Digest == digest {
				continue nextLayer
			}
		}

		layerDigest := digest
		layerStatus := *descriptor.amqpStatus.(*cloudprotocol.LayerInfo)

		log.WithFields(log.Fields{
			"id":         layerStatus.ID,
			"aosVersion": layerStatus.AosVersion,
			"digest":     layerStatus.Digest,
		}).Debug("Remove layer")

		layerStatus.Status = cloudprotocol.PendingStatus
		instance.updateLayerStatus(layerStatus)

		instance.action.PutInQueue(layerDigest, func(digest string) {
			if layerErr := instance.removeLayer(ctx, layerDigest, layerStatus); layerErr != nil {
				layerStatus.Status = cloudprotocol.ErrorStatus
				layerStatus.Error = layerErr.Error()
				instance.updateLayerStatus(layerStatus)

				if err == nil {
					err = aoserrors.Wrap(layerErr)
				}
			}
		})
	}

	instance.action.Wait()

	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *Instance) updateServices(ctx context.Context, desiredServices []cloudprotocol.ServiceInfoFromCloud,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (err error) {
	for _, desiredService := range desiredServices {
		if serviceStatus, ok := instance.serviceStatuses[desiredService.ID]; ok {
			if installed, descriptor := serviceStatus.isInstalled(); installed &&
				descriptor.amqpStatus.(*cloudprotocol.ServiceInfo).AosVersion >= desiredService.AosVersion {
				continue
			}
		}

		serviceInfo := desiredService
		serviceStatus := cloudprotocol.ServiceInfo{
			ID:         desiredService.ID,
			AosVersion: desiredService.AosVersion,
		}

		log.WithFields(log.Fields{
			"id":         serviceStatus.ID,
			"aosVersion": serviceStatus.AosVersion,
		}).Debug("Install service")

		serviceStatus.Status = cloudprotocol.PendingStatus
		instance.updateServiceStatus(serviceStatus)

		instance.action.PutInQueue(serviceInfo.ID, func(id string) {
			if serviceErr := instance.installService(ctx, serviceInfo, chains, certs, serviceStatus); serviceErr != nil {
				serviceErr = aoserrors.Wrap(serviceErr)

				if strings.Contains(serviceErr.Error(), context.Canceled.Error()) {
					log.WithFields(log.Fields{
						"id": serviceStatus.ID}).Errorf("Download service aborted: %s", serviceErr)
				} else {
					serviceStatus.Status = cloudprotocol.ErrorStatus
					serviceStatus.Error = serviceErr.Error()
					instance.updateServiceStatus(serviceStatus)
				}

				if err == nil {
					err = serviceErr
				}
			}
		})
	}

nextService:
	for id, status := range instance.serviceStatuses {
		installed, descriptor := status.isInstalled()
		if !installed {
			continue
		}

		for _, desiredService := range desiredServices {
			if desiredService.ID == id {
				continue nextService
			}
		}

		serviceID := id
		serviceStatus := *descriptor.amqpStatus.(*cloudprotocol.ServiceInfo)

		log.WithFields(log.Fields{
			"id":         serviceStatus.ID,
			"aosVersion": serviceStatus.AosVersion,
		}).Debug("Remove service")

		serviceStatus.Status = cloudprotocol.PendingStatus
		instance.updateServiceStatus(serviceStatus)

		instance.action.PutInQueue(serviceID, func(id string) {
			if serviceErr := instance.removeService(ctx, serviceID, serviceStatus); serviceErr != nil {
				serviceStatus.Status = cloudprotocol.ErrorStatus
				serviceStatus.Error = serviceErr.Error()
				instance.updateServiceStatus(serviceStatus)

				if err == nil {
					err = aoserrors.Wrap(serviceErr)
				}
			}
		})
	}

	instance.action.Wait()

	if err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (instance *Instance) installLayer(ctx context.Context, layerInfo cloudprotocol.LayerInfoFromCloud,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
	layerStatus cloudprotocol.LayerInfo) (err error) {
	layerStatus.Status = cloudprotocol.DownloadingStatus
	instance.updateLayerStatus(layerStatus)

	var result downloader.Result

	if result, err = instance.downloader.DownloadAndDecrypt(
		ctx, layerInfo.DecryptDataStruct, chains, certs); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = result.Wait(); err != nil {
		return aoserrors.Wrap(err)
	}
	defer func() {
		if err := os.RemoveAll(result.GetFileName()); err != nil {
			log.Errorf("Can't remove decrypted file: %s", err)
		}
	}()

	url := url.URL{
		Scheme: "file",
		Path:   result.GetFileName(),
	}

	layerInfo.URLs = []string{url.String()}

	fileInfo, err := image.CreateFileInfo(ctx, result.GetFileName())
	if err != nil {
		return aoserrors.Wrap(err)
	}

	layerInfo.Size = fileInfo.Size
	layerInfo.Sha256 = fileInfo.Sha256
	layerInfo.Sha512 = fileInfo.Sha512

	layerStatus.Status = cloudprotocol.DownloadedStatus
	instance.updateLayerStatus(layerStatus)

	layerStatus.Status = cloudprotocol.InstallingStatus
	instance.updateLayerStatus(layerStatus)

	if err = instance.softwareUpdater.InstallLayer(layerInfo); err != nil {
		return aoserrors.Wrap(err)
	}

	layerStatus.Status = cloudprotocol.InstalledStatus
	instance.updateLayerStatus(layerStatus)

	return nil
}

func (instance *Instance) removeLayer(ctx context.Context, digest string,
	layerStatus cloudprotocol.LayerInfo) (err error) {
	layerStatus.Status = cloudprotocol.RemovingStatus
	instance.updateLayerStatus(layerStatus)

	if err = instance.softwareUpdater.RemoveLayer(layerStatus); err != nil {
		return aoserrors.Wrap(err)
	}

	layerStatus.Status = cloudprotocol.RemovedStatus
	instance.updateLayerStatus(layerStatus)

	return nil
}

func (instance *Instance) installService(ctx context.Context, serviceInfo cloudprotocol.ServiceInfoFromCloud,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
	serviceStatus cloudprotocol.ServiceInfo) (err error) {
	serviceStatus.Status = cloudprotocol.DownloadingStatus
	instance.updateServiceStatus(serviceStatus)

	var result downloader.Result

	if result, err = instance.downloader.DownloadAndDecrypt(
		ctx, serviceInfo.DecryptDataStruct, chains, certs); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = result.Wait(); err != nil {
		return aoserrors.Wrap(err)
	}
	defer func() {
		if err := os.RemoveAll(result.GetFileName()); err != nil {
			log.Errorf("Can't remove decrypted file: %s", err)
		}
	}()

	url := url.URL{
		Scheme: "file",
		Path:   result.GetFileName(),
	}

	serviceInfo.URLs = []string{url.String()}

	fileInfo, err := image.CreateFileInfo(ctx, result.GetFileName())
	if err != nil {
		return aoserrors.Wrap(err)
	}

	serviceInfo.Size = fileInfo.Size
	serviceInfo.Sha256 = fileInfo.Sha256
	serviceInfo.Sha512 = fileInfo.Sha512

	serviceStatus.Status = cloudprotocol.DownloadedStatus
	instance.updateServiceStatus(serviceStatus)

	serviceStatus.Status = cloudprotocol.InstallingStatus
	instance.updateServiceStatus(serviceStatus)

	var stateChecksum string

	if stateChecksum, err = instance.softwareUpdater.InstallService(serviceInfo); err != nil {
		return aoserrors.Wrap(err)
	}

	serviceStatus.Status = cloudprotocol.InstalledStatus
	serviceStatus.StateChecksum = stateChecksum
	instance.updateServiceStatus(serviceStatus)

	return nil
}

func (instance *Instance) removeService(ctx context.Context, id string,
	serviceStatus cloudprotocol.ServiceInfo) (err error) {
	serviceStatus.Status = cloudprotocol.RemovingStatus
	instance.updateServiceStatus(serviceStatus)

	if err = instance.softwareUpdater.RemoveService(serviceStatus); err != nil {
		return aoserrors.Wrap(err)
	}

	serviceStatus.Status = cloudprotocol.RemovedStatus
	instance.updateServiceStatus(serviceStatus)

	return nil
}

func isUsersEqual(users1, users2 []string) (result bool) {
	if users1 == nil && users2 == nil {
		return true
	}

	if users1 == nil || users2 == nil {
		return false
	}

	if len(users1) != len(users2) {
		return false
	}

	for i := range users1 {
		if users1[i] != users2[i] {
			return false
		}
	}

	return true
}
