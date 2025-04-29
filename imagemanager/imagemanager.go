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

package imagemanager

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"path"
	"time"

	"golang.org/x/exp/slices"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/image"
	"github.com/aosedge/aos_common/spaceallocator"
	"github.com/aosedge/aos_common/utils/semverutils"
	imagespec "github.com/opencontainers/image-spec/specs-go/v1"
	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/config"
	"github.com/aosedge/aos_communicationmanager/fcrypt"
	"github.com/aosedge/aos_communicationmanager/fileserver"
	"github.com/aosedge/aos_communicationmanager/unitstatushandler"
	"github.com/aosedge/aos_communicationmanager/utils/uidgidpool"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	decryptedFileExt = ".dec"

	fileScheme = "file"

	blobsFolder = "blobs"

	removePeriod = 24 * time.Hour
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Storage provides API to create, remove or access information from DB.
type Storage interface {
	AddLayer(layer LayerInfo) error
	GetLayerInfo(digest string) (LayerInfo, error)
	GetLayersInfo() ([]LayerInfo, error)
	RemoveLayer(digest string) error
	SetLayerState(digest string, state int) error
	AddService(service ServiceInfo) error
	GetServicesInfo() ([]ServiceInfo, error)
	GetServiceVersions(serviceID string) ([]ServiceInfo, error)
	RemoveService(serviceID string, version string) error
	SetServiceState(serviceID, version string, state int) error
}

// Decrypter interface to decrypt and validate image.
type Decrypter interface {
	DecryptAndValidate(encryptedFile, decryptedFile string, params fcrypt.DecryptParams) error
}

// Imagemanager image manager instance.
type Imagemanager struct {
	layersDir              string
	servicesDir            string
	tmpDir                 string
	storage                Storage
	decrypter              Decrypter
	serviceAllocator       spaceallocator.Allocator
	layerAllocator         spaceallocator.Allocator
	tmpAllocator           spaceallocator.Allocator
	gidPool                *uidgidpool.IdentifierPool
	serviceTTL             time.Duration
	layerTTL               time.Duration
	validateTTLStopChannel chan struct{}
	removeServiceChannel   chan string
	fileServer             *fileserver.FileServer
}

// Service state.
const (
	ServiceActive = iota
	ServiceCached
	ServicePending
)

// ServiceInfo service information.
type ServiceInfo struct {
	aostypes.ServiceInfo
	RemoteURL    string
	Path         string
	Timestamp    time.Time
	State        int
	Config       aostypes.ServiceConfig
	Layers       []string
	ExposedPorts []string
}

// Layer state.
const (
	LayerActive = iota
	LayerCached
)

// LayerInfo service information.
type LayerInfo struct {
	aostypes.LayerInfo
	RemoteURL string
	Path      string
	Timestamp time.Time
	State     int
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	// ErrNotExist not exist item error.
	ErrNotExist = errors.New("item not exist")

	// ErrVersionMismatch new service version <= existing one.
	ErrVersionMismatch = errors.New("version mismatch")

	// NewSpaceAllocator space allocator constructor.
	//nolint:gochecknoglobals // used for unit test mock
	NewSpaceAllocator = spaceallocator.New
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/
// New creates new image manager object.
func New(
	cfg *config.Config, storage Storage, decrypter Decrypter,
) (imagemanager *Imagemanager, err error) {
	imagemanager = &Imagemanager{
		layersDir:              path.Join(cfg.ImageStoreDir, "layers"),
		servicesDir:            path.Join(cfg.ImageStoreDir, "services"),
		tmpDir:                 path.Join(cfg.ImageStoreDir, "tmp"),
		storage:                storage,
		decrypter:              decrypter,
		serviceTTL:             cfg.ServiceTTL.Duration,
		layerTTL:               cfg.LayerTTL.Duration,
		gidPool:                uidgidpool.NewGroupIDPool(),
		validateTTLStopChannel: make(chan struct{}),
		removeServiceChannel:   make(chan string, 1),
	}

	if err := os.MkdirAll(imagemanager.layersDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err := os.MkdirAll(imagemanager.servicesDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err := os.MkdirAll(imagemanager.tmpDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if cfg.SMController.FileServerURL != "" {
		if imagemanager.fileServer, err = fileserver.New(
			cfg.SMController.FileServerURL, cfg.ImageStoreDir); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	serviceAllocator, err := NewSpaceAllocator(imagemanager.servicesDir, 0, imagemanager.removeOutdatedService)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	layerAllocator, err := NewSpaceAllocator(imagemanager.layersDir, 0, imagemanager.removeOutdatedLayer)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	tmpAllocator, err := NewSpaceAllocator(imagemanager.tmpDir, 0, nil)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	imagemanager.serviceAllocator = serviceAllocator
	imagemanager.layerAllocator = layerAllocator
	imagemanager.tmpAllocator = tmpAllocator

	if err = imagemanager.setOutdatedServices(); err != nil {
		return nil, err
	}

	if err = imagemanager.setOutdatedLayers(); err != nil {
		return nil, err
	}

	go imagemanager.validateTTLs()

	return imagemanager, nil
}

// Close close image manager object.
func (imagemanager *Imagemanager) Close() {
	if err := imagemanager.serviceAllocator.Close(); err != nil {
		log.Errorf("Can't close service allocator: %v", err)
	}

	if err := imagemanager.layerAllocator.Close(); err != nil {
		log.Errorf("Can't close layer allocator: %v", err)
	}

	if err := imagemanager.tmpAllocator.Close(); err != nil {
		log.Errorf("Can't close tmp allocator: %v", err)
	}

	if imagemanager.fileServer != nil {
		if err := imagemanager.fileServer.Close(); err != nil {
			log.Errorf("Can't close fileserver: %v", err)
		}
	}

	close(imagemanager.validateTTLStopChannel)
	close(imagemanager.removeServiceChannel)
}

// GetServicesStatus gets all services status.
func (imagemanager *Imagemanager) GetServicesStatus() ([]unitstatushandler.ServiceStatus, error) {
	log.Debug("Get services status")

	servicesInfo, err := imagemanager.storage.GetServicesInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	servicesStatus := make([]unitstatushandler.ServiceStatus, len(servicesInfo))

	for i, service := range servicesInfo {
		if service.State == ServicePending {
			continue
		}

		servicesStatus[i] = unitstatushandler.ServiceStatus{
			ServiceStatus: cloudprotocol.ServiceStatus{
				ServiceID: service.ServiceID,
				Version:   service.Version,
				Status:    cloudprotocol.InstalledStatus,
			}, Cached: service.State != ServiceActive,
		}
	}

	return servicesStatus, nil
}

// GetLayersStatus gets all layers status.
func (imagemanager *Imagemanager) GetLayersStatus() ([]unitstatushandler.LayerStatus, error) {
	log.Debug("Get layers status")

	layersInfo, err := imagemanager.storage.GetLayersInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	layersStatus := make([]unitstatushandler.LayerStatus, len(layersInfo))

	for i, layer := range layersInfo {
		layersStatus[i] = unitstatushandler.LayerStatus{
			LayerStatus: cloudprotocol.LayerStatus{
				LayerID: layer.LayerID,
				Version: layer.Version,
				Digest:  layer.Digest,
				Status:  cloudprotocol.InstalledStatus,
			}, Cached: layer.State != LayerActive,
		}
	}

	return layersStatus, nil
}

func (imagemanager *Imagemanager) GetRemoveServiceChannel() (channel <-chan string) {
	return imagemanager.removeServiceChannel
}

// InstallService installs service to the image store dir.
func (imagemanager *Imagemanager) InstallService(serviceInfo cloudprotocol.ServiceInfo,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) error {
	log.WithFields(log.Fields{"id": serviceInfo.ServiceID, "version": serviceInfo.Version}).Debug("Install service")

	currentServices, err := imagemanager.storage.GetServiceVersions(serviceInfo.ServiceID)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	alreadyInstalled, err := imagemanager.checkCurrentServices(currentServices, serviceInfo)
	if err != nil {
		return err
	}

	if alreadyInstalled {
		return nil
	}

	decryptedFile := path.Join(imagemanager.servicesDir, base64.URLEncoding.EncodeToString(serviceInfo.Sha256))

	space, err := imagemanager.serviceAllocator.AllocateSpace(serviceInfo.Size)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	defer func() {
		if err != nil {
			releaseAllocatedSpace(decryptedFile, space)

			log.WithFields(log.Fields{
				"id":        serviceInfo.ServiceID,
				"version":   serviceInfo.Version,
				"imagePath": decryptedFile,
			}).Errorf("Can't install service: %v", err)

			return
		}

		if err := space.Accept(); err != nil {
			log.Errorf("Can't accept memory: %v", err)
		}
	}()

	encryptedFile, err := getFilePath(serviceInfo.URLs)
	if err != nil {
		return err
	}

	if err = imagemanager.decrypter.DecryptAndValidate(encryptedFile, decryptedFile,
		fcrypt.DecryptParams{
			Chains:         chains,
			Certs:          certs,
			DecryptionInfo: serviceInfo.DecryptionInfo,
			Signs:          serviceInfo.Signs,
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	var gid int

	if len(currentServices) > 0 {
		gid = int(currentServices[0].GID)
	} else {
		if gid, err = imagemanager.gidPool.GetFreeID(); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if err = imagemanager.addService(decryptedFile, serviceInfo, gid); err != nil {
		return err
	}

	return nil
}

func (imagemanager *Imagemanager) addService(
	decryptedFile string, serviceInfo cloudprotocol.ServiceInfo, gid int,
) error {
	layers, exposedPorts, serviceConfig, err := imagemanager.getServiceDataFromManifest(decryptedFile)
	if err != nil {
		return err
	}

	remoteURL, err := imagemanager.createRemoteURL(path.Join("services", path.Base(decryptedFile)))
	if err != nil {
		return err
	}

	fileInfo, err := image.CreateFileInfo(context.Background(), decryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = imagemanager.updatePrevServiceVersions(serviceInfo.ServiceID, serviceInfo.Version); err != nil {
		return err
	}

	if err = imagemanager.storage.AddService(ServiceInfo{
		ServiceInfo: aostypes.ServiceInfo{
			Version:    serviceInfo.Version,
			ServiceID:  serviceInfo.ServiceID,
			ProviderID: serviceInfo.ProviderID,
			URL:        createLocalURL(decryptedFile),
			Size:       fileInfo.Size,
			GID:        uint32(gid),
			Sha256:     fileInfo.Sha256,
		},
		State:        ServiceActive,
		RemoteURL:    remoteURL,
		Path:         decryptedFile,
		Timestamp:    time.Now().UTC(),
		Config:       serviceConfig,
		Layers:       layers,
		ExposedPorts: exposedPorts,
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// RestoreService restores service from a cache.
func (imagemanager *Imagemanager) RestoreService(serviceID string) error {
	log.WithFields(log.Fields{"serviceID": serviceID}).Debug("Restore service")

	existingServices, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if index := slices.IndexFunc(existingServices, func(service ServiceInfo) bool {
		return service.State == ServiceActive
	}); index != -1 {
		log.WithFields(log.Fields{
			"serviceID": existingServices[index].ServiceID, "version": existingServices[index].Version,
		}).Warn("Service already active")

		return nil
	}

	if index := slices.IndexFunc(existingServices, func(service ServiceInfo) bool {
		return service.State == ServicePending
	}); index != -1 {
		log.WithFields(log.Fields{
			"serviceID": existingServices[index].ServiceID, "version": existingServices[index].Version,
		}).Warn("Restore pending service")

		return imagemanager.activateService(existingServices[index])
	}

	if index := slices.IndexFunc(existingServices, func(service ServiceInfo) bool {
		return service.State == ServiceCached
	}); index != -1 {
		log.WithFields(log.Fields{
			"serviceID": existingServices[index].ServiceID, "version": existingServices[index].Version,
		}).Debug("Restore service")

		return imagemanager.activateService(existingServices[index])
	}

	return ErrNotExist
}

// RemoveService makes service cached.
func (imagemanager *Imagemanager) RemoveService(serviceID string) error {
	log.WithFields(log.Fields{"serviceID": serviceID}).Debug("Remove service")

	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, service := range services {
		switch service.State {
		case ServiceActive:
			if err = imagemanager.setServiceState(service, ServiceCached); err != nil {
				return err
			}

		case ServicePending:
			if err := imagemanager.removeService(service); err != nil {
				return err
			}

		default:
			log.WithFields(log.Fields{
				"serviceID": service.ServiceID, "version": service.Version,
			}).Warn("Service already cached")
		}
	}

	return nil
}

// InstallLayer installs layer to the image store dir.
func (imagemanager *Imagemanager) InstallLayer(layerInfo cloudprotocol.LayerInfo,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) error {
	log.WithFields(log.Fields{
		"id": layerInfo.LayerID, "digest": layerInfo.Digest, "version": layerInfo.Version,
	}).Debug("Install layer")

	currentLayer, err := imagemanager.storage.GetLayerInfo(layerInfo.Digest)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	if err == nil {
		alreadyInstalled, err := imagemanager.checkCurrentLayer(currentLayer, layerInfo)
		if err != nil {
			return err
		}

		if alreadyInstalled {
			return nil
		}
	}

	id := base64.URLEncoding.EncodeToString(layerInfo.Sha256)
	decryptedFile := path.Join(imagemanager.layersDir, id+decryptedFileExt)

	space, err := imagemanager.layerAllocator.AllocateSpace(layerInfo.Size)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	defer func() {
		if err != nil {
			releaseAllocatedSpace(decryptedFile, space)

			log.WithFields(log.Fields{
				"id": layerInfo.LayerID, "version": layerInfo.Version, "imagePath": decryptedFile,
			}).Errorf("Can't install layer: %v", err)

			return
		}

		if err := space.Accept(); err != nil {
			log.Errorf("Can't accept memory: %v", err)
		}
	}()

	encryptedFile, err := getFilePath(layerInfo.URLs)
	if err != nil {
		return err
	}

	if err := imagemanager.decrypter.DecryptAndValidate(encryptedFile, decryptedFile,
		fcrypt.DecryptParams{
			Chains:         chains,
			Certs:          certs,
			DecryptionInfo: layerInfo.DecryptionInfo,
			Signs:          layerInfo.Signs,
		}); err != nil {
		return aoserrors.Wrap(err)
	}

	remoteURL, err := imagemanager.createRemoteURL(path.Join("layers", path.Base(decryptedFile)))
	if err != nil {
		return err
	}

	fileInfo, err := image.CreateFileInfo(context.Background(), decryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err := imagemanager.storage.AddLayer(LayerInfo{
		LayerInfo: aostypes.LayerInfo{
			Version: layerInfo.Version,
			LayerID: layerInfo.LayerID,
			Digest:  layerInfo.Digest,
			URL:     createLocalURL(decryptedFile),
			Sha256:  fileInfo.Sha256,
			Size:    fileInfo.Size,
		},
		State:     LayerActive,
		Path:      decryptedFile,
		RemoteURL: remoteURL,
		Timestamp: time.Now().UTC(),
	}); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// RemoveLayer makes layer cached.
func (imagemanager *Imagemanager) RemoveLayer(digest string) error {
	log.WithFields(log.Fields{"digest": digest}).Debug("Remove layer")

	layer, err := imagemanager.storage.GetLayerInfo(digest)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if layer.State == LayerCached {
		log.WithFields(log.Fields{"digest": digest}).Warn("Layer already cached")

		return nil
	}

	if err = imagemanager.setLayerState(layer, LayerCached); err != nil {
		return err
	}

	return nil
}

// RestoreLayer restores layer from a cache.
func (imagemanager *Imagemanager) RestoreLayer(digest string) error {
	log.WithFields(log.Fields{"digest": digest}).Debug("Restore layer")

	layer, err := imagemanager.storage.GetLayerInfo(digest)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if layer.State == LayerActive {
		log.WithField("digest", digest).Warn("Layer already active")

		return nil
	}

	if err = imagemanager.setLayerState(layer, LayerActive); err != nil {
		return err
	}

	return nil
}

// GetServiceInfo returns active service information by id.
func (imagemanager *Imagemanager) GetServiceInfo(serviceID string) (ServiceInfo, error) {
	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return ServiceInfo{}, aoserrors.Wrap(err)
	}

	if index := slices.IndexFunc(services, func(service ServiceInfo) bool {
		return service.State == ServiceActive
	}); index != -1 {
		return services[index], nil
	}

	return ServiceInfo{}, ErrNotExist
}

// GetLayerInfo returns active layer information by id.
func (imagemanager *Imagemanager) GetLayerInfo(digest string) (LayerInfo, error) {
	layerInfo, err := imagemanager.storage.GetLayerInfo(digest)
	if err != nil {
		return LayerInfo{}, aoserrors.Wrap(err)
	}

	if layerInfo.State != LayerActive {
		return LayerInfo{}, ErrNotExist
	}

	return layerInfo, aoserrors.Wrap(err)
}

// RevertService reverts already stored service.
func (imagemanager *Imagemanager) RevertService(serviceID string) error {
	log.WithFields(log.Fields{"serviceID": serviceID}).Debug("Revert service")

	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if !slices.ContainsFunc(services, func(service ServiceInfo) bool {
		return service.State == ServicePending
	}) {
		return ErrNotExist
	}

	for _, service := range services {
		if service.State == ServicePending {
			if err = imagemanager.setServiceState(service, ServiceActive); err != nil {
				return err
			}

			continue
		}

		if err = imagemanager.removeService(service); err != nil {
			return err
		}
	}

	return nil
}

/***********************************************************************************************************************
* Private
**********************************************************************************************************************/

func (imagemanager *Imagemanager) checkCurrentServices(
	currentServices []ServiceInfo, newService cloudprotocol.ServiceInfo,
) (alreadyInstalled bool, err error) {
	if index := slices.IndexFunc(currentServices, func(service ServiceInfo) bool {
		return service.State == ServiceActive
	}); index != -1 {
		versionResult, err := semverutils.Compare(newService.Version, currentServices[index].Version)
		if err != nil {
			return false, aoserrors.Wrap(err)
		}

		if versionResult == 0 {
			log.WithFields(log.Fields{
				"serviceID": newService.ServiceID, "version": newService.Version,
			}).Warn("Service already installed")

			return true, nil
		}

		if versionResult < 0 {
			return false, ErrVersionMismatch
		}
	}

	if index := slices.IndexFunc(currentServices, func(service ServiceInfo) bool {
		return service.Version == newService.Version
	}); index != -1 {
		log.WithFields(log.Fields{
			"serviceID": currentServices[index].ServiceID,
			"version":   currentServices[index].Version,
		}).Warn("Restore service through install")

		if err = imagemanager.activateService(currentServices[index]); err != nil {
			return false, err
		}
	}

	return false, nil
}

func (imagemanager *Imagemanager) checkCurrentLayer(
	currentLayer LayerInfo, newLayer cloudprotocol.LayerInfo,
) (alreadyInstalled bool, err error) {
	if currentLayer.Version == newLayer.Version && currentLayer.LayerID == newLayer.LayerID {
		if currentLayer.State == LayerCached {
			log.WithFields(log.Fields{
				"id":      currentLayer.LayerID,
				"digest":  currentLayer.Digest,
				"version": currentLayer.Version,
			}).Warn("Restore cached layer")

			if err := imagemanager.setLayerState(currentLayer, LayerActive); err != nil {
				return false, err
			}
		} else {
			log.WithFields(log.Fields{
				"id":      currentLayer.LayerID,
				"digest":  currentLayer.Digest,
				"version": currentLayer.Version,
			}).Warn("Layer already installed")
		}

		return true, nil
	}

	log.WithFields(log.Fields{
		"id":      currentLayer.LayerID,
		"digest":  currentLayer.Digest,
		"version": currentLayer.Version,
	}).Warn("Remove same digest layer")

	if err = imagemanager.removeLayer(currentLayer); err != nil {
		return false, err
	}

	return false, nil
}

func (imagemanager *Imagemanager) updatePrevServiceVersions(serviceID, version string) error {
	serviceVersions, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	if curIndex := slices.IndexFunc(serviceVersions, func(service ServiceInfo) bool {
		return service.Version == version
	}); curIndex != -1 {
		serviceVersions = append(serviceVersions[:curIndex], serviceVersions[curIndex+1:]...)
	}

	pendingSet := false

	for _, service := range serviceVersions {
		// previous active service should be in pending state for revert if needed
		if service.State == ServiceActive && !pendingSet {
			if err = imagemanager.setServiceState(service, ServicePending); err != nil {
				return err
			}

			pendingSet = true

			continue
		}

		// other should be removed
		if err = imagemanager.removeService(service); err != nil {
			return err
		}
	}

	return nil
}

func (imagemanager *Imagemanager) activateService(service ServiceInfo) error {
	if err := imagemanager.updatePrevServiceVersions(service.ServiceID, service.Version); err != nil {
		return err
	}

	return imagemanager.setServiceState(service, ServiceActive)
}

func (imagemanager *Imagemanager) setServiceState(service ServiceInfo, state int) error {
	if service.State == state {
		return nil
	}

	if err := imagemanager.storage.SetServiceState(service.ServiceID, service.Version, state); err != nil {
		return aoserrors.Wrap(err)
	}

	if state == ServiceCached {
		if err := imagemanager.serviceAllocator.AddOutdatedItem(
			service.ServiceID, service.Size, service.Timestamp); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil
	}

	if service.State == ServiceCached {
		imagemanager.serviceAllocator.RestoreOutdatedItem(service.ServiceID)
	}

	return nil
}

func (imagemanager *Imagemanager) setLayerState(layer LayerInfo, state int) error {
	if layer.State == state {
		return nil
	}

	if err := imagemanager.storage.SetLayerState(layer.Digest, state); err != nil {
		return aoserrors.Wrap(err)
	}

	if state == LayerCached {
		if err := imagemanager.layerAllocator.AddOutdatedItem(
			layer.Digest, layer.Size, layer.Timestamp); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil
	}

	if layer.State == LayerCached {
		imagemanager.layerAllocator.RestoreOutdatedItem(layer.Digest)
	}

	return nil
}

func (imagemanager *Imagemanager) getServiceDataFromManifest(
	sourceFile string,
) (layers []string, exposedPorts []string, serviceConfig aostypes.ServiceConfig, err error) {
	size, err := image.GetUncompressedTarContentSize(sourceFile)
	if err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	space, err := imagemanager.tmpAllocator.AllocateSpace(uint64(size))
	if err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	defer func() {
		if err := space.Release(); err != nil {
			log.Errorf("Can't release memory: %v", err)
		}
	}()

	imagePath, err := os.MkdirTemp(imagemanager.tmpDir, "")
	if err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	defer os.RemoveAll(imagePath)

	if err = image.UnpackTarImage(sourceFile, imagePath); err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	manifest, err := image.GetImageManifest(imagePath)
	if err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	layers = image.GetLayersFromManifest(manifest)

	imageConfigPath := path.Join(imagePath, blobsFolder, string(manifest.Config.Digest.Algorithm()),
		manifest.Config.Digest.Hex())

	var imageConfig imagespec.Image

	if err = getJSONFromFile(imageConfigPath, &imageConfig); err != nil {
		return nil, nil, serviceConfig, aoserrors.Wrap(err)
	}

	if manifest.AosService != nil {
		if err = image.ValidateDigest(imagePath, manifest.AosService.Digest); err != nil {
			return nil, nil, serviceConfig, aoserrors.Wrap(err)
		}

		byteValue, err := os.ReadFile(path.Join(
			imagePath, blobsFolder, string(manifest.AosService.Digest.Algorithm()), manifest.AosService.Digest.Hex()))
		if err != nil {
			return nil, nil, serviceConfig, aoserrors.Wrap(err)
		}

		if err = json.Unmarshal(byteValue, &serviceConfig); err != nil {
			return nil, nil, serviceConfig, aoserrors.Errorf("invalid Aos service config: %v", err)
		}
	}

	for exposedPort := range imageConfig.Config.ExposedPorts {
		exposedPorts = append(exposedPorts, exposedPort)
	}

	return layers, exposedPorts, serviceConfig, nil
}

func (imagemanager *Imagemanager) clearServiceResource(service ServiceInfo) error {
	if err := os.RemoveAll(service.Path); err != nil {
		return aoserrors.Wrap(err)
	}

	if service.State == ServiceCached {
		imagemanager.serviceAllocator.RestoreOutdatedItem(service.ServiceID)
	}

	imagemanager.serviceAllocator.FreeSpace(service.Size)

	return nil
}

func (imagemanager *Imagemanager) clearLayerResource(layer LayerInfo) error {
	if err := os.RemoveAll(layer.Path); err != nil {
		return aoserrors.Wrap(err)
	}

	if layer.State == LayerCached {
		imagemanager.layerAllocator.RestoreOutdatedItem(layer.Digest)
	}

	imagemanager.layerAllocator.FreeSpace(layer.Size)

	return nil
}

func (imagemanager *Imagemanager) removeService(service ServiceInfo) error {
	log.WithFields(log.Fields{
		"serviceID": service.ServiceID,
		"version":   service.Version,
		"imagePath": service.Path,
		"state":     service.State,
	}).Debug("Remove service")

	if err := imagemanager.clearServiceResource(service); err != nil {
		return err
	}

	if err := imagemanager.storage.RemoveService(service.ServiceID, service.Version); err != nil {
		return aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"serviceID": service.ServiceID}).Info("Service successfully removed")

	return nil
}

func (imagemanager *Imagemanager) removeLayer(layer LayerInfo) error {
	if err := imagemanager.clearLayerResource(layer); err != nil {
		return err
	}

	if err := imagemanager.storage.RemoveLayer(layer.Digest); err != nil {
		return aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"digest": layer.Digest}).Info("Layer successfully removed")

	return nil
}

func (imagemanager *Imagemanager) createRemoteURL(decryptedFile string) (string, error) {
	if imagemanager.fileServer == nil {
		return "", nil
	}

	url, err := imagemanager.fileServer.TranslateURL(false, decryptedFile)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return url, nil
}

func (imagemanager *Imagemanager) removeOutdatedLayer(digest string) error {
	log.WithFields(log.Fields{"digest": digest}).Debug("Remove outdated layer")

	layer, err := imagemanager.storage.GetLayerInfo(digest)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = os.RemoveAll(layer.Path); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = imagemanager.storage.RemoveLayer(digest); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (imagemanager *Imagemanager) removeOutdatedService(serviceID string) error {
	log.WithFields(log.Fields{"serviceID": serviceID}).Debug("Remove outdated service")

	imagemanager.removeServiceChannel <- serviceID

	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	var errRem error

	for _, service := range services {
		if errRem = os.RemoveAll(service.Path); errRem != nil && err == nil {
			err = errRem
		}

		if errRem = imagemanager.storage.RemoveService(service.ServiceID, service.Version); errRem != nil && err == nil {
			err = errRem
		}
	}

	if len(services) > 0 {
		if errRem = imagemanager.gidPool.RemoveID(int(services[0].GID)); errRem != nil && err == nil {
			err = errRem
		}
	}

	return err
}

func (imagemanager *Imagemanager) setOutdatedServices() error {
	services, err := imagemanager.storage.GetServicesInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, service := range services {
		if err = imagemanager.gidPool.AddID(int(service.GID)); err != nil {
			log.Errorf("Can't add service GID to pool: %v", err)
		}

		if service.State == ServiceCached {
			size, err := imagemanager.getServiceSize(service.ServiceID)
			if err != nil {
				return err
			}

			if err = imagemanager.serviceAllocator.AddOutdatedItem(
				service.ServiceID, size, service.Timestamp); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

func (imagemanager *Imagemanager) setOutdatedLayers() error {
	layersInfo, err := imagemanager.storage.GetLayersInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, layer := range layersInfo {
		if layer.State == LayerCached {
			if err = imagemanager.layerAllocator.AddOutdatedItem(
				layer.Digest, layer.Size, layer.Timestamp); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

func (imagemanager *Imagemanager) getServiceSize(serviceID string) (size uint64, err error) {
	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	for _, service := range services {
		size += service.Size
	}

	return size, nil
}

func (imagemanager *Imagemanager) removeOutdatedServices() error {
	services, err := imagemanager.storage.GetServicesInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, service := range services {
		if service.State == ServiceCached &&
			service.Timestamp.Add(imagemanager.serviceTTL).Before(time.Now()) {
			if removeErr := imagemanager.removeService(service); removeErr != nil {
				log.WithField("serviceID", service.ServiceID).Errorf("Can't remove outdated service: %v", removeErr)

				if err == nil {
					err = removeErr
				}
			}
		}
	}

	return err
}

func (imagemanager *Imagemanager) removeOutdatedLayers() error {
	layers, err := imagemanager.storage.GetLayersInfo()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, layer := range layers {
		if layer.State == LayerCached &&
			layer.Timestamp.Add(imagemanager.layerTTL).Before(time.Now()) {
			if err := imagemanager.removeLayer(layer); err != nil {
				return err
			}
		}
	}

	return nil
}

func (imagemanager *Imagemanager) validateTTLs() {
	if err := imagemanager.removeOutdatedServices(); err != nil {
		log.Errorf("Can't remove outdated services: %v", err)
	}

	if err := imagemanager.removeOutdatedLayers(); err != nil {
		log.Errorf("Can't remove outdated layers: %v", err)
	}

	removeTicker := time.NewTicker(removePeriod)
	defer removeTicker.Stop()

	for {
		select {
		case <-removeTicker.C:
			if err := imagemanager.removeOutdatedLayers(); err != nil {
				log.Errorf("Can't remove outdated layers: %v", err)
			}

			if err := imagemanager.removeOutdatedServices(); err != nil {
				log.Errorf("Can't remove outdated layers: %v", err)
			}

		case <-imagemanager.validateTTLStopChannel:
			return
		}
	}
}

func getFilePath(urls []string) (string, error) {
	if len(urls) == 0 {
		return "", aoserrors.New("no download URL")
	}

	urlData, err := url.Parse(urls[0])
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	if urlData.Scheme != fileScheme {
		return "", aoserrors.New("unexpected schema")
	}

	return urlData.Path, nil
}

func createLocalURL(decryptedFile string) string {
	url := url.URL{
		Scheme: fileScheme,
		Path:   decryptedFile,
	}

	return url.String()
}

func releaseAllocatedSpace(filePath string, space spaceallocator.Space) {
	if err := os.RemoveAll(filePath); err != nil {
		log.Errorf("Can't remove decrypted file: %v", err)
	}

	if err := space.Release(); err != nil {
		log.Errorf("Can't release memory: %v", err)
	}
}

func getJSONFromFile(fileName string, data interface{}) error {
	byteValue, err := os.ReadFile(fileName)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if err = json.Unmarshal(byteValue, data); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
