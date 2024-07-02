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

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/image"
	"github.com/aosedge/aos_common/spaceallocator"
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
	GetServiceVersions(serviceID string) ([]ServiceInfo, error)
	GetServicesInfo() ([]ServiceInfo, error)
	GetLayersInfo() ([]LayerInfo, error)
	GetServiceInfo(serviceID string) (ServiceInfo, error)
	GetLayerInfo(digest string) (LayerInfo, error)
	AddLayer(layer LayerInfo) error
	AddService(service ServiceInfo) error
	SetLayerCached(digest string, cached bool) error
	SetServiceCached(serviceID string, cached bool) error
	RemoveService(serviceID string, aosVersion uint64) error
	RemoveLayer(digest string) error
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
	serviceTTLDays         uint64
	layerTTLDays           uint64
	validateTTLStopChannel chan struct{}
	removeServiceChannel   chan string
	fileServer             *fileserver.FileServer
}

// ServiceInfo service information.
type ServiceInfo struct {
	aostypes.ServiceInfo
	RemoteURL    string
	Path         string
	Timestamp    time.Time
	Cached       bool
	Config       aostypes.ServiceConfig
	Layers       []string
	ExposedPorts []string
}

// LayerInfo service information.
type LayerInfo struct {
	aostypes.LayerInfo
	RemoteURL string
	Path      string
	Timestamp time.Time
	Cached    bool
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	// ErrNotExist not exist service error.
	ErrNotExist = errors.New("service not exist")

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
		serviceTTLDays:         cfg.ServiceTTLDays,
		layerTTLDays:           cfg.LayerTTLDays,
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
		servicesStatus[i] = unitstatushandler.ServiceStatus{
			ServiceStatus: cloudprotocol.ServiceStatus{
				ID:         service.ID,
				AosVersion: service.AosVersion,
				Status:     cloudprotocol.InstalledStatus,
			}, Cached: service.Cached,
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
				ID:         layer.ID,
				AosVersion: layer.AosVersion,
				Digest:     layer.Digest,
				Status:     cloudprotocol.InstalledStatus,
			}, Cached: layer.Cached,
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
	log.WithFields(log.Fields{"id": serviceInfo.ID}).Debug("Install service")

	serviceFromStorage, err := imagemanager.storage.GetServiceInfo(serviceInfo.ID)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	var isServiceExist bool

	if err == nil {
		isServiceExist = true

		if serviceInfo.AosVersion <= serviceFromStorage.AosVersion {
			return ErrVersionMismatch
		}
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
				"id":         serviceInfo.ID,
				"aosVersion": serviceInfo.AosVersion,
				"imagePath":  decryptedFile,
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

	if isServiceExist {
		if err = imagemanager.removeObsoleteServiceVersions(serviceFromStorage); err != nil {
			return aoserrors.Wrap(err)
		}

		gid = int(serviceFromStorage.GID)
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

	if err = imagemanager.storage.AddService(ServiceInfo{
		ServiceInfo: aostypes.ServiceInfo{
			VersionInfo: serviceInfo.VersionInfo,
			ID:          serviceInfo.ID,
			ProviderID:  serviceInfo.ProviderID,
			URL:         createLocalURL(decryptedFile),
			Size:        fileInfo.Size,
			GID:         uint32(gid),
			Sha256:      fileInfo.Sha256,
			Sha512:      fileInfo.Sha512,
		},
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

	service, err := imagemanager.storage.GetServiceInfo(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if !service.Cached {
		log.Warningf("Service %s not cached", serviceID)

		return nil
	}

	if err = imagemanager.setServiceCached(service, false); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// RemoveService makes service cached.
func (imagemanager *Imagemanager) RemoveService(serviceID string) error {
	log.WithFields(log.Fields{"serviceID": serviceID}).Debug("Remove service")

	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if len(services) == 0 {
		return nil
	}

	if services[0].Cached {
		log.Warningf("Service %v already cached", serviceID)

		return nil
	}

	var serviceSize uint64

	for _, service := range services {
		serviceSize += service.Size
	}

	if err = imagemanager.setServiceCached(ServiceInfo{
		ServiceInfo: aostypes.ServiceInfo{
			ID:   serviceID,
			Size: serviceSize,
		},
		Timestamp: services[len(services)-1].Timestamp,
	}, true); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

// InstallLayer installs layer to the image store dir.
func (imagemanager *Imagemanager) InstallLayer(layerInfo cloudprotocol.LayerInfo,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
) error {
	log.WithFields(log.Fields{"id": layerInfo.LayerID, "digest": layerInfo.Digest}).Debug("Install layer")

	if layerInfo, err := imagemanager.storage.GetLayerInfo(layerInfo.Digest); err == nil {
		if layerInfo.Cached {
			if err := imagemanager.storage.SetLayerCached(layerInfo.Digest, false); err != nil {
				return aoserrors.Wrap(err)
			}
		}

		return nil
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
				"id":        layerInfo.LayerID,
				"version":   layerInfo.Version,
				"imagePath": decryptedFile,
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
			DecryptionInfo: &layerInfo.DecryptionInfo,
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

	if layer.Cached {
		log.Warningf("Layer %v already cached", digest)

		return nil
	}

	if err = imagemanager.setLayerCached(layer, true); err != nil {
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

	if !layer.Cached {
		log.Warningf("Layer %v not cached", digest)

		return nil
	}

	if err = imagemanager.setLayerCached(layer, false); err != nil {
		return err
	}

	return nil
}

// GetServiceInfo gets service information by id.
func (imagemanager *Imagemanager) GetServiceInfo(serviceID string) (ServiceInfo, error) {
	serviceInfo, err := imagemanager.storage.GetServiceInfo(serviceID)

	return serviceInfo, aoserrors.Wrap(err)
}

// GetLayerInfo gets layer information by id.
func (imagemanager *Imagemanager) GetLayerInfo(digest string) (LayerInfo, error) {
	layerInfo, err := imagemanager.storage.GetLayerInfo(digest)

	return layerInfo, aoserrors.Wrap(err)
}

// RevertService reverts already stored service.
func (imagemanager *Imagemanager) RevertService(serviceID string) error {
	services, err := imagemanager.storage.GetServiceVersions(serviceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if len(services) == 0 {
		return nil
	}

	service := services[len(services)-1]

	if err := imagemanager.removeService(service); err != nil {
		return err
	}

	if service.Cached {
		if err := imagemanager.setServiceCached(service, false); err != nil {
			return err
		}
	}

	return nil
}

/***********************************************************************************************************************
* Private
**********************************************************************************************************************/

func (imagemanager *Imagemanager) setServiceCached(service ServiceInfo, cached bool) error {
	if err := imagemanager.storage.SetServiceCached(service.ID, cached); err != nil {
		return aoserrors.Wrap(err)
	}

	if cached {
		if err := imagemanager.serviceAllocator.AddOutdatedItem(
			service.ID, service.Size, service.Timestamp); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil
	}

	imagemanager.serviceAllocator.RestoreOutdatedItem(service.ID)

	return nil
}

func (imagemanager *Imagemanager) setLayerCached(layer LayerInfo, cached bool) error {
	if err := imagemanager.storage.SetLayerCached(layer.Digest, cached); err != nil {
		return aoserrors.Wrap(err)
	}

	if cached {
		if err := imagemanager.layerAllocator.AddOutdatedItem(
			layer.Digest, layer.Size, layer.Timestamp); err != nil {
			return aoserrors.Wrap(err)
		}

		return nil
	}

	imagemanager.layerAllocator.RestoreOutdatedItem(layer.Digest)

	return nil
}

func (imagemanager *Imagemanager) removeObsoleteServiceVersions(service ServiceInfo) error {
	services, err := imagemanager.storage.GetServiceVersions(service.ID)
	if err != nil && !errors.Is(err, ErrNotExist) {
		return aoserrors.Wrap(err)
	}

	for _, storageService := range services {
		if service.AosVersion != storageService.AosVersion {
			if removeErr := imagemanager.removeService(storageService); removeErr != nil {
				log.WithFields(log.Fields{
					"serviceID":  storageService.ID,
					"AosVersion": storageService.AosVersion,
				}).Errorf("Can't remove service: %v", removeErr)

				if err == nil {
					err = removeErr
				}
			}
		}
	}

	if service.Cached {
		if cacheErr := imagemanager.setServiceCached(service, false); cacheErr != nil {
			log.WithField("serviceID", service.ID).Errorf("Can't cached service: %v", cacheErr)

			if err == nil {
				err = cacheErr
			}
		}
	}

	return err
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

	if service.Cached {
		imagemanager.serviceAllocator.RestoreOutdatedItem(service.ID)
	}

	imagemanager.serviceAllocator.FreeSpace(service.Size)

	return nil
}

func (imagemanager *Imagemanager) clearLayerResource(layer LayerInfo) error {
	if err := os.RemoveAll(layer.Path); err != nil {
		return aoserrors.Wrap(err)
	}

	if layer.Cached {
		imagemanager.serviceAllocator.RestoreOutdatedItem(layer.Digest)
	}

	imagemanager.serviceAllocator.FreeSpace(layer.Size)

	return nil
}

func (imagemanager *Imagemanager) removeService(service ServiceInfo) error {
	if err := imagemanager.clearServiceResource(service); err != nil {
		return err
	}

	if err := imagemanager.storage.RemoveService(service.ID, service.AosVersion); err != nil {
		return aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"serviceID": service.ID}).Info("Service successfully removed")

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

		if errRem = imagemanager.storage.RemoveService(service.ID, service.AosVersion); errRem != nil && err == nil {
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

		if service.Cached {
			size, err := imagemanager.getServiceSize(service.ID)
			if err != nil {
				return err
			}

			if err = imagemanager.serviceAllocator.AddOutdatedItem(
				service.ID, size, service.Timestamp); err != nil {
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
		if layer.Cached {
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
		if service.Cached &&
			service.Timestamp.Add(time.Hour*24*time.Duration(imagemanager.serviceTTLDays)).Before(time.Now()) {
			if removeErr := imagemanager.removeService(service); removeErr != nil {
				log.WithField("serviceID", service.ID).Errorf("Can't remove outdated service: %v", removeErr)

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
		if layer.Cached &&
			layer.Timestamp.Add(time.Hour*24*time.Duration(imagemanager.layerTTLDays)).Before(time.Now()) {
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
