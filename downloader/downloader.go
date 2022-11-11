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

package downloader

import (
	"container/list"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/image"
	"github.com/aoscloud/aos_common/spaceallocator"
	"github.com/aoscloud/aos_common/utils/fs"
	"github.com/aoscloud/aos_common/utils/retryhelper"
	"github.com/cavaliergopher/grab/v3"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const updateDownloadsTime = 30 * time.Second

const (
	encryptedFileExt = ".enc"
	interruptFileExt = ".int"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Downloader instance.
type Downloader struct {
	sync.Mutex

	moduleID         string
	config           config.Downloader
	sender           AlertSender
	currentDownloads map[string]*downloadResult
	waitQueue        *list.List
	allocator        spaceallocator.Allocator
}

// PackageInfo struct contains package info data.
type PackageInfo struct {
	URLs                []string
	Sha256              []byte
	Sha512              []byte
	Size                uint64
	TargetType          string
	TargetID            string
	TargetAosVersion    uint64
	TargetVendorVersion string
}

// AlertSender provides alert sender interface.
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem)
}

// NewSpaceAllocator space allocator constructor.
// nolint:gochecknoglobals // used for unit test mock
var NewSpaceAllocator = spaceallocator.New

/***********************************************************************************************************************
* Public
***********************************************************************************************************************/

// New creates new downloader object.
func New(moduleID string, cfg *config.Config, sender AlertSender) (
	downloader *Downloader, err error,
) {
	log.Debug("Create downloader instance")

	downloader = &Downloader{
		moduleID:         moduleID,
		config:           cfg.Downloader,
		sender:           sender,
		currentDownloads: make(map[string]*downloadResult),
		waitQueue:        list.New(),
	}

	if err = os.MkdirAll(downloader.config.DownloadDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	downloader.allocator, err = NewSpaceAllocator(
		downloader.config.DownloadDir, uint(downloader.config.DownloadPartLimit), downloader.removeOutdatedItem)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = downloader.setDownloadDirOutdated(); err != nil {
		log.Errorf("Can't set download dir outdated: %v", err)
	}

	return downloader, nil
}

// Close closes downloader.
func (downloader *Downloader) Close() (err error) {
	if downloadAllocatorErr := downloader.allocator.Close(); downloadAllocatorErr != nil && err == nil {
		err = aoserrors.Wrap(downloadAllocatorErr)
	}

	return err
}

// Download downloads, decrypts and verifies package.
func (downloader *Downloader) Download(
	ctx context.Context, packageInfo PackageInfo,
) (result Result, err error) {
	downloader.Lock()
	defer downloader.Unlock()

	id := base64.URLEncoding.EncodeToString(packageInfo.Sha256)

	downloadResult := &downloadResult{
		id:                id,
		ctx:               ctx,
		packageInfo:       packageInfo,
		statusChannel:     make(chan error, 1),
		downloadFileName:  path.Join(downloader.config.DownloadDir, id+encryptedFileExt),
		interruptFileName: path.Join(downloader.config.DownloadDir, id+interruptFileExt),
	}

	log.WithField("id", id).Debug("Download")

	if err = downloader.addToQueue(downloadResult); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return downloadResult, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (downloader *Downloader) addToQueue(result *downloadResult) error {
	if len(result.packageInfo.URLs) == 0 {
		return aoserrors.New("download URLs is empty")
	}

	if downloader.isResultInQueue(result) {
		return aoserrors.Errorf("download ID %s is being already processed", result.id)
	}

	// if max concurrent downloads exceeds, put into wait queue
	if len(downloader.currentDownloads) >= downloader.config.MaxConcurrentDownloads {
		log.WithField("id", result.id).Debug("Add download to wait queue due to max concurrent downloads")

		downloader.waitQueue.PushBack(result)

		return nil
	}

	// try to allocate space for download. If there is no current downloads and allocation fails then
	// there is no space left. Otherwise, wait till other downloads finished and we will have more room to download.
	if err := downloader.tryAllocateSpace(result); err != nil {
		if len(downloader.currentDownloads) == 0 {
			return aoserrors.Wrap(err)
		}

		log.WithField("id", result.id).Debugf("Add download to wait queue due to: %v", err)

		downloader.waitQueue.PushBack(result)

		return nil
	}

	downloader.currentDownloads[result.id] = result

	go func() {
		processErr := downloader.process(result)

		if err := downloader.acceptSpace(result); err != nil {
			log.Errorf("Error accepting space: %v", err)
		}

		if err := downloader.setDownloadResultOutdated(result); err != nil {
			log.Errorf("Error setting download outdated: %v", err)
		}

		downloader.Lock()
		defer downloader.Unlock()

		delete(downloader.currentDownloads, result.id)

		result.statusChannel <- processErr

		downloader.handleWaitQueue()
	}()

	return nil
}

func (downloader *Downloader) isResultInQueue(result *downloadResult) (present bool) {
	// check current downloads
	if _, ok := downloader.currentDownloads[result.id]; ok {
		return true
	}

	// check wait queue
	for element := downloader.waitQueue.Front(); element != nil; element = element.Next() {
		downloadResult, ok := element.Value.(*downloadResult)
		if !ok {
			return false
		}

		if downloadResult.id == result.id {
			return true
		}
	}

	return false
}

func (downloader *Downloader) tryAllocateSpace(result *downloadResult) (err error) {
	defer func() {
		if err != nil {
			if result.downloadSpace != nil {
				if err := result.downloadSpace.Release(); err != nil {
					log.Errorf("Can't release download space: %v", err)
				}
			}
		}
	}()

	requiredDownloadSize, err := downloader.getRequiredSize(result.downloadFileName, result.packageInfo.Size)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if result.downloadSpace, err = downloader.allocator.AllocateSpace(requiredDownloadSize); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) acceptSpace(result *downloadResult) (err error) {
	if downloadErr := result.downloadSpace.Accept(); downloadErr != nil && err == nil {
		err = downloadErr
	}

	downloadSize, downloadErr := getFileSize(result.downloadFileName)
	if downloadErr != nil && err == nil {
		err = downloadErr
	}

	// free space if file is not fully downloaded
	if downloadSize < result.packageInfo.Size {
		downloader.allocator.FreeSpace(result.packageInfo.Size - downloadSize)
	}

	return err
}

func (downloader *Downloader) setDownloadResultOutdated(result *downloadResult) (err error) {
	if setOutdatedErr := downloader.setItemOutdated(result.downloadFileName); setOutdatedErr != nil && err == nil {
		err = setOutdatedErr
	}

	if setOutdatedErr := downloader.setItemOutdated(result.interruptFileName); setOutdatedErr != nil && err == nil {
		err = setOutdatedErr
	}

	return err
}

func (downloader *Downloader) setDownloadDirOutdated() (err error) {
	entries, err := os.ReadDir(downloader.config.DownloadDir)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, entry := range entries {
		if setOutdatedErr := downloader.setItemOutdated(
			filepath.Join(downloader.config.DownloadDir, entry.Name())); setOutdatedErr != nil && err == nil {
			err = setOutdatedErr
		}
	}

	return err
}

func (downloader *Downloader) setItemOutdated(itemPath string) error {
	var (
		size      uint64
		timestamp time.Time
	)

	info, err := os.Stat(itemPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	if err == nil {
		timestamp = info.ModTime()

		if info.IsDir() {
			if dirSize, err := fs.GetDirSize(itemPath); err == nil {
				size = uint64(dirSize)
			}
		} else {
			size = uint64(info.Size())
		}
	}

	if err := downloader.allocator.AddOutdatedItem(itemPath, size, timestamp); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) removeOutdatedItem(itemPath string) error {
	log.WithField("itemPath", itemPath).Debug("Remove outdated item")

	if err := os.RemoveAll(itemPath); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) getRequiredSize(fileName string, totalSize uint64) (uint64, error) {
	currentSize, err := getFileSize(fileName)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	if totalSize < currentSize {
		log.WithFields(log.Fields{
			"name":         fileName,
			"expectedSize": totalSize,
			"currentSize":  currentSize,
		}).Warnf("File size is larger than expected")

		return totalSize, nil
	}

	return totalSize - currentSize, nil
}

func (downloader *Downloader) process(result *downloadResult) error {
	log.WithFields(log.Fields{"id": result.id}).Debug("Process download")

	if err := downloader.downloadPackage(result); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) handleWaitQueue() {
	numIter := downloader.waitQueue.Len()

	for i := 0; i < numIter; i++ {
		// get first element from wait queue
		firstElement := downloader.waitQueue.Front()

		if firstElement == nil {
			return
		}

		result, _ := firstElement.Value.(*downloadResult)

		downloader.waitQueue.Remove(firstElement)

		log.WithFields(log.Fields{"id": result.id}).Debug("Take download from wait queue")

		var err error

		// Wait either context done or added into queue again
		select {
		case <-result.ctx.Done():
			err = result.ctx.Err()

		default:
			err = downloader.addToQueue(result)
		}

		if err != nil {
			result.statusChannel <- err
			continue
		}

		if len(downloader.currentDownloads) >= downloader.config.MaxConcurrentDownloads {
			return
		}
	}
}

func (downloader *Downloader) downloadPackage(result *downloadResult) (err error) {
	if err = retryhelper.Retry(result.ctx,
		func() (err error) {
			fileSize, err := getFileSize(result.downloadFileName)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			if fileSize != result.packageInfo.Size {
				if err = downloader.downloadURLs(result); err != nil {
					return aoserrors.Wrap(err)
				}
			}

			if err = image.CheckFileInfo(result.ctx, result.downloadFileName, image.FileInfo{
				Sha256: result.packageInfo.Sha256,
				Sha512: result.packageInfo.Sha512,
				Size:   result.packageInfo.Size,
			}); err != nil {
				if removeErr := os.RemoveAll(result.downloadFileName); removeErr != nil {
					log.Errorf("Can't delete file %s: %s", result.downloadFileName, aoserrors.Wrap(removeErr))
				}

				return aoserrors.Wrap(err)
			}

			return nil
		},
		func(retryCount int, delay time.Duration, err error) {
			log.WithFields(log.Fields{"id": result.id}).Debugf("Retry download in %s", delay)
		},
		0, downloader.config.RetryDelay.Duration, downloader.config.MaxRetryDelay.Duration); err != nil {
		return aoserrors.New("can't download file from any source")
	}

	return nil
}

func (downloader *Downloader) downloadURLs(result *downloadResult) (err error) {
	fileDownloaded := false

	for _, url := range result.packageInfo.URLs {
		log.WithFields(log.Fields{"id": result.id, "url": url}).Debugf("Try to download from URL")

		if err = downloader.download(url, result); err != nil {
			continue
		}

		fileDownloaded = true

		break
	}

	if !fileDownloaded {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) download(url string, result *downloadResult) (err error) {
	timer := time.NewTicker(updateDownloadsTime)
	defer timer.Stop()

	req, err := grab.NewRequest(result.downloadFileName, url)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	req = req.WithContext(result.ctx)
	req.Size = int64(result.packageInfo.Size)

	resp := grab.DefaultClient.Do(req)

	if !resp.DidResume {
		log.WithFields(log.Fields{"url": url, "id": result.id}).Debug("Download started")

		downloader.sender.SendAlert(downloader.prepareDownloadAlert(resp, result, "Download started"))
	} else {
		reason := result.retrieveInterruptReason()

		log.WithFields(log.Fields{"url": url, "id": result.id, "reason": reason}).Debug("Download resumed")

		downloader.sender.SendAlert(downloader.prepareDownloadAlert(resp, result, "Download resumed reason: "+reason))

		result.removeInterruptReason()
	}

	for {
		select {
		case <-timer.C:
			downloader.sender.SendAlert(downloader.prepareDownloadAlert(resp, result, "Download status"))

			log.WithFields(log.Fields{"complete": resp.BytesComplete(), "total": resp.Size}).Debug("Download progress")

		case <-resp.Done:
			if err = resp.Err(); err != nil {
				log.WithFields(log.Fields{
					"id":         result.id,
					"file":       resp.Filename,
					"downloaded": resp.BytesComplete(), "reason": err,
				}).Warn("Download interrupted")

				result.storeInterruptReason(err.Error())

				downloader.sender.SendAlert(downloader.prepareDownloadAlert(
					resp, result, "Download interrupted reason: "+err.Error()))

				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":         result.id,
				"file":       resp.Filename,
				"downloaded": resp.BytesComplete(),
			}).Debug("Download completed")

			downloader.sender.SendAlert(
				downloader.prepareDownloadAlert(
					resp, result, "Download finished code: "+strconv.Itoa(resp.HTTPResponse.StatusCode)))

			return nil
		}
	}
}

func (downloader *Downloader) prepareDownloadAlert(
	resp *grab.Response, result *downloadResult, msg string,
) cloudprotocol.AlertItem {
	return cloudprotocol.AlertItem{
		Timestamp: time.Now(), Tag: cloudprotocol.AlertTagDownloadProgress,
		Payload: cloudprotocol.DownloadAlert{
			TargetType:          result.packageInfo.TargetType,
			TargetID:            result.packageInfo.TargetID,
			TargetAosVersion:    result.packageInfo.TargetAosVersion,
			TargetVendorVersion: result.packageInfo.TargetVendorVersion,
			Progress:            fmt.Sprintf("%.2f%%", resp.Progress()*100),
			URL:                 resp.Request.HTTPRequest.URL.String(),
			DownloadedBytes:     bytefmt.ByteSize(uint64(resp.BytesComplete())),
			TotalBytes:          bytefmt.ByteSize(uint64(resp.Size())),
			Message:             msg,
		},
	}
}

func getFileSize(fileName string) (size uint64, err error) {
	var stat syscall.Stat_t

	if err = syscall.Stat(fileName, &stat); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, aoserrors.Wrap(err)
	}

	return uint64(stat.Size), nil
}
