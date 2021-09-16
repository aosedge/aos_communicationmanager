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

package downloader

import (
	"bufio"
	"container/list"
	"context"
	"encoding/base64"
	"errors"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cavaliercoder/grab"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/image"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/retryhelper"

	"aos_communicationmanager/alerts"
	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
	"aos_communicationmanager/fcrypt"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const updateDownloadsTime = 30 * time.Second

const (
	encryptedFileExt = ".enc"
	decryptedFileExt = ".dec"
	interruptFileExt = ".int"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// CryptoContext interface to access crypto functions
type CryptoContext interface {
	ImportSessionKey(keyInfo fcrypt.CryptoSessionKeyInfo) (symmetricContext fcrypt.SymmetricContextInterface, err error)
	CreateSignContext() (signContext fcrypt.SignContextInterface, err error)
}

// Downloader instance
type Downloader struct {
	sync.Mutex

	moduleID           string
	cryptoContext      CryptoContext
	config             config.Downloader
	downloadMountPoint string
	decryptMountPoint  string
	sender             AlertSender
	lockedFiles        []string
	currentDownloads   map[string]*downloadResult
	waitQueue          *list.List
	availableBlocks    map[string]int64
	blockSizes         map[string]int64
}

// AlertSender provdes alert sender interface
type AlertSender interface {
	SendDownloadStartedAlert(downloadStatus alerts.DownloadStatus)
	SendDownloadFinishedAlert(downloadStatus alerts.DownloadStatus, code int)
	SendDownloadInterruptedAlert(downloadStatus alerts.DownloadStatus, reason string)
	SendDownloadResumedAlert(downloadStatus alerts.DownloadStatus, reason string)
	SendDownloadStatusAlert(downloadStatus alerts.DownloadStatus)
}

/*******************************************************************************
 * Vars
 ******************************************************************************/

// ErrNotDownloaded returns when file can't be downloaded
var ErrNotDownloaded = errors.New("can't download file from any source")

/***********************************************************************************************************************
* Public
***********************************************************************************************************************/

// New creates new downloader object
func New(moduleID string, cfg *config.Config, cryptoContext CryptoContext, sender AlertSender) (downloader *Downloader, err error) {
	log.Debug("Create downloader instance")

	downloader = &Downloader{
		moduleID:         moduleID,
		config:           cfg.Downloader,
		sender:           sender,
		cryptoContext:    cryptoContext,
		currentDownloads: make(map[string]*downloadResult),
		waitQueue:        list.New(),
		availableBlocks:  make(map[string]int64),
		blockSizes:       make(map[string]int64),
	}

	if err = os.MkdirAll(downloader.config.DownloadDir, 755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if err = os.MkdirAll(downloader.config.DecryptDir, 755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if downloader.downloadMountPoint, err = getMountPoint(downloader.config.DownloadDir); err != nil {
		return nil, err
	}

	if downloader.decryptMountPoint, err = getMountPoint(downloader.config.DecryptDir); err != nil {
		return nil, err
	}

	return downloader, nil
}

// DownloadAndDecrypt downloads, decrypts and verifies package
func (downloader *Downloader) DownloadAndDecrypt(ctx context.Context, packageInfo cloudprotocol.DecryptDataStruct,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result Result, err error) {
	downloader.Lock()
	defer downloader.Unlock()

	id := base64.URLEncoding.EncodeToString(packageInfo.Sha256)

	downloadResult := &downloadResult{
		id:                id,
		ctx:               ctx,
		packageInfo:       packageInfo,
		chains:            chains,
		certs:             certs,
		statusChannel:     make(chan error, 1),
		release:           downloader.releaseResult,
		decryptedFileName: path.Join(downloader.config.DecryptDir, id+decryptedFileExt),
		downloadFileName:  path.Join(downloader.config.DownloadDir, id+encryptedFileExt),
		interruptFileName: path.Join(downloader.config.DownloadDir, id+interruptFileExt),
	}

	log.WithField("id", id).Debug("Download and decrypt")

	if err = downloader.addToQueue(downloadResult); err != nil {
		return nil, err
	}

	return downloadResult, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (downloader *Downloader) addToQueue(result *downloadResult) (err error) {
	if len(result.packageInfo.URLs) == 0 {
		return aoserrors.New("download URLs is empty")
	}

	if result.packageInfo.DecryptionInfo == nil {
		return aoserrors.New("no decrypt image info")
	}

	if result.packageInfo.Signs == nil {
		return aoserrors.New("no signs info")
	}

	if downloader.isResultInQueue(result) {
		return aoserrors.Errorf("download ID %s is being already processed", result.id)
	}

	defer func() {
		if err != nil {
			downloader.unlockDownload(result)
			downloader.unlockDecrypt(result)
		}
	}()

	downloader.lockDownload(result)
	downloader.lockDecrypt(result)

	// if max concurrent downloads exceeds, put into wait queue
	if len(downloader.currentDownloads) >= downloader.config.MaxConcurrentDownloads {
		log.WithField("id", result.id).Debug("Add download to wait queue due to max concurrent downloads")

		downloader.waitQueue.PushBack(result)

		return nil
	}

	// set initial value for free size variables
	if len(downloader.currentDownloads) == 0 {
		if err = downloader.initAvailableBlocks(); err != nil {
			return err
		}
	}

	// try to allocate space for download and decrypt. If there is no current downloads and allocation fails then
	// there is no space left. Otherwise, wait till other downloads finished and we will have more room to download.
	if err = downloader.tryAllocateSpace(result); err != nil {
		if len(downloader.currentDownloads) == 0 {
			return err
		} else {
			log.WithField("id", result.id).Debugf("Add download to wait queue due to: %s", err)

			downloader.waitQueue.PushBack(result)

			return nil
		}
	}

	downloader.currentDownloads[result.id] = result

	go downloader.process(result)

	return nil
}

func (downloader *Downloader) isResultInQueue(result *downloadResult) (present bool) {
	// check current downloads
	if _, ok := downloader.currentDownloads[result.id]; ok {
		return true
	}

	// check wait queue
	for element := downloader.waitQueue.Front(); element != nil; element = element.Next() {
		if element.Value.(*downloadResult).id == result.id {
			return true
		}
	}

	return false
}

func (downloader *Downloader) isFileLocked(fileName string) (locked bool) {
	for _, lockedFile := range downloader.lockedFiles {
		if fileName == lockedFile {
			return true
		}
	}

	return false
}

func (downloader *Downloader) lockFile(fileName string) {
	log.WithField("file", fileName).Debug("Lock file")

	for _, lockedFile := range downloader.lockedFiles {
		if fileName == lockedFile {
			return
		}
	}

	downloader.lockedFiles = append(downloader.lockedFiles, fileName)
}

func (downloader *Downloader) unlockFile(fileName string) {
	log.WithField("file", fileName).Debug("Unlock file")

	for i, lockedFile := range downloader.lockedFiles {
		if fileName == lockedFile {
			downloader.lockedFiles[i] = downloader.lockedFiles[len(downloader.lockedFiles)-1]
			downloader.lockedFiles = downloader.lockedFiles[:len(downloader.lockedFiles)-1]

			return
		}
	}
}

func (downloader *Downloader) initAvailableBlocks() (err error) {
	var stat syscall.Statfs_t

	if err = syscall.Statfs(downloader.config.DownloadDir, &stat); err != nil {
		return aoserrors.Wrap(err)
	}

	downloader.availableBlocks[downloader.downloadMountPoint] = int64(stat.Bavail)
	downloader.blockSizes[downloader.downloadMountPoint] = stat.Bsize

	if err = syscall.Statfs(downloader.config.DecryptDir, &stat); err != nil {
		return aoserrors.Wrap(err)
	}

	downloader.availableBlocks[downloader.decryptMountPoint] = int64(stat.Bavail)
	downloader.blockSizes[downloader.decryptMountPoint] = stat.Bsize

	return nil
}

func (downloader *Downloader) tryAllocateSpace(result *downloadResult) (err error) {
	requiredDownloadBlocks, err := downloader.getRequiredBlocks(
		result.downloadFileName, downloader.blockSizes[downloader.downloadMountPoint], result.packageInfo.Size)
	if err != nil {
		return err
	}

	requiredDecryptBlocks, err := downloader.getRequiredBlocks(
		result.decryptedFileName, downloader.blockSizes[downloader.decryptMountPoint], result.packageInfo.Size)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"id":              result.id,
		"availableBlocks": downloader.availableBlocks[downloader.downloadMountPoint],
		"blockSize":       downloader.blockSizes[downloader.downloadMountPoint],
		"requiredBlocks":  requiredDownloadBlocks,
	}).Debugf("Check download space")

	if requiredDownloadBlocks > downloader.availableBlocks[downloader.downloadMountPoint] {
		if err = downloader.tryFreeSpace(
			downloader.downloadMountPoint, downloader.config.DownloadDir, requiredDownloadBlocks); err != nil {
			if downloader.downloadMountPoint != downloader.decryptMountPoint {
				return err
			}

			// if decrypt dir and download dir are on same partition, try to free decrypt dir as well
			if err = downloader.tryFreeSpace(
				downloader.decryptMountPoint, downloader.config.DecryptDir, requiredDownloadBlocks); err != nil {
				return err
			}
		}
	}

	downloader.availableBlocks[downloader.downloadMountPoint] =
		downloader.availableBlocks[downloader.downloadMountPoint] - requiredDownloadBlocks

	log.WithFields(log.Fields{
		"id":              result.id,
		"availableBlocks": downloader.availableBlocks[downloader.decryptMountPoint],
		"blockSize":       downloader.blockSizes[downloader.decryptMountPoint],
		"requiredBlocks":  requiredDecryptBlocks,
	}).Debugf("Check decrypt space")

	if requiredDecryptBlocks > downloader.availableBlocks[downloader.decryptMountPoint] {
		if err = downloader.tryFreeSpace(
			downloader.decryptMountPoint, downloader.config.DecryptDir, requiredDecryptBlocks); err != nil {
			if downloader.downloadMountPoint != downloader.decryptMountPoint {
				return err
			}

			// if decrypt dir and download dir are on same partition, try to free download dir as well
			if err = downloader.tryFreeSpace(
				downloader.downloadMountPoint, downloader.config.DownloadDir, requiredDecryptBlocks); err != nil {
				return err
			}
		}
	}

	downloader.availableBlocks[downloader.decryptMountPoint] =
		downloader.availableBlocks[downloader.decryptMountPoint] - requiredDecryptBlocks

	return nil
}

func (downloader *Downloader) tryFreeSpace(mountPoint, dir string, requiredBlocks int64) (err error) {
	log.WithField("dir", dir).Debug("Try to free space")

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	// Sort by modified time
	sort.Slice(files, func(i, j int) bool { return files[i].ModTime().Before(files[j].ModTime()) })

	for _, file := range files {
		fileName := path.Join(dir, file.Name())

		// Do not delete locked files
		if downloader.isFileLocked(fileName) {
			continue
		}

		_, fsSize, err := getFileSize(fileName)
		if err != nil {
			log.Errorf("Can't get FS file size: %s", err)
		}

		fileBlocks := sizeToBlocks(fsSize, downloader.blockSizes[mountPoint])

		if !file.IsDir() {
			log.WithFields(log.Fields{
				"fileName": fileName,
				"blocks":   fileBlocks}).Debugf("Remove outdated file: %s", fileName)
		} else {
			log.WithFields(log.Fields{
				"directory": fileName,
				"blocks":    fileBlocks}).Warnf("Remove foreign directory: %s", fileName)
		}

		if err = os.RemoveAll(fileName); err != nil {
			log.Errorf("Can't remove outdated file: %s", fileName)
			continue
		}

		downloader.availableBlocks[mountPoint] += int64(fileBlocks)

		if downloader.availableBlocks[mountPoint] >= requiredBlocks {
			return nil
		}
	}

	return aoserrors.New("can't free required space")
}

func (downloader *Downloader) getRequiredBlocks(
	fileName string, blockSize int64, size uint64) (requiredBlocks int64, err error) {
	_, fsSize, err := getFileSize(fileName)
	if err != nil && !os.IsNotExist(err) {
		return 0, aoserrors.Wrap(err)
	}

	return sizeToBlocks(size, blockSize) - sizeToBlocks(fsSize, blockSize), nil
}

func sizeToBlocks(fileSize uint64, blockSize int64) (numBlocks int64) {
	return (int64(fileSize) + blockSize - 1) / blockSize
}

func (downloader *Downloader) process(result *downloadResult) (err error) {
	defer func() {
		downloader.Lock()
		defer downloader.Unlock()

		downloader.unlockDownload(result)

		delete(downloader.currentDownloads, result.id)

		result.statusChannel <- err

		downloader.handleWaitQueue()
	}()

	log.WithFields(log.Fields{"id": result.id}).Debug("Process download")

	if err = downloader.downloadWithMaxTry(result); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = downloader.decryptPackage(result); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = downloader.validateSigns(result); err != nil {
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

		result := firstElement.Value.(*downloadResult)
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

func (downloader *Downloader) downloadWithMaxTry(result *downloadResult) (err error) {
	if err = retryhelper.Retry(result.ctx,
		func() (err error) {
			fileSize, _, err := getFileSize(result.downloadFileName)
			if os.IsNotExist(err) || fileSize != result.packageInfo.Size {
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
		downloader.config.RetryCount, downloader.config.RetryDelay.Duration, 0); err != nil {
		return aoserrors.Wrap(ErrNotDownloaded)
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

		downloader.sender.SendDownloadStartedAlert(downloader.getDownloadStatus(resp))
	} else {
		reason := result.retreiveInterruptReason()

		log.WithFields(log.Fields{"url": url, "id": result.id, "reason": reason}).Debug("Download resumed")

		downloader.sender.SendDownloadResumedAlert(downloader.getDownloadStatus(resp), reason)
		result.removeInterruptReason()
	}

	for {
		select {
		case <-timer.C:
			downloader.sender.SendDownloadStatusAlert(downloader.getDownloadStatus(resp))

			log.WithFields(log.Fields{"complete": resp.BytesComplete(), "total": resp.Size}).Debug("Download progress")

		case <-resp.Done:
			if err = resp.Err(); err != nil {
				log.WithFields(log.Fields{
					"id":         result.id,
					"file":       resp.Filename,
					"downloaded": resp.BytesComplete(), "reason": err}).Warn("Download interrupted")

				result.storeInterruptReason(err.Error())
				downloader.sender.SendDownloadInterruptedAlert(downloader.getDownloadStatus(resp), err.Error())

				return aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{
				"id":         result.id,
				"file":       resp.Filename,
				"downloaded": resp.BytesComplete()}).Debug("Download completed")

			downloader.sender.SendDownloadFinishedAlert(downloader.getDownloadStatus(resp), resp.HTTPResponse.StatusCode)

			return nil
		}
	}
}

func (downloader *Downloader) decryptPackage(result *downloadResult) (err error) {
	symmetricCtx, err := downloader.cryptoContext.ImportSessionKey(fcrypt.CryptoSessionKeyInfo{
		SymmetricAlgName:  result.packageInfo.DecryptionInfo.BlockAlg,
		SessionKey:        result.packageInfo.DecryptionInfo.BlockKey,
		SessionIV:         result.packageInfo.DecryptionInfo.BlockIv,
		AsymmetricAlgName: result.packageInfo.DecryptionInfo.AsymAlg,
		ReceiverInfo: fcrypt.ReceiverInfo{
			Issuer: result.packageInfo.DecryptionInfo.ReceiverInfo.Issuer,
			Serial: result.packageInfo.DecryptionInfo.ReceiverInfo.Serial},
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	srcFile, err := os.Open(result.downloadFileName)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(result.decryptedFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer dstFile.Close()

	log.WithFields(log.Fields{"srcFile": srcFile.Name(), "dstFile": dstFile.Name()}).Debug("Decrypt image")

	if err = symmetricCtx.DecryptFile(result.ctx, srcFile, dstFile); err != nil {
		return aoserrors.Wrap(err)
	}

	return aoserrors.Wrap(err)
}

func (downloader *Downloader) validateSigns(result *downloadResult) (err error) {
	signCtx, err := downloader.cryptoContext.CreateSignContext()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, cert := range result.certs {
		if err = signCtx.AddCertificate(cert.Fingerprint, cert.Certificate); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	for _, chain := range result.chains {
		if err = signCtx.AddCertificateChain(chain.Name, chain.Fingerprints); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	file, err := os.Open(result.decryptedFileName)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer file.Close()

	log.WithField("file", file.Name()).Debug("Check signature")

	if err = signCtx.VerifySign(result.ctx, file,
		result.packageInfo.Signs.ChainName, result.packageInfo.Signs.Alg, result.packageInfo.Signs.Value); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (downloader *Downloader) getDownloadStatus(resp *grab.Response) (status alerts.DownloadStatus) {
	return alerts.DownloadStatus{Source: downloader.moduleID, URL: resp.Request.HTTPRequest.URL.String(),
		Progress: int(resp.Progress() * 100), DownloadedBytes: uint64(resp.BytesComplete()),
		TotalBytes: uint64(resp.Size)}
}

func (downloader *Downloader) lockDownload(result *downloadResult) {
	downloader.lockFile(result.downloadFileName)
	downloader.lockFile(result.interruptFileName)
}

func (downloader *Downloader) unlockDownload(result *downloadResult) {
	downloader.unlockFile(result.downloadFileName)
	downloader.unlockFile(result.interruptFileName)
}

func (downloader *Downloader) lockDecrypt(result *downloadResult) {
	downloader.lockFile(result.decryptedFileName)
}

func (downloader *Downloader) unlockDecrypt(result *downloadResult) {
	downloader.unlockFile(result.decryptedFileName)
}

func (downloader *Downloader) releaseResult(result *downloadResult) {
	downloader.Lock()
	defer downloader.Unlock()

	downloader.unlockDecrypt(result)
}

func fileExists(filename string) (exist bool) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func getFileSize(fileName string) (size uint64, fsSize uint64, err error) {
	var stat syscall.Stat_t

	if err = syscall.Stat(fileName, &stat); err != nil {
		if os.IsNotExist(err) {
			return 0, 0, err
		}

		return 0, 0, aoserrors.Wrap(err)
	}

	return uint64(stat.Size), uint64(stat.Blocks) * 512, nil
}

func getMountPoint(dir string) (mountPoint string, err error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", aoserrors.Wrap(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		relPath, err := filepath.Rel(fields[1], dir)
		if err != nil || strings.Contains(relPath, "..") {
			continue
		}

		if len(fields[1]) > len(mountPoint) {
			mountPoint = fields[1]
		}
	}

	if mountPoint == "" {
		return "", aoserrors.Errorf("failed to find mount point for %s", dir)
	}

	return mountPoint, nil
}
