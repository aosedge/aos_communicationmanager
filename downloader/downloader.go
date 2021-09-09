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
	"context"
	"errors"
	"io/ioutil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/cavaliercoder/grab"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/image"

	"aos_communicationmanager/alerts"
	"aos_communicationmanager/cloudprotocol"
	"aos_communicationmanager/config"
	"aos_communicationmanager/fcrypt"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const updateDownloadsTime = 10 * time.Second
const statusAlertTickCount = 3

const startedFlagSuffix = ".started"
const interruptedFlagSuffix = ".interrupted"
const flagAccessRights = 0644
const interruptionReason = "Internet connection error"

const moduleID = "downloader"

const downloadMaxTry = 3

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// CryptoContext interface to access crypto functions
type CryptoContext interface {
	ImportSessionKey(keyInfo fcrypt.CryptoSessionKeyInfo) (symetricContext fcrypt.SymmetricContextInterface, err error)
	CreateSignContext() (signContext fcrypt.SignContextInterface, err error)
}

// Downloader instance
type Downloader struct {
	crypt           CryptoContext
	downloadDir     string
	sender          AlertSender
	downloadFileTTL time.Duration
}

// AlertSender provdes alert sender interface
type AlertSender interface {
	SendDownloadStartedAlert(downloadStatus alerts.DownloadStatus)
	SendDownloadFinishedAlert(downloadStatus alerts.DownloadStatus, code int)
	SendDownloadInterruptedAlert(downloadStatus alerts.DownloadStatus, reason string)
	SendDownloadResumedAlert(downloadStatus alerts.DownloadStatus, reason string)
	SendDownloadStatusAlert(downloadStatus alerts.DownloadStatus)
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

// ErrNotDownloaded returns when file can't be downloaded
var ErrNotDownloaded = errors.New("can't download file from any source")

/***********************************************************************************************************************
* Public
***********************************************************************************************************************/

// New creates new downloader object
func New(config *config.Config, fcrypt CryptoContext, sender AlertSender) (downloader *Downloader, err error) {
	log.Debug("New downloader")

	if err = os.MkdirAll(config.DownloadDir, 755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	downloader = &Downloader{
		crypt:           fcrypt,
		downloadDir:     config.DownloadDir,
		sender:          sender,
		downloadFileTTL: time.Hour * 24 * time.Duration(config.DownloadFileTTLDays),
	}

	return downloader, nil
}

// DownloadAndDecrypt download decrypt and validate blob
func (downloader *Downloader) DownloadAndDecrypt(packageInfo cloudprotocol.DecryptDataStruct,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate,
	decryptDir string) (resultFile string, err error) {
	if err = checkDescryptDir(decryptDir); err != nil {
		return "", aoserrors.Wrap(err)
	}

	if err = downloader.isEnoughSpaceForPackage(packageInfo, downloader.downloadDir, decryptDir); err != nil {
		return "", aoserrors.Wrap(err)
	}

	fileName, err := downloader.downloadWithMaxTry(packageInfo)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}
	defer os.Remove(fileName)

	defer func() {
		if err != nil && resultFile != "" {
			os.RemoveAll(resultFile)
		}
	}()

	resultFile, err = downloader.decryptPackage(fileName, decryptDir, packageInfo.DecryptionInfo)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	err = downloader.validateSigns(resultFile, packageInfo.Signs, chains, certs)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return resultFile, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func checkDescryptDir(decryptDir string) (err error) {
	if decryptDir == "" {
		return aoserrors.New("decrypt directory is not defined")
	}

	statInfo, err := os.Stat(decryptDir)
	if err != nil && !os.IsNotExist(err) {
		return aoserrors.Wrap(err)
	}

	if os.IsNotExist(err) {
		if err = os.MkdirAll(decryptDir, 755); err != nil {
			return aoserrors.Wrap(err)
		}
	} else {
		if !statInfo.Mode().IsDir() {
			return aoserrors.New("decrypt dir path is the file path")
		}
	}

	return nil
}

func (downloader *Downloader) removeOutdatedFiles() (err error) {
	files, err := ioutil.ReadDir(downloader.downloadDir)
	if err != nil {
		return err
	}

	now := time.Now()

	for _, file := range files {
		if now.Sub(file.ModTime()) > downloader.downloadFileTTL {
			fileName := path.Join(downloader.downloadDir, file.Name())

			log.Debugf("Remove outdated file: %s", fileName)

			if err = os.RemoveAll(fileName); err != nil {
				log.Errorf("Can't remove outdated file: %s", fileName)
			}
		}
	}

	return nil
}

func (downloader *Downloader) downloadWithMaxTry(
	packageInfo cloudprotocol.DecryptDataStruct) (fileName string, err error) {
	for try := 0; try < downloadMaxTry; try++ {
		if try != 0 {
			log.Warnf("Can't download file: %s. Retry...", err)
		}

		if fileName, err = downloader.processURLs(packageInfo.URLs); err != nil {
			continue
		}

		if err = image.CheckFileInfo(context.Background(), fileName, image.FileInfo{
			Sha256: packageInfo.Sha256,
			Sha512: packageInfo.Sha512,
			Size:   packageInfo.Size}); err != nil {
			os.RemoveAll(fileName)
			continue
		}

		return fileName, nil
	}

	return "", err
}

func (downloader *Downloader) processURLs(urls []string) (resultFile string, err error) {
	if len(urls) == 0 {
		return "", aoserrors.New("file list URLs is empty")
	}

	fileDownloaded := false

	for _, rawURL := range urls {
		if resultFile, err = downloader.download(rawURL); err != nil {
			log.WithField("url", rawURL).Warningf("Can't download file: %s", err)
			continue
		}

		fileDownloaded = true

		break
	}

	if !fileDownloaded {
		return resultFile, aoserrors.Wrap(ErrNotDownloaded)
	}

	return resultFile, nil
}

func (downloader *Downloader) download(url string) (fileName string, err error) {
	log.WithField("url", url).Debug("Start downloading file")

	grabClient := grab.NewClient()

	timer := time.NewTicker(updateDownloadsTime)
	defer timer.Stop()

	req, err := grab.NewRequest(downloader.downloadDir, url)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	req.BeforeCopy = downloader.beforeCopyHook
	req.AfterCopy = downloader.afterCopyHook

	resp := grabClient.Do(req)

	defer func() {
		if finalErr := downloader.finalizeDownload(resp); finalErr != nil {
			log.Warningf("Error finalizing download: %s", finalErr)
		}
	}()

	counter := 0
	for {
		select {
		case <-timer.C:
			log.WithFields(log.Fields{"complete": resp.BytesComplete(), "total": resp.Size}).Debug("Download progress")

			counter++

			if downloader.sender != nil && counter >= statusAlertTickCount {
				counter = 0
				downloadStatus, err := getAlertStatusFromResponse(resp)
				if err != nil {
					return "", aoserrors.Wrap(err)
				}

				downloader.sender.SendDownloadStatusAlert(downloadStatus)
			}

		case <-resp.Done:
			if err := resp.Err(); err != nil {
				return "", aoserrors.Wrap(err)
			}

			log.WithFields(log.Fields{"url": url, "file": resp.Filename}).Debug("Download complete")

			return resp.Filename, nil
		}
	}
}

func (downloader *Downloader) beforeCopyHook(resp *grab.Response) (err error) {
	copyStartedFile := resp.Filename + startedFlagSuffix
	copyInterruptedFile := resp.Filename + interruptedFlagSuffix

	if !fileExists(copyStartedFile) {
		if err = createFile(copyStartedFile); err != nil {
			return aoserrors.Wrap(err)
		}

		if downloader.sender != nil {
			downloadStatus, err := getAlertStatusFromResponse(resp)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			downloader.sender.SendDownloadStartedAlert(downloadStatus)
		}
	} else {
		// If download was started and no interrupted file was created,
		// then no interrupted alert was sent. Sending interrupted alert
		if !fileExists(copyInterruptedFile) {
			if err = createFile(copyInterruptedFile); err != nil {
				return aoserrors.Wrap(err)
			}

			log.Debug("Send download interrupted alert")

			if downloader.sender != nil {
				downloadStatus, err := getAlertStatusFromResponse(resp)
				if err != nil {
					return aoserrors.Wrap(err)
				}

				// TODO: read reason from interruption flag file
				downloader.sender.SendDownloadInterruptedAlert(downloadStatus, interruptionReason)
			}
		}
	}

	if resp.DidResume {
		log.Debug("Send download resumed alert")

		if downloader.sender != nil {
			downloadStatus, err := getAlertStatusFromResponse(resp)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			// TODO: read reason from interruption flag file
			downloader.sender.SendDownloadResumedAlert(downloadStatus, interruptionReason)
		}
	}

	return nil
}

func (downloader *Downloader) afterCopyHook(resp *grab.Response) (err error) {
	copyStartedFile := resp.Filename + startedFlagSuffix
	copyInterruptedFile := resp.Filename + interruptedFlagSuffix

	if fileExists(copyStartedFile) {
		log.WithField("file", copyStartedFile).Debug("Flag removed. Download finished successfully")

		if err = os.Remove(copyStartedFile); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if fileExists(copyInterruptedFile) {
		if err = os.Remove(copyInterruptedFile); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	log.Debug("Send download finished alert")

	if downloader.sender != nil {
		downloadStatus, err := getAlertStatusFromResponse(resp)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		downloader.sender.SendDownloadFinishedAlert(downloadStatus, resp.HTTPResponse.StatusCode)
	}

	return nil
}

func (downloader *Downloader) finalizeDownload(resp *grab.Response) (err error) {
	copyStartedFile := resp.Filename + startedFlagSuffix
	copyInterruptedFile := resp.Filename + interruptedFlagSuffix

	if fileExists(copyStartedFile) {
		log.Debug("Send download interrupted alert")

		if downloader.sender != nil {
			downloadStatus, err := getAlertStatusFromResponse(resp)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			// TODO: read reason from interruption flag file
			downloader.sender.SendDownloadInterruptedAlert(downloadStatus, interruptionReason)
		}

		if err = createFile(copyInterruptedFile); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func (downloader *Downloader) decryptPackage(srcFileName string, decryptDir string,
	decryptionInfo *cloudprotocol.DecryptionInfo) (resultFile string, err error) {
	if decryptionInfo == nil {
		return "", aoserrors.New("no decrypt image info")
	}

	if decryptionInfo.ReceiverInfo == nil {
		return "", aoserrors.New("no receiver info")
	}

	context, err := downloader.crypt.ImportSessionKey(fcrypt.CryptoSessionKeyInfo{
		SymmetricAlgName:  decryptionInfo.BlockAlg,
		SessionKey:        decryptionInfo.BlockKey,
		SessionIV:         decryptionInfo.BlockIv,
		AsymmetricAlgName: decryptionInfo.AsymAlg,
		ReceiverInfo: fcrypt.ReceiverInfo{
			Issuer: decryptionInfo.ReceiverInfo.Issuer,
			Serial: decryptionInfo.ReceiverInfo.Serial},
	})
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	srcFile, err := os.Open(srcFileName)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}
	defer srcFile.Close()

	dstFileName := path.Join(decryptDir, filepath.Base(srcFileName))

	dstFile, err := os.OpenFile(dstFileName, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}
	defer func() {
		dstFile.Close()

		if err != nil {
			os.RemoveAll(dstFileName)
		}
	}()

	log.WithFields(log.Fields{"srcFile": srcFile.Name(), "dstFile": dstFile.Name()}).Debug("Decrypt image")

	if err = context.DecryptFile(srcFile, dstFile); err != nil {
		return "", aoserrors.Wrap(err)
	}

	return dstFileName, aoserrors.Wrap(err)
}

func (downloader *Downloader) validateSigns(filePath string, signs *cloudprotocol.Signs,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (err error) {
	context, err := downloader.crypt.CreateSignContext()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, cert := range certs {
		if err = context.AddCertificate(cert.Fingerprint, cert.Certificate); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	for _, chain := range chains {
		if err = context.AddCertificateChain(chain.Name, chain.Fingerprints); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	if signs == nil {
		return aoserrors.New("package does not have signature")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer file.Close()

	log.WithField("file", file.Name()).Debug("Check signature")

	if err = context.VerifySign(file, signs.ChainName, signs.Alg, signs.Value); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func getAlertStatusFromResponse(resp *grab.Response) (status alerts.DownloadStatus, err error) {
	if resp == nil {
		return alerts.DownloadStatus{}, aoserrors.New("invalid response")
	}

	return alerts.DownloadStatus{Source: moduleID, URL: resp.Request.HTTPRequest.URL.String(),
		Progress: int(resp.Progress() * 100), DownloadedBytes: uint64(resp.BytesComplete()),
		TotalBytes: uint64(resp.Size)}, nil
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}

	return !info.IsDir()
}

func createFile(filename string) (err error) {
	return aoserrors.Wrap(ioutil.WriteFile(filename, []byte{}, flagAccessRights))
}

func getMountPoint(dir string) (mountPoint string, err error) {
	file, err := os.Open("/proc/mounts")
	if err != nil {
		return "", aoserrors.Wrap(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			// the last split() item is empty string following the last \n
			continue
		}

		if strings.HasPrefix(dir, fields[1]) {
			return fields[1], nil
		}
	}

	return "", aoserrors.Errorf("failed to find mount point for %s", dir)
}

func (downloader *Downloader) isEnoughSpaceForPackage(
	packageInfo cloudprotocol.DecryptDataStruct, downloadDir, decryptedDir string) (err error) {
	var stat syscall.Statfs_t
	var notCompletedFileSizes uint64

	for _, urlRaw := range packageInfo.URLs {
		url, err := url.Parse(urlRaw)
		if err != nil {
			return aoserrors.Wrap(err)
		}

		filePath := path.Join(downloadDir, filepath.Base(url.Path))
		if fileExists(filePath) {
			fi, err := os.Stat(filePath)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			notCompletedFileSizes += uint64(fi.Size())

			break
		}
	}

	downloadMountPoint, err := getMountPoint(downloadDir)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	decryptMountPoint, err := getMountPoint(decryptedDir)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if downloadMountPoint == decryptMountPoint {
		syscall.Statfs(downloadDir, &stat)

		// Free space is needed for double size of the package - for encrypted package and for decrypted
		if 2*packageInfo.Size > (stat.Bavail*uint64(stat.Bsize) + notCompletedFileSizes) {
			return aoserrors.New("not enough space for downloading and decrypting")
		}
	} else {
		syscall.Statfs(downloadDir, &stat)

		if packageInfo.Size > (stat.Bavail*uint64(stat.Bsize) + notCompletedFileSizes) {
			return aoserrors.New("not enough space for downloading")
		}

		syscall.Statfs(decryptedDir, &stat)

		if packageInfo.Size > stat.Bavail*uint64(stat.Bsize) {
			return aoserrors.New("not enough space for decrypting")
		}
	}

	return nil
}
