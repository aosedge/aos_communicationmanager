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

// Package launcher provides set of API to controls services lifecycle

package downloader_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/image"
	"github.com/aoscloud/aos_common/utils/testtools"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
	"github.com/aoscloud/aos_communicationmanager/fcrypt"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	Kilobyte = uint64(1 << 10)
	Megabyte = uint64(1 << 20)
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testCryptoContext struct{}

type testSymmetricContext struct{}

type testSignContext struct{}

type testAlertSender struct {
	alertStarted     int
	alertFinished    int
	alertInterrupted int
	alertResumed     int
	alertStatus      int
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	tmpDir      string
	serverDir   string
	downloadDir string
	decryptDir  string
)

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	var err error

	if err = setup(); err != nil {
		log.Fatalf("Error setting up: %s", err)
	}

	ret := m.Run()

	if err = cleanup(); err != nil {
		log.Fatalf("Error cleaning up: %s", err)
	}

	os.Exit(ret)
}

func TestDownload(t *testing.T) {
	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	fileName := path.Join(serverDir, "package.txt")

	if err := ioutil.WriteFile(fileName, []byte("Hello downloader\n"), 0o600); err != nil {
		t.Fatalf("Can't create package file: %s", err)
	}
	defer os.RemoveAll(fileName)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	result, err := downloadInstance.DownloadAndDecrypt(
		context.Background(), preparePackageInfo("http://localhost:8001/", fileName), nil, nil)
	if err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}

	if sender.alertStarted != 1 {
		t.Error("Download started alert was not received")
	}

	if sender.alertFinished != 1 {
		t.Error("Download finished alert was not received")
	}
}

func TestInterruptResumeDownload(t *testing.T) {
	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "128"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	fileName := path.Join(serverDir, "package.txt")

	if err := generateFile(fileName, 1*Megabyte); err != nil {
		t.Fatalf("Can't generate file: %s", err)
	}
	defer os.RemoveAll(fileName)

	// Kill connection after 32 secs to receive status alert
	killConnectionIn("localhost", 8001, 32*time.Second)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	result, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil)
	if err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}

	if sender.alertStarted != 1 {
		t.Error("Download started alert was not received")
	}

	if sender.alertStatus == 0 {
		t.Error("Download status was not received")
	}

	if sender.alertInterrupted == 0 {
		t.Error("Download interrupted alert was not received")
	}

	if sender.alertResumed == 0 {
		t.Error("Download resumed alert was not received")
	}

	if sender.alertFinished == 0 {
		t.Error("Download finished alert was not received")
	}
}

// The test checks if available disk size is counted properly after resuming
// download and takes into account files that already have been partially downloaded.
func TestAvailableSize(t *testing.T) {
	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "512"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	var stat syscall.Statfs_t

	if err := syscall.Statfs(downloadDir, &stat); err != nil {
		t.Errorf("Can't make syscall statfs: %s", err)
	}

	fileName := path.Join(serverDir, "package.txt")
	// File should be a little less than required due to extra blocks needed by FS
	fileSize := (stat.Bavail - 16) * uint64(stat.Bsize)

	if err := generateFile(fileName, fileSize); err != nil {
		t.Errorf("Can't generate file: %s", err)
	}
	defer os.RemoveAll(fileName)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	ctx, cancel := context.WithCancel(context.Background())

	cancelDownloadIn(cancel, 10*time.Second)

	result, err := downloadInstance.DownloadAndDecrypt(ctx, packageInfo, nil, nil)
	if err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err == nil {
		t.Error("Error expected")
	}

	if result, err = downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil); err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}
}

func TestResumeDownloadFromTwoServers(t *testing.T) {
	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "256"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	fileName := path.Join(serverDir, "package.txt")

	if err := generateFile(fileName, 1*Megabyte); err != nil {
		t.Fatalf("Can't generate file: %s", err)
	}
	defer os.RemoveAll(fileName)

	go func() {
		log.Fatal(http.ListenAndServe(":8002", http.FileServer(http.Dir(serverDir))))
	}()

	time.Sleep(time.Second)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel first download and try resume from another server
	cancelDownloadIn(cancel, 10*time.Second)

	result, err := downloadInstance.DownloadAndDecrypt(ctx, packageInfo, nil, nil)
	if err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err == nil {
		t.Error("Error expected")
	}

	packageInfo = preparePackageInfo("http://localhost:8002/", fileName)

	if result, err = downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil); err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}
}

func TestConcurrentDownloads(t *testing.T) {
	const (
		numDownloads    = 10
		fileNamePattern = "package%d.txt"
	)

	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "1024"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	for i := 0; i < numDownloads; i++ {
		if err := generateFile(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)), 100*Kilobyte); err != nil {
			t.Fatalf("Can't generate file: %s", err)
		}
	}

	defer func() {
		for i := 0; i < numDownloads; i++ {
			os.RemoveAll(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)))
		}
	}()

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 5,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < numDownloads; i++ {
		packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, i))

		result, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil)
		if err != nil {
			t.Errorf("Can't download and decrypt package: %s", err)
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			if err = result.Wait(); err != nil {
				t.Errorf("Download error: %s", err)
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentLimitSpaceDownloads(t *testing.T) {
	const numDownloads = 3

	const fileNamePattern = "package%d.txt"

	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "4096"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	var stat syscall.Statfs_t

	if err := syscall.Statfs(downloadDir, &stat); err != nil {
		t.Errorf("Can't make syscall statfs: %s", err)
	}

	// Create files half of available size
	fileSize := (stat.Bavail / 2) * uint64(stat.Bsize)

	for i := 0; i < numDownloads; i++ {
		if err := generateFile(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)), fileSize); err != nil {
			t.Fatalf("Can't generate file: %s", err)
		}
	}

	defer func() {
		for i := 0; i < numDownloads; i++ {
			os.RemoveAll(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)))
		}
	}()

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 3,
			DownloadPartLimit:      100,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	wg := sync.WaitGroup{}

	results := make([]downloader.Result, 0, numDownloads)

	for i := 0; i < numDownloads; i++ {
		packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, i))

		result, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil)
		if err != nil {
			t.Errorf("Can't download and decrypt package: %s", err)
			continue
		}

		results = append(results, result)

		wg.Add(1)

		go func() {
			defer wg.Done()

			if err = result.Wait(); err != nil {
				t.Errorf("Download error: %s", err)
			}
		}()
	}

	wg.Wait()

	// Try to download another file. It should fail as there is no space in DecryptDir
	if err := generateFile(path.Join(serverDir, fmt.Sprintf(fileNamePattern, numDownloads)), fileSize); err != nil {
		t.Fatalf("Can't generate file: %s", err)
	}

	packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, numDownloads))

	if _, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil); err == nil {
		t.Error("Error expected")
	}

	// Remove decrypted files
	for _, result := range results {
		if err = os.RemoveAll(result.GetFileName()); err != nil {
			t.Fatalf("Can't remove decrypted file: %s", err)
		}
	}

	// Now it should download successfully
	result, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil)
	if err != nil {
		t.Fatalf("Can't download and decrypt package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}
}

func TestDownloadPartLimit(t *testing.T) {
	const (
		numDownloads      = 10
		fileNamePattern   = "package%d.txt"
		downloadPartLimit = 50
	)

	sender := testAlertSender{}

	if err := clearDisks(); err != nil {
		t.Fatalf("Can't clear disks: %s", err)
	}

	if err := setWondershaperLimit("lo", "4096"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	var stat syscall.Statfs_t

	if err := syscall.Statfs(downloadDir, &stat); err != nil {
		t.Fatalf("Can't get FS status: %s", err)
	}

	expectedFreeBlocks := stat.Bavail - stat.Blocks*downloadPartLimit/100

	fileSize := (stat.Bavail / numDownloads) * uint64(stat.Bsize)

	for i := 0; i < numDownloads; i++ {
		if err := generateFile(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)), fileSize); err != nil {
			t.Fatalf("Can't generate file: %s", err)
		}
	}

	defer func() {
		for i := 0; i < numDownloads; i++ {
			os.RemoveAll(path.Join(serverDir, fmt.Sprintf(fileNamePattern, i)))
		}
	}()

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			DecryptDir:             decryptDir,
			MaxConcurrentDownloads: 3,
			DownloadPartLimit:      downloadPartLimit,
		},
	}, &testCryptoContext{}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	wg := sync.WaitGroup{}

	for i := 0; i < numDownloads; i++ {
		packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, i))

		result, err := downloadInstance.DownloadAndDecrypt(context.Background(), packageInfo, nil, nil)
		if err != nil {
			t.Errorf("Can't download and decrypt package: %s", err)
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			if err = result.Wait(); err != nil {
				t.Errorf("Download error: %s", err)
			}
		}()
	}

	wg.Wait()

	if err := syscall.Statfs(downloadDir, &stat); err != nil {
		t.Fatalf("Can't get FS status: %s", err)
	}

	if stat.Bavail < expectedFreeBlocks {
		t.Errorf("Num of available blocks should be more than expected: %d < %d", stat.Bavail, expectedFreeBlocks)
	}
}

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

func (context *testCryptoContext) ImportSessionKey(
	keyInfo fcrypt.CryptoSessionKeyInfo,
) (fcrypt.SymmetricContextInterface, error) {
	return &testSymmetricContext{}, nil
}

func (context *testCryptoContext) CreateSignContext() (fcrypt.SignContextInterface, error) {
	return &testSignContext{}, nil
}

func (context *testSymmetricContext) DecryptFile(ctx context.Context, encryptedFile, decryptedFile *os.File) error {
	if _, err := io.Copy(decryptedFile, encryptedFile); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func (context *testSignContext) AddCertificate(fingerprint string, asn1Bytes []byte) (err error) {
	return nil
}

func (context *testSignContext) AddCertificateChain(name string, fingerprints []string) (err error) {
	return nil
}

func (context *testSignContext) VerifySign(ctx context.Context, f *os.File, sign *cloudprotocol.Signs) (err error) {
	return nil
}

func (instance *testAlertSender) SendAlert(alert cloudprotocol.AlertItem) {
	downloadAlert, ok := alert.Payload.(cloudprotocol.DownloadAlert)
	if !ok {
		log.Error("Received not download alert")
	}

	switch {
	case strings.Contains(downloadAlert.Message, "Download started"):
		instance.alertStarted++

	case strings.Contains(downloadAlert.Message, "Download resumed reason:"):
		instance.alertResumed++

	case strings.Contains(downloadAlert.Message, "Download status"):
		instance.alertStatus++

	case strings.Contains(downloadAlert.Message, "Download interrupted reason:"):
		instance.alertInterrupted++

	case strings.Contains(downloadAlert.Message, "Download finished code:"):
		instance.alertFinished++
	}
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func setup() (err error) {
	tmpDir, err = ioutil.TempDir("", "cm_")
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if downloadDir, err = createTmpDisk("downloadDir", 2); err != nil {
		return aoserrors.Wrap(err)
	}

	if decryptDir, err = createTmpDisk("decryptDir", 8); err != nil {
		return aoserrors.Wrap(err)
	}

	serverDir = path.Join(tmpDir, "fileServer")

	if err = os.MkdirAll(serverDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	go func() {
		log.Fatal(http.ListenAndServe(":8001", http.FileServer(http.Dir(serverDir))))
	}()

	time.Sleep(time.Second)

	return nil
}

func cleanup() (err error) {
	_ = clearWondershaperLimit("lo")

	_ = closeTmpDisk("downloadDir")
	_ = closeTmpDisk("decryptDir")

	os.RemoveAll(tmpDir)

	return nil
}

func cancelDownloadIn(cancel context.CancelFunc, delay time.Duration) {
	go func() {
		time.Sleep(delay)

		log.Debug("Cancel download")

		cancel()
	}()
}

func killConnectionIn(host string, port int16, delay time.Duration) {
	go func() {
		time.Sleep(delay)

		log.Debug("Kill connection")

		if _, err := exec.Command(
			"ss", "-K", "src", host, "dport", "=", strconv.Itoa(int(port))).CombinedOutput(); err != nil {
			log.Errorf("Can't kill connection: %s", err)
		}
	}()
}

func preparePackageInfo(host, fileName string) (packageInfo cloudprotocol.DecryptDataStruct) {
	fileName = path.Base(fileName)

	packageInfo.URLs = []string{host + fileName}

	filePath := path.Join(serverDir, fileName)

	imageFileInfo, err := image.CreateFileInfo(context.Background(), filePath)
	if err != nil {
		log.Error("error CreateFileInfo", err)
		return packageInfo
	}

	recInfo := struct {
		Serial string `json:"serial"`
		Issuer []byte `json:"issuer"`
	}{
		Serial: "string",
		Issuer: []byte("issuer"),
	}

	packageInfo.Sha256 = imageFileInfo.Sha256
	packageInfo.Sha512 = imageFileInfo.Sha512
	packageInfo.Size = imageFileInfo.Size
	packageInfo.DecryptionInfo = &cloudprotocol.DecryptionInfo{
		BlockAlg:     "AES256/CBC/pkcs7",
		BlockIv:      []byte{},
		BlockKey:     []byte{},
		AsymAlg:      "RSA/PKCS1v1_5",
		ReceiverInfo: &recInfo,
	}
	packageInfo.Signs = new(cloudprotocol.Signs)

	return packageInfo
}

func generateFile(fileName string, size uint64) (err error) {
	if output, err := exec.Command("dd", "if=/dev/urandom", "of="+fileName, "bs=1",
		"count="+strconv.FormatUint(size, 10)).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

// Set traffic limit for interface.
func setWondershaperLimit(iface string, limit string) (err error) {
	if output, err := exec.Command("wondershaper", "-a", iface, "-d", limit).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

func clearWondershaperLimit(iface string) (err error) {
	if output, err := exec.Command("wondershaper", "-ca", iface).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

func createTmpDisk(name string, size uint64) (mountDir string, err error) {
	fileName := path.Join(tmpDir, name+".ext4")

	defer func() {
		if err != nil {
			os.RemoveAll(fileName)
		}
	}()

	mountDir = path.Join(tmpDir, name)

	if err = os.MkdirAll(mountDir, 0o755); err != nil {
		return "", aoserrors.Wrap(err)
	}

	defer func() {
		if err != nil {
			os.RemoveAll(mountDir)
		}
	}()

	if err = testtools.CreateFilePartition(fileName, "ext4", size, nil, false); err != nil {
		return "", aoserrors.Wrap(err)
	}

	if output, err := exec.Command("mount", fileName, mountDir).CombinedOutput(); err != nil {
		return "", aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return mountDir, nil
}

func clearDisks() (err error) {
	if err = clearTmpDisk("downloadDir"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = clearTmpDisk("decryptDir"); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func clearTmpDisk(name string) (err error) {
	mountDir := path.Join(tmpDir, name)

	items, err := ioutil.ReadDir(mountDir)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, item := range items {
		if err = os.RemoveAll(path.Join(mountDir, item.Name())); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

func closeTmpDisk(name string) (err error) {
	fileName := path.Join(tmpDir, name+".ext4")
	mountDir := path.Join(tmpDir, name)

	if output, err := exec.Command("umount", mountDir).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	if err = os.RemoveAll(mountDir); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = os.Remove(fileName); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
