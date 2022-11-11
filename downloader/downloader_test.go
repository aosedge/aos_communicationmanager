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
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/image"
	"github.com/aoscloud/aos_common/spaceallocator"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/downloader"
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

type testAlertSender struct {
	alertStarted     int
	alertFinished    int
	alertInterrupted int
	alertResumed     int
	alertStatus      int
}

type testAllocator struct {
	sync.Mutex

	totalSize     uint64
	allocatedSize uint64
	remover       spaceallocator.ItemRemover
	outdatedItems []testOutdatedItem
}

type testSpace struct {
	allocator *testAllocator
	size      uint64
}

type testOutdatedItem struct {
	id   string
	size uint64
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

	downloadAllocator = &testAllocator{}
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
		log.Errorf("Error cleaning up: %s", err)
	}

	os.Exit(ret)
}

func TestDownload(t *testing.T) {
	sender := testAlertSender{}
	downloadAllocator = &testAllocator{}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear dirs: %v", err)
	}

	fileName := path.Join(serverDir, "package.txt")

	if err := ioutil.WriteFile(fileName, []byte("Hello downloader\n"), 0o600); err != nil {
		t.Fatalf("Can't create package file: %s", err)
	}
	defer os.RemoveAll(fileName)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}
	defer downloadInstance.Close()

	result, err := downloadInstance.Download(
		context.Background(), preparePackageInfo("http://localhost:8001/", fileName))
	if err != nil {
		t.Fatalf("Can't download package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}

	fileInfo, err := os.Stat(result.GetFileName())
	if err != nil {
		t.Fatalf("Can't get file stat: %v", err)
	}

	if downloadAllocator.allocatedSize != uint64(fileInfo.Size()) {
		t.Errorf("Wrong allocated size: %d", downloadAllocator.allocatedSize)
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
	downloadAllocator = &testAllocator{}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear dirs: %v", err)
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
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}
	defer downloadInstance.Close()

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	result, err := downloadInstance.Download(context.Background(), packageInfo)
	if err != nil {
		t.Fatalf("Can't download package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}

	fileInfo, err := os.Stat(result.GetFileName())
	if err != nil {
		t.Fatalf("Can't get file stat: %v", err)
	}

	if downloadAllocator.allocatedSize != uint64(fileInfo.Size()) {
		t.Errorf("Wrong allocated size: %d", downloadAllocator.allocatedSize)
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

func TestContinueDownload(t *testing.T) {
	sender := testAlertSender{}
	downloadAllocator = &testAllocator{}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear dirs: %s", err)
	}

	if err := setWondershaperLimit("lo", "512"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	fileName := path.Join(serverDir, "package.txt")

	if err := generateFile(fileName, 1*Megabyte); err != nil {
		t.Errorf("Can't generate file: %s", err)
	}
	defer os.RemoveAll(fileName)

	downloadInstance, err := downloader.New("testModule", &config.Config{
		Downloader: config.Downloader{
			DownloadDir:            downloadDir,
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	ctx, cancel := context.WithCancel(context.Background())

	cancelDownloadIn(cancel, 10*time.Second)

	result, err := downloadInstance.Download(ctx, packageInfo)
	if err != nil {
		t.Fatalf("Can't download package: %s", err)
	}

	if err = result.Wait(); err == nil {
		t.Error("Error expected")
	}

	if result, err = downloadInstance.Download(context.Background(), packageInfo); err != nil {
		t.Fatalf("Can't download package: %s", err)
	}

	if err = result.Wait(); err != nil {
		t.Errorf("Download error: %s", err)
	}

	fileInfo, err := os.Stat(result.GetFileName())
	if err != nil {
		t.Fatalf("Can't get file stat: %v", err)
	}

	if downloadAllocator.allocatedSize != uint64(fileInfo.Size()) {
		t.Errorf("Wrong allocated size: %d", downloadAllocator.allocatedSize)
	}
}

func TestResumeDownloadFromTwoServers(t *testing.T) {
	sender := testAlertSender{}
	downloadAllocator = &testAllocator{}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear disks: %v", err)
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
			MaxConcurrentDownloads: 1,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}
	defer downloadInstance.Close()

	packageInfo := preparePackageInfo("http://localhost:8001/", fileName)

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel first download and try resume from another server
	cancelDownloadIn(cancel, 10*time.Second)

	result, err := downloadInstance.Download(ctx, packageInfo)
	if err != nil {
		t.Fatalf("Can't download package: %s", err)
	}

	if err = result.Wait(); err == nil {
		t.Error("Error expected")
	}

	packageInfo = preparePackageInfo("http://localhost:8002/", fileName)

	if result, err = downloadInstance.Download(context.Background(), packageInfo); err != nil {
		t.Fatalf("Can't download package: %s", err)
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
	downloadAllocator = &testAllocator{}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear dirs: %v", err)
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
			MaxConcurrentDownloads: 5,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}
	defer downloadInstance.Close()

	wg := sync.WaitGroup{}

	for i := 0; i < numDownloads; i++ {
		packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, i))

		result, err := downloadInstance.Download(context.Background(), packageInfo)
		if err != nil {
			t.Errorf("Can't download package: %s", err)
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
	downloadAllocator = &testAllocator{
		totalSize: 2 * Megabyte,
	}

	if err := clearDirs(); err != nil {
		t.Fatalf("Can't clear dirs: %v", err)
	}

	if err := setWondershaperLimit("lo", "4096"); err != nil {
		t.Fatalf("Can't set speed limit: %s", err)
	}

	defer clearWondershaperLimit("lo") // nolint:errcheck

	// Create files half of available size
	fileSize := downloadAllocator.totalSize / 2

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
			MaxConcurrentDownloads: 3,
			DownloadPartLimit:      100,
		},
	}, &sender)
	if err != nil {
		t.Fatalf("Can't create downloader: %s", err)
	}
	defer downloadInstance.Close()

	wg := sync.WaitGroup{}

	for i := 0; i < numDownloads; i++ {
		packageInfo := preparePackageInfo("http://localhost:8001/", fmt.Sprintf(fileNamePattern, i))

		result, err := downloadInstance.Download(context.Background(), packageInfo)
		if err != nil {
			t.Errorf("Can't download package: %s", err)
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

/***********************************************************************************************************************
 * Interfaces
 **********************************************************************************************************************/

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
 * Interfaces
 **********************************************************************************************************************/

func newSpaceAllocator(
	path string, partLimit uint, remover spaceallocator.ItemRemover,
) (spaceallocator.Allocator, error) {
	switch path {
	case downloadDir:
		downloadAllocator.remover = remover
		return downloadAllocator, nil

	default:
		return &testAllocator{remover: remover}, nil
	}
}

func (allocator *testAllocator) AllocateSpace(size uint64) (spaceallocator.Space, error) {
	allocator.Lock()
	defer allocator.Unlock()

	if allocator.totalSize != 0 && allocator.allocatedSize+size > allocator.totalSize {
		for allocator.allocatedSize+size > allocator.totalSize {
			if len(allocator.outdatedItems) == 0 {
				return nil, spaceallocator.ErrNoSpace
			}

			if err := allocator.remover(allocator.outdatedItems[0].id); err != nil {
				return nil, err
			}

			if allocator.outdatedItems[0].size < allocator.allocatedSize {
				allocator.allocatedSize -= allocator.outdatedItems[0].size
			} else {
				allocator.allocatedSize = 0
			}

			allocator.outdatedItems = allocator.outdatedItems[1:]
		}
	}

	allocator.allocatedSize += size

	return &testSpace{allocator: allocator, size: size}, nil
}

func (allocator *testAllocator) FreeSpace(size uint64) {
	allocator.Lock()
	defer allocator.Unlock()

	if size > allocator.allocatedSize {
		allocator.allocatedSize = 0
	} else {
		allocator.allocatedSize -= size
	}
}

func (allocator *testAllocator) AddOutdatedItem(id string, size uint64, timestamp time.Time) error {
	allocator.outdatedItems = append(allocator.outdatedItems, testOutdatedItem{id: id, size: size})

	return nil
}

func (allocator *testAllocator) RestoreOutdatedItem(id string) {
}

func (allocator *testAllocator) Close() error {
	return nil
}

func (space *testSpace) Accept() error {
	return nil
}

func (space *testSpace) PartiallyAccept(size uint64) error {
	if size > space.size {
		return aoserrors.New("wrong accepted size")
	}

	space.allocator.FreeSpace(space.size - size)

	return nil
}

func (space *testSpace) Release() error {
	space.allocator.FreeSpace(space.size)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func setup() (err error) {
	tmpDir, err = ioutil.TempDir("", "cm_")
	if err != nil {
		return aoserrors.Wrap(err)
	}

	downloadDir = filepath.Join(tmpDir, "download")
	serverDir = path.Join(tmpDir, "fileServer")

	if err = os.MkdirAll(serverDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	go func() {
		log.Fatal(http.ListenAndServe(":8001", http.FileServer(http.Dir(serverDir))))
	}()

	time.Sleep(time.Second)

	downloader.NewSpaceAllocator = newSpaceAllocator

	return nil
}

func cleanup() (err error) {
	_ = clearWondershaperLimit("lo")

	if err = os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

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

	packageInfo.Sha256 = imageFileInfo.Sha256
	packageInfo.Sha512 = imageFileInfo.Sha512
	packageInfo.Size = imageFileInfo.Size

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

func clearDirs() error {
	if err := os.RemoveAll(downloadDir); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := os.RemoveAll(serverDir); err != nil {
		return aoserrors.Wrap(err)
	}

	if err := os.MkdirAll(serverDir, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
