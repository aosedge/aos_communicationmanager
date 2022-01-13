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

package unitstatushandler

import (
	"context"
	"strings"
	"sync"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/image"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type downloadResult struct {
	FileName string         `json:"fileName"`
	FileInfo image.FileInfo `json:"fileInfo"`
	Error    string         `json:"error"`
}

type statusNotifier func(id string, status string, componentErr string)

/***********************************************************************************************************************
 * Interface
 **********************************************************************************************************************/

func (instance *Instance) download(ctx context.Context, request map[string]cloudprotocol.DecryptDataStruct,
	continueOnError bool, updateStatus statusNotifier,
	chains []cloudprotocol.CertificateChain, certs []cloudprotocol.Certificate) (result map[string]*downloadResult) {
	result = make(map[string]*downloadResult)

	for id := range request {
		result[id] = &downloadResult{}
		updateStatus(id, cloudprotocol.DownloadingStatus, "")
	}

	downloadCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()

	var wg sync.WaitGroup

	handleError := func(id string, err error) {
		errorStr := aoserrors.Wrap(err).Error()

		if !isCancelError(errorStr) {
			result[id].Error = errorStr
			updateStatus(id, cloudprotocol.ErrorStatus, errorStr)
		}

		if !continueOnError {
			cancelFunc()
		}
	}

	for id, item := range request {
		itemResult, err := instance.downloader.DownloadAndDecrypt(downloadCtx, item, chains, certs)
		if err != nil {
			handleError(id, err)

			if !continueOnError {
				break
			} else {
				continue
			}
		}

		result[id].FileName = itemResult.GetFileName()

		wg.Add(1)

		go func(id string) {
			defer wg.Done()

			if err := itemResult.Wait(); err != nil {
				handleError(id, err)
				return
			}

			fileInfo, err := image.CreateFileInfo(downloadCtx, itemResult.GetFileName())
			if err != nil {
				handleError(id, err)
				return
			}

			result[id].FileInfo = fileInfo

			updateStatus(id, cloudprotocol.DownloadedStatus, "")
		}(id)
	}

	wg.Wait()

	if downloadCtx.Err() != nil {
		// Download canceled: set cancel state for already downloaded or partially downloaded items
		log.Debug("Download canceled")

		for id, item := range result {
			if item.Error == "" {
				item.Error = aoserrors.Wrap(downloadCtx.Err()).Error()
				updateStatus(id, cloudprotocol.ErrorStatus, item.Error)
			}
		}
	}

	return result
}

func getDownloadError(result map[string]*downloadResult) (downloadErr string) {
	for _, item := range result {
		if item.Error != "" && !isCancelError(item.Error) {
			return item.Error
		}
	}

	return ""
}

func isCancelError(errString string) (result bool) {
	return strings.Contains(errString, context.Canceled.Error())
}
