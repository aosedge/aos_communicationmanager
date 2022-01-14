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
	"context"
	"io/ioutil"
	"os"

	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	maxReasonSize          = 512
	unknownInterruptReason = "unknown"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// Result download result interface.
type Result interface {
	GetFileName() (fileName string)
	Wait() (err error)
}

type downloadResult struct {
	id string

	ctx         context.Context
	packageInfo cloudprotocol.DecryptDataStruct
	chains      []cloudprotocol.CertificateChain
	certs       []cloudprotocol.Certificate

	statusChannel chan error

	decryptedFileName string
	downloadFileName  string
	interruptFileName string
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func (result *downloadResult) GetFileName() (fileName string) {
	return result.decryptedFileName
}

func (result *downloadResult) Wait() (err error) {
	err = <-result.statusChannel

	close(result.statusChannel)

	return aoserrors.Wrap(err)
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (result *downloadResult) storeInterruptReason(reason string) {
	if len(reason) > maxReasonSize {
		reason = reason[:maxReasonSize]
	}

	if err := ioutil.WriteFile(result.interruptFileName, []byte(reason), 0o600); err != nil {
		log.Errorf("Can't store interrupt reason: %s", err)
	}
}

func (result *downloadResult) retreiveInterruptReason() (reason string) {
	data, err := ioutil.ReadFile(result.interruptFileName)
	if err != nil {
		return unknownInterruptReason
	}

	return string(data)
}

func (result *downloadResult) removeInterruptReason() {
	if err := os.RemoveAll(result.interruptFileName); err != nil {
		log.Errorf("Can't remove interrupt reason file: %s", err)
	}
}
