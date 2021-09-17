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

package umcontroller

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"

	"aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type fileStorage struct {
	server        *http.Server
	fileServerAdr string
	updateDir     string
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const fileScheme = "file"
const httpScheme = "http"

/***********************************************************************************************************************
 * public
 **********************************************************************************************************************/

// NewServer create update controller server
func newFileStorage(cfg config.UMController) (storage *fileStorage, err error) {
	if err = os.MkdirAll(cfg.UpdateDir, 0755); err != nil {
		return nil, err
	}

	storage = &fileStorage{
		fileServerAdr: cfg.FileServerURL,
		updateDir:     cfg.UpdateDir,
	}

	if storage.fileServerAdr != "" {
		storage.server = &http.Server{Addr: cfg.FileServerURL, Handler: http.FileServer(http.Dir(cfg.UpdateDir))}
	}

	return storage, nil
}

func (storage *fileStorage) startFileStorage() {
	if storage.server == nil {
		log.Debug("Do not start local file server.")
		return
	}

	log.Debug("Start local file server on ", storage.fileServerAdr, " path ", storage.updateDir)
	if err := storage.server.ListenAndServe(); err != http.ErrServerClosed {
		log.Error("Can't start local file server: ", err)
	}
}

func (storage *fileStorage) stopFileStorage() {
	if storage.server != nil {
		if err := storage.server.Shutdown(context.Background()); err != nil {
			log.Error("HTTP server Shutdown: %", err)
		}
	}
}

func (storage *fileStorage) getImageURL(isLocalClient bool, imagePath string) (imageURL string) {
	if isLocalClient == false && storage.fileServerAdr != "" {
		imgURL := url.URL{
			Scheme: httpScheme,
			Host:   storage.fileServerAdr,
			Path:   filepath.Base(imagePath),
		}

		return imgURL.String()
	}

	imgURL := url.URL{
		Scheme: fileScheme,
		Path:   imagePath,
	}

	return imgURL.String()
}
