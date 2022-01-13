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

package fileserver

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"

	"github.com/aoscloud/aos_common/aoserrors"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// FileServer file server instance
type FileServer struct {
	server *http.Server
}

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	fileScheme = "file" // nolint // deadcode and varcheck
	httpScheme = "http"
)

/***********************************************************************************************************************
 * public
 **********************************************************************************************************************/

// New creates file server
func New(cfg *config.Config) (fileServer *FileServer, err error) {
	if err = os.MkdirAll(cfg.Downloader.DecryptDir, 0o755); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	fileServer = &FileServer{}

	if cfg.FileServerURL != "" {
		fileServer.server = &http.Server{
			Addr:    cfg.FileServerURL,
			Handler: http.FileServer(http.Dir(cfg.Downloader.DecryptDir)),
		}

		go fileServer.startFileStorage()
	}

	return fileServer, nil
}

// Close closes file server
func (fileServer *FileServer) Close() (err error) {
	if fileServer.server != nil {
		if shutdownErr := fileServer.server.Shutdown(context.Background()); shutdownErr != nil {
			if err == nil {
				err = aoserrors.Wrap(shutdownErr)
			}
		}

		if closeErr := fileServer.server.Close(); closeErr != nil {
			if err == nil {
				err = aoserrors.Wrap(closeErr)
			}
		}
	}

	return aoserrors.Wrap(err)
}

// TranslateURL convert image path url (file:// or http://)
func (fileServer *FileServer) TranslateURL(isLocal bool, inURL string) (outURL string, err error) {
	if !isLocal {
		if fileServer.server == nil {
			return "", aoserrors.New("file server not available")
		}

		imgURL, err := url.Parse(inURL)
		if err != nil {
			return "", aoserrors.Wrap(err)
		}

		imgURL.Scheme = httpScheme
		imgURL.Host = fileServer.server.Addr
		imgURL.Path = filepath.Base(imgURL.Path)

		outURL = imgURL.String()
	} else {
		outURL = inURL
	}

	return outURL, nil
}

/***********************************************************************************************************************
 * public
 **********************************************************************************************************************/

func (fileServer *FileServer) startFileStorage() {
	if fileServer.server == nil {
		log.Debug("Do not start local file server")
		return
	}

	log.WithField("host", fileServer.server.Addr).Debug("Start file server")

	if err := fileServer.server.ListenAndServe(); err != http.ErrServerClosed {
		log.Errorf("Can't start local file server: %s", err)
	}
}
