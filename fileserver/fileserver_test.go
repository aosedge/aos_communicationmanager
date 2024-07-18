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

package fileserver_test

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aosedge/aos_communicationmanager/fileserver"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const serverDir = "/tmp/fileserverTest"

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

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

func TestOnlyLocalFileServer(t *testing.T) {
	fileServer, err := fileserver.New("", serverDir)
	if err != nil {
		t.Fatalf("Can't create fileServer: %s", err)
	}
	defer fileServer.Close()

	if _, err = fileServer.TranslateURL(false, "/var/1.txt"); err == nil {
		t.Errorf("Should be error: file server not available")
	}

	outURL, err := fileServer.TranslateURL(true, "/var/1.txt")
	if err != nil {
		t.Errorf("Can't translate local url: %s", err)
	}

	if outURL != "/var/1.txt" {
		t.Errorf("Incorrect translated url: %s", outURL)
	}
}

func TestFileServer(t *testing.T) {
	if err := os.MkdirAll(serverDir, 0o755); err != nil {
		t.Fatalf("Can't create server dir: %v", err)
	}
	defer os.RemoveAll(serverDir)

	fileServer, err := fileserver.New("localhost:8092", serverDir)
	if err != nil {
		t.Fatalf("Can't create fileServer: %s", err)
	}
	defer fileServer.Close()

	outURL, err := fileServer.TranslateURL(true, "/var/1.txt")
	if err != nil {
		t.Errorf("Can't translate local url: %s", err)
	}

	if outURL != "/var/1.txt" {
		t.Errorf("Incorrect translated url: %s", outURL)
	}

	filename := "testFile.txt"

	if err := os.WriteFile(filepath.Join(serverDir, filename), []byte("Hello fileserver"), 0o600); err != nil {
		t.Fatalf("Can't create package file: %s", err)
	}

	outURL, err = fileServer.TranslateURL(false, "file://"+filepath.Join(serverDir, filename))
	if err != nil {
		t.Errorf("Can't translate remote url: %s", err)
	}

	if outURL != "http://localhost:8092/"+filename {
		t.Errorf("Incorrect remote translated url: %s", outURL)
	}

	time.Sleep(1 * time.Second)

	// Get the data
	resp, err := http.Get(outURL) //nolint:gosec
	if err != nil {
		t.Fatalf("Can't download file: %s", err)
	}
	defer resp.Body.Close()

	var buffer bytes.Buffer

	// Write the body to file
	_, err = io.Copy(&buffer, resp.Body)
	if err != nil {
		t.Fatalf("Can't get data from response: %s", err)
	}

	if buffer.String() != "Hello fileserver" {
		t.Errorf("incorrect file content: %s", buffer.String())
	}
}
