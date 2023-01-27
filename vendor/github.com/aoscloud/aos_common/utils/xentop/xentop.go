// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2023 Renesas Electronics Corporation.
// Copyright (C) 2023 EPAM Systems, Inc.
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

package xentop

import (
	"os/exec"
	"reflect"
	"strconv"
	"strings"

	"github.com/aoscloud/aos_common/aoserrors"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// SystemInfo domain info.
type SystemInfo struct {
	Name               string  `field:"NAME"`
	State              string  `field:"STATE"`
	CPUTime            int64   `field:"CPU(sec)"`
	CPUFraction        float32 `field:"CPU(%)"`
	Memory             int64   `field:"MEM(k)"`
	MaxMemory          int64   `field:"MAXMEM(k)"`
	MemoryFraction     float32 `field:"MEM(%)"`
	MaxMemoryFraction  float32 `field:"MAXMEM(%)"`
	VirtualCpus        int64   `field:"VCPUS"`
	NetworkInterfaces  int64   `field:"NETS"`
	NetworkTx          int64   `field:"NETTX(k)"`
	NetworkRx          int64   `field:"NETRX(k)"`
	VirtualDisks       int64   `field:"VBDS"`
	DiskBlockedIO      int64   `field:"VBD_OO"`
	DiskReadOps        int64   `field:"VBD_RD"`
	DiskWriteOps       int64   `field:"VBD_WR"`
	DiskSectorsRead    int64   `field:"VBD_RSECT"`
	DiskSectorsWritten int64   `field:"VBD_WSECT"`
	SSID               int64   `field:"SSID"`
}

type ShellCommand interface {
	CombinedOutput() ([]byte, error)
}

type execShellCommand struct {
	*exec.Cmd
}

/***********************************************************************************************************************
 * Variable
 **********************************************************************************************************************/

// ExecContext global variable are used to be able to mocking the functionality of xentop in tests.
var ExecContext = newExecShellCommander //nolint:gochecknoglobals

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// GetSystemInfo gets system information from xentop.
func GetSystemInfos() (map[string]SystemInfo, error) {
	output, err := ExecContext("xentop", "-b", "-f", "-i").CombinedOutput()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	xentopLines := strings.Split(string(output), "\n")
	if len(xentopLines) < 1 {
		return nil, aoserrors.New("xentop output empty")
	}

	header := strings.Fields(xentopLines[0])
	if len(header) < 1 || header[0] != "NAME" {
		return nil, aoserrors.New("unexpected xentop header")
	}

	systemInfos := make(map[string]SystemInfo)

	for _, line := range xentopLines[1:] {
		// Below strings.Fields function splits a string into an array of strings using unicode.IsSpace for this.
		// Therefore replace the space in no limit with a hyphen since it is a single value
		line = strings.ReplaceAll(line, "no limit", "no-limit")

		info := strings.Fields(line)
		if len(info) != len(header) {
			return nil, aoserrors.New("unexpected xentop data")
		}

		systemInfo, err := fillDomainInfo(header, info)
		if err != nil {
			return nil, err
		}

		systemInfos[systemInfo.Name] = systemInfo
	}

	return systemInfos, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func prepareKeyValueXentopInfo(header []string, info []string) map[string]string {
	xentopInfo := make(map[string]string)

	for i, key := range header {
		if info[i] == "n/a" || info[i] == "no-limit" {
			continue
		}

		xentopInfo[key] = info[i]
	}

	return xentopInfo
}

func fillDomainInfo(header []string, info []string) (SystemInfo, error) {
	xentopInfo := prepareKeyValueXentopInfo(header, info)
	systemInfo := SystemInfo{}

	systemInfoValue := reflect.Indirect(reflect.ValueOf(&systemInfo))

	for i := 0; i < systemInfoValue.NumField(); i++ {
		fieldType := systemInfoValue.Type().Field(i)

		fieldName, ok := fieldType.Tag.Lookup("field")
		if !ok {
			continue
		}

		val, ok := xentopInfo[fieldName]
		if !ok {
			continue
		}

		field := systemInfoValue.FieldByIndex(fieldType.Index)

		switch fieldType.Type.Kind() {
		case reflect.String:
			field.SetString(val)

		case reflect.Float32:
			pVal, err := strconv.ParseFloat(val, 32)
			if err != nil {
				return systemInfo, aoserrors.Wrap(err)
			}

			field.SetFloat(pVal)

		case reflect.Int64:
			pVal, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return systemInfo, aoserrors.Wrap(err)
			}

			field.SetInt(pVal)

		default:
			return systemInfo, aoserrors.New("unexpected field type in SystemInfo struct")
		}
	}

	return systemInfo, nil
}

func newExecShellCommander(name string, arg ...string) ShellCommand {
	return execShellCommand{Cmd: exec.Command(name, arg...)}
}
