// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2024 Renesas Electronics Corporation.
// Copyright (C) 2024 EPAM Systems, Inc.
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

package resourcemonitor

import (
	"bufio"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	cgroupsPath  = "/sys/fs/cgroup"
	cpuUsageFile = "cpu.stat"
	memUsageFile = "memory.current"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type cgroupsSystemUsage struct{}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func (usageInstance *cgroupsSystemUsage) CacheSystemInfos() {
}

func (usageInstance *cgroupsSystemUsage) FillSystemInfo(instanceID string, instance *instanceMonitoring) error {
	now := time.Now()

	cpu, err := usageInstance.getCPUUsage(instanceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	ram, err := usageInstance.getRAMUsage(instanceID)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	if instance.prevCPU > cpu {
		instance.prevCPU = 0
	}

	instance.monitoringData.CPU = uint64(math.Round(float64(cpu-instance.prevCPU) * 100.0 /
		(float64(now.Sub(instance.prevTime).Microseconds())) / float64(cpuCount)))
	instance.monitoringData.RAM = ram

	instance.prevCPU = cpu
	instance.prevTime = now

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (usageInstance *cgroupsSystemUsage) getCPUUsage(instanceID string) (uint64, error) {
	return getFieldFromFile(filepath.Join(cgroupsPath, instanceID, cpuUsageFile), "usage_usec")
}

func (usageInstance *cgroupsSystemUsage) getRAMUsage(instanceID string) (uint64, error) {
	return getLineFromFile(filepath.Join(cgroupsPath, instanceID, memUsageFile), 0)
}

func getFieldFromFile(fileName, field string) (uint64, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return 0, aoserrors.New("field not found")
			}

			return 0, aoserrors.Wrap(err)
		}

		fields := strings.Fields(str)

		if len(fields) > 0 && strings.TrimSpace(fields[0]) == field {
			if len(fields) == 1 {
				return 0, nil
			}

			result, err := strconv.ParseUint(strings.TrimSpace(fields[1]), 10, 64)
			if err != nil {
				return 0, aoserrors.Wrap(err)
			}

			return result, nil
		}
	}
}

func getLineFromFile(fileName string, line int) (uint64, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	var str string

	for i := 0; i < line+1; i++ {
		if str, err = reader.ReadString('\n'); err != nil {
			return 0, aoserrors.Wrap(err)
		}
	}

	result, err := strconv.ParseUint(strings.TrimSpace(str), 10, 64)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	return result, nil
}
