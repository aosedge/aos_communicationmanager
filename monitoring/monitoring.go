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

// Package monitoring AOS Core Monitoring Component
package monitoring

import (
	"container/list"
	"context"
	"math"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// ResourceAlertSender interface to send resource alerts
type ResourceAlertSender interface {
	SendResourceAlert(source, resource string, time time.Time, value uint64)
}

// TrafficMonitor provides traffic statistics
type TrafficMonitor interface {
	GetSystemTraffic() (inputTraffic, outputTraffic uint64, err error)
}

// Sender sends alerts to the cloud
type Sender interface {
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error)
}

// Monitor instance
type Monitor struct {
	sync.Mutex

	dataChannel chan cloudprotocol.MonitoringData

	sender         Sender
	resourceAlerts ResourceAlertSender
	trafficMonitor TrafficMonitor

	config     config.Monitoring
	workingDir string

	sendTimer *time.Ticker
	pollTimer *time.Ticker

	dataToSend cloudprotocol.MonitoringData

	alertProcessors *list.List

	cancelFunction context.CancelFunc
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new monitor instance
func New(config *config.Config, resourceAlerts ResourceAlertSender,
	trafficMonitor TrafficMonitor, sender Sender) (monitor *Monitor, err error) {
	log.Debug("Create monitor")

	monitor = &Monitor{sender: sender, resourceAlerts: resourceAlerts, trafficMonitor: trafficMonitor}

	monitor.dataChannel = make(chan cloudprotocol.MonitoringData, config.Monitoring.MaxOfflineMessages)

	monitor.config = config.Monitoring
	monitor.workingDir = config.WorkingDir

	monitor.alertProcessors = list.New()

	monitor.dataToSend.ServicesData = make([]cloudprotocol.ServiceMonitoringData, 0)

	if monitor.resourceAlerts != nil {
		if config.Monitoring.CPU != nil {
			monitor.alertProcessors.PushBack(createAlertProcessor(
				"System CPU",
				&monitor.dataToSend.Global.CPU,
				func(time time.Time, value uint64) {
					monitor.resourceAlerts.SendResourceAlert("system", "cpu", time, value)
				},
				*config.Monitoring.CPU))
		}

		if config.Monitoring.RAM != nil {
			monitor.alertProcessors.PushBack(createAlertProcessor(
				"System RAM",
				&monitor.dataToSend.Global.RAM,
				func(time time.Time, value uint64) {
					monitor.resourceAlerts.SendResourceAlert("system", "ram", time, value)
				},
				*config.Monitoring.RAM))
		}

		if config.Monitoring.UsedDisk != nil {
			monitor.alertProcessors.PushBack(createAlertProcessor(
				"System Disk",
				&monitor.dataToSend.Global.UsedDisk,
				func(time time.Time, value uint64) {
					monitor.resourceAlerts.SendResourceAlert("system", "disk", time, value)
				},
				*config.Monitoring.UsedDisk))
		}

		if config.Monitoring.InTraffic != nil {
			monitor.alertProcessors.PushBack(createAlertProcessor(
				"IN Traffic",
				&monitor.dataToSend.Global.InTraffic,
				func(time time.Time, value uint64) {
					monitor.resourceAlerts.SendResourceAlert("system", "inTraffic", time, value)
				},
				*config.Monitoring.InTraffic))
		}

		if config.Monitoring.OutTraffic != nil {
			monitor.alertProcessors.PushBack(createAlertProcessor(
				"OUT Traffic",
				&monitor.dataToSend.Global.OutTraffic,
				func(time time.Time, value uint64) {
					monitor.resourceAlerts.SendResourceAlert("system", "outTraffic", time, value)
				},
				*config.Monitoring.OutTraffic))
		}
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	monitor.cancelFunction = cancelFunc

	if monitor.config.EnableSystemMonitoring {
		monitor.pollTimer = time.NewTicker(monitor.config.PollPeriod.Duration)
		monitor.sendTimer = time.NewTicker(monitor.config.SendPeriod.Duration)

		go monitor.run(ctx)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case data := <-monitor.dataChannel:
				if err := monitor.sender.SendMonitoringData(data); err != nil {
					log.Errorf("Can't send monitoring data: %s", err)
				}
			}
		}
	}()

	return monitor, nil
}

// SendMonitoringData sends monitoring data
func (monitor *Monitor) SendMonitoringData(monitoringData cloudprotocol.MonitoringData) (err error) {
	monitor.Lock()
	defer monitor.Unlock()

	monitor.sendMonitoringData(monitoringData)

	return nil
}

// Close closes monitor instance
func (monitor *Monitor) Close() {
	log.Debug("Close monitor")

	if monitor.sendTimer != nil {
		monitor.sendTimer.Stop()
	}

	if monitor.pollTimer != nil {
		monitor.pollTimer.Stop()
	}

	monitor.cancelFunction()
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func (monitor *Monitor) run(ctx context.Context) {
	for {
		select {
		case data := <-monitor.dataChannel:
			if err := monitor.sender.SendMonitoringData(data); err != nil {
				log.Errorf("Can't send monitoring data: %s", err)
			}

		case <-ctx.Done():
			return

		case <-monitor.sendTimer.C:
			monitor.Lock()
			monitor.dataToSend.Timestamp = time.Now()
			monitor.sendMonitoringData(monitor.dataToSend)
			monitor.Unlock()

		case <-monitor.pollTimer.C:
			monitor.Lock()
			monitor.getCurrentSystemData()
			monitor.processAlerts()
			monitor.Unlock()
		}
	}
}

func (monitor *Monitor) sendMonitoringData(monitoringData cloudprotocol.MonitoringData) {
	if len(monitor.dataChannel) < cap(monitor.dataChannel) {
		monitor.dataChannel <- monitoringData
	} else {
		log.Warn("Skip sending monitoring data. Channel full.")
	}
}

func (monitor *Monitor) getCurrentSystemData() {
	cpu, err := getSystemCPUUsage()
	if err != nil {
		log.Errorf("Can't get system CPU: %s", err)
	}

	monitor.dataToSend.Global.CPU = uint64(math.Round(cpu))

	monitor.dataToSend.Global.RAM, err = getSystemRAMUsage()
	if err != nil {
		log.Errorf("Can't get system RAM: %s", err)
	}

	monitor.dataToSend.Global.UsedDisk, err = getSystemDiskUsage(monitor.workingDir)
	if err != nil {
		log.Errorf("Can't get system Disk usage: %s", err)
	}

	if monitor.trafficMonitor != nil {
		monitor.dataToSend.Global.InTraffic, monitor.dataToSend.Global.OutTraffic, err = monitor.trafficMonitor.GetSystemTraffic()
		if err != nil {
			log.Errorf("Can't get system traffic value: %s", err)
		}
	}

	log.WithFields(log.Fields{
		"CPU":  monitor.dataToSend.Global.CPU,
		"RAM":  monitor.dataToSend.Global.RAM,
		"Disk": monitor.dataToSend.Global.UsedDisk,
		"IN":   monitor.dataToSend.Global.InTraffic,
		"OUT":  monitor.dataToSend.Global.OutTraffic,
	}).Debug("Monitoring data")
}

func (monitor *Monitor) processAlerts() {
	currentTime := time.Now()

	for e := monitor.alertProcessors.Front(); e != nil; e = e.Next() {
		e.Value.(*alertProcessor).checkAlertDetection(currentTime)
	}
}

// getSystemCPUUsage returns CPU usage in parcent
func getSystemCPUUsage() (cpuUse float64, err error) {
	v, err := cpu.Percent(0, false)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	cpuUse = v[0]

	return cpuUse, nil
}

// getSystemRAMUsage returns RAM usage in bytes
func getSystemRAMUsage() (ram uint64, err error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return ram, aoserrors.Wrap(err)
	}

	return v.Used, nil
}

// getSystemDiskUsage returns disc usage in bytes
func getSystemDiskUsage(path string) (discUse uint64, err error) {
	v, err := disk.Usage(path)
	if err != nil {
		return discUse, aoserrors.Wrap(err)
	}

	return v.Used, nil
}
