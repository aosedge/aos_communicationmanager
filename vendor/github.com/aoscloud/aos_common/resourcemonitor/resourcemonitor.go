// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2022 Renesas Electronics Corporation.
// Copyright (C) 2022 EPAM Systems, Inc.
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

// Package resourcemonitor AOS Core Monitoring Component
package resourcemonitor

import (
	"container/list"
	"context"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/aostypes"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	"github.com/aoscloud/aos_common/utils/fs"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

// Service status.
const (
	MinutePeriod = iota
	HourPeriod
	DayPeriod
	MonthPeriod
	YearPeriod
)

// For optimization capacity should be equals numbers of measurement values
// 5 - RAM, CPU, UsedDisk, InTraffic, OutTraffic.
const capacityAlertProcessorElements = 5

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// AlertSender interface to send resource alerts.
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem)
}

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendMonitoringData(monitoringData cloudprotocol.MonitoringData)
}

// TrafficMonitoring interface to get network traffic.
type TrafficMonitoring interface {
	GetSystemTraffic() (inputTraffic, outputTraffic uint64, err error)
	GetInstanceTraffic(instanceID string) (inputTraffic, outputTraffic uint64, err error)
}

// Config configuration for resource monitoring.
type Config struct {
	aostypes.ServiceAlertRules
	SendPeriod aostypes.Duration `json:"sendPeriod"`
	PollPeriod aostypes.Duration `json:"pollPeriod"`
	WorkingDir string            `json:"workingDir"`
	StorageDir string            `json:"storageDir"`
}

// ResourceMonitor instance.
type ResourceMonitor struct {
	sync.Mutex

	alertSender      AlertSender
	monitoringSender MonitoringSender

	config Config

	sendTimer *time.Ticker
	pollTimer *time.Ticker

	globalMonitoringData cloudprotocol.GlobalMonitoringData

	alertProcessors *list.List

	instanceMonitoringMap map[string]*instanceMonitoring
	trafficMonitoring     TrafficMonitoring

	cancelFunction context.CancelFunc
}

// ResourceMonitorParams instance resourcemonitor parameters.
type ResourceMonitorParams struct {
	cloudprotocol.InstanceIdent
	UID        int
	GID        int
	AlertRules *aostypes.ServiceAlertRules
}

type instanceMonitoring struct {
	uid                    uint32
	gid                    uint32
	monitoringData         cloudprotocol.InstanceMonitoringData
	alertProcessorElements []*list.Element
}

type processInterface interface {
	Uids() ([]int32, error)
	CPUPercent() (float64, error)
	MemoryInfo() (*process.MemoryInfoStat, error)
}

/***********************************************************************************************************************
 * Variable
 **********************************************************************************************************************/

// These global variables are used to be able to mocking the functionality of getting quota in tests.
// nolint:gochecknoglobals
var (
	systemCPUPersent    = cpu.Percent
	systemVirtualMemory = mem.VirtualMemory
	systemDiskUsage     = disk.Usage
	getUserFSQuotaUsage = fs.GetUserFSQuotaUsage
	getProcesses        = getProcessesList
	numCPU              = runtime.NumCPU()
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new resourcemonitor instance.
func New(
	config Config, alertsSender AlertSender, monitoringSender MonitoringSender,
	trafficMonitoring TrafficMonitoring) (
	monitor *ResourceMonitor, err error,
) {
	log.Debug("Create monitor")

	monitor = &ResourceMonitor{
		alertSender:       alertsSender,
		monitoringSender:  monitoringSender,
		trafficMonitoring: trafficMonitoring,
		config:            config,
	}

	monitor.alertProcessors = list.New()

	if monitor.config.CPU != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"System CPU",
			&monitor.globalMonitoringData.CPU,
			func(time time.Time, value uint64) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("cpu", time, value))
			},
			*monitor.config.CPU))
	}

	if monitor.config.RAM != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"System RAM",
			&monitor.globalMonitoringData.RAM,
			func(time time.Time, value uint64) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("ram", time, value))
			},
			*monitor.config.RAM))
	}

	if monitor.config.UsedDisk != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"System Disk",
			&monitor.globalMonitoringData.UsedDisk,
			func(time time.Time, value uint64) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("disk", time, value))
			},
			*monitor.config.UsedDisk))
	}

	if monitor.config.InTraffic != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"IN Traffic",
			&monitor.globalMonitoringData.InTraffic,
			func(time time.Time, value uint64) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("inTraffic", time, value))
			},
			*monitor.config.InTraffic))
	}

	if monitor.config.OutTraffic != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"OUT Traffic",
			&monitor.globalMonitoringData.OutTraffic,
			func(time time.Time, value uint64) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("outTraffic", time, value))
			},
			*monitor.config.OutTraffic))
	}

	monitor.instanceMonitoringMap = make(map[string]*instanceMonitoring)

	ctx, cancelFunc := context.WithCancel(context.Background())
	monitor.cancelFunction = cancelFunc

	monitor.pollTimer = time.NewTicker(monitor.config.PollPeriod.Duration)
	monitor.sendTimer = time.NewTicker(monitor.config.SendPeriod.Duration)

	go monitor.run(ctx)

	return monitor, nil
}

// Close closes monitor instance.
func (monitor *ResourceMonitor) Close() {
	log.Debug("Close monitor")

	if monitor.sendTimer != nil {
		monitor.sendTimer.Stop()
	}

	if monitor.pollTimer != nil {
		monitor.pollTimer.Stop()
	}

	if monitor.cancelFunction != nil {
		monitor.cancelFunction()
	}
}

// StartInstanceMonitor starts monitoring service.
func (monitor *ResourceMonitor) StartInstanceMonitor(
	instanceID string, monitoringConfig ResourceMonitorParams,
) error {
	monitor.Lock()
	defer monitor.Unlock()

	if _, ok := monitor.instanceMonitoringMap[instanceID]; ok {
		log.WithField("id", instanceID).Warning("Service already under monitoring")

		return nil
	}

	log.WithFields(log.Fields{"id": instanceID}).Debug("Start instance monitoring")

	serviceMonitoring := instanceMonitoring{
		uid:            uint32(monitoringConfig.UID),
		gid:            uint32(monitoringConfig.GID),
		monitoringData: cloudprotocol.InstanceMonitoringData{InstanceIdent: monitoringConfig.InstanceIdent},
	}

	rules := monitoringConfig.AlertRules

	if monitor.alertSender != nil {
		serviceMonitoring.alertProcessorElements = make([]*list.Element, 0, capacityAlertProcessorElements)

		if rules != nil && rules.CPU != nil {
			e := monitor.alertProcessors.PushBack(createAlertProcessor(
				instanceID+" CPU",
				&serviceMonitoring.monitoringData.CPU,
				func(time time.Time, value uint64) {
					monitor.alertSender.SendAlert(
						prepareInstanceAlertItem(monitoringConfig.InstanceIdent, "cpu", time, value))
				}, *rules.CPU))

			serviceMonitoring.alertProcessorElements = append(serviceMonitoring.alertProcessorElements, e)
		}

		if rules != nil && rules.RAM != nil {
			e := monitor.alertProcessors.PushBack(createAlertProcessor(
				instanceID+" RAM",
				&serviceMonitoring.monitoringData.RAM,
				func(time time.Time, value uint64) {
					monitor.alertSender.SendAlert(
						prepareInstanceAlertItem(monitoringConfig.InstanceIdent, "ram", time, value))
				}, *rules.RAM))

			serviceMonitoring.alertProcessorElements = append(serviceMonitoring.alertProcessorElements, e)
		}

		if rules != nil && rules.UsedDisk != nil {
			e := monitor.alertProcessors.PushBack(createAlertProcessor(
				instanceID+" Disk",
				&serviceMonitoring.monitoringData.UsedDisk,
				func(time time.Time, value uint64) {
					monitor.alertSender.SendAlert(
						prepareInstanceAlertItem(monitoringConfig.InstanceIdent, "disk", time, value))
				}, *rules.UsedDisk))

			serviceMonitoring.alertProcessorElements = append(serviceMonitoring.alertProcessorElements, e)
		}

		if rules != nil && rules.InTraffic != nil {
			e := monitor.alertProcessors.PushBack(createAlertProcessor(
				instanceID+" Traffic IN",
				&serviceMonitoring.monitoringData.InTraffic,
				func(time time.Time, value uint64) {
					monitor.alertSender.SendAlert(
						prepareInstanceAlertItem(monitoringConfig.InstanceIdent, "inTraffic", time, value))
				}, *rules.InTraffic))

			serviceMonitoring.alertProcessorElements = append(serviceMonitoring.alertProcessorElements, e)
		}

		if rules != nil && rules.OutTraffic != nil {
			e := monitor.alertProcessors.PushBack(createAlertProcessor(
				instanceID+" Traffic OUT",
				&serviceMonitoring.monitoringData.OutTraffic,
				func(time time.Time, value uint64) {
					monitor.alertSender.SendAlert(
						prepareInstanceAlertItem(monitoringConfig.InstanceIdent, "outTraffic", time, value))
				}, *rules.OutTraffic))

			serviceMonitoring.alertProcessorElements = append(serviceMonitoring.alertProcessorElements, e)
		}
	}

	monitor.instanceMonitoringMap[instanceID] = &serviceMonitoring

	return nil
}

// StopInstanceMonitor stops monitoring service.
func (monitor *ResourceMonitor) StopInstanceMonitor(instanceID string) error {
	monitor.Lock()
	defer monitor.Unlock()

	log.WithField("id", instanceID).Debug("Stop instance monitoring")

	if _, ok := monitor.instanceMonitoringMap[instanceID]; !ok {
		return nil
	}

	for _, e := range monitor.instanceMonitoringMap[instanceID].alertProcessorElements {
		monitor.alertProcessors.Remove(e)
	}

	delete(monitor.instanceMonitoringMap, instanceID)

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (monitor *ResourceMonitor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-monitor.sendTimer.C:
			monitor.Lock()
			monitoringData := monitor.prepareMonitoringData()
			monitor.sendMonitoringData(monitoringData)
			monitor.Unlock()

		case <-monitor.pollTimer.C:
			monitor.Lock()
			monitor.getCurrentSystemData()
			monitor.getCurrentInstanceData()
			monitor.processAlerts()
			monitor.Unlock()
		}
	}
}

func (monitor *ResourceMonitor) prepareMonitoringData() cloudprotocol.MonitoringData {
	monitoringData := cloudprotocol.MonitoringData{
		Global:           monitor.globalMonitoringData,
		Timestamp:        time.Now(),
		ServiceInstances: make([]cloudprotocol.InstanceMonitoringData, 0, len(monitor.instanceMonitoringMap)),
	}

	for _, instance := range monitor.instanceMonitoringMap {
		monitoringData.ServiceInstances = append(monitoringData.ServiceInstances, instance.monitoringData)
	}

	return monitoringData
}

func (monitor *ResourceMonitor) sendMonitoringData(monitoringData cloudprotocol.MonitoringData) {
	monitor.monitoringSender.SendMonitoringData(monitoringData)
}

func (monitor *ResourceMonitor) getCurrentSystemData() {
	cpu, err := getSystemCPUUsage()
	if err != nil {
		log.Errorf("Can't get system CPU: %s", err)
	}

	monitor.globalMonitoringData.CPU = uint64(math.Round(cpu))

	monitor.globalMonitoringData.RAM, err = getSystemRAMUsage()
	if err != nil {
		log.Errorf("Can't get system RAM: %s", err)
	}

	monitor.globalMonitoringData.UsedDisk, err = getSystemDiskUsage(monitor.config.WorkingDir)
	if err != nil {
		log.Errorf("Can't get system Disk usage: %s", err)
	}

	if monitor.trafficMonitoring != nil {
		inTraffic, outTraffic, err := monitor.trafficMonitoring.GetSystemTraffic()
		if err != nil {
			log.Errorf("Can't get system traffic value: %s", err)
		}

		monitor.globalMonitoringData.InTraffic = inTraffic
		monitor.globalMonitoringData.OutTraffic = outTraffic
	}

	log.WithFields(log.Fields{
		"CPU":  monitor.globalMonitoringData.CPU,
		"RAM":  monitor.globalMonitoringData.RAM,
		"Disk": monitor.globalMonitoringData.UsedDisk,
		"IN":   monitor.globalMonitoringData.InTraffic,
		"OUT":  monitor.globalMonitoringData.OutTraffic,
	}).Debug("Monitoring data")
}

func (monitor *ResourceMonitor) getCurrentInstanceData() {
	for instanceID, value := range monitor.instanceMonitoringMap {
		cpuUsage, err := getInstanceCPUUsage(int32(value.uid))
		if err != nil {
			log.Errorf("Can't get service CPU: %s", err)
		}

		value.monitoringData.CPU = uint64(math.Round(cpuUsage / float64(numCPU)))

		value.monitoringData.RAM, err = getInstanceRAMUsage(int32(value.uid))
		if err != nil {
			log.Errorf("Can't get service RAM: %s", err)
		}

		value.monitoringData.UsedDisk, err = getInstanceDiskUsage(monitor.config.StorageDir, value.uid, value.gid)
		if err != nil {
			log.Errorf("Can't get service Disc usage: %s", err)
		}

		if monitor.trafficMonitoring != nil {
			inTraffic, outTraffic, err := monitor.trafficMonitoring.GetInstanceTraffic(instanceID)
			if err != nil {
				log.Errorf("Can't get service traffic: %s", err)
			}

			value.monitoringData.InTraffic = inTraffic
			value.monitoringData.OutTraffic = outTraffic
		}

		log.WithFields(log.Fields{
			"id":   instanceID,
			"CPU":  value.monitoringData.CPU,
			"RAM":  value.monitoringData.RAM,
			"Disk": value.monitoringData.UsedDisk,
			"IN":   value.monitoringData.InTraffic,
			"OUT":  value.monitoringData.OutTraffic,
		}).Debug("Instance monitoring data")
	}
}

func (monitor *ResourceMonitor) processAlerts() {
	currentTime := time.Now()

	for e := monitor.alertProcessors.Front(); e != nil; e = e.Next() {
		alertProcessor, ok := e.Value.(*alertProcessor)

		if !ok {
			log.Error("Unexpected alert processors type")
			return
		}

		alertProcessor.checkAlertDetection(currentTime)
	}
}

// getSystemCPUUsage returns CPU usage in parcent.
func getSystemCPUUsage() (cpuUse float64, err error) {
	v, err := systemCPUPersent(0, false)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	cpuUse = v[0]

	return cpuUse, nil
}

// getSystemRAMUsage returns RAM usage in bytes.
func getSystemRAMUsage() (ram uint64, err error) {
	v, err := systemVirtualMemory()
	if err != nil {
		return ram, aoserrors.Wrap(err)
	}

	return v.Used, nil
}

// getSystemDiskUsage returns disc usage in bytes.
func getSystemDiskUsage(path string) (discUse uint64, err error) {
	v, err := systemDiskUsage(path)
	if err != nil {
		return discUse, aoserrors.Wrap(err)
	}

	return v.Used, nil
}

// getServiceCPUUsage returns service CPU usage in percent.
func getInstanceCPUUsage(uid int32) (cpuUse float64, err error) {
	processes, err := getProcesses()
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	for _, process := range processes {
		uids, err := process.Uids()
		if err != nil {
			continue
		}

		for _, id := range uids {
			if id == uid {
				cpu, err := process.CPUPercent()
				if err != nil {
					return 0, aoserrors.Wrap(err)
				}

				cpuUse += cpu

				break
			}
		}
	}

	return cpuUse, nil
}

// getServiceRAMUsage returns service RAM usage in bytes.
func getInstanceRAMUsage(uid int32) (ram uint64, err error) {
	processes, err := getProcesses()
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	for _, process := range processes {
		uids, err := process.Uids()
		if err != nil {
			continue
		}

		for _, id := range uids {
			if id == uid {
				memInfo, err := process.MemoryInfo()
				if err != nil {
					return 0, aoserrors.Wrap(err)
				}

				ram += memInfo.RSS

				break
			}
		}
	}

	return ram, nil
}

func getProcessesList() (processes []processInterface, err error) {
	proc, err := process.Processes()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	processes = make([]processInterface, len(proc))

	for i, process := range proc {
		processes[i] = process
	}

	return processes, nil
}

// getServiceDiskUsage returns service disk usage in bytes.
func getInstanceDiskUsage(path string, uid, gid uint32) (diskUse uint64, err error) {
	if diskUse, err = getUserFSQuotaUsage(path, uid, gid); err != nil {
		return diskUse, aoserrors.Wrap(err)
	}

	return diskUse, nil
}

func prepareSystemAlertItem(parameter string, timestamp time.Time, value uint64) cloudprotocol.AlertItem {
	return cloudprotocol.AlertItem{
		Timestamp: timestamp,
		Tag:       cloudprotocol.AlertTagSystemQuota,
		Payload: cloudprotocol.SystemQuotaAlert{
			Parameter: parameter,
			Value:     value,
		},
	}
}

func prepareInstanceAlertItem(
	instanceIndent cloudprotocol.InstanceIdent, parameter string, timestamp time.Time, value uint64,
) cloudprotocol.AlertItem {
	return cloudprotocol.AlertItem{
		Timestamp: timestamp,
		Tag:       cloudprotocol.AlertTagInstanceQuota,
		Payload: cloudprotocol.InstanceQuotaAlert{
			InstanceIdent: instanceIndent,
			Parameter:     parameter,
			Value:         value,
		},
	}
}
