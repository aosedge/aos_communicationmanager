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

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/aostypes"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/utils/fs"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
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

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type SystemUsageProvider interface {
	CacheSystemInfos()
	FillSystemInfo(instanceID string, instance *instanceMonitoring) error
}

// QuotaAlert quota alert structure.
type QuotaAlert struct {
	Timestamp time.Time
	Parameter string
	Value     uint64
	Status    string
}

// AlertSender interface to send resource alerts.
type AlertSender interface {
	SendAlert(alert cloudprotocol.AlertItem)
}

// NodeInfoProvider interface to get node information.
type NodeInfoProvider interface {
	GetCurrentNodeInfo() (cloudprotocol.NodeInfo, error)
}

// NodeConfigProvider interface to get node config.
type NodeConfigProvider interface {
	GetCurrentNodeConfig() (cloudprotocol.NodeConfig, error)
	CurrentNodeConfigChannel() <-chan cloudprotocol.NodeConfig
}

// MonitoringSender sends monitoring data.
type MonitoringSender interface {
	SendNodeMonitoring(monitoringData aostypes.NodeMonitoring)
}

// TrafficMonitoring interface to get network traffic.
type TrafficMonitoring interface {
	GetSystemTraffic() (inputTraffic, outputTraffic uint64, err error)
	GetInstanceTraffic(instanceID string) (inputTraffic, outputTraffic uint64, err error)
}

// Config configuration for resource monitoring.
type Config struct {
	PollPeriod    aostypes.Duration `json:"pollPeriod"`
	AverageWindow aostypes.Duration `json:"averageWindow"`
	Source        string            `json:"source"`
}

// ResourceMonitor instance.
type ResourceMonitor struct {
	sync.Mutex

	nodeInfoProvider   NodeInfoProvider
	nodeConfigProvider NodeConfigProvider
	alertSender        AlertSender
	monitoringSender   MonitoringSender
	trafficMonitoring  TrafficMonitoring
	sourceSystemUsage  SystemUsageProvider

	pollTimer             *time.Ticker
	averageWindowCount    uint64
	nodeInfo              cloudprotocol.NodeInfo
	nodeMonitoring        aostypes.MonitoringData
	nodeAverageData       averageMonitoring
	instanceMonitoringMap map[string]*instanceMonitoring
	alertProcessors       *list.List

	cancelFunction context.CancelFunc
}

// PartitionParam partition instance information.
type PartitionParam struct {
	Name string
	Path string
}

// ResourceMonitorParams instance resource monitor parameters.
type ResourceMonitorParams struct {
	aostypes.InstanceIdent
	UID        int
	GID        int
	AlertRules *aostypes.AlertRules
	Partitions []PartitionParam
}

type instanceMonitoring struct {
	uid                    uint32
	gid                    uint32
	partitions             []PartitionParam
	monitoring             aostypes.InstanceMonitoring
	averageData            averageMonitoring
	alertProcessorElements []*list.Element
	prevCPU                uint64
	prevTime               time.Time
}

type averageMonitoring struct {
	ram        *averageCalc
	cpu        *averageCalc
	inTraffic  *averageCalc
	outTraffic *averageCalc
	disks      map[string]*averageCalc
}

/***********************************************************************************************************************
 * Variable
 **********************************************************************************************************************/

// These global variables are used to be able to mocking the functionality of getting quota in tests.
//
//nolint:gochecknoglobals
var (
	systemCPUPercent                        = cpu.Percent
	systemVirtualMemory                     = mem.VirtualMemory
	systemDiskUsage                         = disk.Usage
	getUserFSQuotaUsage                     = fs.GetUserFSQuotaUsage
	cpuCount                                = runtime.NumCPU()
	instanceUsage       SystemUsageProvider = nil
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New creates new resource monitor instance.
func New(
	config Config, nodeInfoProvider NodeInfoProvider, nodeConfigProvider NodeConfigProvider,
	trafficMonitoring TrafficMonitoring, alertsSender AlertSender, monitoringSender MonitoringSender) (
	*ResourceMonitor, error,
) {
	log.Debug("Create monitor")

	monitor := &ResourceMonitor{
		nodeInfoProvider:   nodeInfoProvider,
		nodeConfigProvider: nodeConfigProvider,
		alertSender:        alertsSender,
		monitoringSender:   monitoringSender,
		trafficMonitoring:  trafficMonitoring,
		sourceSystemUsage:  getSourceSystemUsage(config.Source),
	}

	nodeInfo, err := nodeInfoProvider.GetCurrentNodeInfo()
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	monitor.averageWindowCount = uint64(config.AverageWindow.Duration.Nanoseconds()) /
		uint64(config.PollPeriod.Duration.Nanoseconds())
	if monitor.averageWindowCount == 0 {
		monitor.averageWindowCount = 1
	}

	if err := monitor.setupNodeMonitoring(nodeInfo); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	nodeConfig, err := nodeConfigProvider.GetCurrentNodeConfig()
	if err != nil {
		log.Errorf("Can't get node config: %v", err)
	}

	if err := monitor.setupSystemAlerts(nodeConfig); err != nil {
		log.Errorf("Can't setup system alerts: %v", err)
	}

	monitor.instanceMonitoringMap = make(map[string]*instanceMonitoring)

	ctx, cancelFunc := context.WithCancel(context.Background())
	monitor.cancelFunction = cancelFunc

	monitor.pollTimer = time.NewTicker(config.PollPeriod.Duration)

	go monitor.run(ctx)

	return monitor, nil
}

// Close closes monitor instance.
func (monitor *ResourceMonitor) Close() {
	log.Debug("Close monitor")

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

	instanceMonitoring := &instanceMonitoring{
		uid:        uint32(monitoringConfig.UID),
		gid:        uint32(monitoringConfig.GID),
		partitions: monitoringConfig.Partitions,
		monitoring: aostypes.InstanceMonitoring{InstanceIdent: monitoringConfig.InstanceIdent},
	}

	monitor.instanceMonitoringMap[instanceID] = instanceMonitoring

	instanceMonitoring.monitoring.Disk = make(
		[]aostypes.PartitionUsage, len(monitoringConfig.Partitions))

	for i, partitionParam := range monitoringConfig.Partitions {
		instanceMonitoring.monitoring.Disk[i].Name = partitionParam.Name
	}

	instanceMonitoring.averageData = *newAverageMonitoring(
		monitor.averageWindowCount, instanceMonitoring.monitoring.Disk)

	if monitoringConfig.AlertRules != nil && monitor.alertSender != nil {
		if err := monitor.setupInstanceAlerts(
			instanceID, instanceMonitoring, *monitoringConfig.AlertRules); err != nil {
			log.Errorf("Can't setup instance alerts: %v", err)
		}
	}

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

// GetAverageMonitoring returns average monitoring data.
func (monitor *ResourceMonitor) GetAverageMonitoring() (aostypes.NodeMonitoring, error) {
	monitor.Lock()
	defer monitor.Unlock()

	log.Debug("Get average monitoring data")

	timestamp := time.Now()

	averageMonitoringData := aostypes.NodeMonitoring{
		NodeID:        monitor.nodeInfo.NodeID,
		NodeData:      monitor.nodeAverageData.toMonitoringData(timestamp),
		InstancesData: make([]aostypes.InstanceMonitoring, 0, len(monitor.instanceMonitoringMap)),
	}

	for _, instanceMonitoring := range monitor.instanceMonitoringMap {
		averageMonitoringData.InstancesData = append(averageMonitoringData.InstancesData,
			aostypes.InstanceMonitoring{
				InstanceIdent:  instanceMonitoring.monitoring.InstanceIdent,
				MonitoringData: instanceMonitoring.averageData.toMonitoringData(timestamp),
			})
	}

	return averageMonitoringData, nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (monitor *ResourceMonitor) setupNodeMonitoring(nodeInfo cloudprotocol.NodeInfo) error {
	monitor.Lock()
	defer monitor.Unlock()

	if nodeInfo.MaxDMIPs == 0 {
		return aoserrors.Errorf("max DMIPs is 0")
	}

	monitor.nodeInfo = nodeInfo

	monitor.nodeMonitoring = aostypes.MonitoringData{
		Disk: make([]aostypes.PartitionUsage, len(nodeInfo.Partitions)),
	}

	for i, partitionParam := range nodeInfo.Partitions {
		monitor.nodeMonitoring.Disk[i].Name = partitionParam.Name
	}

	monitor.nodeAverageData = *newAverageMonitoring(monitor.averageWindowCount, monitor.nodeMonitoring.Disk)

	return nil
}

func (monitor *ResourceMonitor) setupSystemAlerts(nodeConfig cloudprotocol.NodeConfig) (err error) {
	monitor.Lock()
	defer monitor.Unlock()

	monitor.alertProcessors = list.New()

	if nodeConfig.AlertRules == nil || monitor.alertSender == nil {
		return nil
	}

	if nodeConfig.AlertRules.CPU != nil {
		rules := *nodeConfig.AlertRules.CPU
		rules.High = monitor.cpuToDMIPs(float64(rules.High))
		rules.Low = monitor.cpuToDMIPs(float64(rules.Low))

		monitor.alertProcessors.PushBack(createAlertProcessor(
			"System CPU",
			&monitor.nodeMonitoring.CPU,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("cpu", time, value, status))
			},
			rules))
	}

	if nodeConfig.AlertRules.RAM != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"System RAM",
			&monitor.nodeMonitoring.RAM,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("ram", time, value, status))
			},
			*nodeConfig.AlertRules.RAM))
	}

	for _, diskRule := range nodeConfig.AlertRules.UsedDisks {
		diskUsageValue, findErr := getDiskUsageValue(monitor.nodeMonitoring.Disk, diskRule.Name)
		if findErr != nil && err == nil {
			err = findErr

			log.Errorf("Can't find disk: %s", diskRule.Name)

			continue
		}

		monitor.alertProcessors.PushBack(createAlertProcessor(
			"Partition "+diskRule.Name,
			diskUsageValue,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem(diskRule.Name, time, value, status))
			},
			diskRule.AlertRuleParam))
	}

	if nodeConfig.AlertRules.InTraffic != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"IN Traffic",
			&monitor.nodeMonitoring.InTraffic,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("inTraffic", time, value, status))
			},
			*nodeConfig.AlertRules.InTraffic))
	}

	if nodeConfig.AlertRules.OutTraffic != nil {
		monitor.alertProcessors.PushBack(createAlertProcessor(
			"OUT Traffic",
			&monitor.nodeMonitoring.OutTraffic,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(prepareSystemAlertItem("outTraffic", time, value, status))
			},
			*nodeConfig.AlertRules.OutTraffic))
	}

	return err
}

func getDiskUsageValue(disks []aostypes.PartitionUsage, name string) (*uint64, error) {
	for i, disk := range disks {
		if disk.Name == name {
			return &disks[i].UsedSize, nil
		}
	}

	return nil, aoserrors.Errorf("can't find disk %s", name)
}

func getDiskPath(disks []cloudprotocol.PartitionInfo, name string) (string, error) {
	for _, disk := range disks {
		if disk.Name == name {
			return disk.Path, nil
		}
	}

	return "", aoserrors.Errorf("can't find disk %s", name)
}

func (monitor *ResourceMonitor) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case nodeConfig := <-monitor.nodeConfigProvider.CurrentNodeConfigChannel():
			if err := monitor.setupSystemAlerts(nodeConfig); err != nil {
				log.Errorf("Can't setup system alerts: %v", err)
			}

		case <-monitor.pollTimer.C:
			monitor.Lock()
			monitor.sourceSystemUsage.CacheSystemInfos()
			monitor.getCurrentSystemData()
			monitor.getCurrentInstancesData()
			monitor.processAlerts()
			monitor.sendMonitoringData()
			monitor.Unlock()
		}
	}
}

func (monitor *ResourceMonitor) setupInstanceAlerts(instanceID string, instanceMonitoring *instanceMonitoring,
	rules aostypes.AlertRules,
) (err error) {
	instanceMonitoring.alertProcessorElements = make([]*list.Element, 0)

	if rules.CPU != nil {
		rules := *rules.CPU
		rules.High = monitor.cpuToDMIPs(float64(rules.High))
		rules.Low = monitor.cpuToDMIPs(float64(rules.Low))

		e := monitor.alertProcessors.PushBack(createAlertProcessor(
			instanceID+" CPU",
			&instanceMonitoring.monitoring.CPU,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(
					prepareInstanceAlertItem(
						instanceMonitoring.monitoring.InstanceIdent, "cpu", time, value, status))
			}, rules))

		instanceMonitoring.alertProcessorElements = append(instanceMonitoring.alertProcessorElements, e)
	}

	if rules.RAM != nil {
		e := monitor.alertProcessors.PushBack(createAlertProcessor(
			instanceID+" RAM",
			&instanceMonitoring.monitoring.RAM,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(
					prepareInstanceAlertItem(
						instanceMonitoring.monitoring.InstanceIdent, "ram", time, value, status))
			}, *rules.RAM))

		instanceMonitoring.alertProcessorElements = append(instanceMonitoring.alertProcessorElements, e)
	}

	for _, diskRule := range rules.UsedDisks {
		diskUsageValue, findErr := getDiskUsageValue(instanceMonitoring.monitoring.Disk, diskRule.Name)
		if findErr != nil && err == nil {
			log.Errorf("Can't find disk: %s", diskRule.Name)

			err = findErr

			continue
		}

		e := monitor.alertProcessors.PushBack(createAlertProcessor(
			instanceID+" Partition "+diskRule.Name,
			diskUsageValue,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(
					prepareInstanceAlertItem(
						instanceMonitoring.monitoring.InstanceIdent, diskRule.Name, time, value, status))
			}, diskRule.AlertRuleParam))

		instanceMonitoring.alertProcessorElements = append(instanceMonitoring.alertProcessorElements, e)
	}

	if rules.InTraffic != nil {
		e := monitor.alertProcessors.PushBack(createAlertProcessor(
			instanceID+" Traffic IN",
			&instanceMonitoring.monitoring.InTraffic,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(
					prepareInstanceAlertItem(
						instanceMonitoring.monitoring.InstanceIdent, "inTraffic", time, value, status))
			}, *rules.InTraffic))

		instanceMonitoring.alertProcessorElements = append(instanceMonitoring.alertProcessorElements, e)
	}

	if rules.OutTraffic != nil {
		e := monitor.alertProcessors.PushBack(createAlertProcessor(
			instanceID+" Traffic OUT",
			&instanceMonitoring.monitoring.OutTraffic,
			func(time time.Time, value uint64, status string) {
				monitor.alertSender.SendAlert(
					prepareInstanceAlertItem(
						instanceMonitoring.monitoring.InstanceIdent, "outTraffic", time, value, status))
			}, *rules.OutTraffic))

		instanceMonitoring.alertProcessorElements = append(instanceMonitoring.alertProcessorElements, e)
	}

	return err
}

func (monitor *ResourceMonitor) sendMonitoringData() {
	nodeMonitoringData := aostypes.NodeMonitoring{
		NodeID:        monitor.nodeInfo.NodeID,
		NodeData:      monitor.nodeMonitoring,
		InstancesData: make([]aostypes.InstanceMonitoring, 0, len(monitor.instanceMonitoringMap)),
	}

	for _, instanceMonitoring := range monitor.instanceMonitoringMap {
		nodeMonitoringData.InstancesData = append(nodeMonitoringData.InstancesData,
			instanceMonitoring.monitoring)
	}

	monitor.monitoringSender.SendNodeMonitoring(nodeMonitoringData)
}

func (monitor *ResourceMonitor) getCurrentSystemData() {
	monitor.nodeMonitoring.Timestamp = time.Now()

	cpu, err := getSystemCPUUsage()
	if err != nil {
		log.Errorf("Can't get system CPU: %s", err)
	}

	monitor.nodeMonitoring.CPU = monitor.cpuToDMIPs(cpu)

	monitor.nodeMonitoring.RAM, err = getSystemRAMUsage()
	if err != nil {
		log.Errorf("Can't get system RAM: %s", err)
	}

	for i, disk := range monitor.nodeMonitoring.Disk {
		mountPoint, err := getDiskPath(monitor.nodeInfo.Partitions, disk.Name)
		if err != nil {
			log.Errorf("Can't get disk path: %v", err)

			continue
		}

		monitor.nodeMonitoring.Disk[i].UsedSize, err = getSystemDiskUsage(mountPoint)
		if err != nil {
			log.Errorf("Can't get system Disk usage: %v", err)
		}
	}

	if monitor.trafficMonitoring != nil {
		inTraffic, outTraffic, err := monitor.trafficMonitoring.GetSystemTraffic()
		if err != nil {
			log.Errorf("Can't get system traffic value: %s", err)
		}

		monitor.nodeMonitoring.InTraffic = inTraffic
		monitor.nodeMonitoring.OutTraffic = outTraffic
	}

	monitor.nodeAverageData.updateMonitoringData(monitor.nodeMonitoring)

	log.WithFields(log.Fields{
		"CPU":  monitor.nodeMonitoring.CPU,
		"RAM":  monitor.nodeMonitoring.RAM,
		"Disk": monitor.nodeMonitoring.Disk,
		"IN":   monitor.nodeMonitoring.InTraffic,
		"OUT":  monitor.nodeMonitoring.OutTraffic,
	}).Debug("Monitoring data")
}

func (monitor *ResourceMonitor) getCurrentInstancesData() {
	timestamp := time.Now()

	for instanceID, value := range monitor.instanceMonitoringMap {
		value.monitoring.Timestamp = timestamp

		err := monitor.sourceSystemUsage.FillSystemInfo(instanceID, value)
		if err != nil {
			log.Errorf("Can't fill system usage info: %v", err)
		}

		value.monitoring.CPU = monitor.cpuToDMIPs(float64(value.monitoring.CPU))

		for i, partitionParam := range value.partitions {
			value.monitoring.Disk[i].UsedSize, err = getInstanceDiskUsage(partitionParam.Path, value.uid, value.gid)
			if err != nil {
				log.Errorf("Can't get service disk usage: %v", err)
			}
		}

		if monitor.trafficMonitoring != nil {
			inTraffic, outTraffic, err := monitor.trafficMonitoring.GetInstanceTraffic(instanceID)
			if err != nil {
				log.Errorf("Can't get service traffic: %s", err)
			}

			value.monitoring.InTraffic = inTraffic
			value.monitoring.OutTraffic = outTraffic
		}

		value.averageData.updateMonitoringData(value.monitoring.MonitoringData)

		log.WithFields(log.Fields{
			"id":   instanceID,
			"CPU":  value.monitoring.CPU,
			"RAM":  value.monitoring.RAM,
			"Disk": value.monitoring.Disk,
			"IN":   value.monitoring.InTraffic,
			"OUT":  value.monitoring.OutTraffic,
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

// getSystemCPUUsage returns CPU usage in percent.
func getSystemCPUUsage() (cpuUse float64, err error) {
	v, err := systemCPUPercent(0, false)
	if err != nil {
		return 0, aoserrors.Wrap(err)
	}

	cpuUse = v[0] / float64(cpuCount)

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

// getSystemDiskUsage returns disk usage in bytes.
func getSystemDiskUsage(path string) (diskUse uint64, err error) {
	v, err := systemDiskUsage(path)
	if err != nil {
		return diskUse, aoserrors.Wrap(err)
	}

	return v.Used, nil
}

// getServiceDiskUsage returns service disk usage in bytes.
func getInstanceDiskUsage(path string, uid, gid uint32) (diskUse uint64, err error) {
	if diskUse, err = getUserFSQuotaUsage(path, uid, gid); err != nil {
		return diskUse, aoserrors.Wrap(err)
	}

	return diskUse, nil
}

func prepareSystemAlertItem(
	parameter string, timestamp time.Time, value uint64, status string,
) cloudprotocol.AlertItem {
	return cloudprotocol.AlertItem{
		Timestamp: timestamp,
		Tag:       cloudprotocol.AlertTagSystemQuota,
		Payload: cloudprotocol.SystemQuotaAlert{
			Parameter: parameter,
			Value:     value,
			Status:    status,
		},
	}
}

func prepareInstanceAlertItem(
	instanceIndent aostypes.InstanceIdent, parameter string, timestamp time.Time, value uint64, status string,
) cloudprotocol.AlertItem {
	return cloudprotocol.AlertItem{
		Timestamp: timestamp,
		Tag:       cloudprotocol.AlertTagInstanceQuota,
		Payload: cloudprotocol.InstanceQuotaAlert{
			InstanceIdent: instanceIndent,
			Parameter:     parameter,
			Value:         value,
			Status:        status,
		},
	}
}

func getSourceSystemUsage(source string) SystemUsageProvider {
	if source == "xentop" {
		return &xenSystemUsage{}
	}

	if instanceUsage != nil {
		return instanceUsage
	}

	return &cgroupsSystemUsage{}
}

func (monitor *ResourceMonitor) cpuToDMIPs(cpu float64) uint64 {
	return uint64(math.Round(float64(cpu) * float64(monitor.nodeInfo.MaxDMIPs) / 100.0))
}

func newAverageMonitoring(windowCount uint64, partitions []aostypes.PartitionUsage) *averageMonitoring {
	averageMonitoring := &averageMonitoring{
		ram:        newAverageCalc(windowCount),
		cpu:        newAverageCalc(windowCount),
		inTraffic:  newAverageCalc(windowCount),
		outTraffic: newAverageCalc(windowCount),
		disks:      make(map[string]*averageCalc),
	}

	for _, partition := range partitions {
		averageMonitoring.disks[partition.Name] = newAverageCalc(windowCount)
	}

	return averageMonitoring
}

func (average *averageMonitoring) toMonitoringData(timestamp time.Time) aostypes.MonitoringData {
	data := aostypes.MonitoringData{
		CPU:        average.cpu.getIntValue(),
		RAM:        average.ram.getIntValue(),
		InTraffic:  average.inTraffic.getIntValue(),
		OutTraffic: average.outTraffic.getIntValue(),
		Disk:       make([]aostypes.PartitionUsage, 0, len(average.disks)),
		Timestamp:  timestamp,
	}

	for name, diskUsage := range average.disks {
		data.Disk = append(data.Disk, aostypes.PartitionUsage{
			Name: name, UsedSize: diskUsage.getIntValue(),
		})
	}

	return data
}

func (average *averageMonitoring) updateMonitoringData(data aostypes.MonitoringData) {
	average.cpu.calculate(float64(data.CPU))
	average.ram.calculate(float64(data.RAM))
	average.inTraffic.calculate(float64(data.InTraffic))
	average.outTraffic.calculate(float64(data.OutTraffic))

	for _, disk := range data.Disk {
		averageCalc, ok := average.disks[disk.Name]
		if !ok {
			log.Errorf("Can't find disk: %s", disk.Name)

			continue
		}

		averageCalc.calculate(float64(disk.UsedSize))
	}
}
