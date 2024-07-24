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

package resourcemonitor

import (
	"math"
	"time"

	"github.com/aosedge/aos_common/aostypes"
	log "github.com/sirupsen/logrus"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	AlertStatusRaise    = "raise"
	AlertStatusContinue = "continue"
	AlertStatusFall     = "fall"
)

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

type alertCallback func(time time.Time, value uint64, status string)

// alertProcessor object for detection alerts.
type alertProcessor struct {
	name     string
	source   *uint64
	callback alertCallback

	minTimeout       time.Duration
	minThreshold     uint64
	maxThreshold     uint64
	minThresholdTime time.Time
	maxThresholdTime time.Time
	alertCondition   bool
}

// createAlertProcessorPercents creates alert processor based on percents configuration.
func createAlertProcessorPercents(name string, source *uint64, maxValue uint64,
	callback alertCallback, rule aostypes.AlertRulePercents,
) (alert *alertProcessor) {
	log.WithFields(log.Fields{"rule": rule, "name": name}).Debugf("Create alert percents processor")

	return &alertProcessor{
		name:         name,
		source:       source,
		callback:     callback,
		minTimeout:   rule.MinTimeout.Duration,
		minThreshold: uint64(math.Round(float64(maxValue) * rule.MinThreshold / 100.0)),
		maxThreshold: uint64(math.Round(float64(maxValue) * rule.MaxThreshold / 100.0)),
	}
}

// createAlertProcessorPoints creates alert processor based on points configuration.
func createAlertProcessorPoints(name string, source *uint64,
	callback alertCallback, rule aostypes.AlertRulePoints,
) (alert *alertProcessor) {
	log.WithFields(log.Fields{"rule": rule, "name": name}).Debugf("Create alert points processor")

	return &alertProcessor{
		name:         name,
		source:       source,
		callback:     callback,
		minTimeout:   rule.MinTimeout.Duration,
		minThreshold: rule.MinThreshold,
		maxThreshold: rule.MaxThreshold,
	}
}

// checkAlertDetection checks if alert was detected.
func (alert *alertProcessor) checkAlertDetection(currentTime time.Time) {
	value := *alert.source

	if !alert.alertCondition {
		alert.handleMaxThreshold(currentTime, value)
	} else {
		alert.handleMinThreshold(currentTime, value)
	}
}

func (alert *alertProcessor) handleMaxThreshold(currentTime time.Time, value uint64) {
	if value >= alert.maxThreshold && alert.maxThresholdTime.IsZero() {
		log.WithFields(log.Fields{
			"name":         alert.name,
			"maxThreshold": alert.maxThreshold,
			"value":        value,
			"currentTime":  currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Max threshold crossed")

		alert.maxThresholdTime = currentTime
	}

	if value >= alert.maxThreshold && !alert.maxThresholdTime.IsZero() &&
		currentTime.Sub(alert.maxThresholdTime) >= alert.minTimeout && !alert.alertCondition {
		alert.alertCondition = true
		alert.maxThresholdTime = currentTime
		alert.minThresholdTime = time.Time{}

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusRaise,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusRaise)
	}

	if value < alert.maxThreshold && !alert.maxThresholdTime.IsZero() {
		alert.maxThresholdTime = time.Time{}
	}
}

func (alert *alertProcessor) handleMinThreshold(currentTime time.Time, value uint64) {
	if value <= alert.minThreshold && !alert.minThresholdTime.IsZero() &&
		currentTime.Sub(alert.minThresholdTime) >= alert.minTimeout {
		alert.alertCondition = false
		alert.minThresholdTime = currentTime
		alert.maxThresholdTime = time.Time{}

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusFall,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusFall)
	}

	if currentTime.Sub(alert.maxThresholdTime) >= alert.minTimeout && alert.alertCondition {
		alert.maxThresholdTime = currentTime

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusContinue,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusContinue)
	}

	if value <= alert.minThreshold && alert.minThresholdTime.IsZero() {
		log.WithFields(log.Fields{
			"name": alert.name, "minThreshold": alert.minThreshold, "value": value,
		}).Debugf("Min threshold crossed")

		alert.minThresholdTime = currentTime
	}

	if value > alert.maxThreshold && !alert.minThresholdTime.IsZero() {
		alert.minThresholdTime = time.Time{}
	}
}
