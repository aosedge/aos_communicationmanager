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
	name              string
	source            *uint64
	callback          alertCallback
	rule              aostypes.AlertRuleParam
	highThresholdTime time.Time
	lowThresholdTime  time.Time
	alertCondition    bool
}

// createAlertProcessor creates alert processor based on configuration.
func createAlertProcessor(name string, source *uint64,
	callback alertCallback, rule aostypes.AlertRuleParam,
) (alert *alertProcessor) {
	log.WithFields(log.Fields{"rule": rule, "name": name}).Debugf("Create alert processor")

	return &alertProcessor{name: name, source: source, callback: callback, rule: rule}
}

// checkAlertDetection checks if alert was detected.
func (alert *alertProcessor) checkAlertDetection(currentTime time.Time) {
	value := *alert.source

	if !alert.alertCondition {
		alert.handleHighThreshold(currentTime, value)
	} else {
		alert.handleLowThreshold(currentTime, value)
	}
}

func (alert *alertProcessor) handleHighThreshold(currentTime time.Time, value uint64) {
	if value >= alert.rule.High && alert.highThresholdTime.IsZero() {
		log.WithFields(log.Fields{
			"name":          alert.name,
			"highThreshold": alert.rule.High,
			"value":         value,
			"currentTime":   currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("High threshold crossed")

		alert.highThresholdTime = currentTime
	}

	if value >= alert.rule.High && !alert.highThresholdTime.IsZero() &&
		currentTime.Sub(alert.highThresholdTime) >= alert.rule.Timeout.Duration && !alert.alertCondition {
		alert.alertCondition = true
		alert.highThresholdTime = currentTime
		alert.lowThresholdTime = time.Time{}

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusRaise,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusRaise)
	}

	if value < alert.rule.High && !alert.highThresholdTime.IsZero() {
		alert.highThresholdTime = time.Time{}
	}
}

func (alert *alertProcessor) handleLowThreshold(currentTime time.Time, value uint64) {
	if value <= alert.rule.Low && !alert.lowThresholdTime.IsZero() &&
		currentTime.Sub(alert.lowThresholdTime) >= alert.rule.Timeout.Duration {
		alert.alertCondition = false
		alert.lowThresholdTime = currentTime
		alert.highThresholdTime = time.Time{}

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusFall,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusFall)
	}

	if currentTime.Sub(alert.highThresholdTime) >= alert.rule.Timeout.Duration && alert.alertCondition {
		alert.highThresholdTime = currentTime

		log.WithFields(log.Fields{
			"name":        alert.name,
			"value":       value,
			"status":      AlertStatusContinue,
			"currentTime": currentTime.Format("Jan 2 15:04:05.000"),
		}).Debugf("Resource alert")

		alert.callback(currentTime, value, AlertStatusContinue)
	}

	if value <= alert.rule.Low && alert.lowThresholdTime.IsZero() {
		log.WithFields(log.Fields{
			"name": alert.name, "lowThreshold": alert.rule.Low, "value": value,
		}).Debugf("Low threshold crossed")

		alert.lowThresholdTime = currentTime
	}

	if value > alert.rule.Low && !alert.lowThresholdTime.IsZero() {
		alert.lowThresholdTime = time.Time{}
	}
}
