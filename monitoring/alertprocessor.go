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

package monitoring

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/config"
)

type alertCallback func(time time.Time, value uint64)

// alertProcessor object for detection alerts
type alertProcessor struct {
	name              string
	source            *uint64
	callback          alertCallback
	rule              config.AlertRule
	thresholdTime     time.Time
	thresholdDetected bool
}

// createAlertProcessor creates alert processor based on configuration
func createAlertProcessor(name string, source *uint64,
	callback alertCallback, rule config.AlertRule) (alert *alertProcessor) {
	return &alertProcessor{name: name, source: source, callback: callback, rule: rule}
}

// checkAlertDetection checks if alert was detected
func (alert *alertProcessor) checkAlertDetection(currentTime time.Time) {
	value := *alert.source

	if value >= alert.rule.MaxThreshold && alert.thresholdTime.IsZero() {
		alert.thresholdTime = currentTime
	}

	if value < alert.rule.MinThreshold && !alert.thresholdTime.IsZero() {
		alert.thresholdTime = time.Time{}
		alert.thresholdDetected = false
	}

	if !alert.thresholdTime.IsZero() &&
		currentTime.Sub(alert.thresholdTime) >= alert.rule.MinTimeout.Duration &&
		!alert.thresholdDetected {

		log.WithFields(log.Fields{
			"value": value,
			"time":  currentTime.Format("Jan 2 15:04:05"),
		}).Debugf("%s alert", alert.name)

		alert.thresholdDetected = true

		alert.callback(currentTime, value)
	}
}
