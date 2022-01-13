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

package unitstatushandler

import (
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const maxAvailableTime = 1<<63 - 1

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

func validateTimetable(timetable []cloudprotocol.TimetableEntry) (err error) {
	if len(timetable) == 0 {
		return aoserrors.New("timetable is empty")
	}

	for _, entry := range timetable {
		if entry.DayOfWeek > 7 || entry.DayOfWeek < 1 {
			return aoserrors.New("invalid day of week value")
		}

		if len(entry.TimeSlots) == 0 {
			return aoserrors.New("no time slots")
		}

		for _, slot := range entry.TimeSlots {
			if year, month, day := slot.Start.Date(); year != 0 || month != 1 || day != 1 {
				return aoserrors.New("start value should contain only time")
			}

			if year, month, day := slot.Finish.Date(); year != 0 || month != 1 || day != 1 {
				return aoserrors.New("finish value should contain only time")
			}

			if slot.Start.After(slot.Finish.Time) {
				return aoserrors.New("start value should be before finish value")
			}
		}
	}

	return nil
}

func getAvailableTimetableTime(fromDate time.Time, timetable []cloudprotocol.TimetableEntry) (availableTime time.Duration, err error) {
	defer func() {
		log.WithFields(log.Fields{"fromDate": fromDate, "availableTime": availableTime}).Debug("Get available timetable time")
	}()

	// Set to maximum by default
	availableTime = maxAvailableTime

	if err = validateTimetable(timetable); err != nil {
		return availableTime, err
	}

	startTime := time.Date(fromDate.Year(), fromDate.Month(), fromDate.Day(), 0, 0, 0, 0, time.Local)

	for _, entry := range timetable {
		// Convert to time.Weekday
		entryWeekday := time.Weekday((entry.DayOfWeek) % 7)
		fromWeekday := fromDate.Weekday()

		// Get num of days from weekday to entry weekday
		shiftDays := int(entryWeekday - fromWeekday)
		if shiftDays < 0 {
			shiftDays += 7
		}

		startEntry := startTime.Add(time.Duration(shiftDays) * 24 * time.Hour)

		for _, slot := range entry.TimeSlots {
			startDate := time.Date(startEntry.Year(), startEntry.Month(), startEntry.Day(),
				slot.Start.Hour(), slot.Start.Minute(), slot.Start.Second(), slot.Start.Nanosecond(), time.Local)

			finishDate := time.Date(startEntry.Year(), startEntry.Month(), startEntry.Day(),
				slot.Finish.Hour(), slot.Finish.Minute(), slot.Finish.Second(), slot.Finish.Nanosecond(), time.Local)

			duration := startDate.Sub(fromDate)

			if duration < 0 {
				// We are in the time slot right now
				if fromDate.Before(finishDate) {
					return 0, nil
				}

				// We are ouf of this slot, skip it
				continue
			}

			// Calculate nearest available time
			if duration < availableTime {
				availableTime = duration
			}
		}
	}

	return availableTime, nil
}
