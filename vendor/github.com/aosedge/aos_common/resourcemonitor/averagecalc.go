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
	"math"
)

/***********************************************************************************************************************
 * Structs
 **********************************************************************************************************************/

type averageCalc struct {
	sum         float64
	count       uint64
	windowCount uint64
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func newAverageCalc(windowCount uint64) *averageCalc {
	return &averageCalc{windowCount: windowCount}
}

func (calc *averageCalc) calculate(value float64) float64 {
	if calc.count < calc.windowCount {
		calc.sum += value
		calc.count++
	} else {
		calc.sum -= calc.sum / float64(calc.count)
		calc.sum += value
	}

	return calc.getValue()
}

func (calc *averageCalc) getValue() float64 {
	if calc.count == 0 {
		return 0
	}

	return calc.sum / float64(calc.count)
}

func (calc *averageCalc) getIntValue() uint64 {
	return uint64(math.Round(calc.getValue()))
}
