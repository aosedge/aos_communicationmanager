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

// semverutils is a simple wrapper around go-version library to compare versions

package semverutils

import (
	"github.com/aosedge/aos_common/aoserrors"
	semver "github.com/hashicorp/go-version"
)

func LessThan(version1 string, version2 string) (bool, error) {
	semver1, err := semver.NewSemver(version1)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	semver2, err := semver.NewSemver(version2)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	return semver1.LessThan(semver2), nil
}

func GreaterThan(version1 string, version2 string) (bool, error) {
	semver1, err := semver.NewSemver(version1)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	semver2, err := semver.NewSemver(version2)
	if err != nil {
		return false, aoserrors.Wrap(err)
	}

	return semver1.GreaterThan(semver2), nil
}
