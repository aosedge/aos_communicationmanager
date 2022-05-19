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

package pbconvert

import (
	"github.com/aoscloud/aos_common/api/cloudprotocol"
	pb "github.com/aoscloud/aos_common/api/servicemanager/v2"
)

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

func InstanceFilterToPB(filter cloudprotocol.InstanceFilter) *pb.InstanceIdent {
	ident := &pb.InstanceIdent{ServiceId: filter.ServiceID, SubjectId: "", Instance: -1}

	if filter.SubjectID != nil {
		ident.SubjectId = *filter.SubjectID
	}

	if filter.Instance != nil {
		ident.Instance = (int64)(*filter.Instance)
	}

	return ident
}

func InstanceIdentToPB(ident cloudprotocol.InstanceIdent) *pb.InstanceIdent {
	return &pb.InstanceIdent{ServiceId: ident.ServiceID, SubjectId: ident.SubjectID, Instance: int64(ident.Instance)}
}

func NewInstanceIdentFromPB(ident *pb.InstanceIdent) cloudprotocol.InstanceIdent {
	return cloudprotocol.InstanceIdent{
		ServiceID: ident.ServiceId,
		SubjectID: ident.SubjectId,
		Instance:  uint64(ident.Instance),
	}
}
