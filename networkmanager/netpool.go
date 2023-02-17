// SPDX-License-Identifier: Apache-2.0
//
// Copyright (C) 2023 Renesas Electronics Corporation.
// Copyright (C) 2023 EPAM Systems, Inc.
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

// Package networkmanager provides set of API to configure network

package networkmanager

import (
	"net"

	"github.com/aoscloud/aos_common/aoserrors"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type networkToSplit struct {
	ipSubNet string
	size     int
}

/***********************************************************************************************************************
 * Var
 **********************************************************************************************************************/

// nolint:gochecknoglobals
var predefinedPrivateNetworks = []*networkToSplit{
	{"172.17.0.0/16", 16},
	{"172.18.0.0/16", 16},
	{"172.19.0.0/16", 16},
	{"172.20.0.0/14", 16},
	{"172.24.0.0/14", 16},
	{"172.28.0.0/14", 16},
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func makeNetPools() (listIPNetPool []*net.IPNet, err error) {
	for _, poolNet := range predefinedPrivateNetworks {
		_, b, err := net.ParseCIDR(poolNet.ipSubNet)
		if err != nil {
			return nil, aoserrors.Errorf("invalid base pool %q: %v", poolNet.ipSubNet, err)
		}

		ones, _ := b.Mask.Size()

		if poolNet.size <= 0 || poolNet.size < ones {
			return nil, aoserrors.Errorf("invalid pools size: %d", poolNet.size)
		}

		listIPNetPool = append(listIPNetPool, makeNetPool(poolNet.size, b)...)
	}

	return listIPNetPool, nil
}

func makeNetPool(size int, base *net.IPNet) (listIPNet []*net.IPNet) {
	one, bits := base.Mask.Size()
	mask := net.CIDRMask(size, bits)
	n := 1 << uint(size-one)
	s := uint(bits - size)
	listIPNet = make([]*net.IPNet, n)

	for i := 0; i < n; i++ {
		ip := make([]byte, len(base.IP))

		copy(ip, base.IP)
		addIntToIP(ip, uint(i<<s))

		listIPNet[i] = &net.IPNet{IP: ip, Mask: mask}
	}

	return listIPNet
}

func addIntToIP(array net.IP, ordinal uint) {
	for i := len(array) - 1; i >= 0; i-- {
		array[i] |= (byte)(ordinal & 0xff)
		ordinal >>= 8
	}
}
