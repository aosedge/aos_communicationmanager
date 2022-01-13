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

package fcrypt

import (
	"bytes"
	"testing"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type pkcs7Case struct {
	in, out   []byte
	blocksize int
	ok        bool
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var pkcs7Tests = []pkcs7Case{
	{[]byte{1, 2, 3, 4, 5, 6, 7, 1}, []byte{1, 2, 3, 4, 5, 6, 7}, 8, true},
	{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{}, 8, false},
	{[]byte{
		1, 2, 3, 4, 5, 6, 7, 8,
		8, 8, 8, 8, 8, 8, 8, 8,
	}, []byte{1, 2, 3, 4, 5, 6, 7, 8}, 8, true},
	{[]byte{1, 2, 3, 4, 4, 4, 4, 4}, []byte{1, 2, 3, 4}, 8, true},
	{[]byte{1, 2, 3, 4, 4, 4, 4}, []byte{}, 8, false},
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestRemovePkcs7Padding(t *testing.T) {
	for _, c := range pkcs7Tests {
		out, err := removePkcs7Padding(c.in, c.blocksize)
		if (err == nil) != c.ok {
			t.Errorf("Got unexpected error %v value on test %#v", err, c)
		}
		if !bytes.Equal(c.out, out) {
			t.Errorf("Got unexpected value %#v in test %#v", out, c)
		}
	}
}
