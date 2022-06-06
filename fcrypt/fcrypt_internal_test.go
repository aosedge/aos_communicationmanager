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
	"context"
	"crypto/ecdsa"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"net/url"
	"os"
	"os/exec"
	"path"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"

	"github.com/aoscloud/aos_common/utils/testtools"
	"github.com/aoscloud/aos_communicationmanager/config"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const tmpDir = `/tmp/aos`

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	pkcs11LibPath = "/usr/lib/softhsm/libsofthsm2.so"
	pkcs11DBPath  = "/var/lib/softhsm/tokens/"
	pkcs11Pin     = "1234"
	pkcs11Token   = "aos"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type structSymmetricCipherContextSet struct {
	algName string
	key     []byte
	iv      []byte
	ok      bool
}

type pkcs7PaddingCase struct {
	unpadded, padded []byte
	unpaddedLen      int
	ok               bool
	skipAddPadding   bool
	skipRemPadding   bool
}

type testUpgradeCertificateChain struct {
	Name         string   `json:"name"`
	Fingerprints []string `json:"fingerprints"`
}

type testUpgradeCertificate struct {
	Fingerprint string `json:"fingerprint"`
	Certificate []byte `json:"certificate"`
}

type testUpgradeSigns struct {
	ChainName        string   `json:"chainName"`
	Alg              string   `json:"alg"`
	Value            []byte   `json:"value"`
	TrustedTimestamp string   `json:"trustedTimestamp"`
	OcspValues       []string `json:"ocspValues"`
}

type testUpgradeFileInfo struct {
	FileData []byte
	Signs    *testUpgradeSigns
}

// UpgradeMetadata upgrade metadata.
type testUpgradeMetadata struct {
	Data              []testUpgradeFileInfo         `json:"data"`
	CertificateChains []testUpgradeCertificateChain `json:"certificateChains,omitempty"`
	Certificates      []testUpgradeCertificate      `json:"certificates,omitempty"`
}

type certData struct {
	cert []byte
	key  []byte
}

type testCertificateProvider struct {
	certURL string
	keyURL  string
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var (
	// Symmetric encryption done with
	// openssl aes-128-cbc -a -e -p -nosalt -in plaintext.sh -out encrypted.txt
	// echo '6B86B273FF34FCE19D6B804EFF5A3F57' | perl -e 'print pack "H*", <STDIN>' > aes.key.
	ClearAesKey = "6B86B273FF34FCE19D6B804EFF5A3F57"
	UsedIV      = "47ADA4EAA22F1D49C01E52DDB7875B4B"

	EncryptedKeyPkcs string
	EncryptedKeyOaep string
)

var key128bit = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
}

var key192bit = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
}

var key256bit = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
}

var iv128bit = []byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
}

var structSymmetricCipherContextSetTests = []structSymmetricCipherContextSet{
	{"", nil, nil, false},
	{"AES1", nil, nil, false},
	{"AES128", []byte{0}, nil, false},
	{"AES128", key128bit, []byte{0}, false},
	{"AES128", key128bit, key256bit, false},
	{"AES128", key256bit, iv128bit, false},
	{"AES128", key128bit, iv128bit, true},
	{"AES192", key128bit, iv128bit, false},
	{"AES192", key192bit, iv128bit, true},
	{"AES256", key128bit, iv128bit, false},
	{"AES256", key256bit, iv128bit, true},
	{"AES/CBC/PKCS7Padding", key128bit, iv128bit, false},
	{"AES128/CBC/PKCS7Padding", key128bit, iv128bit, true},
	{"AES128/ECB/PKCS7Padding", key128bit, iv128bit, false},
}

var testCerts map[string]certData

var pkcs7PaddingTests = []pkcs7PaddingCase{
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
		0, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15},
		1, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 14},
		2, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13, 13},
		3, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12},
		4, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11},
		5, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10},
		6, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 9, 9, 9, 9, 9, 9, 9, 9, 9},
		7, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8, 8, 8, 8, 8},
		8, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 7, 7, 7, 7, 7, 7},
		9, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 6, 6, 6, 6, 6},
		10, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 5, 5, 5, 5},
		11, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4},
		12, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3},
		13, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 2},
		14, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		15, true, false, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
		15, false, false, true,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{11, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16},
		0, false, true, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
		1, false, true, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2},
		1, false, true, false,
	},
	{
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		1, false, true, false,
	},
}

/***********************************************************************************************************************
 * Init
 **********************************************************************************************************************/

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableTimestamp: false,
		TimestampFormat:  "2006-01-02 15:04:05.000",
		FullTimestamp:    true,
	})
	log.SetLevel(log.DebugLevel)
	log.SetOutput(os.Stdout)
}

/***********************************************************************************************************************
 * Main
 **********************************************************************************************************************/

func TestMain(m *testing.M) {
	var err error

	if err = os.MkdirAll(tmpDir, 0o755); err != nil {
		log.Fatalf("Error creating tmp dir: %s", err)
	}

	testCerts = make(map[string]certData)

	if err := prepareTestCert(); err != nil {
		log.Fatalf("Can't generate test certificates: %v", err)
	}

	if err = setupFileStorage(); err != nil {
		log.Fatalf("Can't setup file storage: %s", err)
	}

	if err = setupPkcs11Storage(); err != nil {
		log.Fatalf("Can't setup PKCS11 storage: %s", err)
	}

	ret := m.Run()

	if err = clearFileStorage(); err != nil {
		log.Fatalf("Can't clear file storage: %s", err)
	}

	if err = clearPkcs11Storage(); err != nil {
		log.Fatalf("Can't clear PKCS11 storage: %s", err)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestSymmetricCipherContext_Set(t *testing.T) {
	for _, testItem := range structSymmetricCipherContextSetTests {
		symmetricContext := CreateSymmetricCipherContext()
		err := symmetricContext.set(testItem.algName, testItem.key, testItem.iv)

		if (err == nil) != testItem.ok {
			t.Errorf("Got unexpected error '%v' value on test %#v", err, testItem)
		}
	}
}

func TestSymmetricCipherContext_EncryptFile(t *testing.T) {
	testSizes := []int{0, 15, fileBlockSize, fileBlockSize + 100}

	for _, testItem := range testSizes {
		symmetricContext := CreateSymmetricCipherContext()
		if err := symmetricContext.generateKeyAndIV("AES128/CBC"); err != nil {
			t.Fatalf("Error creating context: '%v'", err)
		}

		clearFile, err := ioutil.TempFile("", "aos_test_fcrypt.bin.")
		if err != nil {
			t.Fatalf("Error creating file: '%v'", err)
		}

		zeroMemory := make([]byte, testItem)
		if _, err = clearFile.Write(zeroMemory); err != nil {
			t.Errorf("Error writing file")
		}

		encFile, err := ioutil.TempFile("", "aos_test_fcrypt.enc.")
		if err != nil {
			t.Fatalf("Error creating file: '%v'", err)
		}

		decFile, err := ioutil.TempFile("", "aos_test_fcrypt.dec.")
		if err != nil {
			t.Fatalf("Error creating file: '%v'", err)
		}

		if err = symmetricContext.encryptFile(context.Background(), clearFile, encFile); err != nil {
			t.Errorf("Error encrypting file: %v", err)
		}

		fi, err := encFile.Stat()
		if err != nil {
			t.Errorf("Error stat file (%v): %v", encFile.Name(), err)
		}

		if fi.Size() != int64((1+testItem/16)*16) {
			t.Errorf("Invalid file (%v) size: %v vs %v", encFile.Name(), fi.Size(), int64((1+testItem/16)*16))
		}

		if err = symmetricContext.DecryptFile(context.Background(), encFile, decFile); err != nil {
			t.Errorf("Error encrypting file: %v", err)
		}

		fi, err = decFile.Stat()
		if err != nil {
			t.Errorf("Error stat file (%v): %v", decFile.Name(), err)
		}

		if fi.Size() != int64(testItem) {
			t.Errorf("Invalid file (%v) size: %v vs %v", decFile.Name(), fi.Size(), testItem)
		}

		test := make([]byte, 64*1024)

		for {
			readSiz, err := decFile.Read(test)
			if err != nil {
				if err != io.EOF {
					t.Errorf("Error reading file: %v", err)
				} else {
					break
				}
			}

			for i := 0; i < readSiz; i++ {
				if test[i] != 0 {
					t.Errorf("Error decrypted file: non zero byte")
				}
			}
		}

		clearFile.Close()
		encFile.Close()
		decFile.Close()
		os.Remove(clearFile.Name())
		os.Remove(encFile.Name())
		os.Remove(decFile.Name())
	}
}

func TestSymmetricCipherContext_appendPadding(t *testing.T) {
	symmetricContext := CreateSymmetricCipherContext()
	if err := symmetricContext.generateKeyAndIV("AES128/CBC"); err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	for _, item := range pkcs7PaddingTests {
		if item.skipAddPadding {
			continue
		}

		testItem := &pkcs7PaddingCase{
			unpadded:    make([]byte, len(item.unpadded)),
			padded:      make([]byte, len(item.padded)),
			unpaddedLen: item.unpaddedLen,
			ok:          item.ok,
		}

		copy(testItem.unpadded, item.unpadded)
		copy(testItem.padded, item.padded)

		resultSize, err := symmetricContext.appendPadding(testItem.unpadded, testItem.unpaddedLen)
		if err != nil {
			if testItem.ok {
				t.Errorf("Got unexpected result: error='%v' siz='%v', value on test %#v", err, resultSize, testItem)
			}
		} else {
			if !testItem.ok || resultSize != len(testItem.padded) || !bytes.Equal(testItem.padded, testItem.unpadded) {
				t.Errorf("Got unexpected result: error='%v' siz='%v', value on test %#v", err, resultSize, testItem)
			}
		}
	}
}

func TestSymmetricCipherContext_getPaddingSize(t *testing.T) {
	symmetricContext := CreateSymmetricCipherContext()
	if err := symmetricContext.generateKeyAndIV("AES128/CBC"); err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	for _, item := range pkcs7PaddingTests {
		if item.skipRemPadding {
			continue
		}

		testItem := &pkcs7PaddingCase{
			unpadded:    make([]byte, len(item.unpadded)),
			padded:      make([]byte, len(item.padded)),
			unpaddedLen: item.unpaddedLen,
			ok:          item.ok,
		}

		copy(testItem.unpadded, item.unpadded)
		copy(testItem.padded, item.padded)

		resultSize, err := symmetricContext.getPaddingSize(testItem.padded, len(testItem.padded))
		if err != nil {
			if testItem.ok {
				t.Errorf("Got unexpected result: error='%v' siz='%v', value on test %#v", err, resultSize, testItem)
			}
		} else {
			if !testItem.ok || (len(testItem.padded)-resultSize) != testItem.unpaddedLen {
				t.Errorf("Got unexpected result: error='%v' siz='%v', value on test %#v",
					err, resultSize, len(testItem.padded)-resultSize-testItem.unpaddedLen)
			}
		}
	}
}

func TestInvalidParams(t *testing.T) {
	encryptedKey, err := base64.StdEncoding.DecodeString(EncryptedKeyPkcs)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}

	// Create or use context
	certProvider := testCertificateProvider{keyURL: keyNameToFileURL("offline1")}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	cryptoContext, err := New(&certProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	var keyInfo CryptoSessionKeyInfo

	if _, err = cryptoContext.ImportSessionKey(keyInfo); err == nil {
		t.Fatalf("Import session key not failed")
	}

	keyInfo.SessionKey = encryptedKey
	keyInfo.SessionIV = []byte{1, 2}
	keyInfo.SymmetricAlgName = "AES128/CBC/PKCS7PADDING" // nolint:goconst
	keyInfo.AsymmetricAlgName = "RSA/PKCS1v1_5"          // nolint:goconst

	if _, err = cryptoContext.ImportSessionKey(keyInfo); err == nil {
		t.Fatalf("Import session key not failed")
	}
}

// For testing only.
func TestDecryptSessionKeyPkcs1v15(t *testing.T) {
	iv, err := hex.DecodeString(UsedIV)
	if err != nil {
		t.Fatalf("Error decode IV: '%v'", err)
	}

	clearAesKey, err := hex.DecodeString(ClearAesKey)
	if err != nil {
		t.Fatalf("Error decode ClearKey: '%v'", err)
	}

	encryptedKey, err := base64.StdEncoding.DecodeString(EncryptedKeyPkcs)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}

	// End of: For testing only

	certName := "offline1"

	testCertProviders := []*testCertificateProvider{
		{keyURL: keyNameToFileURL(certName)},
		{keyURL: nameToPkcs11URL(certName)},
	}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	for _, certProvider := range testCertProviders {
		// Create and use context
		cryptoContext, err := New(certProvider, cryptoCtx)
		if err != nil {
			t.Fatalf("Error creating context: '%v'", err)
		}

		var keyInfo CryptoSessionKeyInfo
		keyInfo.SessionKey = encryptedKey
		keyInfo.SessionIV = iv
		keyInfo.SymmetricAlgName = "AES128/CBC/PKCS7PADDING"
		keyInfo.AsymmetricAlgName = "RSA/PKCS1v1_5"

		sessionKey, err := cryptoContext.ImportSessionKey(keyInfo)
		if err != nil {
			t.Fatalf("Error decode key: '%v'", err)
		}

		chipperContex, ok := sessionKey.(*SymmetricCipherContext)
		if !ok {
			t.Fatalf("Can't cast to SymmetricCipherContext")
		}

		if len(chipperContex.key) != len(clearAesKey) {
			t.Fatalf("Error decrypt key: invalid key len")
		}

		if !bytes.Equal(chipperContex.key, clearAesKey) {
			t.Fatalf("Error decrypt key: invalid key")
		}
	}
}

func TestDecryptSessionKeyOAEP(t *testing.T) {
	// For testing only
	iv, err := hex.DecodeString(UsedIV)
	if err != nil {
		t.Fatalf("Error decode IV: '%v'", err)
	}

	clearAesKey, err := hex.DecodeString(ClearAesKey)
	if err != nil {
		t.Fatalf("Error decode ClearKey: '%v'", err)
	}

	encryptedKey, err := base64.StdEncoding.DecodeString(EncryptedKeyOaep)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}
	// End of: For testing only

	// Create or use context
	certProvider := testCertificateProvider{keyURL: keyNameToFileURL("offline1")}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	cryptoContext, err := New(&certProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	var keyInfo CryptoSessionKeyInfo
	keyInfo.SessionKey = encryptedKey
	keyInfo.SessionIV = iv
	keyInfo.SymmetricAlgName = "AES128/CBC/PKCS7PADDING"
	keyInfo.AsymmetricAlgName = "RSA/OAEP"

	ctxSym, err := cryptoContext.ImportSessionKey(keyInfo)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}

	chipperContex, ok := ctxSym.(*SymmetricCipherContext)
	if !ok {
		t.Errorf("Can't cast to SymmetricCipherContext")
	}

	if len(chipperContex.key) != len(clearAesKey) {
		t.Fatalf("Error decrypt key: invalid key len")
	}

	if !bytes.Equal(chipperContex.key, clearAesKey) {
		t.Fatalf("Error decrypt key: invalid key")
	}
}

func TestInvalidSessionKeyPkcs1v15(t *testing.T) {
	// For testing only
	iv, err := hex.DecodeString(UsedIV)
	if err != nil {
		t.Fatalf("Error decode IV: '%v'", err)
	}

	clearAesKey, err := hex.DecodeString(ClearAesKey)
	if err != nil {
		t.Fatalf("Error decode ClearKey: '%v'", err)
	}

	encryptedKey, err := base64.StdEncoding.DecodeString(EncryptedKeyPkcs)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}
	// End of: For testing only

	// Create or use context
	certProvider := testCertificateProvider{keyURL: keyNameToFileURL("offline2")}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	cryptoContext, err := New(&certProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	var keyInfo CryptoSessionKeyInfo
	keyInfo.SessionKey = encryptedKey
	keyInfo.SessionIV = iv
	keyInfo.SymmetricAlgName = "AES128/CBC/PKCS7PADDING"
	keyInfo.AsymmetricAlgName = "RSA/PKCS1v1_5"

	ctxSym, err := cryptoContext.ImportSessionKey(keyInfo)
	if err != nil {
		t.Fatalf("Error importing key: '%v'", err)
	}

	chipperContex, ok := ctxSym.(*SymmetricCipherContext)
	if !ok {
		t.Errorf("Can't cast to SymmetricCipherContext")
	}

	if len(chipperContex.key) != len(clearAesKey) {
		t.Fatalf("Error decrypt key: invalid key len")
	}

	// Key should be different
	if bytes.Equal(chipperContex.key, clearAesKey) {
		t.Fatalf("Error decrypt key: invalid key")
	}
}

func TestInvalidSessionKeyOAEP(t *testing.T) {
	// For testing only
	iv, err := hex.DecodeString(UsedIV)
	if err != nil {
		t.Fatalf("Error decode IV: '%v'", err)
	}

	encryptedKey, err := base64.StdEncoding.DecodeString(EncryptedKeyOaep)
	if err != nil {
		t.Fatalf("Error decode key: '%v'", err)
	}
	// End of: For testing only

	// Create or use context
	certProvider := testCertificateProvider{keyURL: keyNameToFileURL("offline2")}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	cryptoContext, err := New(&certProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	var keyInfo CryptoSessionKeyInfo
	keyInfo.SessionKey = encryptedKey
	keyInfo.SessionIV = iv
	keyInfo.SymmetricAlgName = "AES128/CBC/PKCS7PADDING"
	keyInfo.AsymmetricAlgName = "RSA/OAEP"

	if _, err = cryptoContext.ImportSessionKey(keyInfo); err == nil {
		t.Fatalf("Error decode key: decrypt should raise error")
	}
}

func TestVerifySignOfComponent(t *testing.T) {
	// Create or use context
	certURL, err := url.Parse(certNameToFileURL("root"))
	if err != nil {
		t.Fatalf("Can't parse cert URL: '%v'", err)
	}

	cryptoCtx, err := createCryptoContext(config.Crypt{CACert: certURL.Path})
	if err != nil {
		t.Fatal(err)
	}

	secondaryCertURL := certNameToFileURL("secondary")

	secondaryCert, err := cryptoCtx.LoadCertificateByURL(secondaryCertURL)
	if err != nil {
		t.Fatalf("Can't get secondary CA: '%v'", err)
	}

	secondaryKeyPath := keyNameToFileURL("secondary")

	secondaryKey, _, err := cryptoCtx.LoadPrivateKeyByURL(secondaryKeyPath)
	if err != nil {
		t.Fatalf("Can't get secondary private key: '%v'", err)
	}

	rsaCecondaryKey, ok := secondaryKey.(*rsa.PrivateKey)
	if !ok {
		t.Fatalf("Can't get secondary RSA private key: '%v'", err)
	}

	certInter, _, keyInter, err := GenCA(secondaryCert[0], rsaCecondaryKey, pkix.Name{
		Country:            []string{"UA"},
		Organization:       []string{"EPAM"},
		OrganizationalUnit: []string{"AOS"},
		CommonName:         "AOS OEM Intermediate CA",
		Locality:           []string{"Kyiv"},
	})
	if err != nil {
		t.Fatalf("Can't generate intermediate CA: '%v'", err)
	}

	cert, _, key, err := GenCertificate(pkix.Name{
		Organization:       []string{"EPAM"},
		OrganizationalUnit: []string{"AoS"},
		CommonName:         "AoS Target Updates Signer",
	}, nil, certInter, keyInter)
	if err != nil {
		t.Fatalf("Can't generate OEM certificate: '%v'", err)
	}

	privKey := path.Join(tmpDir, "privKey.pem")
	if err = ioutil.WriteFile(privKey, testtools.RSAPrivateKeyToPEM(key), 0o600); err != nil {
		t.Fatalf("Can't save certificate: '%v'", err)
	}

	helloWorld := path.Join(tmpDir, "helloWorld.txt")
	if err = ioutil.WriteFile(helloWorld, []byte("Hello"), 0o600); err != nil {
		t.Fatalf("Can't save certificate: '%v'", err)
	}

	signFile := path.Join(tmpDir, "data.txt.signature")

	var out []byte
	// openssl dgst -sha256 -sign key.pem -out data.txt.signature hello.txt
	if out, err = exec.Command("openssl", "dgst", "-sha256", "-sign", privKey,
		"-out", signFile, helloWorld).CombinedOutput(); err != nil {
		t.Fatalf("message: %s, %s", string(out), err)
	}

	signValue, err := ioutil.ReadFile(signFile)
	if err != nil {
		t.Fatal(err)
	}

	fingerprints1 := md5.Sum(cert.Raw) // nolint:gosec
	fingerprints1Hex := strings.ToUpper(hex.EncodeToString(fingerprints1[:]))

	fingerprints2 := md5.Sum(certInter.Raw) // nolint:gosec
	fingerprints2Hex := strings.ToUpper(hex.EncodeToString(fingerprints2[:]))

	fingerprints3 := md5.Sum(secondaryCert[0].Raw) // nolint:gosec
	fingerprints3Hex := strings.ToUpper(hex.EncodeToString(fingerprints3[:]))

	upgradeMetadata := testUpgradeMetadata{
		Data: []testUpgradeFileInfo{
			{
				FileData: []byte("test"),
				Signs: &testUpgradeSigns{
					ChainName:        "8D28D60220B8D08826E283B531A0B1D75359C5EE",
					Alg:              "RSA/SHA256",
					Value:            signValue,
					TrustedTimestamp: "",
				},
			},
		},
		CertificateChains: []testUpgradeCertificateChain{
			{
				Name: "8D28D60220B8D08826E283B531A0B1D75359C5EE",
				Fingerprints: []string{
					fingerprints1Hex,
					fingerprints2Hex,
					fingerprints3Hex,
				},
			},
		},
		Certificates: []testUpgradeCertificate{
			{
				Fingerprint: fingerprints1Hex,
				Certificate: cert.Raw,
			},
			{
				Fingerprint: fingerprints2Hex,
				Certificate: certInter.Raw,
			},
			{
				Fingerprint: fingerprints3Hex,
				Certificate: secondaryCert[0].Raw,
			},
		},
	}

	certProvider := testCertificateProvider{}

	cryptoContext, err := New(&certProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Error creating context: '%v'", err)
	}

	signCtx, err := cryptoContext.CreateSignContext()
	if err != nil {
		t.Fatalf("Error creating sign context: '%v'", err)
	}

	if len(upgradeMetadata.CertificateChains) == 0 {
		t.Fatal("Empty certificate chain")
	}

	for _, cert := range upgradeMetadata.Certificates {
		err = signCtx.AddCertificate(cert.Fingerprint, cert.Certificate)
		if err != nil {
			t.Fatalf("Error parse and add sign certificate: '%v'", err)
		}
	}

	for _, certChain := range upgradeMetadata.CertificateChains {
		err = signCtx.AddCertificateChain(certChain.Name, certChain.Fingerprints)
		if err != nil {
			t.Fatalf("Error add sign certificate chain: '%v'", err)
		}
	}

	for _, data := range upgradeMetadata.Data {
		tmpFile, err := ioutil.TempFile(os.TempDir(), "aos_update-")
		if err != nil {
			t.Fatal("Cannot create temporary file", err)
		}
		defer tmpFile.Close()
		defer os.Remove(tmpFile.Name())

		if _, err = tmpFile.Write(data.FileData); err != nil {
			t.Errorf("Can't write tmp file: %s", err)
		}

		if _, err = tmpFile.Seek(0, 0); err != nil {
			t.Errorf("Can't seek tmp file: %s", err)
		}

		f, _ := os.Open(helloWorld)
		defer f.Close()

		err = signCtx.VerifySign(context.Background(), f, data.Signs.ChainName, data.Signs.Alg, data.Signs.Value)
		if err != nil {
			t.Fatal("Verify fail", err)
		}
	}
}

func TestGetCertificateOrganization(t *testing.T) {
	certName := "online"

	testCertProviders := []*testCertificateProvider{
		{certURL: certNameToFileURL(certName)},
		{certURL: nameToPkcs11URL(certName)},
	}

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatal(err)
	}

	for _, certProvider := range testCertProviders {
		cryptoContext, err := New(certProvider, cryptoCtx)
		if err != nil {
			t.Fatalf("Can't create crypto context: %s", err)
		}

		names, err := cryptoContext.GetOrganization()
		if err != nil {
			t.Fatalf("Can't get organization: %s", err)
		}

		if len(names) != 1 {
			t.Fatalf("Wrong organization names count: %d", len(names))
		}

		if names[0] != "staging-fusion.westeurope.cloudapp.azure.com" {
			t.Fatalf("Wrong organization name: %s", names[0])
		}
	}
}

func TestGetServiceDiscovery(t *testing.T) {
	certName := "online"

	cryptoCtx, err := createCryptoContext(config.Crypt{})
	if err != nil {
		t.Fatalf("Can't create crypto context: %v", err)
	}

	testCertProvider := testCertificateProvider{certURL: certNameToFileURL(certName)}

	cryptoContext, err := New(&testCertProvider, cryptoCtx)
	if err != nil {
		t.Fatalf("Can't create crypto context: %s", err)
	}

	urls, err := cryptoContext.GetServiceDiscoveryFromExtensions()
	if err != nil {
		t.Fatalf("Can't get service discovery URLs: %v", err)
	}

	if len(urls) != 1 {
		t.Fatalf("Unexpected urls count: %v", len(urls))
	}

	if urls[0] != "https://www.mytest.com" {
		t.Fatalf("Unexpected url: %v", urls[0])
	}
}

/*******************************************************************************
 * Private
 **********************************************************************************************************************/

func (provider *testCertificateProvider) GetCertificate(
	certType string, issuer []byte, serial string,
) (certURL, keyURL string, err error) {
	return provider.certURL, provider.keyURL, nil
}

func certNameToFileURL(name string) (file string) {
	return cryptutils.SchemeFile + "://" + path.Join(tmpDir, "cert_"+name+".pem")
}

func keyNameToFileURL(name string) (file string) {
	return cryptutils.SchemeFile + "://" + path.Join(tmpDir, "key_"+name+".pem")
}

func nameToPkcs11URL(name string) (file string) {
	return cryptutils.SchemePKCS11 + ":" + fmt.Sprintf("token=%s;id=%s?module-path=%s&pin-value=%s",
		pkcs11Token, name, pkcs11LibPath, pkcs11Pin)
}

func setupFileStorage() (err error) {
	for name, certData := range testCerts {
		if certData.cert != nil {
			certURL, err := url.Parse(certNameToFileURL(name))
			if err != nil {
				return aoserrors.Wrap(err)
			}

			if err := ioutil.WriteFile(certURL.Path, certData.cert, 0o600); err != nil {
				return aoserrors.Wrap(err)
			}
		}

		if certData.key != nil {
			keyURL, err := url.Parse(keyNameToFileURL(name))
			if err != nil {
				return aoserrors.Wrap(err)
			}

			if err := ioutil.WriteFile(keyURL.Path, certData.key, 0o600); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

func clearFileStorage() (err error) {
	if err = os.RemoveAll(tmpDir); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func execPkcs11Tool(args ...string) (err error) {
	if output, err := exec.Command("pkcs11-tool", append([]string{
		"--module", pkcs11LibPath,
	}, args...)...).CombinedOutput(); err != nil {
		return aoserrors.Errorf("%s (%s)", err, (string(output)))
	}

	return nil
}

func pkcs11ImportCert(name string, data []byte) (err error) {
	certs, err := cryptutils.PEMToX509Cert(data)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	fileName := path.Join(tmpDir, "data.tmp")

	id := name

	for i, cert := range certs {
		if err := ioutil.WriteFile(fileName, cert.Raw, 0o600); err != nil {
			return aoserrors.Wrap(err)
		}

		if err = execPkcs11Tool("--token-label", pkcs11Token, "--login", "--pin", pkcs11Pin,
			"--write-object", fileName, "--type", "cert", "--id", hex.EncodeToString([]byte(id))); err != nil {
			return aoserrors.Wrap(err)
		}

		id = fmt.Sprintf("%s_%d", name, i)
	}

	return nil
}

func pkcs11ImportKey(name string, data []byte) (err error) {
	key, err := cryptutils.PEMToX509Key(data)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	switch privateKey := key.(type) {
	case *rsa.PrivateKey:
		data = x509.MarshalPKCS1PrivateKey(privateKey)

	case *ecdsa.PrivateKey:
		if data, err = x509.MarshalECPrivateKey(privateKey); err != nil {
			return aoserrors.Wrap(err)
		}

	default:
		return aoserrors.Errorf("unsupported key type: %v", reflect.TypeOf(privateKey))
	}

	fileName := path.Join(tmpDir, "data.tmp")

	if err := ioutil.WriteFile(fileName, data, 0o600); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = execPkcs11Tool("--token-label", pkcs11Token, "--login", "--pin", pkcs11Pin,
		"--write-object", fileName, "--type", "privkey", "--id", hex.EncodeToString([]byte(name))); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func initPkcs11Slot() (err error) {
	if err = execPkcs11Tool("--init-token", "--label", pkcs11Token, "--so-pin", "0000"); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = execPkcs11Tool(
		"--token-label", pkcs11Token, "--so-pin", "0000", "--init-pin", "--pin", pkcs11Pin); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func setupPkcs11Storage() (err error) {
	if err = clearPkcs11Storage(); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = initPkcs11Slot(); err != nil {
		return aoserrors.Wrap(err)
	}

	for name, certData := range testCerts {
		if certData.cert != nil {
			if err = pkcs11ImportCert(name, certData.cert); err != nil {
				return aoserrors.Wrap(err)
			}
		}

		if certData.key != nil {
			if err = pkcs11ImportKey(name, certData.key); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}

	return nil
}

func clearPkcs11Storage() (err error) {
	if err = os.RemoveAll(pkcs11DBPath); err != nil {
		return aoserrors.Wrap(err)
	}

	if err = os.MkdirAll(pkcs11DBPath, 0o755); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func createCryptoContext(conf config.Crypt) (cryptoContext *cryptutils.CryptoContext, err error) {
	if cryptoContext, err = cryptutils.NewCryptoContext(conf.CACert); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return cryptoContext, nil
}

func GenCARoot() (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	rootTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		Version:               3,
		NotBefore:             time.Now().Add(-10 * time.Second),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		Subject: pkix.Name{
			Country:            []string{"UA"},
			Organization:       []string{"EPAM"},
			OrganizationalUnit: []string{"Novus Ordo Seclorum"},
			CommonName:         "Fusion Root CA",
			Locality:           []string{"Kyiv"},
		},
	}

	cert, certPem, key, err := testtools.GenerateCertifcateAndKeys(&rootTemplate, &rootTemplate, nil)
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	return cert, certPem, key, nil
}

func GenCA(
	parent *x509.Certificate, privateKey *rsa.PrivateKey, subject pkix.Name,
) (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	rootTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		Version:               3,
		NotBefore:             time.Now().Add(-10 * time.Second),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		BasicConstraintsValid: true,
		IsCA:                  true,
		KeyUsage: x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature |
			x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		Subject:     subject,
	}

	cert, certPem, key, err := testtools.GenerateCertifcateAndKeys(&rootTemplate, parent, privateKey)
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	return cert, certPem, key, nil
}

func GenCertificate(
	subject pkix.Name, extraExtension []pkix.Extension, parent *x509.Certificate, privateKey *rsa.PrivateKey,
) (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	rootTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		Version:               3,
		NotBefore:             time.Now().Add(-10 * time.Second),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		Subject:               subject,
		ExtraExtensions:       extraExtension,
	}

	cert, certPem, key, err := testtools.GenerateCertifcateAndKeys(&rootTemplate, parent, privateKey)
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	return cert, certPem, key, nil
}

func GenOEMCertificate(
	subject pkix.Name, parent *x509.Certificate, privateKey *rsa.PrivateKey,
) (*x509.Certificate, []byte, *rsa.PrivateKey, error) {
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	rootTemplate := x509.Certificate{
		SerialNumber:          serialNumber,
		Version:               3,
		NotBefore:             time.Now().Add(-10 * time.Second),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		BasicConstraintsValid: true,
		IsCA:                  false,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageCodeSigning},
		Subject:               subject,
	}

	cert, certPem, key, err := testtools.GenerateCertifcateAndKeys(&rootTemplate, parent, privateKey)
	if err != nil {
		return nil, nil, nil, aoserrors.Wrap(err)
	}

	return cert, certPem, key, nil
}

func prepareTestCert() error {
	cert, certPEM, key, err := GenCARoot()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	testCerts["root"] = certData{
		cert: certPEM,
	}

	certSecond, certSecondaryPEM, keySecond, err := GenCA(cert, key, pkix.Name{
		Country:            []string{"UA"},
		Organization:       []string{"EPAM"},
		OrganizationalUnit: []string{"AoS"},
		CommonName:         "AoS Secondary CA",
		Locality:           []string{"Kyiv"},
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	testCerts["secondary"] = certData{
		cert: certSecondaryPEM,
		key:  testtools.RSAPrivateKeyToPEM(keySecond),
	}

	certInter, certInterPem, keyInter, err := GenCA(certSecond, keySecond, pkix.Name{
		Country:            []string{"UA"},
		Organization:       []string{"EPAM"},
		OrganizationalUnit: []string{"Novus Ordo Seclorum"},
		CommonName:         "AOS vehicles Intermediate CA",
		Locality:           []string{"Kyiv"},
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	_, certOffline2Pem, keyOffline2, err := GenCertificate(pkix.Name{
		Organization:       []string{"EPAM Systems, Inc."},
		OrganizationalUnit: []string{"Test vehicle model"},
		CommonName:         "YV1SW58D202057528-offline",
	}, nil, certInter, keyInter)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	offline2CertChain := testtools.AppendCert(certOffline2Pem, certInterPem, certSecondaryPEM)

	keyOffline2PEM := testtools.RSAPrivateKeyToPEM(keyOffline2)

	testCerts["offline2"] = certData{
		cert: offline2CertChain,
		key:  keyOffline2PEM,
	}

	_, certOffline1Pem, keyOffline1, err := GenCertificate(pkix.Name{
		Organization:       []string{"EPAM Systems, Inc."},
		OrganizationalUnit: []string{"Test vehicle model"},
		CommonName:         "YV1SW58D900034248-offline",
	}, nil, certInter, keyInter)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	offline1CertChain := testtools.AppendCert(certOffline1Pem, certInterPem, certSecondaryPEM)

	keyOffline1PEM := testtools.RSAPrivateKeyToPEM(keyOffline1)

	testCerts["offline1"] = certData{
		cert: offline1CertChain,
		key:  keyOffline1PEM,
	}

	testObjectOid := asn1.ObjectIdentifier{2, 5, 29, 18}

	rawValues := []asn1.RawValue{
		{Class: 2, Tag: 6, Bytes: []byte("https://www.mytest.com")},
	}

	values, err := asn1.Marshal(rawValues)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	extraExtensions := []pkix.Extension{
		{
			Id:    testObjectOid,
			Value: values,
		},
	}

	online, certOnlinePem, _, err := GenCertificate(pkix.Name{
		Organization:       []string{"staging-fusion.westeurope.cloudapp.azure.com"},
		OrganizationalUnit: []string{"OEM Test unit model"},
		CommonName:         "c183de63-e2b7-4776-90e0-b7c9b8f740e8-online",
	}, extraExtensions, certInter, keyInter)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	onlineCertChain := testtools.AppendCert(certOnlinePem, certInterPem, certSecondaryPEM)

	testCerts["online"] = certData{
		cert: onlineCertChain,
	}

	if err := testtools.Verify(online, []*x509.Certificate{cert}, []*x509.Certificate{certSecond, certInter}); err != nil {
		return aoserrors.Wrap(err)
	}

	aesKey := path.Join(tmpDir, "aes.key")

	if err := writeToFileAesBinKey(aesKey); err != nil {
		return aoserrors.Wrap(err)
	}

	offlineCert := path.Join(tmpDir, "offline.pem")
	if err = ioutil.WriteFile(offlineCert, certOffline1Pem, 0o600); err != nil {
		return aoserrors.Wrap(err)
	}

	encAesKey := path.Join(tmpDir, "aes.key.enc")

	var out []byte

	// openssl rsautl -encrypt -certin -inkey ./certificate.pem -in aes.key -out aes.key.enc
	if out, err = exec.Command("openssl", "rsautl", "-encrypt", "-certin", "-inkey",
		offlineCert, "-in", aesKey, "-out", encAesKey).CombinedOutput(); err != nil {
		return aoserrors.Errorf("message: %s, %s", string(out), err)
	}

	encAesOaepKey := path.Join(tmpDir, "aes.key.oaep.enc")

	// openssl rsautl -encrypt -oaep -certin -inkey ./certificate.pem -in aes.key -out aes.key.oaep.enc
	if out, err = exec.Command("openssl", "rsautl", "-encrypt", "-oaep", "-certin", "-inkey",
		offlineCert, "-in", aesKey, "-out", encAesOaepKey).CombinedOutput(); err != nil {
		return aoserrors.Errorf("message: %s, %s", string(out), err)
	}

	aesKeyData, err := ioutil.ReadFile(encAesKey)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	aesKeyOaepData, err := ioutil.ReadFile(encAesOaepKey)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	EncryptedKeyPkcs = base64.StdEncoding.EncodeToString(aesKeyData)
	EncryptedKeyOaep = base64.StdEncoding.EncodeToString(aesKeyOaepData)

	return nil
}

func writeToFileAesBinKey(filename string) error {
	bytes, _ := hex.DecodeString(ClearAesKey)

	f, err := os.Create(filename)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer f.Close()

	if _, err := f.Write(bytes); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}
