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
	"context"
	"os"
	"strings"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/api/cloudprotocol"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// CryptoContext interface to access crypto functions.
type CryptoContext interface {
	ImportSessionKey(keyInfo CryptoSessionKeyInfo) (SymmetricContextInterface, error)
	CreateSignContext() (SignContextInterface, error)
}

// CryptoContext contains necessary parameters for decryption.
type DecryptParams struct {
	Chains         []cloudprotocol.CertificateChain
	Certs          []cloudprotocol.Certificate
	DecryptionInfo *cloudprotocol.DecryptionInfo
	Signs          *cloudprotocol.Signs
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// DecryptAndValidate decrypts and validates encrypted image.
func DecryptAndValidate(cryptoContext CryptoContext, encryptedFile, decryptedFile string, params DecryptParams) error {
	if err := decrypt(cryptoContext, encryptedFile, decryptedFile, &params); err != nil {
		return err
	}

	if err := validateSigns(cryptoContext, decryptedFile, &params); err != nil {
		return err
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func decrypt(cryptoContext CryptoContext, encryptedFile, decryptedFile string, params *DecryptParams) (err error) {
	symmetricCtx, err := cryptoContext.ImportSessionKey(CryptoSessionKeyInfo{
		SymmetricAlgName:  params.DecryptionInfo.BlockAlg,
		SessionKey:        params.DecryptionInfo.BlockKey,
		SessionIV:         params.DecryptionInfo.BlockIv,
		AsymmetricAlgName: params.DecryptionInfo.AsymAlg,
		ReceiverInfo: ReceiverInfo{
			Issuer: params.DecryptionInfo.ReceiverInfo.Issuer,
			Serial: params.DecryptionInfo.ReceiverInfo.Serial,
		},
	})
	if err != nil {
		return aoserrors.Wrap(err)
	}

	srcFile, err := os.Open(encryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(decryptedFile, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer dstFile.Close()

	if err = symmetricCtx.DecryptFile(context.Background(), srcFile, dstFile); err != nil {
		return aoserrors.Wrap(err)
	}

	return aoserrors.Wrap(err)
}

func validateSigns(cryptoContext CryptoContext, decryptedFile string, params *DecryptParams) (err error) {
	signCtx, err := cryptoContext.CreateSignContext()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	for _, cert := range params.Certs {
		if err = signCtx.AddCertificate(cert.Fingerprint, cert.Certificate); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	for _, chain := range params.Chains {
		if err = signCtx.AddCertificateChain(chain.Name, chain.Fingerprints); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	file, err := os.Open(decryptedFile)
	if err != nil {
		return aoserrors.Wrap(err)
	}
	defer file.Close()

	if err = signCtx.VerifySign(context.Background(), file, params.Signs); err != nil {
		return aoserrors.Wrap(err)
	}

	return nil
}

func removePkcs7Padding(in []byte, blocklen int) ([]byte, error) {
	l := len(in)

	if l%blocklen != 0 {
		return nil, aoserrors.New("padding error")
	}

	pl := int(in[l-1])

	if pl < 1 || pl > blocklen {
		return nil, aoserrors.New("padding error")
	}

	for i := l - pl; i < l; i++ {
		if in[i] != byte(pl) {
			return nil, aoserrors.New("padding error")
		}
	}

	return in[0 : l-pl], nil
}

func decodeAlgNames(algString string) (algName, modeName, paddingName string) {
	// alg string example: AES128/CBC/PKCS7PADDING
	algNamesSlice := strings.Split(algString, "/")
	if len(algNamesSlice) >= 1 {
		algName = algNamesSlice[0]
	} else {
		algName = ""
	}

	if len(algNamesSlice) >= 2 {
		modeName = algNamesSlice[1]
	} else {
		modeName = "CBC"
	}

	if len(algNamesSlice) >= 3 {
		paddingName = algNamesSlice[2]
	} else {
		paddingName = "PKCS7PADDING"
	}

	return algName, modeName, paddingName
}

func decodeSignAlgNames(algString string) (algName, hashName, paddingName string) {
	// alg string example: RSA/SHA256/PKCS1v1_5 or RSA/SHA256
	algNamesSlice := strings.Split(algString, "/")

	if len(algNamesSlice) >= 1 {
		algName = algNamesSlice[0]
	} else {
		algName = ""
	}

	if len(algNamesSlice) >= 2 {
		hashName = algNamesSlice[1]
	} else {
		hashName = "SHA256"
	}

	if len(algNamesSlice) >= 3 {
		paddingName = algNamesSlice[2]
	} else {
		paddingName = "PKCS1v1_5"
	}

	return algName, hashName, paddingName
}

func getSymmetricAlgInfo(algName string) (keySize int, ivSize int, err error) {
	switch strings.ToUpper(algName) {
	case "AES128":
		return 16, 16, nil

	case "AES192":
		return 24, 16, nil

	case "AES256":
		return 32, 16, nil

	default:
		return 0, 0, aoserrors.New("unsupported symmetric algorithm")
	}
}
