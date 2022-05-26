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
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"os/exec"
	"path"
	"reflect"
	"testing"

	"github.com/aoscloud/aos_common/aoserrors"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"

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

	// openssl rsautl -encrypt -certin -inkey ./offline_certificate.pem -in aes.key -out aes.key.enc
	// base64 ./aes.key.enc.
	EncryptedKeyPkcs = `Tu2LjQY142xBey2iPNTE1r0UmH/059IHFelnvbdkUbtkwh23H6T5UNXmEweWzuImv10Bo+cxDq0c
69+hkj81PYLdnVTefiNGvyVAlta1se4uTeA28hQuGu1Egsqjsu/zagfZImTDZvysWQ5+u5Ucku0i
hTtR3E31D+CtwQeBoS4o23a8VW2m8p+wHt3wsKP1ekESvKZcvVNDQjD/oVho+TR03Eeqn8R4U05v
idEIjmTK1HqJoy3sGQei1PRFHxN2QMHcdwmt70l33Z39qWA5K5UJAR+lkgL8aH3NpLeFrhJxp/X5
R6sYwcoRj4QBrGzAX2BAQ3l/eRpT8ptU/dGc4A==`

	// openssl rsautl -encrypt -oaep -certin -inkey ./offline_certificate.pem -in aes.key -out aes.key.oaep.enc
	// base64 ./aes.key.oaep.enc.
	EncryptedKeyOaep = `lRCdwX8okje6nlBdMZyythM6/CTWZvpRILw7f9GnoWUZH2B0SblHNYtryf8RPONef8UgZ0rSfpnw
Ezke/wGWZ7OdMtXr9vLz5t0AlxwUZFq9/WcstWg4UwB11fIDZkZN/pnDL2BB0/iweJfLHteUtz3A
YvghkaCW2FrFXMeEnaT9b+YRPi8RTFeg8HAJ8IzLx20dGMA6eAqFI9q0ksIV+6tZwKDAeEM6ywpH
Iq3bNnYQNNVLgB3mwpvhHtxxSHZYLapa59zMPh2zACA5aqZWoA9NfLCZejUPgd9PnyOhhYmKy55S
VYKOdXQNBJuSOUQKQyBX8hByK4gyukpQlRIgqw==`
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

var testCerts = map[string]certData{
	"online": {
		cert: []byte(`-----BEGIN CERTIFICATE-----
MIIEDTCCAvWgAwIBAgIIXmI8ZAAO3zcwDQYJKoZIhvcNAQELBQAwcDElMCMGA1UE
AwwcQU9TIHZlaGljbGVzIEludGVybWVkaWF0ZSBDQTENMAsGA1UECgwERVBBTTEc
MBoGA1UECwwTTm92dXMgT3JkbyBTZWNsb3J1bTENMAsGA1UEBwwES3lpdjELMAkG
A1UEBhMCVUEwHhcNMjAwMzA2MTIwNDUyWhcNMzAwMzA0MTIwNDUyWjCBnjE0MDIG
A1UEAwwrYzE4M2RlNjMtZTJiNy00Nzc2LTkwZTAtYjdjOWI4Zjc0MGU4LW9ubGlu
ZTE1MDMGA1UECgwsc3RhZ2luZy1mdXNpb24ud2VzdGV1cm9wZS5jbG91ZGFwcC5h
enVyZS5jb20xLzAtBgNVBAsMJk9FTSBrb3N0eWFudHluX2lnbmF0b3YgVGVzdCB1
bml0IG1vZGVsMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA22up8faZ
cb7KMjCqf/HWRLZHN6wsyhTQPL0+/HVlnAz1URtRp+EhtuUXlzIrO+XACHKW0qDG
aus77Vsmf65asuBojlbLRfl5bofqvT7hnY94NFPhHAl0r9iONT5HlsGJfy+2KJy8
9dU7cIJrwBxvO2T1UEQ8jBNPu0kJMLvz3o+kQv//8OhG1Z8ykxDyRyXMMjYl/WRO
nTIyZ8qTNrElitTUA7WrQVDoX1TXXYYkSR9mBG6uK5jsEzazSgcZYfzgmANwLMCX
mQAlorvlxu3iBUHwPYaPVoTutQ7yqeC4irbTrULvjR7KjmlaFxK162p1crZXauy+
cSiSb3wi5uMN7wIDAQABo3wwejAMBgNVHRMBAf8EAjAAMAsGA1UdDwQEAwIFoDAd
BgNVHSUEFjAUBggrBgEFBQcDAgYIKwYBBQUHAwEwHQYDVR0OBBYEFMJRHgqJpOpq
5wjM+dhPdwS/QIhOMB8GA1UdIwQYMBaAFMyIR9SjshF4Z5rxI9KyZhD6CjjCMA0G
CSqGSIb3DQEBCwUAA4IBAQAYBWnHljDu19TUDKeU6cRv2k5sowrVpn/HaneJ9PAu
pmHZtjDWOYMKCYGpWfscCyhkLaek3AgH22fzj0P2bWuGQE5ORdQ912u3HiVaF9bE
A7HobN2J0/CC3soKuM8pQDk9DfWDGy500v4dQiPFVG4h+WJp82VWSAdu7chFydKP
LKsOnzTfW1+DUyoYBztZp06sudkh8M/aIaGfU1f/79AYumAVBhOTwoEVQ4P8K7QE
0c1Vc5G/2X4Mweu8LBrf2kpdy5aqFYwTldcqE+el9KfvnssiDC3B1LCLW2JqV2RX
iWCeajoiW4IqzTHL0DvrqnFdoHYN2XqYYdpQldfPkTpn
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDwzCCAqugAwIBAgIJAO2BVuwqJLb8MA0GCSqGSIb3DQEBCwUAMFQxGTAXBgNV
BAMMEEFvUyBTZWNvbmRhcnkgQ0ExDTALBgNVBAoMBEVQQU0xDDAKBgNVBAsMA0Fv
UzENMAsGA1UEBwwES3lpdjELMAkGA1UEBhMCVUEwHhcNMTkwMzIxMTMyMjQwWhcN
MjUwMzE5MTMyMjQwWjBwMSUwIwYDVQQDDBxBT1MgdmVoaWNsZXMgSW50ZXJtZWRp
YXRlIENBMQ0wCwYDVQQKDARFUEFNMRwwGgYDVQQLDBNOb3Z1cyBPcmRvIFNlY2xv
cnVtMQ0wCwYDVQQHDARLeWl2MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEB
BQADggEPADCCAQoCggEBAKs2DANC2BAGU/rzUpOy3HpcShNdC7+vjcZ2fX6kFF9k
RZumS58dHQjj+UW6VQXFd5QS1Bb6lL/psc7svYEE4c212fWkkw84Un+ZibbIQvsF
LfAz9lqYLtzJPY3bjHRwe9bZUjO1YNxjxupB6o0R7yRGiFVA7ajrSkpNG8xrCVg6
OkN/B6hGXfv1Vn+t7lo3+JAGhEJ+/3sQ6lmyLBTtnr+qMUDwWDqKarqY9gBZbGyY
K+Jj1M0axtUtO2wNFa0UCK36aFaA/0DdoltpnenCyIngKmDBYJPwKQiqOoKEtKan
tTIa5uM6PJgrhDPjfquODfbxqxZBYnY4+WUTWNpwa7sCAwEAAaN8MHowDAYDVR0T
BAUwAwEB/zAdBgNVHQ4EFgQUzIhH1KOyEXhnmvEj0rJmEPoKOMIwHwYDVR0jBBgw
FoAUNrDxTEYV6uDVs6xHNU77q9zVmMowCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQG
CCsGAQUFBwMBBggrBgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAF3YtoIs6HrcC
XXJH//FGm4SlWGfhQ7l4k2PbC4RqrZvkMMIci7oT2xfdIAzbPUBiaVXMEw7HR7eI
iOqRzjR2ZUqIz3VD6fGVyw5Y3JLqMuT7DuirQ9BWeBTf+BXm40cvLsnWbQD7r6RD
x1a8E9uOLdt7/9C2utoQVdAZLu7UgUqRyFVeF8zHT98INDtYi8bp8nZ/de64fZbN
5pmBi2OdQGcvXUj/SRt/4OCmRqBqrYjgSl7TaAlyvf4/xk2uBG4AaKFZWWlth244
KgfaSRGKUZuvyQwTKerc8AwUFu5r3tZwAlwT9dyRM1fg+EGbmKaadyegb3AtItyN
d2r/FFIYWg==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID4TCCAsmgAwIBAgIJAO2BVuwqJLb4MA0GCSqGSIb3DQEBCwUAMIGNMRcwFQYD
VQQDDA5GdXNpb24gUm9vdCBDQTEpMCcGCSqGSIb3DQEJARYadm9sb2R5bXlyX2Jh
YmNodWtAZXBhbS5jb20xDTALBgNVBAoMBEVQQU0xHDAaBgNVBAsME05vdnVzIE9y
ZG8gU2VjbG9ydW0xDTALBgNVBAcMBEt5aXYxCzAJBgNVBAYTAlVBMB4XDTE5MDMy
MTEzMTQyNVoXDTI1MDMxOTEzMTQyNVowVDEZMBcGA1UEAwwQQW9TIFNlY29uZGFy
eSBDQTENMAsGA1UECgwERVBBTTEMMAoGA1UECwwDQW9TMQ0wCwYDVQQHDARLeWl2
MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALyD
uKMpBZN/kQFHzKo8N8y1EoPgG5sazSRe0O5xL7lm78hBmp4Vpsm/BYSI8NElkxdO
TjqQG6KK0HAyCCfQJ7MnI3G/KnJ9wxD/SWjye0/Wr5ggo1H3kFhSd9HKtuRsZJY6
E4BSz4yzburCIILC4ZvS/755OAAFX7g1IEsPeKh8sww1oGLL0xeg8W0CWmWO9PRn
o5Dl7P5QHR02BKrEwZ/DrpSpsE+ftTczxaPp/tzqp2CDGWYT5NoBfxP3W7zjKmTC
ECVgM/c29P2/AL4J8xXydDlSujvE9QG5g5UUz/dlBbVXFv0cK0oneADe0D4aRK5s
MH2ZsVFaaZAd2laa7+MCAwEAAaN8MHowDAYDVR0TBAUwAwEB/zAdBgNVHQ4EFgQU
NrDxTEYV6uDVs6xHNU77q9zVmMowHwYDVR0jBBgwFoAUdEoYczrjPeQYQ9JlsQtY
/iqxOlIwCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcD
AjANBgkqhkiG9w0BAQsFAAOCAQEAe1IT/RhZ690PIBlkzLDutf0zfs2Ei6jxTyCY
xiEmTExrU0qCZECxu/8Up6jpgqHN5upEdL/kDWwtogn0K0NGBqMNiDyc7f18rVvq
/5nZBl7P+56h5DcuLJsUb3tCC5pIkV9FYeVCg+Ub5c59b3hlFpqCmxSvDzNnRZZc
r+dInAdjcVZWmAisIpoBPrtCrqGydBtP9wy5PPxUW2bwhov4FV58C+WZ7GOLMqF+
G0wAlE7RUWvuUcKYVukkDjAg0g2qE01LnPBtpJ4dsYtEJnQknJR4swtnWfCcmlHQ
rbDoi3MoksAeGSFZePQKpht0vWiimHFQCHV2RS9P8oMqFhZN0g==
-----END CERTIFICATE-----`),
	},
	"offline1": {
		key: []byte(`-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDIBQTx5E8YLrzj
RtSjmY3Luay+bFuP8A+tTsYpiIXs8sVO4D0xYD2OkwKbBEZTVXXKI2FODroasWEI
8pXn81teijUc5jPk6QfujRihO4rP0lIth9+csvSdiX7kggJ7Co0AjaiksNIzFgkC
++koMmNFGPAssdLb6RzCoxHpV35L1QZ2JjgJqRangltKLsSjlNlNgpFumdmVIayZ
aE7TSCBeneIK87z/xNV0ubrxdZkBmoeZsZfr0vo8w8VqYlE9V2IUSaUlwh29ANdU
IGOyKqpwhD2JqSoNXfbzIx92BYD5ZnHJaUkyDig1FzXrkBgzkCul08HTxqRLCVMK
FHdsSIHBAgMBAAECggEBALDv773jTyx/O8x5feTzEwIiz/Lre9vKarPOuXFIOeCv
qWbq6nbhQdL7rRRgJa3WLYqQ3aTlVjACtWnq3jz/g9YPwIg+A639jmyyGBWYzGSn
EtcAGQlPLSCm3r9ZWsRpQu44YfS+DlPurC4dldVfLX2UX/HJpFOw1SZAhrm6EhkV
WnNweYN9GcWLLeo0LPF0LlALsVfWlNveshKbMcYVqrcAf6sgfPLSJtmpqas1Wwbj
hVMWD6oByqObe4sFF8uEEOCDF9me9vdiravZY8wvesAYXu/dm6ZI2EOGMv5ciYYN
XfINOg+RSRqN4S7tawDnmncAxQE2l5B7PO2Mvpa8IFkCgYEA5AHScKTwcx49d3mT
LlvlbKGidGy0kWeqtc2vRKG+EwNsk+UXUMQc17LY5fxd3/6+1K96ej51F8zGcYyD
lIO5b4mUehur+EsQ4+Vi/nTTgxwmgS/4WF7K0x1QxAN4DinqbvlRzlTeI0GQyTkV
qOcgNobrQ4gDYzikUD9lh0b6kmcCgYEA4JOPcw1mquzkwIkbb63RLgByQGgD3xAD
cpu6AnXTxj3d05gdMWi+xzAjSIpyfgYxsNGznqcdzMtDVsmm38QVPgE8aa2rbFxf
S1trqTxNMAEG2LCSsmInOZOuIwLsgwFDj0x/G9nKUjXYfQy1HuUPuRKHnHt0cCOl
2s7PIyQwQZcCgYBj2HRqBaCSGMz7885C/9UQ5Bs69puADTCRWpgE6vtMYjR681hp
cufagSRAWmpVe73fb1SoEY+/M1o3QTwhnilnMY1Gh7WgDmdAFSRrn4c8I+isq/AJ
6sDRAEZs/8PkF/DkVePAAiQgtkaMB6Z3h3bwydZehUJOgfBaf9ibC7cQwwKBgAlu
KNfr+CO1TuXG3CAUbHRCEIoj1AXJ5lspruXrjLkGYApCmPc6LsiufMzPA3/HQs7p
/2DqI5Y18t3yGc/LrBiudJr7b/dc6aOAc0ToA1XAtUjkIUTcWklQqj9OICBgLTYX
QD8rJhPNrwmRPwnNFJvw60Dm7jzHQm+tv4T6QAyBAoGBALIOpRJ3CjHWEkCG3ShM
EAG1+69OPpnDbErY68436ljq5CgHs/AIIaZf4vdApkOxp3ZAGuteA5rcNlvnRIvc
/4ViherQxwVN5jmQu5aKeccLTsZqyk89KFSmshdzdQQyj+iGQtTeKEPPcLIZ1RvW
Q7ni/2g3ZF+PVTJKZ/XlHdv+
-----END PRIVATE KEY-----`),
		cert: []byte(`-----BEGIN CERTIFICATE-----
MIIDzDCCArSgAwIBAgIIXK89NAAOlCIwDQYJKoZIhvcNAQELBQAwcDElMCMGA1UE
AwwcQU9TIHZlaGljbGVzIEludGVybWVkaWF0ZSBDQTENMAsGA1UECgwERVBBTTEc
MBoGA1UECwwTTm92dXMgT3JkbyBTZWNsb3J1bTENMAsGA1UEBwwES3lpdjELMAkG
A1UEBhMCVUEwHhcNMTkwNDExMTMxMjIwWhcNMjkwNDA4MTMxMjIwWjBeMSIwIAYD
VQQDDBlZVjFTVzU4RDkwMDAzNDI0OC1vZmZsaW5lMRswGQYDVQQKDBJFUEFNIFN5
c3RlbXMsIEluYy4xGzAZBgNVBAsMElRlc3QgdmVoaWNsZSBtb2RlbDCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMgFBPHkTxguvONG1KOZjcu5rL5sW4/w
D61OximIhezyxU7gPTFgPY6TApsERlNVdcojYU4OuhqxYQjylefzW16KNRzmM+Tp
B+6NGKE7is/SUi2H35yy9J2JfuSCAnsKjQCNqKSw0jMWCQL76SgyY0UY8Cyx0tvp
HMKjEelXfkvVBnYmOAmpFqeCW0ouxKOU2U2CkW6Z2ZUhrJloTtNIIF6d4grzvP/E
1XS5uvF1mQGah5mxl+vS+jzDxWpiUT1XYhRJpSXCHb0A11QgY7IqqnCEPYmpKg1d
9vMjH3YFgPlmcclpSTIOKDUXNeuQGDOQK6XTwdPGpEsJUwoUd2xIgcECAwEAAaN8
MHowDAYDVR0TAQH/BAIwADALBgNVHQ8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUH
AwIGCCsGAQUFBwMBMB0GA1UdDgQWBBR5X9OA+7raO5fnXW7LpfZhaXHxDDAfBgNV
HSMEGDAWgBTMiEfUo7IReGea8SPSsmYQ+go4wjANBgkqhkiG9w0BAQsFAAOCAQEA
T+IHIR/nUDBCdzlov6QYPNOOz3LpRUw5MjQtI0EKVRDqRQ77QceHyTle4owdeVJ8
KRK3DtNPVFYVgPJZdHivHdxVpjEnneJgrEjIRI/eN3fAwmCDCvN1gZguchKuOwK2
NXitMFpENYntyEC3Kfj+8GOhmSN8ZTgAOY2J2ynQCYnl2J68ST5J7yog3s0pAqn8
mPc6X1Yh1NNjkxMnmbIlZZk2+01qAQAeZnepGy5wmTaqHGAn7pj546zSt5CV+3N8
IGQc3y5kFuDHthElbaMtdrlzQ3ADsV8Tk0hp7hM8UOmhTa/ddK8AUoSjGg5N2dm7
sCqjIf1UqmkoLIj/7yA/zw==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDwzCCAqugAwIBAgIJAO2BVuwqJLb8MA0GCSqGSIb3DQEBCwUAMFQxGTAXBgNV
BAMMEEFvUyBTZWNvbmRhcnkgQ0ExDTALBgNVBAoMBEVQQU0xDDAKBgNVBAsMA0Fv
UzENMAsGA1UEBwwES3lpdjELMAkGA1UEBhMCVUEwHhcNMTkwMzIxMTMyMjQwWhcN
MjUwMzE5MTMyMjQwWjBwMSUwIwYDVQQDDBxBT1MgdmVoaWNsZXMgSW50ZXJtZWRp
YXRlIENBMQ0wCwYDVQQKDARFUEFNMRwwGgYDVQQLDBNOb3Z1cyBPcmRvIFNlY2xv
cnVtMQ0wCwYDVQQHDARLeWl2MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEB
BQADggEPADCCAQoCggEBAKs2DANC2BAGU/rzUpOy3HpcShNdC7+vjcZ2fX6kFF9k
RZumS58dHQjj+UW6VQXFd5QS1Bb6lL/psc7svYEE4c212fWkkw84Un+ZibbIQvsF
LfAz9lqYLtzJPY3bjHRwe9bZUjO1YNxjxupB6o0R7yRGiFVA7ajrSkpNG8xrCVg6
OkN/B6hGXfv1Vn+t7lo3+JAGhEJ+/3sQ6lmyLBTtnr+qMUDwWDqKarqY9gBZbGyY
K+Jj1M0axtUtO2wNFa0UCK36aFaA/0DdoltpnenCyIngKmDBYJPwKQiqOoKEtKan
tTIa5uM6PJgrhDPjfquODfbxqxZBYnY4+WUTWNpwa7sCAwEAAaN8MHowDAYDVR0T
BAUwAwEB/zAdBgNVHQ4EFgQUzIhH1KOyEXhnmvEj0rJmEPoKOMIwHwYDVR0jBBgw
FoAUNrDxTEYV6uDVs6xHNU77q9zVmMowCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQG
CCsGAQUFBwMBBggrBgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAF3YtoIs6HrcC
XXJH//FGm4SlWGfhQ7l4k2PbC4RqrZvkMMIci7oT2xfdIAzbPUBiaVXMEw7HR7eI
iOqRzjR2ZUqIz3VD6fGVyw5Y3JLqMuT7DuirQ9BWeBTf+BXm40cvLsnWbQD7r6RD
x1a8E9uOLdt7/9C2utoQVdAZLu7UgUqRyFVeF8zHT98INDtYi8bp8nZ/de64fZbN
5pmBi2OdQGcvXUj/SRt/4OCmRqBqrYjgSl7TaAlyvf4/xk2uBG4AaKFZWWlth244
KgfaSRGKUZuvyQwTKerc8AwUFu5r3tZwAlwT9dyRM1fg+EGbmKaadyegb3AtItyN
d2r/FFIYWg==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID4TCCAsmgAwIBAgIJAO2BVuwqJLb4MA0GCSqGSIb3DQEBCwUAMIGNMRcwFQYD
VQQDDA5GdXNpb24gUm9vdCBDQTEpMCcGCSqGSIb3DQEJARYadm9sb2R5bXlyX2Jh
YmNodWtAZXBhbS5jb20xDTALBgNVBAoMBEVQQU0xHDAaBgNVBAsME05vdnVzIE9y
ZG8gU2VjbG9ydW0xDTALBgNVBAcMBEt5aXYxCzAJBgNVBAYTAlVBMB4XDTE5MDMy
MTEzMTQyNVoXDTI1MDMxOTEzMTQyNVowVDEZMBcGA1UEAwwQQW9TIFNlY29uZGFy
eSBDQTENMAsGA1UECgwERVBBTTEMMAoGA1UECwwDQW9TMQ0wCwYDVQQHDARLeWl2
MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALyD
uKMpBZN/kQFHzKo8N8y1EoPgG5sazSRe0O5xL7lm78hBmp4Vpsm/BYSI8NElkxdO
TjqQG6KK0HAyCCfQJ7MnI3G/KnJ9wxD/SWjye0/Wr5ggo1H3kFhSd9HKtuRsZJY6
E4BSz4yzburCIILC4ZvS/755OAAFX7g1IEsPeKh8sww1oGLL0xeg8W0CWmWO9PRn
o5Dl7P5QHR02BKrEwZ/DrpSpsE+ftTczxaPp/tzqp2CDGWYT5NoBfxP3W7zjKmTC
ECVgM/c29P2/AL4J8xXydDlSujvE9QG5g5UUz/dlBbVXFv0cK0oneADe0D4aRK5s
MH2ZsVFaaZAd2laa7+MCAwEAAaN8MHowDAYDVR0TBAUwAwEB/zAdBgNVHQ4EFgQU
NrDxTEYV6uDVs6xHNU77q9zVmMowHwYDVR0jBBgwFoAUdEoYczrjPeQYQ9JlsQtY
/iqxOlIwCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcD
AjANBgkqhkiG9w0BAQsFAAOCAQEAe1IT/RhZ690PIBlkzLDutf0zfs2Ei6jxTyCY
xiEmTExrU0qCZECxu/8Up6jpgqHN5upEdL/kDWwtogn0K0NGBqMNiDyc7f18rVvq
/5nZBl7P+56h5DcuLJsUb3tCC5pIkV9FYeVCg+Ub5c59b3hlFpqCmxSvDzNnRZZc
r+dInAdjcVZWmAisIpoBPrtCrqGydBtP9wy5PPxUW2bwhov4FV58C+WZ7GOLMqF+
G0wAlE7RUWvuUcKYVukkDjAg0g2qE01LnPBtpJ4dsYtEJnQknJR4swtnWfCcmlHQ
rbDoi3MoksAeGSFZePQKpht0vWiimHFQCHV2RS9P8oMqFhZN0g==
-----END CERTIFICATE-----`),
	},
	"offline2": {
		key: []byte(`-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDG74i+FYJ1gQmi
87uUSlidBtC1nxXAlkLMXc6vR+8a19IIUH7lgHdOUx/qb7jaqIr4GGtBZeYOaqYL
ISJF5tDtQk6+UJRxoUf8YLC/odWYmitJwNPmwlFt9oba1JKCizFNHQKG9uAXeRQ+
2d+tP9o5U5s7DgVM+RiiE/XpOCGn4dOUQ8x5CtFD03yJKfAm1fwCXVZxf4R5FTsT
Y9s5F97RAA+R2FkVhHHwhYAMapMSDuWdZLvVDipgFC23h1SdzQJZ2crgBzc7hCJK
Doiau9Kgumv1fvjaVDRoLm27jT+bjoP6EFs2jxNH6RBDg5Iw0Slcj0D7nYlwudXr
2Ign/QIlAgMBAAECggEAZ+UjojpzltCcaskmFw04+FFd4OzDnIAdRMRdNDe6TWeX
npYDn/KW3IYXLgXJIhFR+r4uDcqc+ryCGV/lmWIxjSfLHiPRUwLrKIiK5porhnZF
00/smyCzDF3rEhBgr+LoDaDv9/KpGDk49JYu9jlZzAS5Fn99DzUsw0DvdizFjvo6
izLoM3cI00T6uNHJaocuFt+WTUbCK7Hx0C4mIz5nS1eQM4ovMAiahxjU84Vta0Nn
vxK1siaxgK7KcD4ZN+/TfrNStNP+PJwuF9f5JA0MJBTW/JtLzJ4neO4vkZFCcxT9
p9aPWnIWmhU76xzyKJwRTGD+9IsP6apLkvQ3mil8AQKBgQDyHn/wFhCO755uqKl/
a7SHTBiX8733y5gVEBvmxniv5WKHyR6CImiu1eRSUAfcKePLmIKKoHHOw3yo3YsW
reO3xC6Fk2x1alpPw5MaH0ElwnMeMWlOOPglouHTBh+Ol3gsGC4wQUH+K5TZUxvU
nc3SOgUmLHAcqFjUSjb43tsxJQKBgQDSVz7V3oWTvaHEuMJsKohzfhAMxb7cbY2u
is84A66jq14jRi0hXH7tpFyIG5TfTk1KTZDRNwf9dvfJT5asuQ52Cz9rzWz/R2No
uHHOPAHId3GAoWpHXu9KXdSqzm8D5Lri3+4BolIMaSbhqbmnWT1pZX5YMHZ9D2M0
G8XBlvw9AQKBgQDkMerTFXi1vxHLqhtWhOS5P/dN/+Rjz/eeongpoZXN8pxS7jNa
46NWZTG0gsllr/WKxksC7QVWotizL1sQHQQrBzPxoWjvoTVNSD80t5BnTkXBh0CB
ASCgGExO386OTiRtKr0drePM8rZvvezVD4YVRankuK1R1TkjnG8DUMe2IQKBgCUV
8uM8d6rD3ZjUxprRqPtL98J4vx0YR8nFeaGzrH/5AAESJ3ThXRPDTflFe6sfoCsA
oA7zN/ptlmStHrDXdABGHWmBb71WteVJ1+73z4yr2pxGWXm5+FDRWGTBPvudwYGs
38bz+qlrhMp25V/nMRe7KFqeONX195TBbM2kNFcBAoGALr3CHi5CbrHZtA1TqOAz
QcIUi9Z731Ya3Zv0fZ8l3NhdnCtqLC9MX6B9y5qVUbDOjPRyZiSLmIChC3n4tXFz
AvS7I2T7ylvu5BOOKIPdT05es22VZtYQm0qIvkjOsDAe4wB9lQDz8Wnwqmiazyls
1c3b7h7B5HOH9/fkzHVE8+M=
-----END PRIVATE KEY-----`),
		cert: []byte(`-----BEGIN CERTIFICATE-----
MIIDzDCCArSgAwIBAgIIXK89XQALv5IwDQYJKoZIhvcNAQELBQAwcDElMCMGA1UE
AwwcQU9TIHZlaGljbGVzIEludGVybWVkaWF0ZSBDQTENMAsGA1UECgwERVBBTTEc
MBoGA1UECwwTTm92dXMgT3JkbyBTZWNsb3J1bTENMAsGA1UEBwwES3lpdjELMAkG
A1UEBhMCVUEwHhcNMTkwNDExMTMxMzAxWhcNMjkwNDA4MTMxMzAxWjBeMSIwIAYD
VQQDDBlZVjFTVzU4RDIwMjA1NzUyOC1vZmZsaW5lMRswGQYDVQQKDBJFUEFNIFN5
c3RlbXMsIEluYy4xGzAZBgNVBAsMElRlc3QgdmVoaWNsZSBtb2RlbDCCASIwDQYJ
KoZIhvcNAQEBBQADggEPADCCAQoCggEBAMbviL4VgnWBCaLzu5RKWJ0G0LWfFcCW
Qsxdzq9H7xrX0ghQfuWAd05TH+pvuNqoivgYa0Fl5g5qpgshIkXm0O1CTr5QlHGh
R/xgsL+h1ZiaK0nA0+bCUW32htrUkoKLMU0dAob24Bd5FD7Z360/2jlTmzsOBUz5
GKIT9ek4Iafh05RDzHkK0UPTfIkp8CbV/AJdVnF/hHkVOxNj2zkX3tEAD5HYWRWE
cfCFgAxqkxIO5Z1ku9UOKmAULbeHVJ3NAlnZyuAHNzuEIkoOiJq70qC6a/V++NpU
NGgubbuNP5uOg/oQWzaPE0fpEEODkjDRKVyPQPudiXC51evYiCf9AiUCAwEAAaN8
MHowDAYDVR0TAQH/BAIwADALBgNVHQ8EBAMCBaAwHQYDVR0lBBYwFAYIKwYBBQUH
AwIGCCsGAQUFBwMBMB0GA1UdDgQWBBSlrxlFrr5Fbd5odHG2LrKB6F68VjAfBgNV
HSMEGDAWgBTMiEfUo7IReGea8SPSsmYQ+go4wjANBgkqhkiG9w0BAQsFAAOCAQEA
Bm2rAPIEMKwZ5xszTI80izEG1VszNbKPf2YhkUNgbU0yizTE7bxBt7zJqeazcmYD
vJvZ9pw2Mmq6GYTU8Js4ILi/oVeiTsb3nB05gNz+jjDZkJFmRbVy9/DjRP1MNDCq
J+J7ZwR7qXb+dtK9TP44rmEpKkW6GqEyuuKu5vA/GZ23jlTBdMQq/E2U/dNI5+Kx
J4I5+Fai83qy7VkR3aebpgeUJ2WByWbvyvP+gm8vv2BqWwRn6ndAzQpGIUdHex4V
c9p+4C5iJAE4QFNVTTxwX3vR8sh5cKn/jsUE5FUePJh2npfdTSY8bVU51r1x0SzA
Z4DSIXf+MunCkFCTxBlm0A==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIIDwzCCAqugAwIBAgIJAO2BVuwqJLb8MA0GCSqGSIb3DQEBCwUAMFQxGTAXBgNV
BAMMEEFvUyBTZWNvbmRhcnkgQ0ExDTALBgNVBAoMBEVQQU0xDDAKBgNVBAsMA0Fv
UzENMAsGA1UEBwwES3lpdjELMAkGA1UEBhMCVUEwHhcNMTkwMzIxMTMyMjQwWhcN
MjUwMzE5MTMyMjQwWjBwMSUwIwYDVQQDDBxBT1MgdmVoaWNsZXMgSW50ZXJtZWRp
YXRlIENBMQ0wCwYDVQQKDARFUEFNMRwwGgYDVQQLDBNOb3Z1cyBPcmRvIFNlY2xv
cnVtMQ0wCwYDVQQHDARLeWl2MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEB
BQADggEPADCCAQoCggEBAKs2DANC2BAGU/rzUpOy3HpcShNdC7+vjcZ2fX6kFF9k
RZumS58dHQjj+UW6VQXFd5QS1Bb6lL/psc7svYEE4c212fWkkw84Un+ZibbIQvsF
LfAz9lqYLtzJPY3bjHRwe9bZUjO1YNxjxupB6o0R7yRGiFVA7ajrSkpNG8xrCVg6
OkN/B6hGXfv1Vn+t7lo3+JAGhEJ+/3sQ6lmyLBTtnr+qMUDwWDqKarqY9gBZbGyY
K+Jj1M0axtUtO2wNFa0UCK36aFaA/0DdoltpnenCyIngKmDBYJPwKQiqOoKEtKan
tTIa5uM6PJgrhDPjfquODfbxqxZBYnY4+WUTWNpwa7sCAwEAAaN8MHowDAYDVR0T
BAUwAwEB/zAdBgNVHQ4EFgQUzIhH1KOyEXhnmvEj0rJmEPoKOMIwHwYDVR0jBBgw
FoAUNrDxTEYV6uDVs6xHNU77q9zVmMowCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQG
CCsGAQUFBwMBBggrBgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAF3YtoIs6HrcC
XXJH//FGm4SlWGfhQ7l4k2PbC4RqrZvkMMIci7oT2xfdIAzbPUBiaVXMEw7HR7eI
iOqRzjR2ZUqIz3VD6fGVyw5Y3JLqMuT7DuirQ9BWeBTf+BXm40cvLsnWbQD7r6RD
x1a8E9uOLdt7/9C2utoQVdAZLu7UgUqRyFVeF8zHT98INDtYi8bp8nZ/de64fZbN
5pmBi2OdQGcvXUj/SRt/4OCmRqBqrYjgSl7TaAlyvf4/xk2uBG4AaKFZWWlth244
KgfaSRGKUZuvyQwTKerc8AwUFu5r3tZwAlwT9dyRM1fg+EGbmKaadyegb3AtItyN
d2r/FFIYWg==
-----END CERTIFICATE-----
-----BEGIN CERTIFICATE-----
MIID4TCCAsmgAwIBAgIJAO2BVuwqJLb4MA0GCSqGSIb3DQEBCwUAMIGNMRcwFQYD
VQQDDA5GdXNpb24gUm9vdCBDQTEpMCcGCSqGSIb3DQEJARYadm9sb2R5bXlyX2Jh
YmNodWtAZXBhbS5jb20xDTALBgNVBAoMBEVQQU0xHDAaBgNVBAsME05vdnVzIE9y
ZG8gU2VjbG9ydW0xDTALBgNVBAcMBEt5aXYxCzAJBgNVBAYTAlVBMB4XDTE5MDMy
MTEzMTQyNVoXDTI1MDMxOTEzMTQyNVowVDEZMBcGA1UEAwwQQW9TIFNlY29uZGFy
eSBDQTENMAsGA1UECgwERVBBTTEMMAoGA1UECwwDQW9TMQ0wCwYDVQQHDARLeWl2
MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALyD
uKMpBZN/kQFHzKo8N8y1EoPgG5sazSRe0O5xL7lm78hBmp4Vpsm/BYSI8NElkxdO
TjqQG6KK0HAyCCfQJ7MnI3G/KnJ9wxD/SWjye0/Wr5ggo1H3kFhSd9HKtuRsZJY6
E4BSz4yzburCIILC4ZvS/755OAAFX7g1IEsPeKh8sww1oGLL0xeg8W0CWmWO9PRn
o5Dl7P5QHR02BKrEwZ/DrpSpsE+ftTczxaPp/tzqp2CDGWYT5NoBfxP3W7zjKmTC
ECVgM/c29P2/AL4J8xXydDlSujvE9QG5g5UUz/dlBbVXFv0cK0oneADe0D4aRK5s
MH2ZsVFaaZAd2laa7+MCAwEAAaN8MHowDAYDVR0TBAUwAwEB/zAdBgNVHQ4EFgQU
NrDxTEYV6uDVs6xHNU77q9zVmMowHwYDVR0jBBgwFoAUdEoYczrjPeQYQ9JlsQtY
/iqxOlIwCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcD
AjANBgkqhkiG9w0BAQsFAAOCAQEAe1IT/RhZ690PIBlkzLDutf0zfs2Ei6jxTyCY
xiEmTExrU0qCZECxu/8Up6jpgqHN5upEdL/kDWwtogn0K0NGBqMNiDyc7f18rVvq
/5nZBl7P+56h5DcuLJsUb3tCC5pIkV9FYeVCg+Ub5c59b3hlFpqCmxSvDzNnRZZc
r+dInAdjcVZWmAisIpoBPrtCrqGydBtP9wy5PPxUW2bwhov4FV58C+WZ7GOLMqF+
G0wAlE7RUWvuUcKYVukkDjAg0g2qE01LnPBtpJ4dsYtEJnQknJR4swtnWfCcmlHQ
rbDoi3MoksAeGSFZePQKpht0vWiimHFQCHV2RS9P8oMqFhZN0g==
-----END CERTIFICATE-----`),
	},
	"root": {
		cert: []byte(`-----BEGIN CERTIFICATE-----
MIIEAjCCAuqgAwIBAgIJAPwk2NFfSDPjMA0GCSqGSIb3DQEBCwUAMIGNMRcwFQYD
VQQDDA5GdXNpb24gUm9vdCBDQTEpMCcGCSqGSIb3DQEJARYadm9sb2R5bXlyX2Jh
YmNodWtAZXBhbS5jb20xDTALBgNVBAoMBEVQQU0xHDAaBgNVBAsME05vdnVzIE9y
ZG8gU2VjbG9ydW0xDTALBgNVBAcMBEt5aXYxCzAJBgNVBAYTAlVBMB4XDTE4MDQx
MDExMzMwMFoXDTI2MDYyNzExMzMwMFowgY0xFzAVBgNVBAMMDkZ1c2lvbiBSb290
IENBMSkwJwYJKoZIhvcNAQkBFhp2b2xvZHlteXJfYmFiY2h1a0BlcGFtLmNvbTEN
MAsGA1UECgwERVBBTTEcMBoGA1UECwwTTm92dXMgT3JkbyBTZWNsb3J1bTENMAsG
A1UEBwwES3lpdjELMAkGA1UEBhMCVUEwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAw
ggEKAoIBAQC+K2ow2HO7+SUVfOq5tTtmHj4LQijHJ803mLk9pkPef+Glmeyp9HXe
jDlQC04MeovMBeNTaq0wibf7qas9niXbeXRVzheZIFziMXqRuwLqc0KXdDxIDPTb
TW3K0HE6M/eAtTfn9+Z/LnkWt4zMXasc02hvufsmIVEuNbc1VhrsJJg5uk88ldPM
LSF7nff9eYZTHYgCyBkt9aL+fwoXO6eSDSAhjopX3lhdidkM+ni7EOhlN7STmgDM
WKh9nMjXD5f28PGhtW/dZvn4SzasRE5MeaExIlBmhkWEUgVCyP7LvuQGRUPK+NYz
FE2CLRuirLCWy1HIt9lLziPjlZ4361mNAgMBAAGjYzBhMB0GA1UdDgQWBBR0Shhz
OuM95BhD0mWxC1j+KrE6UjAMBgNVHRMEBTADAQH/MAsGA1UdDwQEAwIBBjAlBgNV
HREEHjAcgRp2b2xvZHlteXJfYmFiY2h1a0BlcGFtLmNvbTANBgkqhkiG9w0BAQsF
AAOCAQEAl8bv1HTYe3l4Y+g0TVZR7bYL5BNsnGgqy0qS5fu991khXWf+Zwa2MLVn
YakMnLkjvdHqUpWMJ/S82o2zWGmmuxca56ehjxCiP/nkm4M74yXz2R8cu52WxYnF
yMvgawzQ6c1yhvZiv/gEE7KdbYRVKLHPgBzfyup21i5ngSlTcMRRS7oOBmoye4qc
6adq6HtY6X/OnZ9I5xoRN1GcvaLUgUE6igTiVa1pF8kedWhHY7wzTXBxzSvIZkCU
VHEOzvaGk9miP6nBrDfNv7mIkgEKARrjjSpmJasIEU+mNtzeOIEiMtW1EMRc457o
0PdFI3jseyLVPVhEzUkuC7mwjb7CeQ==
-----END CERTIFICATE-----`),
	},
}

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
	cert1, _ := base64.StdEncoding.DecodeString("MIIDmTCCAoGgAwIBAgIUPQwLpw4adg7kY7MUGg3/SkGgAbEwDQYJKoZIhvcNAQELBQAwWzEgMB4GA1UEAwwXQU9TIE9FTSBJbnRlcm1lZGlhdGUgQ0ExDTALBgNVBAoMBEVQQU0xDDAKBgNVBAsMA0FPUzENMAsGA1UEBwwES3lpdjELMAkGA1UEBhMCVUEwHhcNMTkwNzMxMDg0MzU5WhcNMjIwNzMwMDg0MzU5WjBBMSIwIAYDVQQDDBlBb1MgVGFyZ2V0IFVwZGF0ZXMgU2lnbmVyMQ0wCwYDVQQKDARFUEFNMQwwCgYDVQQLDANBb1MwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDJlnvb/zEu1N9C3xRnEjLivUr/1LeLeBNgFtb9BTk7SJXDkWFzoTryhzeKhTF0EhoFx5qLbxSpwhcoKHi9ds0AngP2amQ7AhWHRgWIwX/phZVHY+rh3iTTYDsdorReUAVDCPEYpus+XfaZfH9/0E/byNq3kIzyL4u7YvZS2+F+LTV/7nvh8U5d9RCPoUfnfGj7InZF7adVLzh32KlBeDkVLJWGdfWQ6lTdk6uoRgsXtT944Q01TGJfUhbgr9MUeyi4k3L0vfXM7wMEIrzf7horhNcT/UyU5Ftc2BI3FQv+zy3LrbnwFZdJ+0gKV7Ibp9LbVSBJb+fBc4U047EGX117AgMBAAGjbzBtMAkGA1UdEwQCMAAwHQYDVR0OBBYEFNGkZujhIhZqnHHAdptjquk1k2uVMB8GA1UdIwQYMBaAFHGorK9rv0A9+zixyID2S7vfxyfgMAsGA1UdDwQEAwIFoDATBgNVHSUEDDAKBggrBgEFBQcDAzANBgkqhkiG9w0BAQsFAAOCAQEAKjS2CTWEvck1ZFVpNL81Er9MzVvnMNmHDXmkBPO+WcIcQ4RnOZGKfe9z0UX3/bVoAVyBPpVHAkczmxJQTijGC5c6anhXt06uGXyutArlrNFD75ToQOi8JBdBNvB9QfnoYqZ9CHgyYN4sKXGluyGwKMjtwADBBGkV+PR/1KqI+qHvP831Ujylb7WeSOH5RBC6jYB34Mmp5AcnIX4B7HHKFb8NitxX7Kxynua2sUgs5D1eJeOx4v6hTnP8Hto7bBkU9qYaOyJD7H9V6lfyQvxkA8iva5zYvNoQ2zWLnnSK78yrVRphW0J1gB1FW4ZsKvfsbk9fyxpCARyRXhjU8H7SDg==")                                                                                                 // nolint:lll
	cert2, _ := base64.StdEncoding.DecodeString("MIIDrjCCApagAwIBAgIJAO2BVuwqJLb6MA0GCSqGSIb3DQEBCwUAMFQxGTAXBgNVBAMMEEFvUyBTZWNvbmRhcnkgQ0ExDTALBgNVBAoMBEVQQU0xDDAKBgNVBAsMA0FvUzENMAsGA1UEBwwES3lpdjELMAkGA1UEBhMCVUEwHhcNMTkwMzIxMTMyMjM2WhcNMjUwMzE5MTMyMjM2WjBbMSAwHgYDVQQDDBdBT1MgT0VNIEludGVybWVkaWF0ZSBDQTENMAsGA1UECgwERVBBTTEMMAoGA1UECwwDQU9TMQ0wCwYDVQQHDARLeWl2MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJpf1Zj3yNj/Gl7PRPevf7PYpvMZVgwZRLEuwqfXxvDHYhfb/Kp7xAH7QeZVfB8rINSpJbv1+KcqiCzCZig32H+Iv5cGlyn1xmXCihHySH4/XRyduHGue385dNDyXRExpFGXAru/AhgXGKVaxbfDwE9lnz8yWRFvhNrdPO8/nRNZf1ZOqRaq7YNYC7kRQLgp76Da64/H7OWWy9B82r+fgEKc63ixDWSqaLGcNgIBqHU+Rky/SX/gPUtaCIqJb+CpWZZQlJ2wQ+dv+s+K2AG7O0HSHQkh2BbMcjVDeCcdu477+Mal8+MhhjYzkQmAi1tVOYAzX2H/StCGSYohtpxqT5ECAwEAAaN8MHowDAYDVR0TBAUwAwEB/zAdBgNVHQ4EFgQUcaisr2u/QD37OLHIgPZLu9/HJ+AwHwYDVR0jBBgwFoAUNrDxTEYV6uDVs6xHNU77q9zVmMowCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAM+yZJvfkSQmXYt554Zsy2Wqm1w/ixUV55T3r0JFRFbf5AQ7RNBp7t2dn1cEUA6O4wSK3U1Y7eyrPn/ECNmbZ5QUlHUAN/QUnuJUIe0UEd9k+lO2VLbLK+XamzDlOxPBn94s9C1jeXrwGdeTRVcq73rH1CvIOMhD7rp/syQKFuBfQBwCgfH0CbSRsHRm9xQii/HQYMfD8TMyqrjMKF7s68r7shQG2OGo1HJqfA6f9Cb+i4A1BfeP97lFeyr3OjQtLcQJ/a6nPdGs1Cg94Zl2PBEPFH9ecuYpKt0UqK8x8HRsYru7Wp8wkzMbvlYShI5mwdIpvksg5aqnIhWWGqhDRqg==")                                                                     // nolint:lll
	cert3, _ := base64.StdEncoding.DecodeString("MIID4TCCAsmgAwIBAgIJAO2BVuwqJLb4MA0GCSqGSIb3DQEBCwUAMIGNMRcwFQYDVQQDDA5GdXNpb24gUm9vdCBDQTEpMCcGCSqGSIb3DQEJARYadm9sb2R5bXlyX2JhYmNodWtAZXBhbS5jb20xDTALBgNVBAoMBEVQQU0xHDAaBgNVBAsME05vdnVzIE9yZG8gU2VjbG9ydW0xDTALBgNVBAcMBEt5aXYxCzAJBgNVBAYTAlVBMB4XDTE5MDMyMTEzMTQyNVoXDTI1MDMxOTEzMTQyNVowVDEZMBcGA1UEAwwQQW9TIFNlY29uZGFyeSBDQTENMAsGA1UECgwERVBBTTEMMAoGA1UECwwDQW9TMQ0wCwYDVQQHDARLeWl2MQswCQYDVQQGEwJVQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALyDuKMpBZN/kQFHzKo8N8y1EoPgG5sazSRe0O5xL7lm78hBmp4Vpsm/BYSI8NElkxdOTjqQG6KK0HAyCCfQJ7MnI3G/KnJ9wxD/SWjye0/Wr5ggo1H3kFhSd9HKtuRsZJY6E4BSz4yzburCIILC4ZvS/755OAAFX7g1IEsPeKh8sww1oGLL0xeg8W0CWmWO9PRno5Dl7P5QHR02BKrEwZ/DrpSpsE+ftTczxaPp/tzqp2CDGWYT5NoBfxP3W7zjKmTCECVgM/c29P2/AL4J8xXydDlSujvE9QG5g5UUz/dlBbVXFv0cK0oneADe0D4aRK5sMH2ZsVFaaZAd2laa7+MCAwEAAaN8MHowDAYDVR0TBAUwAwEB/zAdBgNVHQ4EFgQUNrDxTEYV6uDVs6xHNU77q9zVmMowHwYDVR0jBBgwFoAUdEoYczrjPeQYQ9JlsQtY/iqxOlIwCwYDVR0PBAQDAgGmMB0GA1UdJQQWMBQGCCsGAQUFBwMBBggrBgEFBQcDAjANBgkqhkiG9w0BAQsFAAOCAQEAe1IT/RhZ690PIBlkzLDutf0zfs2Ei6jxTyCYxiEmTExrU0qCZECxu/8Up6jpgqHN5upEdL/kDWwtogn0K0NGBqMNiDyc7f18rVvq/5nZBl7P+56h5DcuLJsUb3tCC5pIkV9FYeVCg+Ub5c59b3hlFpqCmxSvDzNnRZZcr+dInAdjcVZWmAisIpoBPrtCrqGydBtP9wy5PPxUW2bwhov4FV58C+WZ7GOLMqF+G0wAlE7RUWvuUcKYVukkDjAg0g2qE01LnPBtpJ4dsYtEJnQknJR4swtnWfCcmlHQrbDoi3MoksAeGSFZePQKpht0vWiimHFQCHV2RS9P8oMqFhZN0g==") // nolint:lll

	signValue, _ := base64.StdEncoding.DecodeString("UX+xLpoUDUROtuER/YU1mFRpXx5SD9AVgHT1SRRNadxqtJVz/S0xJcdJw6A8KRmUms7wl4TLe8z0utESJMUZPhgQY06ERSlX3yuBain26qKPDZaMielogQKW5oYgAI9TdGQdTNtHsB0AT4cHVz+E+GKPMjuc3tkc2fsQwWPZPl0mukcjpm6tsUwwX+3WSJfMGQ0P6itgDBLvSxaWZWuS0fZlZ8FSXd8NQjlWWCNf8goQWknxkVptPefORwg5DRb6lGF/dVWGa1miguiHerZVSJfykhEQUK/6zIWFpaGuxm5DG6DjeaRM3V1jbriEV+w1SZR+sCj9BECm5BPVYx5e/w==") // nolint:lll

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
					"48FAC66F9994BA0EA0BC71EE6E0CAB79A0A2E6DF",
					"FE232D8F645F9550D2AB559BF3144CAEB6534F69",
					"EE9E93D52D84CF3CBEB2E327635770458457B7C2",
				},
			},
		},
		Certificates: []testUpgradeCertificate{
			{
				Fingerprint: "48FAC66F9994BA0EA0BC71EE6E0CAB79A0A2E6DF",
				Certificate: cert1,
			},
			{
				Fingerprint: "FE232D8F645F9550D2AB559BF3144CAEB6534F69",
				Certificate: cert2,
			},
			{
				Fingerprint: "EE9E93D52D84CF3CBEB2E327635770458457B7C2",
				Certificate: cert3,
			},
		},
	}

	// Create or use context

	certURL, err := url.Parse(certNameToFileURL("root"))
	if err != nil {
		t.Fatalf("Can't parse cert URL: '%v'", err)
	}

	certProvider := testCertificateProvider{}

	cryptoCtx, err := createCryptoContext(config.Crypt{CACert: certURL.Path})
	if err != nil {
		t.Fatal(err)
	}

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

		err = signCtx.VerifySign(context.Background(), tmpFile, data.Signs.ChainName, data.Signs.Alg, data.Signs.Value)
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

/***********************************************************************************************************************
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
