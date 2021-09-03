// SPDX-License-Identifier: Apache-2.0
//
// Copyright 2021 Renesas Inc.
// Copyright 2021 EPAM Systems Inc.
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
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/ThalesIgnite/crypto11"
	"github.com/google/go-tpm/tpm2"
	"github.com/google/go-tpm/tpmutil"
	log "github.com/sirupsen/logrus"
	"gitpct.epam.com/epmd-aepr/aos_common/aoserrors"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/cryptutils"
	"gitpct.epam.com/epmd-aepr/aos_common/utils/tpmkey"

	"aos_communicationmanager/config"
)

const (
	fileBlockSize = 64 * 1024
)

const (
	onlineCertificate  = "online"
	offlineCertificate = "offline"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// ReceiverInfo receiver info
type ReceiverInfo struct {
	Serial string
	Issuer []byte
}

// CryptoSessionKeyInfo crypto session key info
type CryptoSessionKeyInfo struct {
	SessionKey        []byte       `json:"sessionKey"`
	SessionIV         []byte       `json:"sessionIV"`
	SymmetricAlgName  string       `json:"symmetricAlgName"`
	AsymmetricAlgName string       `json:"asymmetricAlgName"`
	ReceiverInfo      ReceiverInfo `json:"recipientInfo"`
}

// CryptoContext crypto context
type CryptoContext struct {
	rootCertPool  *x509.CertPool
	tpmDevice     io.ReadWriteCloser
	pkcs11Ctx     map[pkcs11Descriptor]*crypto11.Context
	pkcs11Library string
	certProvider  CertificateProvider
}

// SymmetricContextInterface interface for SymmetricCipherContext
type SymmetricContextInterface interface {
	DecryptFile(encryptedFile, clearFile *os.File) (err error)
}

// SymmetricCipherContext symmetric cipher context
type SymmetricCipherContext struct {
	key         []byte
	iv          []byte
	algName     string
	modeName    string
	paddingName string
	decrypter   cipher.BlockMode
	encrypter   cipher.BlockMode
}

// SignContext sign context
type SignContext struct {
	cryptoContext         *CryptoContext
	signCertificates      []certificateInfo
	signCertificateChains []certificateChainInfo
}

// SignContextInterface interface for SignContext
type SignContextInterface interface {
	AddCertificate(fingerprint string, asn1Bytes []byte) (err error)
	AddCertificateChain(name string, fingerprints []string) (err error)
	VerifySign(f *os.File, chainName string, algName string, signValue []byte) (err error)
}

// CertificateProvider interface to get certificate
type CertificateProvider interface {
	GetCertificate(certType string, issuer []byte, serial string) (certURL, ketURL string, err error)
}

type certificateInfo struct {
	fingerprint string
	certificate *x509.Certificate
}

type certificateChainInfo struct {
	name         string
	fingerprints []string
}

type pkcs11Descriptor struct {
	library string
	token   string
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New create context for crypto operations
func New(conf config.Crypt, provider CertificateProvider) (ctx *CryptoContext, err error) {
	// Create context
	ctx = &CryptoContext{
		certProvider:  provider,
		pkcs11Ctx:     make(map[pkcs11Descriptor]*crypto11.Context),
		pkcs11Library: conf.Pkcs11Library}

	if conf.CACert != "" {
		if ctx.rootCertPool, err = cryptutils.GetCaCertPool(conf.CACert); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	if conf.TpmDevice != "" {
		if ctx.tpmDevice, err = tpm2.OpenTPM(conf.TpmDevice); err != nil {
			return nil, aoserrors.Wrap(err)
		}
	}

	return ctx, nil
}

// Close closes crypto context
func (ctx *CryptoContext) Close() (err error) {
	if ctx.tpmDevice != nil {
		if tpmErr := ctx.tpmDevice.Close(); tpmErr != nil {
			if err == nil {
				err = tpmErr
			}
		}
	}

	for pkcs11Desc, pkcs11ctx := range ctx.pkcs11Ctx {
		log.WithFields(log.Fields{"library": pkcs11Desc.library, "token": pkcs11Desc.token}).Debug("Close PKCS11 context")

		if pkcs11Err := pkcs11ctx.Close(); pkcs11Err != nil {
			log.WithFields(log.Fields{"library": pkcs11Desc.library, "token": pkcs11Desc.token}).Errorf("Can't PKCS11 context: %s", err)

			if err == nil {
				err = pkcs11Err
			}
		}
	}

	return aoserrors.Wrap(err)
}

// GetOrganization returns online certificate origanizarion names
func (ctx *CryptoContext) GetOrganization() (names []string, err error) {
	certURLStr, _, err := ctx.certProvider.GetCertificate(onlineCertificate, nil, "")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	certs, err := ctx.loadCertificateByURL(certURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if certs[0].Subject.Organization == nil {
		return nil, aoserrors.New("online certificate does not have organizations")
	}

	return certs[0].Subject.Organization, nil
}

// GetCertSerial returns certificate serial number
func (ctx *CryptoContext) GetCertSerial(certURL string) (serial string, err error) {
	certs, err := ctx.loadCertificateByURL(certURL)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return fmt.Sprintf("%X", certs[0].SerialNumber), nil
}

// CreateSignContext creates sign context
func (ctx *CryptoContext) CreateSignContext() (signContext SignContextInterface, err error) {
	if ctx == nil || ctx.rootCertPool == nil {
		return nil, aoserrors.New("asymmetric context not initialized")
	}

	return &SignContext{cryptoContext: ctx}, nil
}

// GetTLSConfig Provides TLS configuration for HTTPS client
func (ctx *CryptoContext) GetTLSConfig() (cfg *tls.Config, err error) {
	cfg = &tls.Config{}

	certURLStr, keyURLStr, err := ctx.certProvider.GetCertificate(onlineCertificate, nil, "")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	clientCert, err := ctx.loadCertificateByURL(certURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	onlinePrivate, _, err := ctx.loadPrivateKeyByURL(keyURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	cfg.RootCAs = ctx.rootCertPool
	cfg.Certificates = []tls.Certificate{{PrivateKey: onlinePrivate, Certificate: getRawCertificate(clientCert)}}
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) (err error) {
		return nil
	}

	cfg.BuildNameToCertificate()

	return cfg, nil
}

// DecryptMetadata decrypt envelope
func (ctx *CryptoContext) DecryptMetadata(input []byte) (output []byte, err error) {
	ci, err := unmarshallCMS(input)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, recipient := range ci.EnvelopedData.RecipientInfos {
		dkey, err := ctx.getKeyForEnvelope(recipient.(keyTransRecipientInfo))
		if err != nil {
			log.Warnf("Can't get key for envelope: %s", err)

			continue
		}

		output, err = decryptMessage(&ci.EnvelopedData.EncryptedContentInfo, dkey)
		if err != nil {
			log.Warnf("Can't decrypt message: %s", err)

			continue
		}

		return output, nil
	}

	return output, aoserrors.New("can't decrypt metadata")
}

// ImportSessionKey function retrieves a symmetric key from crypto context
func (ctx *CryptoContext) ImportSessionKey(keyInfo CryptoSessionKeyInfo) (symContext SymmetricContextInterface, err error) {
	_, keyURLStr, err := ctx.certProvider.GetCertificate(offlineCertificate, keyInfo.ReceiverInfo.Issuer, keyInfo.ReceiverInfo.Serial)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	privKey, supportPKCS1v15SessionKey, err := ctx.loadPrivateKeyByURL(keyURLStr)
	if err != nil {
		log.Errorf("Cant load private key: %s", err)

		return nil, aoserrors.Wrap(err)
	}

	algName, _, _ := decodeAlgNames(keyInfo.SymmetricAlgName)
	keySize, ivSize, err := getSymmetricAlgInfo(algName)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if ivSize != len(keyInfo.SessionIV) {
		return nil, aoserrors.New("invalid IV length")
	}

	var opts crypto.DecrypterOpts

	switch strings.ToUpper(keyInfo.AsymmetricAlgName) {
	case "RSA/PKCS1V1_5":
		if !supportPKCS1v15SessionKey {
			keySize = 0
		}

		opts = &rsa.PKCS1v15DecryptOptions{SessionKeyLen: keySize}

	case "RSA/OAEP":
		opts = &rsa.OAEPOptions{Hash: crypto.SHA1}

	case "RSA/OAEP-256":
		opts = &rsa.OAEPOptions{Hash: crypto.SHA256}

	case "RSA/OAEP-512":
		opts = &rsa.OAEPOptions{Hash: crypto.SHA512}

	default:
		return nil, aoserrors.Errorf("unsupported asymmetric alg in import key: %s", keyInfo.AsymmetricAlgName)
	}

	decrypter, ok := privKey.(crypto.Decrypter)
	if !ok {
		return nil, aoserrors.New("private key doesn't implement decrypter interface")
	}

	decryptedKey, err := decrypter.Decrypt(rand.Reader, keyInfo.SessionKey, opts)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	ctxSym := CreateSymmetricCipherContext()

	if err = ctxSym.set(keyInfo.SymmetricAlgName, decryptedKey, keyInfo.SessionIV); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return ctxSym, nil
}

// AddCertificate adds certificate to context
func (ctx *SignContext) AddCertificate(fingerprint string, asn1Bytes []byte) error {
	fingerUpper := strings.ToUpper(fingerprint)

	// Check certificate presents
	for _, item := range ctx.signCertificates {
		if item.fingerprint == fingerUpper {
			return nil
		}
	}

	// Parse certificate from ASN1 bytes
	cert, err := x509.ParseCertificate(asn1Bytes)
	if err != nil {
		return aoserrors.Errorf("error parsing sign certificate: %s", err)
	}

	// Append the new certificate to the collection
	ctx.signCertificates = append(ctx.signCertificates, certificateInfo{fingerprint: fingerUpper, certificate: cert})

	return nil
}

// AddCertificateChain adds certificate chain to context
func (ctx *SignContext) AddCertificateChain(name string, fingerprints []string) error {
	if len(fingerprints) == 0 {
		return aoserrors.New("can't add certificate chain with empty fingerprint list")
	}

	// Check certificate presents
	for _, item := range ctx.signCertificateChains {
		if item.name == name {
			return nil
		}
	}

	ctx.signCertificateChains = append(ctx.signCertificateChains, certificateChainInfo{name, fingerprints})

	return nil
}

// VerifySign verifies signature
func (ctx *SignContext) VerifySign(f *os.File, chainName string, algName string, signValue []byte) (err error) {
	if len(ctx.signCertificateChains) == 0 || len(ctx.signCertificates) == 0 {
		return aoserrors.New("sign context not initialized (no certificates)")
	}

	var chain certificateChainInfo
	var signCertFingerprint string

	// Find chain
	for _, chainTmp := range ctx.signCertificateChains {
		if chainTmp.name == chainName {
			chain = chainTmp
			signCertFingerprint = chain.fingerprints[0]

			break
		}
	}

	if chain.name == "" || len(chain.name) == 0 {
		return aoserrors.New("unknown chain name")
	}

	signCert := ctx.getCertificateByFingerprint(signCertFingerprint)

	if signCert == nil {
		return aoserrors.New("signing certificate is absent")
	}

	signAlgName, signHash, signPadding := decodeSignAlgNames(algName)

	var hashFunc crypto.Hash

	switch strings.ToUpper(signHash) {
	case "SHA256":
		hashFunc = crypto.SHA256
	case "SHA384":
		hashFunc = crypto.SHA384
	case "SHA512":
		hashFunc = crypto.SHA512
	case "SHA512/224":
		hashFunc = crypto.SHA512_224
	case "SHA512/256":
		hashFunc = crypto.SHA512_256
	default:
		return aoserrors.New("unknown or unsupported hashing algorithm: " + signHash)
	}

	hash := hashFunc.New()

	if _, err = io.Copy(hash, f); err != nil {
		log.Errorf("Error hashing file: %s", err)

		return aoserrors.Wrap(err)
	}

	hashValue := hash.Sum(nil)

	switch signAlgName {
	case "RSA":
		publicKey := signCert.PublicKey.(*rsa.PublicKey)

		switch signPadding {
		case "PKCS1v1_5":
			if err = rsa.VerifyPKCS1v15(publicKey, hashFunc.HashFunc(), hashValue, signValue); err != nil {
				return aoserrors.Wrap(err)
			}

		case "PSS":
			if err = rsa.VerifyPSS(publicKey, hashFunc.HashFunc(), hashValue, signValue, nil); err != nil {
				return aoserrors.Wrap(err)
			}

		default:
			return aoserrors.New("unknown scheme for RSA signature: " + signPadding)
		}

	default:
		return aoserrors.New("unknown or unsupported signature alg: " + signAlgName)
	}

	// Sign ok, verify certs

	intermediatePool := x509.NewCertPool()

	for _, certFingerprints := range chain.fingerprints[1:] {
		crt := ctx.getCertificateByFingerprint(certFingerprints)
		if crt == nil {
			return aoserrors.Errorf("cannot find certificate in chain fingerprint: %s", certFingerprints)
		}

		intermediatePool.AddCert(crt)
	}

	verifyOptions := x509.VerifyOptions{
		Intermediates: intermediatePool,
		Roots:         ctx.cryptoContext.rootCertPool,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	if _, err = signCert.Verify(verifyOptions); err != nil {
		log.Errorf("Error verifying certificate chain: %s", err)

		return aoserrors.Wrap(err)
	}

	return nil
}

// CreateSymmetricCipherContext creates symmetric cipher context
func CreateSymmetricCipherContext() (symContext *SymmetricCipherContext) {
	return &SymmetricCipherContext{}
}

// DecryptFile decrypts file
func (ctx *SymmetricCipherContext) DecryptFile(encryptedFile, clearFile *os.File) (err error) {
	if !ctx.isReady() {
		return aoserrors.New("symmetric key is not ready")
	}

	// Get file stat (we need to know file size)
	inputFileStat, err := encryptedFile.Stat()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	fileSize := inputFileStat.Size()
	if fileSize%int64(ctx.decrypter.BlockSize()) != 0 {
		return aoserrors.New("file size is incorrect")
	}

	if _, err = encryptedFile.Seek(0, io.SeekStart); err != nil {
		return aoserrors.Wrap(err)
	}

	chunkEncrypted := make([]byte, fileBlockSize)
	chunkDecrypted := make([]byte, fileBlockSize)
	totalReadSize := int64(0)

	for totalReadSize < fileSize {
		readSize, err := encryptedFile.Read(chunkEncrypted)
		if err != nil && err != io.EOF {
			return aoserrors.Wrap(err)
		}

		totalReadSize += int64(readSize)

		ctx.decrypter.CryptBlocks(chunkDecrypted[:readSize], chunkEncrypted[:readSize])

		if totalReadSize == fileSize {
			// Remove padding from the last block if needed
			padSize, err := ctx.getPaddingSize(chunkDecrypted[:readSize], readSize)
			if err != nil {
				return aoserrors.Wrap(err)
			}

			readSize -= padSize
		}

		// Write decrypted chunk to the out file.
		// We can remove padding, so we should use slice with computed size.
		if _, err = clearFile.Write(chunkDecrypted[:readSize]); err != nil {
			return aoserrors.Wrap(err)
		}
	}

	return nil
}

/***********************************************************************************************************************
 * Private
 **********************************************************************************************************************/

func (ctx *CryptoContext) loadPkcs11PrivateKey(keyURL *url.URL) (key crypto.PrivateKey, err error) {
	library, token, label, id, userPin, err := parsePkcs11Url(keyURL)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"label": label, "id": id}).Debug("Load PKCS11 certificate")

	pkcs11Ctx, err := ctx.getPkcs11Context(library, token, userPin)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if key, err = pkcs11Ctx.FindKeyPair([]byte(id), []byte(label)); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if key == nil {
		return nil, aoserrors.Errorf("private key label: %s, id: %s not found", label, id)
	}

	return key, nil
}

func (ctx *CryptoContext) loadPrivateKeyByURL(keyURLStr string) (privKey crypto.PrivateKey,
	supportPKCS1v15SessionKey bool, err error) {
	keyURL, err := url.Parse(keyURLStr)
	if err != nil {
		return nil, false, aoserrors.Wrap(err)
	}

	switch keyURL.Scheme {
	case cryptutils.SchemeFile:
		if privKey, err = cryptutils.LoadKey(keyURL.Path); err != nil {
			return nil, false, aoserrors.Wrap(err)
		}

		supportPKCS1v15SessionKey = true

	case cryptutils.SchemeTPM:
		if ctx.tpmDevice == nil {
			return nil, false, aoserrors.Errorf("TPM device is not configured")
		}

		var handle uint64

		if handle, err = strconv.ParseUint(keyURL.Hostname(), 0, 32); err != nil {
			return nil, false, aoserrors.Wrap(err)
		}

		if privKey, err = tpmkey.CreateFromPersistent(ctx.tpmDevice, tpmutil.Handle(handle)); err != nil {
			return nil, false, aoserrors.Wrap(err)
		}

	case cryptutils.SchemePKCS11:
		if privKey, err = ctx.loadPkcs11PrivateKey(keyURL); err != nil {
			return nil, false, aoserrors.Wrap(err)
		}

	default:
		return nil, false, aoserrors.Errorf("unsupported schema %s for private key", keyURL.Scheme)
	}

	return privKey, supportPKCS1v15SessionKey, nil
}

func getRawCertificate(certs []*x509.Certificate) (rawCerts [][]byte) {
	rawCerts = make([][]byte, 0, len(certs))

	for _, cert := range certs {
		rawCerts = append(rawCerts, cert.Raw)
	}

	return rawCerts
}

func parsePkcs11Url(pkcs11Url *url.URL) (library, token, label, id, userPin string, err error) {
	opaqueValues := make(map[string]string)

	for _, field := range strings.Split(pkcs11Url.Opaque, ";") {
		items := strings.Split(field, "=")
		if len(items) < 2 {
			continue
		}

		opaqueValues[items[0]] = items[1]
	}

	for name, value := range opaqueValues {
		switch name {
		case "token":
			token = value

		case "object":
			label = value

		case "id":
			id = value
		}
	}

	for name, item := range pkcs11Url.Query() {
		if len(item) == 0 {
			continue
		}

		switch name {
		case "module-path":
			library = item[0]

		case "pin-value":
			userPin = item[0]
		}
	}

	return library, token, label, id, userPin, nil
}

func (ctx *CryptoContext) getPkcs11Context(library, token, userPin string) (pkcs11Ctx *crypto11.Context, err error) {
	log.WithFields(log.Fields{"library": library, "token": token}).Debug("Get PKCS11 context")

	if library == "" && ctx.pkcs11Library == "" {
		return nil, aoserrors.New("PKCS11 library is not defined")
	}

	if library == "" {
		library = ctx.pkcs11Library
	}

	var (
		ok         bool
		pkcs11Desc = pkcs11Descriptor{library: library, token: token}
	)

	if pkcs11Ctx, ok = ctx.pkcs11Ctx[pkcs11Desc]; !ok {
		log.WithFields(log.Fields{"library": library, "token": token}).Debug("Create PKCS11 context")

		if pkcs11Ctx, err = crypto11.Configure(&crypto11.Config{Path: library, TokenLabel: token, Pin: userPin}); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		ctx.pkcs11Ctx[pkcs11Desc] = pkcs11Ctx
	}

	return pkcs11Ctx, nil
}

func (ctx *CryptoContext) loadPkcs11Certificate(certURL *url.URL) (certs []*x509.Certificate, err error) {
	library, token, label, id, userPin, err := parsePkcs11Url(certURL)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	log.WithFields(log.Fields{"label": label, "id": id}).Debug("Load PKCS11 certificate")

	pkcs11Ctx, err := ctx.getPkcs11Context(library, token, userPin)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if certs, err = pkcs11Ctx.FindCertificateChain([]byte(id), []byte(label), nil); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	if len(certs) == 0 {
		return nil, aoserrors.Errorf("certificate chain label: %s, id: %s not found", label, id)
	}

	return certs, nil
}

func (ctx *CryptoContext) loadCertificateByURL(certURLStr string) (certs []*x509.Certificate, err error) {
	certURL, err := url.Parse(certURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	switch certURL.Scheme {
	case cryptutils.SchemeFile:
		if certs, err = cryptutils.LoadCertificate(certURL.Path); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		return certs, nil

	case cryptutils.SchemePKCS11:
		if certs, err = ctx.loadPkcs11Certificate(certURL); err != nil {
			return nil, aoserrors.Wrap(err)
		}

		return certs, aoserrors.Wrap(err)

	default:
		return nil, aoserrors.Errorf("unsupported schema %s for certificate", certURL.Scheme)
	}
}

func (ctx *CryptoContext) getKeyForEnvelope(keyInfo keyTransRecipientInfo) (key []byte, err error) {
	issuer, err := asn1.Marshal(keyInfo.Rid.Issuer)
	if err != nil {
		return key, aoserrors.Wrap(err)
	}

	_, keyURLStr, err := ctx.certProvider.GetCertificate(offlineCertificate, issuer, fmt.Sprintf("%X", keyInfo.Rid.SerialNumber))
	if err != nil {
		return key, aoserrors.Wrap(err)
	}

	privKey, _, err := ctx.loadPrivateKeyByURL(keyURLStr)
	if err != nil {
		return key, aoserrors.Wrap(err)
	}

	decrypter, ok := privKey.(crypto.Decrypter)
	if !ok {
		return nil, aoserrors.New("private key doesn't have a decryption suite")
	}

	if key, err = decryptCMSKey(&keyInfo, decrypter); err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return key, nil
}

func (ctx *SymmetricCipherContext) generateKeyAndIV(algString string) (err error) {
	// Get alg name
	algName, _, _ := decodeAlgNames(algString)

	// Get alg key and IV size in bytes
	keySize, ivSize, err := getSymmetricAlgInfo(algName)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	// Allocate memory and generate cryptographically resistant random values
	key := make([]byte, keySize)
	iv := make([]byte, ivSize)

	if _, err := rand.Read(key); err != nil {
		return aoserrors.Wrap(err)
	}

	if _, err := rand.Read(iv); err != nil {
		return aoserrors.Wrap(err)
	}

	// Check and set values
	return aoserrors.Wrap(ctx.set(algString, key, iv))
}

func (ctx *SignContext) getCertificateByFingerprint(fingerprint string) (cert *x509.Certificate) {
	// Find certificate in the chain
	for _, certTmp := range ctx.signCertificates {
		if certTmp.fingerprint == fingerprint {
			return certTmp.certificate
		}
	}

	return nil
}

func (ctx *SymmetricCipherContext) encryptFile(clearFile, encryptedFile *os.File) (err error) {
	if !ctx.isReady() {
		return aoserrors.New("symmetric key is not ready")
	}

	// Get file stat (we need to know file size)
	inputFileStat, err := clearFile.Stat()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	fileSize := inputFileStat.Size()
	writtenSize := int64(0)
	readSize := 0

	if _, err = clearFile.Seek(0, io.SeekStart); err != nil {
		return aoserrors.Wrap(err)
	}

	// We need more space if the input file size proportional to the fileBlockSize
	chunkEncrypted := make([]byte, fileBlockSize+128)
	chunkClear := make([]byte, fileBlockSize+128)

	for i := 0; i <= int(fileSize/fileBlockSize); i++ {
		currentChunkSize := fileBlockSize

		if writtenSize+int64(currentChunkSize) > fileSize {
			// The last block may need a padding appending
			currentChunkSize = int(fileSize - writtenSize)
			if _, err = clearFile.Read(chunkClear[:currentChunkSize]); err != nil {
				return aoserrors.Wrap(err)
			}
			readSize, err = ctx.appendPadding(chunkClear, currentChunkSize)
			if err != nil {
				return aoserrors.Wrap(err)
			}
		} else {
			if readSize, err = clearFile.Read(chunkClear[:fileBlockSize]); err != nil {
				return aoserrors.Wrap(err)
			}
		}

		ctx.encrypter.CryptBlocks(chunkEncrypted, chunkClear[:readSize])

		if _, err = encryptedFile.Write(chunkEncrypted[:readSize]); err != nil {
			return aoserrors.Wrap(err)
		}

		writtenSize += int64(readSize)
	}

	return aoserrors.Wrap(encryptedFile.Sync())
}

func (ctx *SymmetricCipherContext) set(algString string, key []byte, iv []byte) error {
	if algString == "" {
		return aoserrors.New("construct symmetric alg context: empty conf string")
	}

	ctx.key = key
	ctx.iv = iv

	ctx.setAlg(algString)

	return aoserrors.Wrap(ctx.loadKey())
}

func (ctx *SymmetricCipherContext) setAlg(algString string) {
	ctx.algName, ctx.modeName, ctx.paddingName = decodeAlgNames(algString)
}

func (ctx *SymmetricCipherContext) appendPadding(dataIn []byte, dataLen int) (fullSize int, err error) {
	switch strings.ToUpper(ctx.paddingName) {
	case "PKCS7PADDING", "PKCS7":
		if fullSize, err = ctx.appendPkcs7Padding(dataIn, dataLen); err != nil {
			return 0, aoserrors.Wrap(err)
		}

		return fullSize, nil

	default:
		return 0, aoserrors.New("unsupported padding type")
	}
}

func (ctx *SymmetricCipherContext) getPaddingSize(dataIn []byte, dataLen int) (removedSize int, err error) {
	switch strings.ToUpper(ctx.paddingName) {
	case "PKCS7PADDING", "PKCS7":
		if removedSize, err = ctx.removePkcs7Padding(dataIn, dataLen); err != nil {
			return 0, aoserrors.Wrap(err)
		}

		return removedSize, nil

	default:
		return 0, aoserrors.New("unsupported padding type")
	}
}

func (ctx *SymmetricCipherContext) appendPkcs7Padding(dataIn []byte, dataLen int) (fullSize int, err error) {
	blockSize := ctx.encrypter.BlockSize()
	appendSize := blockSize - (dataLen % blockSize)

	fullSize = dataLen + appendSize
	if dataLen+appendSize > len(dataIn) {
		return 0, aoserrors.New("no enough space to add padding")
	}

	for i := dataLen; i < fullSize; i++ {
		dataIn[i] = byte(appendSize)
	}

	return fullSize, nil
}

func (ctx *SymmetricCipherContext) removePkcs7Padding(dataIn []byte, dataLen int) (removedSize int, err error) {
	blockLen := ctx.decrypter.BlockSize()

	if dataLen%blockLen != 0 || dataLen == 0 {
		return 0, aoserrors.New("padding error (invalid total size)")
	}

	removedSize = int(dataIn[dataLen-1])

	if removedSize < 1 || removedSize > blockLen || removedSize > dataLen {
		return 0, aoserrors.New("padding error")
	}

	for i := dataLen - removedSize; i < dataLen; i++ {
		if dataIn[i] != byte(removedSize) {
			return 0, aoserrors.New("padding error")
		}
	}

	return removedSize, nil
}

func (ctx *SymmetricCipherContext) loadKey() (err error) {
	var block cipher.Block
	keySizeBits := 0

	switch strings.ToUpper(ctx.algName) {
	case "AES128", "AES192", "AES256":
		if keySizeBits, err = strconv.Atoi(ctx.algName[3:]); err != nil {
			return aoserrors.Wrap(err)
		}

		if keySizeBits/8 != len(ctx.key) {
			return aoserrors.New("invalid symmetric key size")
		}

		block, err = aes.NewCipher(ctx.key)
		if err != nil {
			log.Errorf("can't create cipher: %s", err)

			return aoserrors.Wrap(err)
		}

	default:
		return aoserrors.New("unsupported cryptographic algorithm: " + ctx.algName)
	}

	if len(ctx.iv) != block.BlockSize() {
		return aoserrors.New("invalid IV size")
	}

	switch ctx.modeName {
	case "CBC":
		ctx.decrypter = cipher.NewCBCDecrypter(block, ctx.iv)
		ctx.encrypter = cipher.NewCBCEncrypter(block, ctx.iv)

	default:
		return aoserrors.New("unsupported encryption mode: " + ctx.modeName)
	}

	return nil
}

func (ctx *SymmetricCipherContext) isReady() bool {
	return ctx.encrypter != nil || ctx.decrypter != nil
}
