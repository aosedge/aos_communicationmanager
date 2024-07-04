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
	"crypto"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aosedge/aos_common/aoserrors"
	"github.com/aosedge/aos_common/api/cloudprotocol"
	"github.com/aosedge/aos_common/utils/contextreader"
	"github.com/aosedge/aos_common/utils/cryptutils"
	log "github.com/sirupsen/logrus"
)

const (
	fileBlockSize = 64 * 1024
)

const (
	onlineCertificate  = "online"
	offlineCertificate = "offline"
)

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

//nolint:gochecknoglobals // use as const
var issuerAltNameExtID = asn1.ObjectIdentifier{2, 5, 29, 18}

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

// ReceiverInfo receiver info.
type ReceiverInfo struct {
	Serial string
	Issuer []byte
}

// CryptoSessionKeyInfo crypto session key info.
type CryptoSessionKeyInfo struct {
	SessionKey        []byte       `json:"sessionKey"`
	SessionIV         []byte       `json:"sessionIv"`
	SymmetricAlgName  string       `json:"symmetricAlgName"`
	AsymmetricAlgName string       `json:"asymmetricAlgName"`
	ReceiverInfo      ReceiverInfo `json:"recipientInfo"`
}

// CryptoHandler crypto handler.
type CryptoHandler struct {
	certProvider        CertificateProvider
	cryptoContext       *cryptutils.CryptoContext
	serviceDiscoveryURL string
}

// SymmetricContextInterface interface for SymmetricCipherContext.
type SymmetricContextInterface interface {
	DecryptFile(ctx context.Context, encryptedFile, clearFile *os.File) (err error)
}

// SymmetricCipherContext symmetric cipher context.
type SymmetricCipherContext struct {
	key         []byte
	iv          []byte
	algName     string
	modeName    string
	paddingName string
	decrypter   cipher.BlockMode
	encrypter   cipher.BlockMode
}

// SignContext sign context.
type SignContext struct {
	handler               *CryptoHandler
	signCertificates      []certificateInfo
	signCertificateChains []certificateChainInfo
}

// SignContextInterface interface for SignContext.
type SignContextInterface interface {
	AddCertificate(fingerprint string, asn1Bytes []byte) (err error)
	AddCertificateChain(name string, fingerprints []string) (err error)
	VerifySign(ctx context.Context, f *os.File, sign cloudprotocol.Signs) (err error)
}

// CertificateProvider interface to get certificate.
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

// DecryptParams contains necessary parameters for decryption.
type DecryptParams struct {
	Chains         []cloudprotocol.CertificateChain
	Certs          []cloudprotocol.Certificate
	DecryptionInfo cloudprotocol.DecryptionInfo
	Signs          cloudprotocol.Signs
}

/***********************************************************************************************************************
 * Public
 **********************************************************************************************************************/

// New create context for crypto operations.
func New(
	provider CertificateProvider, cryptocontext *cryptutils.CryptoContext, serviceDiscoveryURL string,
) (handler *CryptoHandler, err error) {
	handler = &CryptoHandler{
		certProvider:        provider,
		cryptoContext:       cryptocontext,
		serviceDiscoveryURL: serviceDiscoveryURL,
	}

	return handler, nil
}

// GetServiceDiscoveryURLs returns service discovery URLs.
func (handler *CryptoHandler) GetServiceDiscoveryURLs() (serviceDiscoveryURLs []string) {
	defer func() {
		if len(serviceDiscoveryURLs) == 0 {
			log.Warning("Service discovery URL can't be found in certificate and will be used from config")

			serviceDiscoveryURLs = append(serviceDiscoveryURLs, handler.serviceDiscoveryURL)
		}
	}()

	certs, err := handler.getOnlineCert()
	if err != nil || len(certs) == 0 {
		return nil
	}

	if serviceDiscoveryURLs, err = handler.getServiceDiscoveryFromExtensions(certs[0]); err == nil {
		return serviceDiscoveryURLs
	}

	if serviceDiscoveryURLs, err = handler.getServiceDiscoveryFromOrgName(certs[0]); err == nil {
		return serviceDiscoveryURLs
	}

	return nil
}

// GetCertSerial returns certificate serial number.
func (handler *CryptoHandler) GetCertSerial(certURL string) (serial string, err error) {
	certs, err := handler.cryptoContext.LoadCertificateByURL(certURL)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	return fmt.Sprintf("%X", certs[0].SerialNumber), nil
}

// CreateSignContext creates sign context.
func (handler *CryptoHandler) CreateSignContext() (signContext SignContextInterface, err error) {
	if handler == nil || handler.cryptoContext.GetCACertPool() == nil {
		return nil, aoserrors.New("asymmetric context not initialized")
	}

	return &SignContext{handler: handler}, nil
}

// GetTLSConfig Provides TLS configuration for HTTPS client.
func (handler *CryptoHandler) GetTLSConfig() (cfg *tls.Config, err error) {
	cfg = &tls.Config{MinVersion: tls.VersionTLS12}

	certURLStr, keyURLStr, err := handler.certProvider.GetCertificate(onlineCertificate, nil, "")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	clientCert, err := handler.cryptoContext.LoadCertificateByURL(certURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	onlinePrivate, _, err := handler.cryptoContext.LoadPrivateKeyByURL(keyURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	cfg.RootCAs = handler.cryptoContext.GetCACertPool()
	cfg.Certificates = []tls.Certificate{{PrivateKey: onlinePrivate, Certificate: getRawCertificate(clientCert)}}
	cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) (err error) {
		return nil
	}

	return cfg, nil
}

// DecryptAndValidate decrypts and validates encrypted image.
func (handler *CryptoHandler) DecryptAndValidate(
	encryptedFile, decryptedFile string, params DecryptParams,
) (err error) {
	defer func() {
		if err != nil {
			os.RemoveAll(decryptedFile)
		}
	}()

	if err = handler.decrypt(encryptedFile, decryptedFile, &params); err != nil {
		return err
	}

	if err = handler.validateSigns(decryptedFile, &params); err != nil {
		return err
	}

	return nil
}

// DecryptMetadata decrypt envelope.
func (handler *CryptoHandler) DecryptMetadata(input []byte) (output []byte, err error) {
	ci, err := unmarshallCMS(input)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	for _, recipient := range ci.EnvelopedData.RecipientInfos {
		dkey, err := handler.getKeyForEnvelope(recipient.(keyTransRecipientInfo))
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

// ImportSessionKey function retrieves a symmetric key from crypto context.
func (handler *CryptoHandler) ImportSessionKey(
	keyInfo CryptoSessionKeyInfo,
) (symContext SymmetricContextInterface, err error) {
	_, keyURLStr, err := handler.certProvider.GetCertificate(
		offlineCertificate, keyInfo.ReceiverInfo.Issuer, keyInfo.ReceiverInfo.Serial)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	privKey, supportPKCS1v15SessionKey, err := handler.cryptoContext.LoadPrivateKeyByURL(keyURLStr)
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

	//nolint:goconst
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

// AddCertificate adds certificate to context.
func (signContext *SignContext) AddCertificate(fingerprint string, asn1Bytes []byte) error {
	fingerUpper := strings.ToUpper(fingerprint)

	// Check certificate presents
	for _, item := range signContext.signCertificates {
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
	signContext.signCertificates = append(
		signContext.signCertificates, certificateInfo{fingerprint: fingerUpper, certificate: cert})

	return nil
}

// AddCertificateChain adds certificate chain to context.
func (signContext *SignContext) AddCertificateChain(name string, fingerprints []string) error {
	if len(fingerprints) == 0 {
		return aoserrors.New("can't add certificate chain with empty fingerprint list")
	}

	// Check certificate presents
	for _, item := range signContext.signCertificateChains {
		if item.name == name {
			return nil
		}
	}

	signContext.signCertificateChains = append(
		signContext.signCertificateChains, certificateChainInfo{name, fingerprints})

	return nil
}

// VerifySign verifies signature.
func (signContext *SignContext) VerifySign(
	ctx context.Context, f *os.File, sign cloudprotocol.Signs,
) (err error) {
	if len(signContext.signCertificateChains) == 0 || len(signContext.signCertificates) == 0 {
		return aoserrors.New("sign context not initialized (no certificates)")
	}

	signCert, chain, err := signContext.getSignCertificate(sign.ChainName)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	signAlgName, signHash, signPadding := decodeSignAlgNames(sign.Alg)

	hashFunc, err := getHashFuncBySignHash(signHash)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	hash := hashFunc.New()
	if _, err = io.Copy(hash, contextreader.New(ctx, f)); err != nil {
		log.Errorf("Error hashing file: %s", err)

		return aoserrors.Wrap(err)
	}

	switch signAlgName {
	case "RSA":
		publicKey, ok := signCert.PublicKey.(*rsa.PublicKey)
		if !ok {
			return aoserrors.New("incorrect RSA public key data type")
		}

		switch signPadding {
		case "PKCS1v1_5":
			if err = rsa.VerifyPKCS1v15(publicKey, hashFunc.HashFunc(), hash.Sum(nil), sign.Value); err != nil {
				return aoserrors.Wrap(err)
			}

		case "PSS":
			if err = rsa.VerifyPSS(publicKey, hashFunc.HashFunc(), hash.Sum(nil), sign.Value, nil); err != nil {
				return aoserrors.Wrap(err)
			}

		default:
			return aoserrors.New("unknown scheme for RSA signature: " + signPadding)
		}

	default:
		return aoserrors.New("unknown or unsupported signature alg: " + signAlgName)
	}

	// Sign ok, verify certs

	intermediatePool, err := signContext.getIntermediateCertPool(chain)
	if err != nil {
		return err
	}

	signTime, err := time.Parse(time.RFC3339, sign.TrustedTimestamp)
	if err != nil {
		return aoserrors.Wrap(err)
	}

	verifyOptions := x509.VerifyOptions{
		CurrentTime:   signTime,
		Intermediates: intermediatePool,
		Roots:         signContext.handler.cryptoContext.GetCACertPool(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
	}

	if _, err = signCert.Verify(verifyOptions); err != nil {
		log.Errorf("Error verifying certificate chain: %s", err)

		return aoserrors.Wrap(err)
	}

	return nil
}

// CreateSymmetricCipherContext creates symmetric cipher context.
func CreateSymmetricCipherContext() (symContext *SymmetricCipherContext) {
	return &SymmetricCipherContext{}
}

// DecryptFile decrypts file.
func (symmetricContext *SymmetricCipherContext) DecryptFile(
	ctx context.Context, encryptedFile, clearFile *os.File,
) (err error) {
	if !symmetricContext.isReady() {
		return aoserrors.New("symmetric key is not ready")
	}

	// Get file stat (we need to know file size)
	inputFileStat, err := encryptedFile.Stat()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	fileSize := inputFileStat.Size()
	if fileSize%int64(symmetricContext.decrypter.BlockSize()) != 0 {
		return aoserrors.New("file size is incorrect")
	}

	if _, err = encryptedFile.Seek(0, io.SeekStart); err != nil {
		return aoserrors.Wrap(err)
	}

	chunkEncrypted := make([]byte, fileBlockSize)
	chunkDecrypted := make([]byte, fileBlockSize)
	totalReadSize := int64(0)

	contextReader := contextreader.New(ctx, encryptedFile)

	for totalReadSize < fileSize {
		readSize, err := contextReader.Read(chunkEncrypted)
		if err != nil && !errors.Is(err, io.EOF) {
			return aoserrors.Wrap(err)
		}

		totalReadSize += int64(readSize)

		symmetricContext.decrypter.CryptBlocks(chunkDecrypted[:readSize], chunkEncrypted[:readSize])

		if totalReadSize == fileSize {
			// Remove padding from the last block if needed
			padSize, err := symmetricContext.getPaddingSize(chunkDecrypted[:readSize], readSize)
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

func (handler *CryptoHandler) decrypt(encryptedFile, decryptedFile string, params *DecryptParams) (err error) {
	symmetricCtx, err := handler.ImportSessionKey(CryptoSessionKeyInfo{
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

func (handler *CryptoHandler) validateSigns(decryptedFile string, params *DecryptParams) (err error) {
	signCtx, err := handler.CreateSignContext()
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

func getRawCertificate(certs []*x509.Certificate) (rawCerts [][]byte) {
	rawCerts = make([][]byte, 0, len(certs))

	for _, cert := range certs {
		rawCerts = append(rawCerts, cert.Raw)
	}

	return rawCerts
}

func (handler *CryptoHandler) getOnlineCert() ([]*x509.Certificate, error) {
	certURLStr, _, err := handler.certProvider.GetCertificate(onlineCertificate, nil, "")
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	certs, err := handler.cryptoContext.LoadCertificateByURL(certURLStr)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	return certs, nil
}

func (handler *CryptoHandler) getKeyForEnvelope(keyInfo keyTransRecipientInfo) (key []byte, err error) {
	issuer, err := asn1.Marshal(keyInfo.Rid.Issuer)
	if err != nil {
		return key, aoserrors.Wrap(err)
	}

	_, keyURLStr, err := handler.certProvider.GetCertificate(
		offlineCertificate, issuer, fmt.Sprintf("%X", keyInfo.Rid.SerialNumber))
	if err != nil {
		return key, aoserrors.Wrap(err)
	}

	privKey, _, err := handler.cryptoContext.LoadPrivateKeyByURL(keyURLStr)
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

func (handler *CryptoHandler) getServiceDiscoveryFromExtensions(
	cert *x509.Certificate,
) (serviceDiscoveryURLs []string, err error) {
	for _, ext := range cert.Extensions {
		if !issuerAltNameExtID.Equal(ext.Id) {
			continue
		}

		var aosNames asn1.RawValue

		rest, err := asn1.Unmarshal(ext.Value, &aosNames)
		if err != nil {
			return nil, aoserrors.Wrap(err)
		}

		if len(rest) != 0 {
			return nil, aoserrors.New("x509: trailing data after X.509 authority information")
		}

		rest = aosNames.Bytes

		for len(rest) > 0 {
			var aosName asn1.RawValue

			rest, err = asn1.Unmarshal(rest, &aosName)
			if err != nil {
				return nil, aoserrors.Wrap(err)
			}

			if aosName.Tag == asn1.TagOID {
				serviceDiscoveryURLs = append(serviceDiscoveryURLs, string(aosName.Bytes))
			}
		}
	}

	if len(serviceDiscoveryURLs) == 0 {
		return nil, aoserrors.New("URL is not in the certificate")
	}

	return serviceDiscoveryURLs, nil
}

func (handler *CryptoHandler) getServiceDiscoveryFromOrgName(
	cert *x509.Certificate,
) (serviceDiscoveryURLs []string, err error) {
	if cert.Subject.Organization == nil {
		return nil, aoserrors.New("certificate does not have organizations")
	}

	if len(cert.Subject.Organization) == 0 || cert.Subject.Organization[0] == "" {
		return nil, aoserrors.New("URLs are not in the certificate")
	}

	url := url.URL{
		Scheme: "https",
		Host:   cert.Subject.Organization[0],
	}

	return append(serviceDiscoveryURLs, url.String()+":9000"), nil
}

func (symmetricContext *SymmetricCipherContext) generateKeyAndIV(algString string) (err error) {
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
	return aoserrors.Wrap(symmetricContext.set(algString, key, iv))
}

func (symmetricContext *SymmetricCipherContext) encryptFile(
	ctx context.Context, clearFile, encryptedFile *os.File,
) (err error) {
	if !symmetricContext.isReady() {
		return aoserrors.New("symmetric key is not ready")
	}

	// Get file stat (we need to know file size)
	inputFileStat, err := clearFile.Stat()
	if err != nil {
		return aoserrors.Wrap(err)
	}

	var (
		fileSize    = inputFileStat.Size()
		writtenSize = int64(0)
		readSize    int
	)

	if _, err = clearFile.Seek(0, io.SeekStart); err != nil {
		return aoserrors.Wrap(err)
	}

	contextReader := contextreader.New(ctx, clearFile)

	// We need more space if the input file size proportional to the fileBlockSize
	chunkEncrypted := make([]byte, fileBlockSize+128)
	chunkClear := make([]byte, fileBlockSize+128)

	for i := 0; i <= int(fileSize/fileBlockSize); i++ {
		currentChunkSize := fileBlockSize

		if writtenSize+int64(currentChunkSize) > fileSize {
			// The last block may need a padding appending
			currentChunkSize = int(fileSize - writtenSize)
			if _, err = contextReader.Read(chunkClear[:currentChunkSize]); err != nil {
				return aoserrors.Wrap(err)
			}

			readSize, err = symmetricContext.appendPadding(chunkClear, currentChunkSize)
			if err != nil {
				return aoserrors.Wrap(err)
			}
		} else {
			if readSize, err = contextReader.Read(chunkClear[:fileBlockSize]); err != nil {
				return aoserrors.Wrap(err)
			}
		}

		symmetricContext.encrypter.CryptBlocks(chunkEncrypted, chunkClear[:readSize])

		if _, err = encryptedFile.Write(chunkEncrypted[:readSize]); err != nil {
			return aoserrors.Wrap(err)
		}

		writtenSize += int64(readSize)
	}

	return aoserrors.Wrap(encryptedFile.Sync())
}

func (symmetricContext *SymmetricCipherContext) set(algString string, key []byte, iv []byte) error {
	if algString == "" {
		return aoserrors.New("construct symmetric alg context: empty conf string")
	}

	symmetricContext.key = key
	symmetricContext.iv = iv

	symmetricContext.setAlg(algString)

	return aoserrors.Wrap(symmetricContext.loadKey())
}

func (symmetricContext *SymmetricCipherContext) setAlg(algString string) {
	symmetricContext.algName, symmetricContext.modeName, symmetricContext.paddingName = decodeAlgNames(algString)
}

func (symmetricContext *SymmetricCipherContext) appendPadding(dataIn []byte, dataLen int) (fullSize int, err error) {
	switch strings.ToUpper(symmetricContext.paddingName) {
	//nolint:goconst
	case "PKCS7PADDING", "PKCS7":
		if fullSize, err = symmetricContext.appendPkcs7Padding(dataIn, dataLen); err != nil {
			return 0, aoserrors.Wrap(err)
		}

		return fullSize, nil

	default:
		return 0, aoserrors.New("unsupported padding type")
	}
}

func (symmetricContext *SymmetricCipherContext) getPaddingSize(dataIn []byte, dataLen int) (
	removedSize int, err error,
) {
	switch strings.ToUpper(symmetricContext.paddingName) {
	case "PKCS7PADDING", "PKCS7":
		if removedSize, err = symmetricContext.removePkcs7Padding(dataIn, dataLen); err != nil {
			return 0, aoserrors.Wrap(err)
		}

		return removedSize, nil

	default:
		return 0, aoserrors.New("unsupported padding type")
	}
}

func (symmetricContext *SymmetricCipherContext) appendPkcs7Padding(dataIn []byte, dataLen int) (
	fullSize int, err error,
) {
	blockSize := symmetricContext.encrypter.BlockSize()
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

func (symmetricContext *SymmetricCipherContext) removePkcs7Padding(dataIn []byte, dataLen int) (
	removedSize int, err error,
) {
	blockLen := symmetricContext.decrypter.BlockSize()

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

func (symmetricContext *SymmetricCipherContext) loadKey() (err error) {
	var (
		block       cipher.Block
		keySizeBits int
	)

	switch strings.ToUpper(symmetricContext.algName) {
	case "AES128", "AES192", "AES256":
		if keySizeBits, err = strconv.Atoi(symmetricContext.algName[3:]); err != nil {
			return aoserrors.Wrap(err)
		}

		if keySizeBits/8 != len(symmetricContext.key) {
			return aoserrors.New("invalid symmetric key size")
		}

		block, err = aes.NewCipher(symmetricContext.key)
		if err != nil {
			log.Errorf("can't create cipher: %s", err)

			return aoserrors.Wrap(err)
		}

	default:
		return aoserrors.New("unsupported cryptographic algorithm: " + symmetricContext.algName)
	}

	if len(symmetricContext.iv) != block.BlockSize() {
		return aoserrors.New("invalid IV size")
	}

	switch symmetricContext.modeName {
	case "CBC":
		symmetricContext.decrypter = cipher.NewCBCDecrypter(block, symmetricContext.iv)
		symmetricContext.encrypter = cipher.NewCBCEncrypter(block, symmetricContext.iv)

	default:
		return aoserrors.New("unsupported encryption mode: " + symmetricContext.modeName)
	}

	return nil
}

func (symmetricContext *SymmetricCipherContext) isReady() bool {
	return symmetricContext.encrypter != nil || symmetricContext.decrypter != nil
}

func (signContext *SignContext) getCertificateByFingerprint(fingerprint string) (cert *x509.Certificate) {
	// Find certificate in the chain
	for _, certTmp := range signContext.signCertificates {
		if certTmp.fingerprint == fingerprint {
			return certTmp.certificate
		}
	}

	return nil
}

func (signContext *SignContext) getSignCertificate(
	chainName string,
) (signCert *x509.Certificate, chain certificateChainInfo, err error) {
	var signCertFingerprint string

	// Find chain
	for _, chainTmp := range signContext.signCertificateChains {
		if chainTmp.name == chainName {
			chain = chainTmp
			signCertFingerprint = chain.fingerprints[0]

			break
		}
	}

	if chain.name == "" || len(chain.name) == 0 {
		return nil, chain, aoserrors.New("unknown chain name")
	}

	signCert = signContext.getCertificateByFingerprint(signCertFingerprint)

	if signCert == nil {
		return nil, chain, aoserrors.New("signing certificate is absent")
	}

	return signCert, chain, nil
}

func (signContext *SignContext) getIntermediateCertPool(chain certificateChainInfo) (*x509.CertPool, error) {
	intermediatePool := x509.NewCertPool()

	for _, certFingerprints := range chain.fingerprints[1:] {
		crt := signContext.getCertificateByFingerprint(certFingerprints)
		if crt == nil {
			return nil, aoserrors.Errorf("cannot find certificate in chain fingerprint: %v", certFingerprints)
		}

		intermediatePool.AddCert(crt)
	}

	return intermediatePool, nil
}

func getHashFuncBySignHash(hash string) (hashFunc crypto.Hash, err error) {
	switch strings.ToUpper(hash) {
	case "SHA256":
		return crypto.SHA256, nil
	case "SHA384":
		return crypto.SHA384, nil
	case "SHA512":
		return crypto.SHA512, nil
	case "SHA512/224":
		return crypto.SHA512_224, nil
	case "SHA512/256":
		return crypto.SHA512_256, nil
	default:
		return hashFunc, aoserrors.New("unknown or unsupported hashing algorithm: " + hash)
	}
}
