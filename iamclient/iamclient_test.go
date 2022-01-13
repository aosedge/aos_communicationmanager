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

package iamclient_test

import (
	"context"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/aoscloud/aos_common/aoserrors"
	pb "github.com/aoscloud/aos_common/api/iamanager/v1"
	"github.com/aoscloud/aos_common/utils/cryptutils"
	"github.com/golang/protobuf/ptypes/empty"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/aoscloud/aos_communicationmanager/cloudprotocol"
	"github.com/aoscloud/aos_communicationmanager/config"
	"github.com/aoscloud/aos_communicationmanager/iamclient"
)

/***********************************************************************************************************************
 * Consts
 **********************************************************************************************************************/

const (
	serverURL     = "localhost:8089"
	secretLength  = 8
	secretSymbols = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

/***********************************************************************************************************************
 * Types
 **********************************************************************************************************************/

type testServer struct {
	pb.UnimplementedIAMProtectedServiceServer
	pb.UnimplementedIAMPublicServiceServer

	grpcServer          *grpc.Server
	systemID            string
	users               []string
	usersChangedChannel chan []string
	csr                 map[string]string
	certURL             map[string]string
	keyURL              map[string]string
	permissionsCache    map[string]servicePermissions
}

type testSender struct {
	csr    map[string]string
	serial map[string]string
}

type testCertProvider struct{}

type servicePermissions struct {
	serviceID   string
	permissions map[string]map[string]string
}

/***********************************************************************************************************************
 * Vars
 **********************************************************************************************************************/

var tmpDir string

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

	tmpDir, err = ioutil.TempDir("", "iam_")
	if err != nil {
		log.Fatalf("Error create temporary dir: %s", err)
	}

	ret := m.Run()

	if err := os.RemoveAll(tmpDir); err != nil {
		log.Fatalf("Error removing tmp dir: %s", err)
	}

	os.Exit(ret)
}

/***********************************************************************************************************************
 * Tests
 **********************************************************************************************************************/

func TestGetSystemID(t *testing.T) {
	server, err := newTestServer(serverURL)
	if err != nil {
		t.Fatalf("Can't create test server: %s", err)
	}

	defer server.close()

	server.systemID = "testID"

	client, err := iamclient.New(&config.Config{IAMServerURL: serverURL}, &testSender{}, true)
	if err != nil {
		t.Fatalf("Can't create IAM client: %s", err)
	}
	defer client.Close()

	if client.GetSystemID() != server.systemID {
		t.Errorf("Invalid system ID: %s", client.GetSystemID())
	}
}

func TestGetUsers(t *testing.T) {
	server, err := newTestServer(serverURL)
	if err != nil {
		t.Fatalf("Can't create test server: %s", err)
	}

	defer server.close()

	server.users = []string{"user1", "user2", "user3"}

	client, err := iamclient.New(&config.Config{IAMServerURL: serverURL}, &testSender{}, true)
	if err != nil {
		t.Fatalf("Can't create IAM client: %s", err)
	}
	defer client.Close()

	if !reflect.DeepEqual(server.users, client.GetUsers()) {
		t.Errorf("Invalid users: %s", client.GetUsers())
	}

	newUsers := []string{"newUser1", "newUser2", "newUser3"}

	server.usersChangedChannel <- newUsers

	select {
	case users := <-client.UsersChangedChannel():
		if !reflect.DeepEqual(users, newUsers) {
			t.Errorf("Invalid users: %s", users)
		}

	case <-time.After(5 * time.Second):
		t.Error("Wait users changed timeout")
	}
}

func TestRenewCertificatesNotification(t *testing.T) {
	sender := &testSender{}

	server, err := newTestServer(serverURL)
	if err != nil {
		t.Fatalf("Can't create test server: %s", err)
	}

	defer server.close()

	server.csr = map[string]string{"online": "onlineCSR", "offline": "offlineCSR"}

	client, err := iamclient.New(&config.Config{IAMServerURL: serverURL}, sender, true)
	if err != nil {
		t.Fatalf("Can't create IAM client: %s", err)
	}
	defer client.Close()

	certInfo := []cloudprotocol.RenewCertData{
		{Type: "online", Serial: "serail1", ValidTill: time.Now()},
		{Type: "offline", Serial: "serail2", ValidTill: time.Now()},
	}

	if err = client.RenewCertificatesNotification("pwd", certInfo); err != nil {
		t.Fatalf("Can't process renew certificate notification: %s", err)
	}

	if !reflect.DeepEqual(server.csr, sender.csr) {
		t.Errorf("Wrong sender CSR: %v", sender.csr)
	}
}

func TestInstallCertificates(t *testing.T) {
	sender := &testSender{}

	server, err := newTestServer(serverURL)
	if err != nil {
		t.Fatalf("Can't create test server: %s", err)
	}

	defer server.close()

	// openssl req -newkey rsa:2048 -nodes -keyout online_key.pem -x509 -days 365 -out online_cert.pem -set_serial 1
	onlineCert := `
-----BEGIN CERTIFICATE-----
MIIDTjCCAjagAwIBAgIBATANBgkqhkiG9w0BAQsFADBAMQswCQYDVQQGEwJVQTET
MBEGA1UECAwKU29tZS1TdGF0ZTENMAsGA1UEBwwES3lpdjENMAsGA1UECgwERVBB
TTAeFw0yMDA5MTAxNDE1MzNaFw0yMTA5MTAxNDE1MzNaMEAxCzAJBgNVBAYTAlVB
MRMwEQYDVQQIDApTb21lLVN0YXRlMQ0wCwYDVQQHDARLeWl2MQ0wCwYDVQQKDARF
UEFNMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvc1utrAEKlJ5rB9W
envNiEjGDW36NKmEc69nITCmI7wedg00oRSIOZTfZVdKp/Nsh0uuFnOYKt9unXso
fYGzCk4KxWL+t9HDsWMbpL7S/QNB1UF7P+rQFKp7gsj8wQmy+rpvRxwuTC7BfpJ9
az9WoreF4W43m3zDF4lIetnFDilx1NBYBfvW7/KW3e/iJcjs8WPSQCDVU3rOhFRd
8Y/qt4EOiJY5xTya0YFxgF37fKH/asy+ija54Wy7DhbLlkyE2JUqxp/SaomzUAQX
uf/kWYPh/s7VewjfW9xouT9aDLZEsLNQMEk6HRPY9DW2pCdYFjE8qdVYAs/f0V5y
QhPUaQIDAQABo1MwUTAdBgNVHQ4EFgQUCwq4ojzTld4lTka0POLVqgkyMJ4wHwYD
VR0jBBgwFoAUCwq4ojzTld4lTka0POLVqgkyMJ4wDwYDVR0TAQH/BAUwAwEB/zAN
BgkqhkiG9w0BAQsFAAOCAQEAIDdNqOeK4JyZtETgfwj5gqneZ9EH+euJ6HaOict8
7PZzyEt2p0QHZOWBm0s07kd+7SiS+UCLIYjJPCMWDAEN7V6zZqqp9oQL5bR1hSSe
7tDrOgVkEf8T1x4u4F2bpLJsz+GDX7t8H5lcvPGq0YkPDHluSASFJc//GN3TrSDV
yGuI5R8+7XalVAaCteo/Y2zhERMDsVm0KTGeTP2sblaBVFux7GTcWA1DHqVDYFnw
ExQScaNHy16y+a/nlDSpTraFIQdSG3twwlfyjR/Ua5SbkzzTEVG+Kll/3VvOagwV
8xTUZedoJTC7G5C2DGs+syl/B8WVrn7QPK+VSVU2QEG50Q==
-----END CERTIFICATE-----
`

	// openssl req -newkey rsa:2048 -nodes -keyout offline_key.pem -x509 -days 365 -out offline_cert.pem -set_serial 2
	offlineCert := `
-----BEGIN CERTIFICATE-----
MIIDTjCCAjagAwIBAgIBAjANBgkqhkiG9w0BAQsFADBAMQswCQYDVQQGEwJVQTET
MBEGA1UECAwKU29tZS1TdGF0ZTENMAsGA1UEBwwES3lpdjENMAsGA1UECgwERVBB
TTAeFw0yMDA5MTAxNDE1NTdaFw0yMTA5MTAxNDE1NTdaMEAxCzAJBgNVBAYTAlVB
MRMwEQYDVQQIDApTb21lLVN0YXRlMQ0wCwYDVQQHDARLeWl2MQ0wCwYDVQQKDARF
UEFNMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoDW6XO7bHeTRnF10
5FZ6wFKnyp0v6aACJxuWrLdOGlOZeaO/7q1I1qD5ogxV2Fh6IHm6+4e+wo4lLAUX
F7Fi/ab/FZjHU6a2XNxNi6VnE7C7wp/UDt8/KfHV6ZzkyubHoraMl5Xi8g+3IN1N
kTc90OCZPL6aZHpMXaP+5dOXad/Pv2/6xHKB/Qx+VIQlo1c7oGrYxFc29LsUtlrP
C0tcDVAfMBBlRIhkw/dXoGcBSfqBIAnORYq/kGcbEewWDLdBiExr+TtJh837NgiE
q1+NIvWF4AS3vL+tm/rvXfz71v3Gy4JqL/8Eqn489VE2Vw/XJfLjte8fPpKc5NVN
Ah7bsQIDAQABo1MwUTAdBgNVHQ4EFgQUdjLMDiQH1r6a4ra3GX9hwdVzlWkwHwYD
VR0jBBgwFoAUdjLMDiQH1r6a4ra3GX9hwdVzlWkwDwYDVR0TAQH/BAUwAwEB/zAN
BgkqhkiG9w0BAQsFAAOCAQEAYCm82d7bMk3bROnNvIp/IcZdO6SItuhTy6iPQRd8
ZqIIPsOG/8uMTKimCvJZIhsb+P9rRj3Ubb4EAAHpftZJx6Y1yrGlFUVYhsDNSsQR
RqT4gg71B7R3Mgu0T9tV96nNa7P0w32wvNtc7B/it8DsFMz1wOcq/PPh3ufoR9Lm
onsMCZ7ep/quYwXutmvMrE3SLGApTc7lnqPqBtVq4ju29VS6wDsvLgEyuZQO9HlJ
KbjNq7kDX6ZbgJTVgVwEVY16464lTJ3j6/Osi3R3bUs5cg4onCFAS5KUTsfkbZ+G
KzpDMr/kcScwzmmNcN8aLp31TSRVee64QrK7yF3YJxL+rA==
-----END CERTIFICATE-----	
`

	if err = ioutil.WriteFile(path.Join(tmpDir, "online_cert.pem"), []byte(onlineCert), 0o644); err != nil {
		log.Fatalf("Error create online cert: %s", err)
	}

	if err = ioutil.WriteFile(path.Join(tmpDir, "offline_cert.pem"), []byte(offlineCert), 0o644); err != nil {
		log.Fatalf("Error create offline cert: %s", err)
	}

	onlineURL := url.URL{Scheme: "file", Path: path.Join(tmpDir, "online_cert.pem")}
	offlineURL := url.URL{Scheme: "file", Path: path.Join(tmpDir, "offline_cert.pem")}

	server.certURL = map[string]string{
		"online":  onlineURL.String(),
		"offline": offlineURL.String(),
	}

	client, err := iamclient.New(&config.Config{IAMServerURL: serverURL}, sender, true)
	if err != nil {
		t.Fatalf("Can't create IAM client: %s", err)
	}
	defer client.Close()

	certInfo := []cloudprotocol.IssuedCertData{
		{Type: "online", CertificateChain: "onlineCert"},
		{Type: "offline", CertificateChain: "offlineCert"},
	}

	if err = client.InstallCertificates(certInfo, &testCertProvider{}); err != nil {
		t.Fatalf("Can't process install certificates request: %s", err)
	}

	serial := map[string]string{"online": "1", "offline": "2"}

	if !reflect.DeepEqual(serial, sender.serial) {
		t.Errorf("Wrong certificate serials: %v", sender.serial)
	}
}

func TestGetCertificates(t *testing.T) {
	sender := &testSender{}

	server, err := newTestServer(serverURL)
	if err != nil {
		t.Fatalf("Can't create test server: %s", err)
	}

	defer server.close()

	server.certURL = map[string]string{"online": "onlineCertURL", "offline": "offlineCertURL"}
	server.keyURL = map[string]string{"online": "onlineKeyURL", "offline": "offlineKeyURL"}

	client, err := iamclient.New(&config.Config{IAMServerURL: serverURL}, sender, true)
	if err != nil {
		t.Fatalf("Can't create IAM client: %s", err)
	}
	defer client.Close()

	for serial, certType := range []string{"online", "offline"} {
		certURL, keyURL, err := client.GetCertificate(certType, nil, strconv.Itoa(serial))
		if err != nil {
			t.Errorf("Can't get %s certificate: %s", certType, err)
			continue
		}

		if certURL != server.certURL[certType] {
			t.Errorf("Wrong %s cert URL: %s", certType, certURL)
		}

		if keyURL != server.keyURL[certType] {
			t.Errorf("Wrong %s key URL: %s", certType, keyURL)
		}
	}
}

/*******************************************************************************
 * Private
 ******************************************************************************/

func newTestServer(url string) (server *testServer, err error) {
	server = &testServer{usersChangedChannel: make(chan []string, 1)}

	listener, err := net.Listen("tcp", url)
	if err != nil {
		return nil, aoserrors.Wrap(err)
	}

	server.grpcServer = grpc.NewServer()

	pb.RegisterIAMProtectedServiceServer(server.grpcServer, server)
	pb.RegisterIAMPublicServiceServer(server.grpcServer, server)

	server.permissionsCache = make(map[string]servicePermissions)

	go func() {
		if err := server.grpcServer.Serve(listener); err != nil {
			log.Errorf("Can't serve gRPC server: %s", err)
		}
	}()

	return server, nil
}

func (server *testServer) close() (err error) {
	if server.grpcServer != nil {
		server.grpcServer.Stop()
	}

	return nil
}

func (server *testServer) CreateKey(
	context context.Context, req *pb.CreateKeyRequest) (rsp *pb.CreateKeyResponse, err error) {
	rsp = &pb.CreateKeyResponse{Type: req.Type}

	csr, ok := server.csr[req.Type]
	if !ok {
		return rsp, aoserrors.New("not found")
	}

	rsp.Csr = csr

	return rsp, nil
}

func (server *testServer) ApplyCert(
	context context.Context, req *pb.ApplyCertRequest) (rsp *pb.ApplyCertResponse, err error) {
	rsp = &pb.ApplyCertResponse{Type: req.Type}

	certURL, ok := server.certURL[req.Type]
	if !ok {
		return rsp, aoserrors.New("not found")
	}

	rsp.CertUrl = certURL

	return rsp, nil
}

func (server *testServer) GetCert(
	context context.Context, req *pb.GetCertRequest) (rsp *pb.GetCertResponse, err error) {
	rsp = &pb.GetCertResponse{Type: req.Type}

	certURL, ok := server.certURL[req.Type]
	if !ok {
		return rsp, aoserrors.New("not found")
	}

	keyURL, ok := server.keyURL[req.Type]
	if !ok {
		return rsp, aoserrors.New("not found")
	}

	rsp.CertUrl = certURL
	rsp.KeyUrl = keyURL

	return rsp, nil
}

func (server *testServer) GetCertTypes(context context.Context, req *empty.Empty) (rsp *pb.CertTypes, err error) {
	return rsp, nil
}

func (server *testServer) FinishProvisioning(context context.Context, req *empty.Empty) (rsp *empty.Empty, err error) {
	return rsp, nil
}

func (server *testServer) Clear(context context.Context, req *pb.ClearRequest) (rsp *empty.Empty, err error) {
	return rsp, nil
}

func (server *testServer) SetOwner(context context.Context, req *pb.SetOwnerRequest) (rsp *empty.Empty, err error) {
	return rsp, nil
}

func (server *testServer) GetSystemInfo(
	context context.Context, req *empty.Empty) (rsp *pb.SystemInfo, err error) {
	rsp = &pb.SystemInfo{SystemId: server.systemID}

	return rsp, nil
}

func (server *testServer) SetUsers(context context.Context, req *pb.Users) (rsp *empty.Empty, err error) {
	return rsp, nil
}

func (server *testServer) GetUsers(context context.Context, req *empty.Empty) (rsp *pb.Users, err error) {
	rsp = &pb.Users{Users: server.users}

	return rsp, nil
}

func (server *testServer) SubscribeUsersChanged(
	req *empty.Empty, stream pb.IAMPublicService_SubscribeUsersChangedServer) (err error) {
	for {
		select {
		case <-stream.Context().Done():
			return nil

		case users := <-server.usersChangedChannel:
			if err := stream.Send(&pb.Users{Users: users}); err != nil {
				return aoserrors.Wrap(err)
			}
		}
	}
}

func (server *testServer) RegisterService(
	context context.Context, req *pb.RegisterServiceRequest) (rsp *pb.RegisterServiceResponse, err error) {
	rsp = &pb.RegisterServiceResponse{}

	secret := server.findServiceID(req.ServiceId)
	if secret != "" {
		return rsp, aoserrors.Errorf("service %s is already registered", req.ServiceId)
	}

	secret = randomString()
	rsp.Secret = secret

	permissions := make(map[string]map[string]string)
	for key, value := range req.Permissions {
		permissions[key] = value.Permissions
	}

	server.permissionsCache[secret] = servicePermissions{serviceID: req.ServiceId, permissions: permissions}

	return rsp, nil
}

func (server *testServer) UnregisterService(
	ctx context.Context, req *pb.UnregisterServiceRequest) (rsp *empty.Empty, err error) {
	rsp = &empty.Empty{}

	secret := server.findServiceID(req.ServiceId)
	if secret == "" {
		return rsp, aoserrors.Errorf("service %s is not registered ", req.ServiceId)
	}

	delete(server.permissionsCache, secret)

	return rsp, nil
}

func (server *testServer) GetPermissions(
	ctx context.Context, req *pb.PermissionsRequest) (rsp *pb.PermissionsResponse, err error) {
	rsp = &pb.PermissionsResponse{}

	funcServersPermissions, ok := server.permissionsCache[req.Secret]
	if !ok {
		return rsp, aoserrors.New("secret not found")
	}

	permissions, ok := funcServersPermissions.permissions[req.FunctionalServerId]
	if !ok {
		return rsp, aoserrors.New("permissions for functional server not found")
	}

	rsp.Permissions = &pb.Permissions{Permissions: permissions}
	rsp.ServiceId = funcServersPermissions.serviceID

	return rsp, nil
}

func (server *testServer) EncryptDisk(context.Context, *pb.EncryptDiskRequest) (*empty.Empty, error) {
	return nil, nil
}

func (server *testServer) findServiceID(serviceID string) (secret string) {
	for key, value := range server.permissionsCache {
		if value.serviceID == serviceID {
			return key
		}
	}

	return ""
}

func randomString() string {
	secret := make([]byte, secretLength)

	rand.Seed(time.Now().UnixNano())

	for i := range secret {
		secret[i] = secretSymbols[rand.Intn(len(secretSymbols))]
	}

	return string(secret)
}

func (sender *testSender) SendIssueUnitCerts(requests []cloudprotocol.IssueCertData) (err error) {
	sender.csr = make(map[string]string)

	for _, request := range requests {
		sender.csr[request.Type] = request.Csr
	}

	return nil
}

func (sender *testSender) SendInstallCertsConfirmation(
	confirmations []cloudprotocol.InstallCertData) (err error) {
	sender.serial = make(map[string]string)

	for _, confirmation := range confirmations {
		sender.serial[confirmation.Type] = confirmation.Serial
	}

	return nil
}

func (provider *testCertProvider) GetCertSerial(certURLStr string) (serial string, err error) {
	certURL, err := url.Parse(certURLStr)
	if err != nil {
		return "", aoserrors.Wrap(err)
	}

	var certs []*x509.Certificate

	switch certURL.Scheme {
	case cryptutils.SchemeFile:
		if certs, err = cryptutils.LoadCertificate(certURL.Path); err != nil {
			return "", aoserrors.Wrap(err)
		}

	default:
		return "", aoserrors.Errorf("unsupported schema %s for certificate", certURL.Scheme)
	}

	return fmt.Sprintf("%X", certs[0].SerialNumber), nil
}
