// Copyright 2026 Google LLC
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

package csi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/api/v1alpha1"
	listersv1alpha1 "github.com/agent-substrate/substrate/pkg/client/listers/api/v1alpha1"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// mockLister implements CSIDriverConfigLister for testing.
type mockLister struct {
	configs map[string]*v1alpha1.CSIDriverConfig
}

func (m *mockLister) List(selector labels.Selector) (ret []*v1alpha1.CSIDriverConfig, err error) {
	return nil, nil
}

func (m *mockLister) Get(name string) (*v1alpha1.CSIDriverConfig, error) {
	cfg, ok := m.configs[name]
	if !ok {
		return nil, errors.NewNotFound(schema.GroupResource{Group: "ate.dev", Resource: "csidriverconfig"}, name)
	}
	return cfg, nil
}

var _ listersv1alpha1.CSIDriverConfigLister = (*mockLister)(nil)

// Test helpers for cert generation
type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA certificate: %v", err)
	}
	return &testCA{cert: cert, certDER: der, key: key}
}

func (ca *testCA) issueClientBundle(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshal PKCS8 key: %v", err)
	}
	return append(
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})...,
	)
}

func (ca *testCA) issueServerCert(t *testing.T, dnsName string) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{dnsName},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

func startMockCSIDriverTLS(t *testing.T, driver csi.IdentityServer, serverCert tls.Certificate, clientCA *x509.CertPool) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	creds := credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    clientCA,
	})

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	csi.RegisterIdentityServer(grpcServer, driver)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.GracefulStop()
		lis.Close()
	}

	return lis.Addr().String(), cleanup
}

func TestCSIClient_mTLS(t *testing.T) {
	ctx := context.Background()

	// 1. Setup CAs and Certs
	ca := newTestCA(t)
	serverCert := ca.issueServerCert(t, "localhost")
	clientBundle := ca.issueClientBundle(t)

	clientCAPool := x509.NewCertPool()
	clientCAPool.AppendCertsFromPEM(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw}))

	// 2. Start TLS Mock Server
	driver := &mockCSIDriver{}
	addr, cleanup := startMockCSIDriverTLS(t, driver, serverCert, clientCAPool)
	defer cleanup()

	// 3. Write certs to temp files and override defaults
	tmpDir, err := os.MkdirTemp("", "csi-tls-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	clientCertPath := filepath.Join(tmpDir, "client-credential-bundle.pem")
	caCertPath := filepath.Join(tmpDir, "ca-trust-bundle.pem")

	if err := os.WriteFile(clientCertPath, clientBundle, 0600); err != nil {
		t.Fatalf("failed to write client cert: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
	if err := os.WriteFile(caCertPath, caPEM, 0644); err != nil {
		t.Fatalf("failed to write CA cert: %v", err)
	}

	// Override global paths in plugin.go
	origClientPath := DefaultClientCertPath
	origCAPath := DefaultCACertPath
	DefaultClientCertPath = clientCertPath
	DefaultCACertPath = caCertPath
	defer func() {
		DefaultClientCertPath = origClientPath
		DefaultCACertPath = origCAPath
	}()

	// 4. Configure CSIDriverConfig
	driverName := "mock-driver"
	cfg := &v1alpha1.CSIDriverConfig{
		Spec: v1alpha1.CSIDriverConfigSpec{
			DriverName:         driverName,
			ControllerEndpoint: "tcp://" + addr,
			TLS: &v1alpha1.CSIDriverTLSConfig{
				Enabled:        true,
				UsePodIdentity: true,
				ServerName:     "localhost",
			},
		},
	}

	lister := &mockLister{
		configs: map[string]*v1alpha1.CSIDriverConfig{
			driverName: cfg,
		},
	}

	// 5. Test connection
	plugin, err := NewCSIPlugin(ctx, lister, driverName, true /*isController*/)
	if err != nil {
		t.Fatalf("NewCSIPlugin failed: %v", err)
	}
	defer plugin.client.Close()

	// Verify we can make a call
	info, err := plugin.DriverName(ctx)
	if err != nil {
		t.Fatalf("DriverName call failed: %v", err)
	}
	if info != driverName {
		t.Errorf("expected driver name %q, got %q", driverName, info)
	}
}
