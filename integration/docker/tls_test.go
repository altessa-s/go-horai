// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"bytes"
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

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/storagetest"

	"github.com/altessa-s/go-tlsutils"

	redisstore "github.com/altessa-s/go-horai/storage/redis"
)

func TestRedisTLS(t *testing.T) {
	t.Parallel()
	certPEM, keyPEM, config := tlsMaterial(t)
	c, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
		Image: "redis/redis-stack-server:7.4.0-v3", ExposedPorts: []string{"6379/tcp"}, Entrypoint: []string{"redis-stack-server"}, Cmd: []string{"--port", "0", "--tls-port", "6379", "--tls-cert-file", "/tls/cert.pem", "--tls-key-file", "/tls/key.pem", "--tls-ca-cert-file", "/tls/cert.pem", "--tls-auth-clients", "no"},
		Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(certPEM), ContainerFilePath: "/tls/cert.pem", FileMode: 0644}, {Reader: bytes.NewReader(keyPEM), ContainerFilePath: "/tls/key.pem", FileMode: 0644}}, WaitingFor: wait.ForListeningPort("6379/tcp"), Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Terminate(context.WithoutCancel(t.Context()))) })
	host, err := c.Host(t.Context())
	require.NoError(t, err)
	port, err := c.MappedPort(t.Context(), "6379/tcp")
	require.NoError(t, err)
	client := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(host, port.Port()), TLSConfig: config})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	storagetest.Run(t, func(t testing.TB) horai.Store {
		t.Helper()
		s, err := redisstore.New(client, redisstore.WithKeyPrefix(namespace()))
		require.NoError(t, err)
		require.NoError(t, s.EnsureIndexes(t.Context()))
		return s
	})
}

func tlsMaterial(t testing.TB) ([]byte, []byte, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	require.NoError(t, err)
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, certPEM, 0600))
	config := tlsutils.ClientTLSConfig(nil, "localhost")
	config.RootCAs, err = tlsutils.BuildCAPool(false, caFile)
	require.NoError(t, err)

	return certPEM, keyPEM, config
}
