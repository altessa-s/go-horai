// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"bytes"
	"context"
	"database/sql"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/sqldb"
	"github.com/altessa-s/go-horai/storage/storagetest"

	mongostore "github.com/altessa-s/go-horai/storage/mongo"
	redisstore "github.com/altessa-s/go-horai/storage/redis"
	rdb "github.com/redis/go-redis/v9"
	mo "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func containerAddress(t testing.TB, c testcontainers.Container, port string) string {
	t.Helper()
	host, err := c.Host(t.Context())
	require.NoError(t, err)
	mapped, err := c.MappedPort(t.Context(), port)
	require.NoError(t, err)
	return net.JoinHostPort(host, mapped.Port())
}
func TestRedisCluster(t *testing.T) {
	t.Parallel()
	c, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{Image: "redis/redis-stack-server:7.4.0-v3", ExposedPorts: []string{"6379/tcp"}, Entrypoint: []string{"redis-stack-server"}, Cmd: []string{"--cluster-enabled", "yes", "--cluster-config-file", "/tmp/nodes.conf"}, WaitingFor: wait.ForListeningPort("6379/tcp"), Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Terminate(context.WithoutCancel(t.Context()))) })
	addr := containerAddress(t, c, "6379/tcp")
	admin := rdb.NewClient(&rdb.Options{Addr: addr})
	t.Cleanup(func() { require.NoError(t, admin.Close()) })
	require.NoError(t, admin.Do(t.Context(), "CLUSTER", "ADDSLOTSRANGE", 0, 16383).Err())
	// A deterministic slot map exposes the container's mapped address to the host.
	client := rdb.NewClusterClient(&rdb.ClusterOptions{Addrs: []string{addr}, ClusterSlots: func(context.Context) ([]rdb.ClusterSlot, error) {
		return []rdb.ClusterSlot{{Start: 0, End: 16383, Nodes: []rdb.ClusterNode{{Addr: addr}}}}, nil
	}})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.Eventually(t, func() bool { return client.Set(t.Context(), "cluster-ready", "1", time.Minute).Err() == nil }, time.Minute, 100*time.Millisecond)
	storagetest.Run(t, func(t testing.TB) horai.Store {
		t.Helper()
		s, err := redisstore.New(client, redisstore.WithKeyPrefix(namespace()))
		require.NoError(t, err)
		require.NoError(t, s.EnsureIndexes(t.Context()))
		return s
	})
}
func TestMongoReplicaTLS(t *testing.T) {
	t.Parallel()
	for _, image := range []string{"mongo:7", "mongo:8"} {
		t.Run(image, func(t *testing.T) {
			t.Parallel()
			cert, key, config := tlsMaterial(t)
			pem := append(append([]byte{}, cert...), key...)
			c, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{Image: image, ExposedPorts: []string{"27017/tcp"}, Cmd: []string{"--replSet", "rs0", "--bind_ip_all", "--tlsMode", "requireTLS", "--tlsCertificateKeyFile", "/tls/server.pem", "--tlsCAFile", "/tls/ca.pem", "--tlsAllowConnectionsWithoutCertificates"}, Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(pem), ContainerFilePath: "/tls/server.pem", FileMode: 0644}, {Reader: bytes.NewReader(cert), ContainerFilePath: "/tls/ca.pem", FileMode: 0644}}, WaitingFor: wait.ForListeningPort("27017/tcp"), Started: true})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, c.Terminate(context.WithoutCancel(t.Context()))) })
			code, _, err := c.Exec(t.Context(), []string{"mongosh", "--tls", "--tlsCAFile", "/tls/ca.pem", "--host", "localhost", "--eval", `rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})`})
			require.NoError(t, err)
			require.Zero(t, code)
			client, err := mongo.Connect(mo.Client().ApplyURI("mongodb://" + containerAddress(t, c, "27017/tcp")).SetDirect(true).SetReplicaSet("rs0").SetTLSConfig(config))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Disconnect(context.WithoutCancel(t.Context()))) })
			// Ping succeeds before election. Check the same TLS client used by the store.
			readyCtx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				var hello struct {
					Writable bool `bson:"isWritablePrimary"`
				}
				err := client.Database("admin").RunCommand(readyCtx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
				if err == nil && hello.Writable {
					break
				}
				select {
				case <-readyCtx.Done():
					t.Fatalf("MongoDB rs0 did not elect a writable primary within one minute: %v (last hello error: %v)", readyCtx.Err(), err)
				case <-ticker.C:
				}
			}
			storagetest.Run(t, func(t testing.TB) horai.Store {
				t.Helper()
				s, err := mongostore.New(client.Database(namespace()))
				require.NoError(t, err)
				require.NoError(t, s.EnsureIndexes(t.Context()))
				return s
			})
		})
	}
}
func TestPostgresTLS(t *testing.T) {
	t.Parallel()
	cert, key, config := tlsMaterial(t)
	c, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{Image: "postgres:17", ExposedPorts: []string{"5432/tcp"}, Env: map[string]string{"POSTGRES_PASSWORD": "horai-test", "POSTGRES_DB": "horai"}, Entrypoint: []string{"sh", "-c"}, Cmd: []string{"cp /tls/key.pem /tmp/server.key && chown postgres:postgres /tmp/server.key && chmod 600 /tmp/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tls/cert.pem -c ssl_key_file=/tmp/server.key"}, Files: []testcontainers.ContainerFile{{Reader: bytes.NewReader(cert), ContainerFilePath: "/tls/cert.pem", FileMode: 0644}, {Reader: bytes.NewReader(key), ContainerFilePath: "/tls/key.pem", FileMode: 0644}}, WaitingFor: wait.ForListeningPort("5432/tcp"), Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Terminate(context.WithoutCancel(t.Context()))) })
	cfg, err := pgx.ParseConfig("postgres://postgres:horai-test@" + containerAddress(t, c, "5432/tcp") + "/horai?sslmode=require")
	require.NoError(t, err)
	cfg.TLSConfig = config
	cfg.Fallbacks = nil
	var db *sql.DB = stdlib.OpenDB(*cfg)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.Eventually(t, func() bool { return db.PingContext(t.Context()) == nil }, time.Minute, 100*time.Millisecond)
	storagetest.Run(t, func(t testing.TB) horai.Store {
		t.Helper()
		s, err := sqldb.New(db, sqldb.Postgres, sqldb.WithTasksTable(namespace()), sqldb.WithHistoryTable(namespace()))
		require.NoError(t, err)
		require.NoError(t, s.EnsureSchema(t.Context()))
		return s
	})
}
