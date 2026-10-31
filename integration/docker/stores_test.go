// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/sqldb"
	"github.com/altessa-s/go-horai/storage/storagetest"
	"github.com/altessa-s/go-horai/task"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chstore "github.com/altessa-s/go-horai/storage/clickhouse"
	mongostore "github.com/altessa-s/go-horai/storage/mongo"
	redisstore "github.com/altessa-s/go-horai/storage/redis"
	rdb "github.com/redis/go-redis/v9"
	mo "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func namespace() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "horai_" + hex.EncodeToString(b)
}
func endpoint(t testing.TB, image, port string, env map[string]string) string {
	t.Helper()
	ctx := t.Context()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{Image: image, ExposedPorts: []string{port}, Env: env, WaitingFor: wait.ForListeningPort(port).WithStartupTimeout(3 * time.Minute), Started: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, c.Terminate(context.WithoutCancel(ctx))) })
	host, err := c.Host(ctx)
	require.NoError(t, err)
	mapped, err := c.MappedPort(ctx, port)
	require.NoError(t, err)
	return net.JoinHostPort(host, mapped.Port())
}
func TestRedis(t *testing.T) {
	t.Parallel()
	addr := os.Getenv("HORAI_REDIS_ADDR")
	if addr == "" {
		addr = endpoint(t, "redis/redis-stack-server:7.4.0-v3", "6379/tcp", nil)
	}
	client := rdb.NewClient(&rdb.Options{Addr: addr})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	factory := func(t testing.TB) horai.Store {
		t.Helper()
		s, err := redisstore.New(client, redisstore.WithKeyPrefix(namespace()))
		require.NoError(t, err)
		require.NoError(t, s.EnsureIndexes(t.Context()))
		return s
	}
	storagetest.Run(t, factory)
	t.Run("ownership", func(t *testing.T) { endToEnd(t, factory(t)) })
	t.Run("crash_recovery", func(t *testing.T) { crashRecovery(t, "redis", addr) })
	t.Run("script_reload", func(t *testing.T) {
		s := factory(t)
		ok, err := s.CreateTask(t.Context(), &horai.TaskState{ID: "reload", Status: horai.TaskStatusActive})
		require.NoError(t, err)
		require.True(t, ok)
		require.NoError(t, client.ScriptFlush(t.Context()).Err())
		ok, err = s.ClaimRun(t.Context(), "reload", horai.RunClaim{RunID: "owner/run", StartedAt: 100})
		require.NoError(t, err)
		require.True(t, ok)
	})
}
func TestMongo(t *testing.T) {
	t.Parallel()
	for _, image := range []string{"mongo:7", "mongo:8"} {
		t.Run(image, func(t *testing.T) {
			t.Parallel()
			uri := os.Getenv("HORAI_MONGO_URI")
			if uri == "" {
				uri = "mongodb://" + endpoint(t, image, "27017/tcp", nil)
			}
			client, err := mongo.Connect(mo.Client().ApplyURI(uri))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, client.Disconnect(context.WithoutCancel(t.Context()))) })
			factory := func(t testing.TB) horai.Store {
				t.Helper()
				db := client.Database(namespace())
				s, err := mongostore.New(db)
				require.NoError(t, err)
				require.NoError(t, s.EnsureIndexes(t.Context()))
				require.NoError(t, s.EnsureIndexes(t.Context()))
				t.Cleanup(func() { require.NoError(t, db.Drop(context.WithoutCancel(t.Context()))) })
				return s
			}
			storagetest.Run(t, factory)
			t.Run("ownership", func(t *testing.T) { endToEnd(t, factory(t)) })
			t.Run("crash_recovery", func(t *testing.T) { crashRecovery(t, "mongo", uri) })
		})
	}
}
func TestSQL(t *testing.T) {
	t.Parallel()
	for _, image := range []string{"postgres:16", "postgres:17", "mysql:8.4", "mariadb:11"} {
		t.Run(image, func(t *testing.T) {
			t.Parallel()
			var db *sql.DB
			var dsn string
			dialect := sqldb.MySQL
			if image[:8] == "postgres" {
				dialect = sqldb.Postgres
				dsn = os.Getenv("HORAI_POSTGRES_DSN")
				if dsn == "" {
					addr := endpoint(t, image, "5432/tcp", map[string]string{"POSTGRES_PASSWORD": "horai-test", "POSTGRES_DB": "horai"})
					dsn = "postgres://postgres:horai-test@" + addr + "/horai?sslmode=disable"
				}
				cfg, err := pgx.ParseConfig(dsn)
				require.NoError(t, err)
				db = stdlib.OpenDB(*cfg)
			} else {
				dsn = os.Getenv("HORAI_MYSQL_DSN")
				if dsn == "" {
					addr := endpoint(t, image, "3306/tcp", map[string]string{"MYSQL_ROOT_PASSWORD": "horai-test", "MYSQL_DATABASE": "horai"})
					dsn = "root:horai-test@tcp(" + addr + ")/horai"
				}
				cfg, err := mysql.ParseDSN(dsn)
				require.NoError(t, err)
				conn, err := mysql.NewConnector(cfg)
				require.NoError(t, err)
				db = sql.OpenDB(conn)
			}
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			require.Eventually(t, func() bool { return db.PingContext(t.Context()) == nil }, time.Minute, 100*time.Millisecond)
			factory := func(t testing.TB) horai.Store {
				t.Helper()
				prefix := namespace()
				s, err := sqldb.New(db, dialect, sqldb.WithTasksTable(prefix+"_tasks"), sqldb.WithHistoryTable(prefix+"_history"))
				require.NoError(t, err)
				require.NoError(t, s.EnsureSchema(t.Context()))
				require.NoError(t, s.EnsureSchema(t.Context()))
				return s
			}
			storagetest.Run(t, factory)
			t.Run("ownership", func(t *testing.T) { endToEnd(t, factory(t)) })
			t.Run("crash_recovery", func(t *testing.T) {
				kind := "mysql"
				if dialect == sqldb.Postgres {
					kind = "postgres"
				}
				crashRecovery(t, kind, dsn)
				schemaUpgrade(t, db, kind, dsn)
			})
		})
	}
}
func TestClickHouse(t *testing.T) {
	t.Parallel()
	for _, image := range []string{"clickhouse/clickhouse-server:24.8", "clickhouse/clickhouse-server:25.3"} {
		t.Run(image, func(t *testing.T) {
			t.Parallel()
			addr := os.Getenv("HORAI_CLICKHOUSE_DSN")
			if addr == "" {
				addr = endpoint(t, image, "9000/tcp", map[string]string{"CLICKHOUSE_PASSWORD": "horai-test"})
			}
			conn, err := ch.Open(&ch.Options{Addr: []string{addr}, Auth: ch.Auth{Database: "default", Username: "default", Password: "horai-test"}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			storagetest.RunHistory(t, func(t testing.TB) horai.HistoryStore {
				t.Helper()
				s, err := chstore.New(conn, chstore.WithTableName(namespace()))
				require.NoError(t, err)
				require.NoError(t, s.EnsureSchema(t.Context()))
				return s
			})
		})
	}
}
func endToEnd(t *testing.T, st horai.Store) {
	t.Helper()
	var calls atomic.Int32
	a, err := scheduler.New(st, scheduler.WithTickInterval(100*time.Millisecond))
	require.NoError(t, err)
	b, err := scheduler.New(st, scheduler.WithTickInterval(100*time.Millisecond))
	require.NoError(t, err)
	cfg := task.Config{ID: namespace(), RunAt: time.Now(), Func: func(context.Context) error { calls.Add(1); return nil }}
	require.NoError(t, a.Register(t.Context(), cfg))
	require.NoError(t, b.Register(t.Context(), cfg))
	require.NoError(t, a.Start(t.Context()))
	require.NoError(t, b.Start(t.Context()))
	require.Eventually(t, func() bool {
		v, err := st.GetTask(t.Context(), cfg.ID)
		return err == nil && v.Status == horai.TaskStatusCompleted
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, a.Stop(t.Context()))
	require.NoError(t, b.Stop(t.Context()))
	require.Equal(t, int32(1), calls.Load(), fmt.Sprint("one winner per occurrence"))
}
