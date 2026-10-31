// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/sqldb"
	"github.com/altessa-s/go-horai/task"

	mongostore "github.com/altessa-s/go-horai/storage/mongo"
	redisstore "github.com/altessa-s/go-horai/storage/redis"
	rdb "github.com/redis/go-redis/v9"
	mo "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func openStore(t testing.TB, kind, addr, ns string) horai.Store {
	t.Helper()
	switch kind {
	case "redis":
		c := rdb.NewClient(&rdb.Options{Addr: addr})
		t.Cleanup(func() { require.NoError(t, c.Close()) })
		s, err := redisstore.New(c, redisstore.WithKeyPrefix(ns))
		require.NoError(t, err)
		require.NoError(t, s.EnsureIndexes(t.Context()))
		return s
	case "mongo":
		c, err := mongo.Connect(mo.Client().ApplyURI(addr))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, c.Disconnect(context.WithoutCancel(t.Context()))) })
		s, err := mongostore.New(c.Database(ns))
		require.NoError(t, err)
		require.NoError(t, s.EnsureIndexes(t.Context()))
		return s
	default:
		var db *sql.DB
		dialect := sqldb.Postgres
		if kind == "postgres" {
			cfg, err := pgx.ParseConfig(addr)
			require.NoError(t, err)
			db = stdlib.OpenDB(*cfg)
		} else {
			dialect = sqldb.MySQL
			cfg, err := mysql.ParseDSN(addr)
			require.NoError(t, err)
			conn, err := mysql.NewConnector(cfg)
			require.NoError(t, err)
			db = sql.OpenDB(conn)
		}
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		s, err := sqldb.New(db, dialect, sqldb.WithTasksTable(ns+"_tasks"), sqldb.WithHistoryTable(ns+"_history"))
		require.NoError(t, err)
		require.NoError(t, s.EnsureSchema(t.Context()))
		return s
	}
}

// TestCrashWorker runs only in the child selected by crashRecovery.
func TestCrashWorker(t *testing.T) {
	kind := os.Getenv("HORAI_CRASH_KIND")
	if kind == "" {
		return
	}
	st := openStore(t, kind, os.Getenv("HORAI_CRASH_ADDR"), os.Getenv("HORAI_CRASH_NS"))
	s, err := scheduler.New(st, scheduler.WithInstanceID("crash-worker"), scheduler.WithTickInterval(100*time.Millisecond), scheduler.WithStaleTaskTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, s.Register(t.Context(), task.Config{ID: "crash", RunAt: time.Now(), Func: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}))
	require.NoError(t, s.Start(t.Context()))
	<-t.Context().Done()
}
func crashRecovery(t *testing.T, kind, addr string) {
	t.Helper()
	ns := namespace()
	st := openStore(t, kind, addr, ns)
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestCrashWorker$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "HORAI_CRASH_KIND="+kind, "HORAI_CRASH_ADDR="+addr, "HORAI_CRASH_NS="+ns)
	require.NoError(t, cmd.Start())
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	var state *horai.TaskState
	require.Eventually(t, func() bool {
		v, err := st.GetTask(t.Context(), "crash")
		if err != nil || v == nil || v.RunStartedAt == 0 {
			return false
		}
		state = v
		return true
	}, 15*time.Second, 20*time.Millisecond)
	var calls atomic.Int32
	cfg := task.Config{ID: "crash", RunAt: time.Unix(state.RunAt, 0), Func: func(context.Context) error { calls.Add(1); return nil }}
	survivor, err := scheduler.New(st, scheduler.WithInstanceID("survivor"), scheduler.WithTickInterval(100*time.Millisecond), scheduler.WithStaleTaskTimeout(time.Second))
	require.NoError(t, err)
	require.NoError(t, survivor.Register(t.Context(), cfg))
	require.NoError(t, survivor.Pause(t.Context(), "crash"))
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	waited = true
	require.NoError(t, survivor.Start(t.Context()))
	require.Eventually(t, func() bool {
		v, err := st.GetTask(t.Context(), "crash")
		return err == nil && v.RunStartedAt == 0 && v.Status == horai.TaskStatusPaused
	}, 15*time.Second, 20*time.Millisecond)
	require.Zero(t, calls.Load())
	require.NoError(t, survivor.Resume(t.Context(), "crash"))
	require.Eventually(t, func() bool {
		v, err := st.GetTask(t.Context(), "crash")
		return err == nil && v.Status == horai.TaskStatusCompleted
	}, 10*time.Second, 20*time.Millisecond)
	require.NoError(t, survivor.Stop(t.Context()))
	require.Equal(t, int32(1), calls.Load())
	restarted, err := scheduler.New(st, scheduler.WithTickInterval(100*time.Millisecond))
	require.NoError(t, err)
	require.NoError(t, restarted.Register(t.Context(), cfg))
	require.NoError(t, restarted.Start(t.Context()))
	time.Sleep(time.Second)
	require.NoError(t, restarted.Stop(t.Context()))
	require.Equal(t, int32(1), calls.Load())
}

func TestSchemaWorker(t *testing.T) {
	kind := os.Getenv("HORAI_SCHEMA_KIND")
	if kind == "" {
		return
	}
	_ = openStore(t, kind, os.Getenv("HORAI_SCHEMA_ADDR"), os.Getenv("HORAI_SCHEMA_NS"))
}
func schemaUpgrade(t *testing.T, db *sql.DB, kind, addr string) {
	t.Helper()
	ns := namespace()
	st := openStore(t, kind, addr, ns)
	ok, err := st.CreateTask(t.Context(), &horai.TaskState{ID: "preserved", Status: horai.TaskStatusActive})
	require.NoError(t, err)
	require.True(t, ok)
	indexName := ns + "_tasks_unfinished_idx"
	drop := "DROP INDEX " + indexName
	if kind == "mysql" {
		drop += " ON " + ns + "_tasks"
	}
	_, err = db.ExecContext(t.Context(), drop)
	require.NoError(t, err)
	for _, column := range []string{"run_lease_until", "run_lease_id", "run_at"} {
		_, err := db.ExecContext(t.Context(), "ALTER TABLE "+ns+"_tasks DROP COLUMN "+column)
		require.NoError(t, err)
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	commands := make([]*exec.Cmd, 2)
	for i := range commands {
		cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestSchemaWorker$", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), "HORAI_SCHEMA_KIND="+kind, "HORAI_SCHEMA_ADDR="+addr, "HORAI_SCHEMA_NS="+ns)
		require.NoError(t, cmd.Start())
		commands[i] = cmd
	}
	for _, cmd := range commands {
		require.NoError(t, cmd.Wait())
	}
	got, err := st.GetTask(t.Context(), "preserved")
	require.NoError(t, err)
	require.NotNil(t, got)
	var indexes int
	if kind == "mysql" {
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?", ns+"_tasks", indexName).Scan(&indexes))
	} else {
		require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM pg_indexes WHERE tablename = $1 AND indexname = $2", ns+"_tasks", indexName).Scan(&indexes))
	}
	require.Equal(t, 1, indexes)
	require.Zero(t, got.RunLeaseUntil)
}
