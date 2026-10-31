// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb_test

import (
	"database/sql/driver"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/internal/testhelpers"
	"github.com/altessa-s/go-horai/storage/sqldb"
)

func TestSQLHistoryMappingAndDeletion(t *testing.T) {
	t.Parallel()
	for _, d := range []sqldb.Dialect{sqldb.Postgres, sqldb.MySQL} {
		db, fake := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
			if strings.HasPrefix(q, "SELECT") {
				return testhelpers.FakeSQLReply{Columns: []string{"id", "task_id", "run_id", "error", "started_at", "ended_at", "duration_ms", "success"}, Rows: [][]driver.Value{{"h", "task", "node/run", "failure", int64(10), int64(11), int64(1000), false}}}
			}
			return testhelpers.FakeSQLReply{Affected: 1}
		})
		s, err := sqldb.New(db, d)
		require.NoError(t, err)
		want := &horai.TaskHistory{ID: "h", TaskID: "task", RunID: "node/run", Error: "failure", StartedAt: 10, EndedAt: 11, DurationMs: 1000}
		for h, err := range s.History(t.Context(), "task") {
			require.NoError(t, err)
			require.Equal(t, want, h)
			break
		}
		rows, err := s.ListHistory(t.Context(), "task", horai.HistoryPage{}, nil)
		require.NoError(t, err)
		require.Equal(t, []*horai.TaskHistory{want}, rows)
		require.NoError(t, s.CleanupHistory(t.Context(), 12))
		require.NoError(t, s.DeleteHistory(t.Context(), "task"))
		require.NoError(t, s.DeleteTask(t.Context(), "task"))
		require.Equal(t, 1, fake.Commits())
		calls := fake.Calls()
		require.True(t, calls[len(calls)-1].InTx)
	}
}
func TestSQLReadAndDeleteFailures(t *testing.T) {
	t.Parallel()
	failure := errors.New("driver failure")
	for _, failAt := range []string{"SELECT", "DELETE FROM \"scheduler_history\"", "DELETE FROM \"scheduler_tasks\""} {
		db, fake := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
			if strings.HasPrefix(q, failAt) {
				return testhelpers.FakeSQLReply{Err: failure}
			}
			return testhelpers.FakeSQLReply{Affected: 1}
		})
		s, err := sqldb.New(db, sqldb.Postgres)
		require.NoError(t, err)
		if failAt == "SELECT" {
			for _, seq := range []func() error{
				func() error {
					for _, err := range s.History(t.Context(), "task") {
						return err
					}
					return nil
				},
				func() error {
					for _, err := range s.DueTasks(t.Context(), 10) {
						return err
					}
					return nil
				},
				func() error {
					for _, err := range s.UnfinishedRuns(t.Context()) {
						return err
					}
					return nil
				},
			} {
				require.ErrorIs(t, seq(), failure)
			}
		} else {
			require.ErrorIs(t, s.DeleteTask(t.Context(), "task"), failure)
			require.Equal(t, 1, fake.Rollbacks())
		}
	}
}
func TestMySQLUnfinishedIndexUpgrade(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"absent", "present", "concurrent", "failure", "probe_failure"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			created := false
			failure := errors.New("index failure")
			db, fake := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
				if strings.Contains(q, "information_schema.COLUMNS") {
					return testhelpers.FakeSQLReply{Columns: []string{"name"}, Rows: [][]driver.Value{{"run_at"}, {"run_lease_id"}, {"run_lease_until"}}}
				}
				if strings.Contains(q, "information_schema.STATISTICS") {
					if mode == "probe_failure" {
						return testhelpers.FakeSQLReply{Err: failure}
					}
					r := testhelpers.FakeSQLReply{Columns: []string{"name"}}
					if mode == "present" || created && mode == "concurrent" {
						r.Rows = [][]driver.Value{{"scheduler_tasks_unfinished_idx"}}
					}
					return r
				}
				if strings.HasPrefix(q, "CREATE INDEX") {
					created = true
					if mode == "concurrent" || mode == "failure" {
						return testhelpers.FakeSQLReply{Err: failure}
					}
				}
				return testhelpers.FakeSQLReply{}
			})
			s, err := sqldb.New(db, sqldb.MySQL)
			require.NoError(t, err)
			err = s.EnsureSchema(t.Context())
			if mode == "failure" || mode == "probe_failure" {
				require.ErrorIs(t, err, failure)
			} else {
				require.NoError(t, err)
			}
			if mode == "absent" {
				require.Equal(t, "CREATE INDEX `scheduler_tasks_unfinished_idx` ON `scheduler_tasks` (run_started_at)", fake.Calls()[len(fake.Calls())-1].Query)
			}
		})
	}
}

func TestUnfinishedIndexDDLGolden(t *testing.T) {
	t.Parallel()
	golden, err := os.ReadFile("testdata/unfinished-indexes.golden")
	require.NoError(t, err)
	var got []string
	for _, d := range []sqldb.Dialect{sqldb.Postgres, sqldb.MySQL} {
		db, fake := testhelpers.NewFakeSQL(t, nil)
		s, err := sqldb.New(db, d)
		require.NoError(t, err)
		require.NoError(t, s.EnsureSchema(t.Context()))
		for _, c := range fake.Calls() {
			if strings.HasPrefix(c.Query, "CREATE INDEX") && strings.Contains(c.Query, "unfinished") {
				got = append(got, c.Query)
			}
		}
	}
	require.Equal(t, string(golden), strings.Join(got, "\n")+"\n")
}
