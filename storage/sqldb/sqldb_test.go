// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/internal/testhelpers"
	"github.com/altessa-s/go-horai/query"
	"github.com/altessa-s/go-horai/storage/sqldb"
)

var errBoom = errors.New("boom")

var (
	indexNameRE     = regexp.MustCompile(`INDEX IF NOT EXISTS "([^"]+)"`)
	numberedParamRE = regexp.MustCompile(`\$\d+`)
	setAssignmentRE = regexp.MustCompile(`(?:^|, )(\w+) = `)
)

func TestNewValidation(t *testing.T) {
	t.Parallel()
	db, _ := testhelpers.NewFakeSQL(t, nil)

	_, err := sqldb.New(nil, sqldb.Postgres)
	require.Error(t, err)
	_, err = sqldb.New(db, "oracle")
	require.ErrorIs(t, err, sqldb.ErrUnsupportedDialect)
	for _, name := range []string{"t; DROP TABLE x", "a.b.c", "1tasks", `"quoted"`, "tab le", strings.Repeat("t", 64), "s." + strings.Repeat("t", 64)} {
		_, err = sqldb.New(db, sqldb.Postgres, sqldb.WithTasksTable(name))
		require.ErrorIs(t, err, sqldb.ErrInvalidTableName, name)
	}
	_, err = sqldb.New(db, sqldb.MySQL, sqldb.WithTasksTable("app.sched_tasks"), sqldb.WithHistoryTable("sched_history"))
	require.NoError(t, err)
	_, err = sqldb.New(db, sqldb.Postgres, sqldb.WithTasksTable(strings.Repeat("t", 63)))
	require.NoError(t, err, "63 characters is the limit, not over it")
}

// TestEnsureSchemaMySQLBinaryTypes pins that the MySQL schema declares every
// string column as a binary type and probes nothing but the tasks columns: no
// character set, no collation, no server version.
func TestEnsureSchemaMySQLBinaryTypes(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, nil)
	store, err := sqldb.New(db, sqldb.MySQL)
	require.NoError(t, err)
	require.NoError(t, store.EnsureSchema(t.Context()))

	calls := fake.Calls()
	// Two tables, the column probe and one ALTER per added column (the fake
	// reports none).
	require.Len(t, calls, 6)
	for _, c := range calls {
		require.NotContains(t, c.Query, "VERSION()")
		require.NotContains(t, c.Query, "COLLATE")
		require.NotContains(t, c.Query, "CHARACTER SET")
		require.NotRegexp(t, `\b(VARCHAR|TEXT|MEDIUMTEXT|LONGTEXT)\b`, c.Query)
	}
	for _, want := range []string{"id              VARBINARY(1020)", "schedule        VARBINARY(4096)", "description     MEDIUMBLOB",
		"meta            MEDIUMBLOB", "run_lease_id    VARBINARY(1020)", "last_run_id     VARBINARY(1020)"} {
		require.Contains(t, calls[0].Query, want)
	}
	for _, want := range []string{"id          VARBINARY(1020)", "task_id     VARBINARY(1020)", "run_id      VARBINARY(1020)", "error       LONGBLOB"} {
		require.Contains(t, calls[1].Query, want)
	}
}

func TestEnsureSchemaIndexNamesFitIdentifierLimit(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, nil)
	long := strings.Repeat("t", 62)
	store, err := sqldb.New(db, sqldb.Postgres, sqldb.WithTasksTable(long+"a"), sqldb.WithHistoryTable(long+"b"))
	require.NoError(t, err)
	require.NoError(t, store.EnsureSchema(t.Context()))

	names := map[string]bool{}
	for _, c := range fake.Calls() {
		for _, m := range indexNameRE.FindAllStringSubmatch(c.Query, -1) {
			require.LessOrEqual(t, len(m[1]), 63, m[1])
			names[m[1]] = true
		}
	}
	require.Len(t, names, 4, "truncated index names must stay distinct")
}

func TestPlaceholderStyle(t *testing.T) {
	t.Parallel()
	for _, d := range []sqldb.Dialect{sqldb.Postgres, sqldb.MySQL} {
		t.Run(string(d), func(t *testing.T) {
			t.Parallel()
			db, fake := testhelpers.NewFakeSQL(t, nil)
			store, err := sqldb.New(db, d)
			require.NoError(t, err)
			ctx := t.Context()
			require.NoError(t, createState(store, ctx, &horai.TaskState{ID: "a"}))
			_, err = store.CreateTask(ctx, &horai.TaskState{ID: "a"})
			require.NoError(t, err)
			_, err = store.ClaimRun(ctx, "a", horai.RunClaim{NextRunAt: 1, RunAt: 1, StartedAt: 2, RunID: "r"})
			require.NoError(t, err)
			_, err = store.ClaimRun(ctx, "a", horai.RunClaim{StartedAt: 2, RunID: "r"})
			require.NoError(t, err)
			_, err = store.RenewRun(ctx, "a", "r", 3)
			require.NoError(t, err)
			_, err = store.FinishRun(ctx, "a", "r", horai.RunResult{})
			require.NoError(t, err)
			_, err = store.ReplaceTaskIf(ctx, &horai.TaskState{ID: "a"}, horai.TaskFence{})
			require.NoError(t, err)

			for _, c := range fake.Calls() {
				if d == sqldb.Postgres {
					require.NotContains(t, c.Query, "?", c.Query)
					require.Len(t, numberedParamRE.FindAllString(c.Query, -1), len(c.Args), c.Query)
				} else {
					require.NotRegexp(t, `\$\d`, c.Query)
					require.Equal(t, strings.Count(c.Query, "?"), len(c.Args), c.Query)
				}
			}
		})
	}
}

func TestFilteredTaskPageNumbersPlaceholdersAfterTheFilter(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, func(string, []any) testhelpers.FakeSQLReply { return testhelpers.FakeSQLReply{Columns: taskCols()} })
	store, err := sqldb.New(db, sqldb.Postgres)
	require.NoError(t, err)
	node := query.And{query.Cmp{Field: "status", Op: query.Eq, Value: int64(1)}, query.Cmp{Field: "priority", Op: query.In, Value: []int64{1, 2}}}

	_, err = store.ListTasks(t.Context(), horai.TaskPage{AfterID: "k", Limit: 5}, node)
	require.NoError(t, err)
	c := fake.Calls()[0]
	require.Contains(t, c.Query, "id > $4")
	require.Contains(t, c.Query, "LIMIT $5")
	require.Equal(t, []any{int64(1), int64(1), int64(2), "k", int64(6)}, c.Args)

	_, err = store.ListHistory(t.Context(), "task", horai.HistoryPage{
		AfterID: "h", Limit: 5, AfterStartedAt: 9,
	}, nil)
	require.NoError(t, err)
	c = fake.Calls()[1]
	require.Contains(t, c.Query, "task_id = $1 AND (started_at < $2 OR (started_at = $3 AND id < $4))")
	require.Contains(t, c.Query, "LIMIT $5")
}

func TestResultMapping(t *testing.T) {
	t.Parallel()
	var affected int64
	db, fake := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
		if strings.HasPrefix(q, "SELECT") {
			return testhelpers.FakeSQLReply{Columns: taskCols()} // no rows
		}
		return testhelpers.FakeSQLReply{Affected: affected}
	})
	store, err := sqldb.New(db, sqldb.MySQL)
	require.NoError(t, err)
	ctx := t.Context()

	got, err := store.GetTask(ctx, "missing")
	require.NoError(t, err)
	require.Nil(t, got, "a missing task is (nil, nil)")

	for _, n := range []int64{0, 1} {
		affected = n
		ok, err := store.ClaimRun(ctx, "a", horai.RunClaim{StartedAt: 1, RunID: "r"})
		require.NoError(t, err)
		require.Equal(t, n == 1, ok)
		ok, err = store.RenewRun(ctx, "a", "r", 2)
		require.NoError(t, err)
		require.Equal(t, n == 1, ok)
		ok, err = store.CreateTask(ctx, &horai.TaskState{ID: "a"})
		require.NoError(t, err)
		require.Equal(t, n == 1, ok)
	}

	before := len(fake.Calls())
	ok, err := store.FinishRun(ctx, "a", "", horai.RunResult{})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = store.RenewRun(ctx, "a", "", 1)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = store.ClaimRun(ctx, "a", horai.RunClaim{StartedAt: 0, RunID: "r"})
	require.ErrorIs(t, err, horai.ErrInvalidRunClaim)
	require.False(t, ok)
	require.Len(t, fake.Calls(), before, "an empty run ID or invalid claim must not reach the database")
}

// TestMySQLCreateTaskResolvesInsertFailure pins how the MySQL dialect, which
// runs a plain INSERT, tells a duplicate key from another failure: by whether
// the task exists afterwards.
func TestMySQLCreateTaskResolvesInsertFailure(t *testing.T) {
	t.Parallel()
	row := []driver.Value{"a", "", int64(1), int64(0), "", int64(0), int64(0), "", int64(0), int64(0), "", int64(0), int64(0),
		false, false, false, false, "{}", int64(0), int64(0), int64(1)}
	for _, tc := range []struct {
		name   string
		exists bool
	}{{"duplicate", true}, {"other_error", false}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db, _ := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
				switch {
				case strings.HasPrefix(q, "INSERT"):
					return testhelpers.FakeSQLReply{Err: errBoom}
				case tc.exists:
					return testhelpers.FakeSQLReply{Columns: taskCols(), Rows: [][]driver.Value{row}}
				default:
					return testhelpers.FakeSQLReply{Columns: taskCols()}
				}
			})
			store, err := sqldb.New(db, sqldb.MySQL)
			require.NoError(t, err)
			created, err := store.CreateTask(t.Context(), &horai.TaskState{ID: "a"})
			require.False(t, created)
			if tc.exists {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, errBoom)
		})
	}
}

// TestEnsureSchemaUpgradesExistingTables pins the column upgrade of a tasks
// table created by an earlier release: PostgreSQL adds the columns with ADD
// COLUMN IF NOT EXISTS; MySQL probes information_schema (in the table's schema
// qualifier) and adds only the missing columns.
func TestEnsureSchemaUpgradesExistingTables(t *testing.T) {
	t.Parallel()
	t.Run("postgres", func(t *testing.T) {
		t.Parallel()
		db, fake := testhelpers.NewFakeSQL(t, nil)
		store, err := sqldb.New(db, sqldb.Postgres)
		require.NoError(t, err)
		require.NoError(t, store.EnsureSchema(t.Context()))
		var alters []string
		for _, c := range fake.Calls() {
			if strings.HasPrefix(c.Query, "ALTER TABLE") {
				alters = append(alters, c.Query)
			}
		}
		require.Equal(t, []string{`ALTER TABLE "scheduler_tasks" ADD COLUMN IF NOT EXISTS run_lease_until BIGINT NOT NULL DEFAULT 0, ` +
			`ADD COLUMN IF NOT EXISTS run_lease_id TEXT NOT NULL DEFAULT '', ADD COLUMN IF NOT EXISTS run_at BIGINT NOT NULL DEFAULT 0`}, alters)
	})
	t.Run("mysql", func(t *testing.T) {
		t.Parallel()
		db, fake := testhelpers.NewFakeSQL(t, func(q string, _ []any) testhelpers.FakeSQLReply {
			switch {
			case strings.Contains(q, "information_schema"):
				return testhelpers.FakeSQLReply{Columns: []string{"COLUMN_NAME"}, Rows: [][]driver.Value{{"id"}, {"RUN_LEASE_UNTIL"}}}
			}
			return testhelpers.FakeSQLReply{}
		})
		store, err := sqldb.New(db, sqldb.MySQL, sqldb.WithTasksTable("app.sched_tasks"))
		require.NoError(t, err)
		require.NoError(t, store.EnsureSchema(t.Context()))
		var alters []string
		for _, c := range fake.Calls() {
			if strings.Contains(c.Query, "information_schema") {
				require.Equal(t, []any{"app", "sched_tasks"}, c.Args)
			}
			if strings.HasPrefix(c.Query, "ALTER TABLE") {
				alters = append(alters, c.Query)
			}
		}
		require.Equal(t, []string{
			"ALTER TABLE `app`.`sched_tasks` ADD COLUMN run_lease_id VARBINARY(1020) NOT NULL DEFAULT ''",
			"ALTER TABLE `app`.`sched_tasks` ADD COLUMN run_at BIGINT NOT NULL DEFAULT 0",
		}, alters)
	})
}

func TestWriteErrorsAreWrapped(t *testing.T) {
	t.Parallel()
	db, _ := testhelpers.NewFakeSQL(t, func(string, []any) testhelpers.FakeSQLReply { return testhelpers.FakeSQLReply{Err: errBoom} })
	store, err := sqldb.New(db, sqldb.Postgres)
	require.NoError(t, err)
	_, err = store.ClaimRun(t.Context(), "a", horai.RunClaim{StartedAt: 1, RunID: "r"})
	require.ErrorIs(t, err, errBoom)
	for _, err := range store.Tasks(t.Context()) {
		require.ErrorIs(t, err, errBoom)
	}
}

func TestIteratorClosesRowsOnBreak(t *testing.T) {
	t.Parallel()
	row := []driver.Value{"a", "", int64(1), int64(0), "", int64(0), int64(0), "", int64(0), int64(0), "", int64(0), int64(0),
		false, false, false, false, `{"k":"v"}`, int64(0), int64(0), int64(1)}
	db, fake := testhelpers.NewFakeSQL(t, func(string, []any) testhelpers.FakeSQLReply {
		return testhelpers.FakeSQLReply{Columns: taskCols(), Rows: [][]driver.Value{row, row, row}}
	})
	store, err := sqldb.New(db, sqldb.Postgres)
	require.NoError(t, err)

	for state, err := range store.Tasks(t.Context()) {
		require.NoError(t, err)
		require.Equal(t, map[string]string{"k": "v"}, state.Meta)
		break
	}
	require.Equal(t, 1, fake.RowsClosed())
}

func taskCols() []string {
	return strings.Split("id,description,status,priority,schedule,last_run_at,next_run_at,last_run_id,run_started_at,run_lease_until,"+
		"run_lease_id,run_at,failures,skip_next_run,disable_history,unmanaged,one_shot,meta,created_at,updated_at,revision", ",")
}

func TestOverlongValuesAreRejected(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, nil)
	store, err := sqldb.New(db, sqldb.Postgres)
	require.NoError(t, err)
	ctx := t.Context()
	limit := strings.Repeat("a", sqldb.MaxIDLength)

	require.NoError(t, createState(store, ctx, &horai.TaskState{ID: limit}))
	writes := len(fake.Calls())

	// One trailing space past the limit: PostgreSQL would trim it and hit the
	// row above, so it must never reach the database.
	over := &horai.TaskState{ID: limit + " "}
	require.ErrorIs(t, createState(store, ctx, over), sqldb.ErrValueTooLong)
	_, err = store.ReplaceTaskIf(ctx, over, horai.FenceOf(over))
	require.ErrorIs(t, err, sqldb.ErrValueTooLong)
	require.ErrorIs(t, store.AddHistory(ctx, &horai.TaskHistory{ID: "h", TaskID: limit + " "}), sqldb.ErrValueTooLong)
	require.ErrorIs(t, store.AddHistory(ctx, &horai.TaskHistory{ID: limit + "x", TaskID: "t"}), sqldb.ErrValueTooLong)
	_, err = store.ClaimRun(ctx, "t", horai.RunClaim{StartedAt: 1, RunID: limit + "x"})
	require.ErrorIs(t, err, sqldb.ErrValueTooLong)
	_, err = store.CreateTask(ctx, over)
	require.ErrorIs(t, err, sqldb.ErrValueTooLong)
	require.ErrorIs(t, createState(store, ctx, &horai.TaskState{ID: "t", RunLeaseID: limit + "x"}),
		sqldb.ErrValueTooLong)
	long := &horai.TaskState{ID: "t", Schedule: strings.Repeat("*", sqldb.MaxScheduleLength+1)}
	require.ErrorIs(t, createState(store, ctx, long), sqldb.ErrValueTooLong)
	require.Len(t, fake.Calls(), writes, "rejected writes must not reach the database")

	// The limit counts characters, not bytes.
	require.NoError(t, createState(store, ctx, &horai.TaskState{ID: strings.Repeat("é", sqldb.MaxIDLength)}))
}

func TestMySQLFinishRunAssignmentOrder(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, nil)
	store, err := sqldb.New(db, sqldb.MySQL)
	require.NoError(t, err)
	_, err = store.FinishRun(t.Context(), "a", "r", horai.RunResult{})
	require.NoError(t, err)

	// MySQL evaluates SET left to right with already-updated values, so no
	// assignment may read a column assigned before it.
	q := fake.Calls()[0].Query
	set := q[strings.Index(q, " SET ")+5 : strings.Index(q, " WHERE ")]
	starts := setAssignmentRE.FindAllStringSubmatchIndex(set, -1)
	var assigned []string
	for k, m := range starts {
		end := len(set)
		if k+1 < len(starts) {
			end = starts[k+1][0]
		}
		col, expr := set[m[2]:m[3]], set[m[1]:end]
		for _, prev := range assigned {
			require.NotContains(t, expr, prev, "%s reads %s, which is assigned earlier", col, prev)
		}
		assigned = append(assigned, col)
	}
	require.Contains(t, assigned, "status")
	require.Contains(t, assigned, "next_run_at")
}

// TestMySQLFilteredQueryReadsTextViews pins the shape of a filtered MySQL page
// query: the filter reads the utf8mb4 views of the derived table while the
// cursor, the task_id predicate and the ordering stay on the raw columns, and
// the arguments follow the placeholders in textual order.
func TestPostgresSchemaLocksHistoryFirst(t *testing.T) {
	t.Parallel()
	db, fake := testhelpers.NewFakeSQL(t, nil)
	store, err := sqldb.New(db, sqldb.Postgres)
	require.NoError(t, err)
	require.NoError(t, store.EnsureSchema(t.Context()))

	calls := fake.Calls()
	require.Equal(t, 1, fake.Commits())
	lastHistory, firstTasks := -1, len(calls)
	for i, c := range calls {
		require.True(t, c.InTx, c.Query)
		switch {
		case strings.Contains(c.Query, `"scheduler_history"`):
			lastHistory = i
		case strings.Contains(c.Query, `"scheduler_tasks"`):
			firstTasks = min(firstTasks, i)
		}
	}
	require.Equal(t, "SELECT pg_advisory_xact_lock($1)", calls[0].Query)
	require.Less(t, lastHistory, firstTasks, "every history statement precedes every tasks statement")
}

func createState(s *sqldb.Store, ctx context.Context, v *horai.TaskState) error {
	_, err := s.CreateTask(ctx, v)
	return err
}
