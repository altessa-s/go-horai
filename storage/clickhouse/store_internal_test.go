// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

type recordingConn struct {
	stmt string
	args []any
	err  error
	rows driver.Rows
}

func (c *recordingConn) Exec(_ context.Context, q string, a ...any) error {
	c.stmt = q
	c.args = a

	return c.err
}
func (c *recordingConn) Query(_ context.Context, q string, a ...any) (driver.Rows, error) {
	c.stmt = q
	c.args = a

	return c.rows, c.err
}

type historyRows struct {
	driver.Rows
	n                int
	scanErr, iterErr error
	closed           bool
}

func (r *historyRows) Next() bool { r.n++; return r.n == 1 }
func (r *historyRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}

	for i, v := range []any{"h", "task", "node/run", "failure", int64(1), int64(2), int64(1000), false} {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(v))
	}

	return nil
}
func (r *historyRows) Close() error { r.closed = true; return nil }
func (r *historyRows) Err() error   { return r.iterErr }
func TestHistoryCommandsAndMapping(t *testing.T) {
	t.Parallel()

	c := &recordingConn{}
	s, err := New(c, WithTableName("history"), WithCluster("cluster"), WithTTL(time.Hour))
	require.NoError(t, err)
	require.NoError(t, s.EnsureSchema(t.Context()))
	require.Contains(t, c.stmt, "TTL")

	h := &horai.TaskHistory{ID: "h", TaskID: "task", RunID: "node/run", Error: "failure", StartedAt: 1, EndedAt: 2, DurationMs: 1000}
	require.NoError(t, s.AddHistory(t.Context(), h))
	require.Equal(t, []any{"h", "task", "node/run", "failure", int64(1), int64(2), int64(1000), false}, c.args)
	require.NoError(t, s.DeleteHistory(t.Context(), "task"))
	require.Contains(t, c.stmt, "ON CLUSTER")
	require.NoError(t, s.CleanupHistory(t.Context(), 3))
	require.Equal(t, []any{int64(3)}, c.args)
	c.rows = &historyRows{}

	for got, err := range s.History(t.Context(), "task") {
		require.NoError(t, err)
		require.Equal(t, h, got)

		break
	}

	require.True(t, c.rows.(*historyRows).closed)
	c.rows = &historyRows{}
	got, err := s.ListHistory(t.Context(), "task", horai.HistoryPage{AfterID: "after", AfterStartedAt: 5, Limit: 10}, query.Cmp{Field: "success", Op: query.Eq, Value: false})
	require.NoError(t, err)
	require.Equal(t, []*horai.TaskHistory{h}, got)
	require.Equal(t, strings.Count(c.stmt, "?"), len(c.args))
	_, err = s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 1}, query.Cmp{Field: "bad", Op: query.Eq, Value: "x"})
	require.Error(t, err)
}
func TestHistoryErrorMapping(t *testing.T) {
	t.Parallel()

	failure := errors.New("driver failure")
	c := &recordingConn{err: failure}
	s, err := New(c)
	require.NoError(t, err)
	require.ErrorIs(t, s.EnsureSchema(t.Context()), failure)
	require.ErrorIs(t, s.AddHistory(t.Context(), &horai.TaskHistory{}), failure)
	require.ErrorIs(t, s.DeleteHistory(t.Context(), "task"), failure)
	_, err = s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 1}, nil)
	require.ErrorIs(t, err, failure)

	for _, rows := range []*historyRows{{scanErr: failure}, {iterErr: failure}} {
		c.err = nil
		c.rows = rows
		_, err = s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 1}, nil)
		require.ErrorIs(t, err, failure)
		require.True(t, rows.closed)
	}

	for _, args := range [][3]string{{"bad-name", "MergeTree", ""}, {"good", "MergeTree", "bad cluster"}, {"good", "bad engine", ""}} {
		_, err := SchemaDDL(args[0], args[1], args[2], 0)
		require.Error(t, err)
	}

	_, err = New(nil)
	require.ErrorIs(t, err, ErrNoConn)
	_, err = New(c, WithTableName("bad-name"))
	require.ErrorIs(t, err, ErrInvalidIdentifier)
}
