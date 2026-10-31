// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package clickhouse

import (
	"context"
	"errors"
	"iter"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/altessa-s/go-atlas-core/types/nilcheck"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	coreerrs "github.com/altessa-s/go-atlas-core/errors"
)

// deleteSyncAllReplicas is the lightweight_deletes_sync value that waits for
// the delete on every replica.
const deleteSyncAllReplicas = 2

// ErrNoConn is returned by [New] when no ClickHouse connection is supplied.
var ErrNoConn = errors.New("scheduler/clickhouse: connection is required")

// Conn is the subset of driver.Conn the storage needs. The caller owns the
// connection and its lifecycle.
type Conn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
}

var (
	_ horai.HistoryStore = (*Store)(nil)
	_ Conn               = (driver.Conn)(nil)
)

// Store implements [horai.HistoryStore] on a ClickHouse table, for use
// with scheduler.WithHistoryStore. It is safe for concurrent use.
type Store struct {
	conn  Conn
	opts  *options
	table string // backtick-quoted table name

	insertStmt, historyStmt, deleteStmt string
}

// New creates a ClickHouse history storage over an existing connection. It
// performs no I/O: the table is expected to exist, created by
// [Store.EnsureSchema] or applied from [SchemaDDL] through a migration. The
// table, cluster and engine names are validated as [SchemaDDL] validates them.
func New(conn Conn, opts ...Option) (*Store, error) {
	if nilcheck.IsNil(conn) {
		return nil, ErrNoConn
	}
	o := newOptions(opts...)
	if err := validateSchemaNames(o.tableName, o.engine, o.cluster); err != nil {
		return nil, err
	}
	table := "`" + o.tableName + "`"
	return &Store{
		conn:        conn,
		opts:        o,
		table:       table,
		insertStmt:  "INSERT INTO " + table + " (" + historyColumns + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
		historyStmt: "SELECT " + historyColumns + " FROM " + table + " WHERE `task_id` = ? ORDER BY `started_at` DESC, `id` DESC",
		deleteStmt:  "DELETE FROM " + table + onCluster(o.cluster) + " WHERE `task_id` = ?",
	}, nil
}

// EnsureSchema creates the history table from [SchemaDDL] unless it exists. A
// table that exists is left as it is, whatever its definition.
func (s *Store) EnsureSchema(ctx context.Context) error {
	ddl, err := SchemaDDL(s.opts.tableName, s.opts.engine, s.opts.cluster, s.opts.ttl)
	if err != nil {
		return err
	}
	if err := s.conn.Exec(ctx, ddl); err != nil {
		return coreerrs.WrapOperation(err, "create scheduler history table")
	}
	return nil
}

// AddHistory inserts one history entry. The scheduler writes one row per run,
// which ClickHouse handles badly as a part of its own, so the row goes through
// an asynchronous insert that the server buffers into a shared block. The call
// waits for the flush, so the entry is visible once it returns.
func (s *Store) AddHistory(ctx context.Context, h *horai.TaskHistory) error {
	asyncCtx := chgo.Context(ctx, chgo.WithSettings(chgo.Settings{
		"async_insert":          1,
		"wait_for_async_insert": 1,
	}))
	if err := s.conn.Exec(asyncCtx, s.insertStmt,
		h.ID, h.TaskID, h.RunID, h.Error, h.StartedAt, h.EndedAt, h.DurationMs, h.Success); err != nil {
		return coreerrs.WrapOperation(err, "add scheduler history")
	}
	return nil
}

// History yields the task's history ordered by StartedAt descending, ID
// descending as tie-breaker — the order [Store.ListHistory] uses.
func (s *Store) History(ctx context.Context, id string) iter.Seq2[*horai.TaskHistory, error] {
	return s.query(ctx, s.historyStmt, id)
}

// CleanupHistory deletes records strictly before the engine's cutoff and waits for visibility.
func (s *Store) CleanupHistory(ctx context.Context, cutoff int64) error {
	syncCtx := chgo.Context(ctx, chgo.WithSettings(chgo.Settings{"lightweight_deletes_sync": deleteSyncAllReplicas,
		"mutations_sync": deleteSyncAllReplicas}))
	return s.conn.Exec(syncCtx, "DELETE FROM "+s.table+onCluster(s.opts.cluster)+" WHERE ended_at < ?", cutoff)
}

// DeleteHistory removes the history of task id with a lightweight DELETE,
// which marks the rows deleted and purges them as parts merge. The statement
// sets lightweight_deletes_sync and mutations_sync to 2, so it returns only once every replica
// hides the rows, whatever the session or profile default; the setting
// requires ClickHouse 24.x.
func (s *Store) DeleteHistory(ctx context.Context, id string) error {
	syncCtx := chgo.Context(ctx, chgo.WithSettings(chgo.Settings{
		"lightweight_deletes_sync": deleteSyncAllReplicas,
		"mutations_sync":           deleteSyncAllReplicas,
	}))
	if err := s.conn.Exec(syncCtx, s.deleteStmt, id); err != nil {
		return coreerrs.WrapOperation(err, "delete scheduler history")
	}
	return nil
}

// ListHistory returns up to pg.Limit+1 history entries of taskID after the
// cursor, ordered by StartedAt descending and ID descending. A non-nil f is
// evaluated by ClickHouse.
func (s *Store) ListHistory(ctx context.Context, taskID string, pg horai.HistoryPage, f query.Expr) ([]*horai.TaskHistory, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}
	stmt, args, err := s.pageStatement(taskID, pg, f)
	if err != nil {
		return nil, err
	}
	var page []*horai.TaskHistory
	for h, err := range s.query(ctx, stmt, args...) {
		if err != nil {
			return nil, err
		}
		page = append(page, h)
	}
	return page, nil
}

// pageStatement renders the ListHistory query and its arguments, in the
// order of their placeholders. Translators are not safe for concurrent use, so
// one is built per call.
func (s *Store) pageStatement(taskID string, pg horai.HistoryPage, f query.Expr) (string, []any, error) {
	var b strings.Builder
	b.WriteString("SELECT " + historyColumns + " FROM " + s.table + " WHERE `task_id` = ?")
	args := []any{taskID}
	if pg.AfterID != "" {
		b.WriteString(" AND (`started_at` < ? OR (`started_at` = ? AND `id` < ?))")
		args = append(args, pg.AfterStartedAt, pg.AfterStartedAt, pg.AfterID)
	}
	if f != nil {
		where, filterArgs, err := translate(f, query.HistoryFields())
		if err != nil {
			return "", nil, err
		}
		b.WriteString(" AND (" + where + ")")
		args = append(args, filterArgs...)
	}
	b.WriteString(" ORDER BY `started_at` DESC, `id` DESC LIMIT ?")
	return b.String(), append(args, pg.Limit+1), nil
}

// query yields the history rows stmt selects.
func (s *Store) query(ctx context.Context, stmt string, args ...any) iter.Seq2[*horai.TaskHistory, error] {
	return func(yield func(*horai.TaskHistory, error) bool) {
		rows, err := s.conn.Query(ctx, stmt, args...)
		if err != nil {
			yield(nil, coreerrs.WrapOperation(err, "query scheduler history"))
			return
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			h := new(horai.TaskHistory)
			if err := rows.Scan(&h.ID, &h.TaskID, &h.RunID, &h.Error, &h.StartedAt, &h.EndedAt, &h.DurationMs, &h.Success); err != nil {
				yield(nil, coreerrs.WrapOperation(err, "scan scheduler history"))
				return
			}
			if !yield(h, nil) {
				return
			}
		}
		if err := rows.Err(); err != nil {
			yield(nil, coreerrs.WrapOperation(err, "iterate scheduler history"))
		}
	}
}
