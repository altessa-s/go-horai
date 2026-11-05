// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

// DefaultPageSize is used for an unspecified limit.
const DefaultPageSize = 50

// MaxPageSize bounds application pages.
const MaxPageSize = 1000

// Page holds results and an optional typed continuation cursor.
type Page[T, C any] struct {
	Items []T
	Next  *C
}

// TaskListRequest describes a task page. Signing and encoding cursors belongs to the transport.
type TaskListRequest struct {
	After  *horai.TaskCursor
	Limit  int
	Filter query.Expr
}

// HistoryListRequest describes one task's history page.
type HistoryListRequest struct {
	TaskID string
	After  *horai.HistoryCursor
	Limit  int
	Filter query.Expr
}

// ClampLimit applies the default and maximum page sizes.
func ClampLimit(n int) int {
	if n <= 0 {
		return DefaultPageSize
	}

	return min(n, MaxPageSize)
}

// ListTasks returns tasks ordered by ID, plus a continuation cursor when needed.
func (s *Scheduler) ListTasks(ctx context.Context, r TaskListRequest) (*Page[*horai.TaskSummary, horai.TaskCursor], error) {
	if err := query.Validate(r.Filter, query.TaskFields()); err != nil {
		return nil, err
	}

	n := ClampLimit(r.Limit)

	p := horai.TaskPage{Limit: int64(n)}
	if r.After != nil {
		p.AfterID = r.After.AfterID
	}

	rows, err := s.storage.ListTasks(ctx, p, r.Filter)
	if err != nil {
		return nil, err
	}

	out := &Page[*horai.TaskSummary, horai.TaskCursor]{}
	if len(rows) > n {
		out.Next = &horai.TaskCursor{AfterID: rows[n-1].ID}
		rows = rows[:n]
	}

	for _, row := range rows {
		v := row.TaskSummary
		out.Items = append(out.Items, &v)
	}

	return out, nil
}

// ListHistory returns history ordered by start time and ID descending.
func (s *Scheduler) ListHistory(ctx context.Context, r HistoryListRequest) (*Page[*horai.TaskHistory, horai.HistoryCursor], error) {
	if err := query.Validate(r.Filter, query.HistoryFields()); err != nil {
		return nil, err
	}

	n := ClampLimit(r.Limit)

	p := horai.HistoryPage{Limit: int64(n)}
	if r.After != nil {
		p.AfterID = r.After.AfterID
		p.AfterStartedAt = r.After.AfterStartedAt
	}

	rows, err := s.history.ListHistory(ctx, r.TaskID, p, r.Filter)
	if err != nil {
		return nil, err
	}

	out := &Page[*horai.TaskHistory, horai.HistoryCursor]{Items: rows}
	if len(rows) > n {
		out.Next = &horai.HistoryCursor{AfterID: rows[n-1].ID, AfterStartedAt: rows[n-1].StartedAt}
		out.Items = rows[:n]
	}

	return out, nil
}
