// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import (
	"context"
	"iter"
	"time"

	"github.com/altessa-s/go-horai/query"
	"github.com/altessa-s/go-horai/task"
)

// TaskStore owns registration and fenced management writes. Reads return independent copies.
type TaskStore interface {
	GetTask(context.Context, string) (*TaskState, error)
	CreateTask(context.Context, *TaskState) (bool, error)
	ReplaceTaskIf(context.Context, *TaskState, TaskFence) (bool, error)
	DeleteTask(context.Context, string) error
	Tasks(context.Context) iter.Seq2[*TaskState, error]
}

// RunStore atomically owns execution transitions. Every write increments Revision.
// ClaimRun requires an active task with no unfinished run and matching NextRunAt and RunAt.
// Claims must pass RunClaim.Validate before any write. RenewRun and FinishRun require a
// nonempty matching LastRunID and nonzero RunStartedAt, regardless of status.
// FinishRun preserves concurrent configuration and management changes. A matching one-shot
// occurrence completes regardless of status; otherwise running becomes active. NextRunAt
// changes only for an unchanged periodic schedule. Finishing clears the lease and start,
// updates LastRunAt and UpdatedAt, and resets failures on success or increments on failure.
type RunStore interface {
	DueTasks(context.Context, int64) iter.Seq2[*TaskState, error]
	// UnfinishedRuns returns every claimed unfinished run, including paused and disabled
	// tasks. Persistent backends must use an index independent of status. The engine alone
	// decides whether the lease and process-local ownership allow a fenced reset.
	UnfinishedRuns(context.Context) iter.Seq2[*TaskState, error]
	ClaimRun(context.Context, string, RunClaim) (bool, error)
	RenewRun(context.Context, string, string, int64) (bool, error)
	FinishRun(context.Context, string, string, RunResult) (bool, error)
}

// TaskLister returns ID-ascending pages with one lookahead item.
type TaskLister interface {
	ListTasks(context.Context, TaskPage, query.Expr) ([]*TaskState, error)
}

// HistoryStore persists execution records. Cleanup uses a strict ended-before cutoff.
type HistoryStore interface {
	AddHistory(context.Context, *TaskHistory) error
	History(context.Context, string) iter.Seq2[*TaskHistory, error]
	ListHistory(context.Context, string, HistoryPage, query.Expr) ([]*TaskHistory, error)
	CleanupHistory(context.Context, int64) error
	DeleteHistory(context.Context, string) error
}

// Store is the complete task persistence contract.
type Store interface {
	TaskStore
	RunStore
	TaskLister
	HistoryStore
}

// TaskCursor identifies the last task returned.
type TaskCursor struct{ AfterID string }

// HistoryCursor identifies the last history item in descending timestamp and ID order.
type HistoryCursor struct {
	AfterStartedAt int64
	AfterID        string
}

// TaskPage selects an ID-ascending page, plus one lookahead item.
type TaskPage struct {
	AfterID string
	Limit   int64
}

// HistoryPage selects a descending history page, plus one lookahead item.
type HistoryPage struct {
	AfterStartedAt int64
	AfterID        string
	Limit          int64
}

// Clock supplies the engine's timestamps.
type Clock interface{ Now() time.Time }

// ReadinessFunc gates execution without stopping lifecycle processing.
type ReadinessFunc func() bool

// Metrics receives execution observations. Implementations must be concurrency safe.
type Metrics interface {
	TaskDispatched(string, task.Priority)
	TaskFinished(string, task.Priority, time.Duration, error)
	TaskSkipped(string)
	DispatchLag(string, time.Duration)
	StaleRecovered()
	TickDone(time.Duration)
	RunningChanged(int)
	RegisteredChanged(int)
	StorageError(string)
}
