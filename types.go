// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import (
	"fmt"

	"github.com/altessa-s/go-horai/task"
)

// TaskStatus represents the lifecycle status of a scheduled task. The numeric
// values are independent of transport enums.
type TaskStatus int32

const (
	// TaskStatusUnspecified is the zero value. It should not be used when
	// creating or updating tasks; its presence signals an uninitialized field.
	TaskStatusUnspecified TaskStatus = 0
	// TaskStatusActive indicates the task is scheduled and will be dispatched
	// when its next run time arrives. This is the normal steady-state for
	// periodic tasks.
	TaskStatusActive TaskStatus = 1
	// TaskStatusPaused indicates the task has been paused via
	// [Scheduler.PauseTask] and will not execute until [Scheduler.ResumeTask]
	// is called.
	TaskStatusPaused TaskStatus = 2
	// TaskStatusDisabled indicates the task has been disabled via
	// [Scheduler.DisableTask]. Disabled tasks cannot be triggered manually
	// either. Use [Scheduler.EnableTask] to re-activate.
	TaskStatusDisabled TaskStatus = 3
	// TaskStatusRunning indicates the task's function is currently executing.
	// The scheduler transitions a task to this status just before invoking its
	// function and resets it to [TaskStatusActive] (or [TaskStatusCompleted]
	// for one-shot tasks) upon completion.
	TaskStatusRunning TaskStatus = 4
	// TaskStatusCompleted indicates a one-shot task that has finished its
	// single execution. Completed tasks are inert and reject most management
	// operations with [ErrTaskCompleted].
	TaskStatusCompleted TaskStatus = 5
)

// String returns a lowercase label for the status (e.g. "active", "paused").
// Unrecognized values, including [TaskStatusUnspecified], return "unspecified".
func (s TaskStatus) String() string {
	switch s {
	case TaskStatusActive:
		return "active"
	case TaskStatusPaused:
		return "paused"
	case TaskStatusDisabled:
		return "disabled"
	case TaskStatusRunning:
		return "running"
	case TaskStatusCompleted:
		return "completed"
	default:
		return "unspecified"
	}
}

// TaskState represents the persistent state of a scheduled task as stored by a
// [Store] backend. It embeds [TaskSummary] for the shared summary fields and
// adds detail fields (LastRunID, Meta, timestamps) that are only relevant when
// inspecting a single task. Fields with JSON tags are serialized by storage
// implementations that use JSON encoding.
type TaskState struct {
	TaskSummary
	LastRunID    string `json:"last_run_id,omitempty"`
	RunStartedAt int64  `json:"run_started_at,omitempty"`
	// RunLeaseUntil is the Unix second until which the instance executing the
	// unfinished run (RunStartedAt != 0) vouches for it. [Store.ClaimRun]
	// sets it with the claim and the owner renews it through [Store.RenewRun]
	// while the run is in flight; stale recovery on any instance leaves the run
	// alone until it has passed. [Store.FinishRun] and stale recovery clear
	// it. A lease counts only while RunLeaseID names the unfinished run: zero,
	// or a lease bound to another run, means the run was claimed by a release
	// without leases, for which recovery falls back to RunStartedAt plus its
	// own stale timeout.
	RunLeaseUntil int64 `json:"run_lease_until,omitempty"`
	// RunLeaseID is the run ID RunLeaseUntil was written for, stored by
	// [Store.ClaimRun] and [Store.RenewRun] together with the lease. A
	// release without leases rewrites LastRunID but not the lease, so an
	// inherited lease no longer matches LastRunID and is not trusted.
	RunLeaseID string `json:"run_lease_id,omitempty"`
	// RunAt is the occurrence a one-shot task was registered for, as Unix
	// seconds before any clamping to the registration time; always positive for
	// one-shot tasks registered by this release and zero for periodic tasks.
	// [Scheduler.Register] compares it to decide whether re-registering a
	// completed one-shot task schedules a new run.
	RunAt     int64             `json:"run_at,omitempty"`
	Meta      map[string]string `json:"meta,omitempty"`
	CreatedAt int64             `json:"created_at"`
	UpdatedAt int64             `json:"updated_at"`
	// Revision is incremented atomically by the storage on every write
	// (CreateTask, ClaimRun, RenewRun, FinishRun, ReplaceTaskIf).
	// Values set by callers are ignored; it lets ReplaceTaskIf detect any
	// concurrent change.
	Revision int64 `json:"revision,omitempty"`
}

// TaskHistory represents a record of a single task execution, including timing
// information and whether the execution succeeded. History entries are created
// automatically by the scheduler unless the task has DisableHistory set.
// They are pruned periodically according to the retention configured via
// [WithHistoryRetention].
type TaskHistory struct {
	ID         string `json:"id"` // Unique ID for this history entry
	TaskID     string `json:"task_id"`
	RunID      string `json:"run_id"`
	Error      string `json:"error,omitempty"`
	StartedAt  int64  `json:"started_at"`
	EndedAt    int64  `json:"ended_at"`
	DurationMs int64  `json:"duration_ms"`
	Success    bool   `json:"success"`
}

// TaskSummary provides a read-only summary view of a task, suitable for listing
// endpoints and management UIs. It mirrors a subset of [TaskState] fields but
// omits mutable execution bookkeeping (e.g., LastRunID, Meta) to keep listing
// payloads lightweight.
type TaskSummary struct {
	ID             string        `json:"id"`
	Description    string        `json:"description,omitempty"`
	Status         TaskStatus    `json:"status"`
	Priority       task.Priority `json:"priority"`
	Schedule       string        `json:"schedule"`
	LastRunAt      int64         `json:"last_run_at,omitempty"`
	NextRunAt      int64         `json:"next_run_at,omitempty"`
	SkipNextRun    bool          `json:"skip_next_run,omitempty"`
	DisableHistory bool          `json:"disable_history,omitempty"`
	Unmanaged      bool          `json:"unmanaged,omitempty"`
	OneShot        bool          `json:"one_shot,omitempty"`
	Failures       int32         `json:"failures"`
}

// TaskFence is the compare-and-swap precondition of Storage.ReplaceTaskIf: the
// run-ownership fields and revision a writer observed when it read the task.
type TaskFence struct {
	Status       TaskStatus
	NextRunAt    int64
	LastRunID    string
	RunStartedAt int64
	Revision     int64
}

// FenceOf returns the [TaskFence] describing state as currently read.
func FenceOf(state *TaskState) TaskFence {
	return TaskFence{
		Status:       state.Status,
		NextRunAt:    state.NextRunAt,
		LastRunID:    state.LastRunID,
		RunStartedAt: state.RunStartedAt,
		Revision:     state.Revision,
	}
}

// RunClaim describes the run [Store.ClaimRun] starts and the occurrence it
// fences on; see the Run ownership rules of [Store].
type RunClaim struct {
	// NextRunAt and RunAt identify the occurrence the caller observed as due.
	// The claim succeeds only while both are still stored, zero included.
	NextRunAt int64
	RunAt     int64
	// StartedAt, RunID and LeaseUntil are stored as RunStartedAt, LastRunID
	// (and RunLeaseID) and RunLeaseUntil, so the run owns a lease from the
	// moment it exists.
	StartedAt  int64
	RunID      string
	LeaseUntil int64
}

// Validate reports whether c describes a run that can be owned: StartedAt
// must be positive (zero means "no unfinished run"), RunID non-empty and
// LeaseUntil not negative. The error wraps [ErrInvalidRunClaim].
func (c RunClaim) Validate() error {
	switch {
	case c.StartedAt <= 0:
		return fmt.Errorf("%w: StartedAt %d is not positive", ErrInvalidRunClaim, c.StartedAt)
	case c.RunID == "":
		return fmt.Errorf("%w: empty RunID", ErrInvalidRunClaim)
	case c.LeaseUntil < 0:
		return fmt.Errorf("%w: LeaseUntil %d is negative", ErrInvalidRunClaim, c.LeaseUntil)
	}
	return nil
}

// RunResult contains the execution fields committed by [Store.FinishRun]
// under the finish transition of [Store]'s Run ownership rules. Schedule
// identifies the schedule used to compute NextRunAt, so a concurrently changed
// schedule and its next occurrence are preserved.
//
// RunAt identifies the one-shot occurrence the run executed (the claimed
// state's [TaskState.RunAt]; zero for periodic runs). A task re-registered for
// a different occurrence while the run executed keeps that new occurrence
// (status back to active if still running, NextRunAt kept).
type RunResult struct {
	StartedAt int64
	EndedAt   int64
	NextRunAt int64
	RunAt     int64
	Schedule  string
	Success   bool
}
