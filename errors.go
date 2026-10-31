// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import "errors"

// ErrTaskNotFound is returned by the scheduler Pause method, the scheduler Resume method,
// the scheduler Disable method, the scheduler Enable method, the scheduler SkipNextRun method,
// and the scheduler Trigger method when no task with the given ID exists in storage.
var ErrTaskNotFound = errors.New("task not found")

// ErrTaskNotRegistered is returned by the scheduler Trigger method when the task ID is not
// present in the in-memory registration map, and by the scheduler Unregister method when it is
// neither registered in this process nor persisted in storage. This differs from
// [ErrTaskNotFound], which checks storage only.
var ErrTaskNotRegistered = errors.New("task not registered")

// ErrTaskUnmanaged is returned by the scheduler Pause method and the scheduler Disable method
// when the target task has its Unmanaged flag set, indicating it cannot be
// controlled through runtime management operations.
var ErrTaskUnmanaged = errors.New("task is unmanaged")

// ErrTaskNotPaused is returned by the scheduler Resume method when the task's current
// status is not [TaskStatusPaused].
var ErrTaskNotPaused = errors.New("task is not paused")

// ErrTaskNotDisabled is returned by the scheduler Enable method when the task's current
// status is not [TaskStatusDisabled].
var ErrTaskNotDisabled = errors.New("task is not disabled")

// ErrTaskDisabled is returned by the scheduler Trigger method when the task's current
// status is [TaskStatusDisabled].
var ErrTaskDisabled = errors.New("task is disabled")

// ErrTaskCompleted is returned by the scheduler Pause method, the scheduler Resume method,
// the scheduler Disable method, the scheduler Enable method, and the scheduler Trigger method when the target task has
// [TaskStatusCompleted], indicating a finished one-shot task or an exhausted schedule.
var ErrTaskCompleted = errors.New("task is completed")

// ErrTaskAlreadyDispatched is returned by the scheduler Trigger method when the task
// already has a dispatch queued or running. Each task holds at most one
// dispatch slot, so the trigger is rejected rather than queued behind the
// in-flight run; retry once the current run finishes.
var ErrTaskAlreadyDispatched = errors.New("task already dispatched")

// ErrNotReady is returned by the scheduler Trigger method when a readiness probe
// is configured and it returns false, indicating that subsystems are not yet ready.
var ErrNotReady = errors.New("subsystems not ready")

// ErrScheduleConflict is returned by the scheduler Register method when the provided
// [task.Config] sets both RunAt and Schedule, which are mutually exclusive.
var ErrScheduleConflict = errors.New("RunAt and Schedule are mutually exclusive")

// ErrConcurrentUpdate is returned by the task management methods
// (the scheduler Register method, the scheduler Pause method, the scheduler Resume method,
// the scheduler Disable method, the scheduler Enable method, the scheduler SkipNextRun method)
// when concurrent writes kept invalidating the read-modify-write for
// the configured retry limit attempts. Retrying the call is safe.
var ErrConcurrentUpdate = errors.New("task was modified concurrently")

// ErrInvalidRunClaim is returned by [RunStore.ClaimRun] implementations, via
// [RunClaim.Validate], for a claim whose run could never be owned afterwards.
var ErrInvalidRunClaim = errors.New("invalid run claim")

// ErrConcurrencyLimit indicates that a manual trigger could not acquire a dynamic slot.
var ErrConcurrencyLimit = errors.New("concurrency limit reached")

// ErrInvalidArgument indicates an invalid public storage argument.
var ErrInvalidArgument = errors.New("invalid argument")
