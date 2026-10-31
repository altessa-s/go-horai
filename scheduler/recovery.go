// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// runAbandoned reports whether state carries an unfinished run that stale
// recovery may reset at now (Unix seconds). A run of this instance is abandoned
// as soon as it is no longer executing here. Any other run is abandoned only
// once its lease has expired. A lease bound to the run (RunLeaseID ==
// LastRunID), persisted by the owner with the claim and its renewals, is
// authoritative, so instances with different stale timeouts agree. A run
// without a lease of its own — claimed by a release without leases, which
// leaves an earlier run's lease in place — falls back to its start plus this
// instance's own lease. An unbound lease (RunLeaseID empty), written by a
// lease-aware build that did not bind leases yet, may belong to this run or an
// earlier one, so the later of both deadlines applies.
func (s *Scheduler) runAbandoned(state *horai.TaskState, now, leaseSec int64) bool {
	if state.RunStartedAt == 0 && state.Status != horai.TaskStatusRunning {
		return false
	}
	if state.LastRunID != "" && runOwner(state.LastRunID) == s.opts.instanceID {
		_, live := s.liveRuns.Load(state.LastRunID)
		return !live
	}
	if state.RunLeaseUntil != 0 && state.RunLeaseID != "" && state.RunLeaseID == state.LastRunID {
		return now > state.RunLeaseUntil
	}
	since := state.RunStartedAt
	if since == 0 {
		// States persisted before RunStartedAt was introduced.
		since = state.UpdatedAt
	}
	deadline := since + leaseSec
	if state.RunLeaseID == "" {
		deadline = max(deadline, state.RunLeaseUntil)
	}
	return now > deadline
}

// recoverStaleTasks resets abandoned runs (see [Scheduler.runAbandoned]): those
// of this instance that are no longer executing — after a restart with a stable
// [WithInstanceID], or when recording a result failed — and those of any
// instance whose lease has expired. Runs still held by a live instance are left
// alone, at startup as well as periodically, so recovery cannot hand a live run
// to a second executor. startup only selects the log message.
//
// One-shot tasks are also recovered: their NextRunAt is set to now — or kept,
// when the task was re-registered for a later RunAt during the run — so they
// re-execute on the next tick. This is intentional — a one-shot task whose run
// was abandoned never completed successfully, so it should be retried. If this
// is undesirable for a particular task, callers should use idempotency checks
// inside the task function.
func (s *Scheduler) recoverStaleTasks(ctx context.Context, startup bool) {
	now := s.opts.clock.Now()
	leaseSec := leaseSeconds(s.runLease())

	// States are materialized up front so the reset pass below never writes
	// while the storage iterator is open (which would deadlock the memory
	// backend), and so each write gets its own storage deadline.
	allStates, err := s.fetchStates(ctx, 0)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to iterate tasks for stale recovery",
			slog.Any("error", err))
		return
	}

	stale := make([]*horai.TaskState, 0)
	for _, state := range allStates {
		if s.runAbandoned(state, now.Unix(), leaseSec) {
			stale = append(stale, state)
		}
	}

	// Now reset each stale task outside the iterator
	for _, state := range stale {
		s.resetStaleTask(ctx, state.ID, horai.FenceOf(state), now, startup)
	}
}

// resetStaleTask clears the abandoned run of a single task. The state is
// re-read under its own storage deadline and written with
// [horai.Store.ReplaceTaskIf] fenced on the run observed as stale — revision
// included — so a run that finished, renewed its lease or was re-claimed since
// the collection pass is never overwritten. A task still in
// [horai.TaskStatusRunning] returns to [horai.TaskStatusActive]; one that management paused,
// disabled or resumed during the run keeps that status and its next run time.
func (s *Scheduler) resetStaleTask(ctx context.Context, id string, stale horai.TaskFence, now time.Time, startup bool) {
	opCtx, cancel := s.storageCtx(ctx)
	defer cancel()

	state, err := s.storage.GetTask(opCtx, id)
	if err != nil || state == nil {
		s.logger.ErrorContext(opCtx, "failed to get stale task for recovery",
			slog.String("task_id", id),
			slog.Any("error", err))
		return
	}

	// The run observed as stale has finished, renewed or been replaced.
	if horai.FenceOf(state) != stale {
		return
	}

	since := state.RunStartedAt
	if since == 0 {
		since = state.UpdatedAt
	}
	staleDuration := time.Duration(now.Unix()-since) * time.Second

	if state.Status == horai.TaskStatusRunning {
		state.Status = horai.TaskStatusActive
		if !state.OneShot && state.Schedule != "" {
			state.NextRunAt = s.calculateNextRun(opCtx, now, state).Unix()
		} else if state.OneShot {
			// Retry the abandoned occurrence now — unless the task was
			// re-registered for a later one during the run, which keeps its time.
			state.NextRunAt = max(state.NextRunAt, now.Unix())
		}
	}
	state.RunStartedAt = 0
	state.RunLeaseUntil = 0
	state.RunLeaseID = ""
	state.Failures++
	state.UpdatedAt = now.Unix()

	replaced, err := s.storage.ReplaceTaskIf(opCtx, state, stale)
	if err != nil {
		s.logger.ErrorContext(opCtx, "failed to reset stale task",
			slog.String("task_id", id),
			slog.Any("error", err))
		return
	}
	if !replaced {
		return
	}

	s.metrics.StaleRecovered()

	if startup {
		s.logger.WarnContext(opCtx, "recovered stale task on startup",
			slog.String("task_id", id))
	} else {
		s.logger.WarnContext(opCtx, "recovered stale task",
			slog.String("task_id", id),
			slog.Duration("stale_duration", staleDuration))
	}
}

func invoke(ctx context.Context, fn schedtask.Func) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("task panicked")
		}
	}()
	return fn(ctx)
}
