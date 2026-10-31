// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/altessa-s/go-horai/schedule"

	corectx "github.com/altessa-s/go-atlas-core/context"
	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// executeTask runs a single task and records the result.
func (s *Scheduler) executeTask(ctx context.Context, task *registeredTask, state *horai.TaskState) {
	if !task.running.CompareAndSwap(false, true) {
		return // Already running
	}
	defer task.running.Store(false)

	if !task.matches(state) {
		s.logger.DebugContext(ctx, "task state belongs to another registration, skipping execution",
			slog.String("task_id", state.ID))
		return
	}

	runID := s.opts.instanceID + runIDSeparator + generateID()
	startTime := s.opts.clock.Now()

	// Record dispatch lag: delay between scheduled time and actual execution
	if state.NextRunAt > 0 {
		lag := startTime.Sub(time.Unix(state.NextRunAt, 0))
		if lag > 0 {
			s.metrics.DispatchLag(state.ID, lag)
		}
	}

	s.logger.DebugContext(ctx, "task execution started",
		slog.String("task_id", state.ID),
		slog.String("priority", task.config.Priority.String()),
		slog.String("run_id", runID))

	// Atomically claim this occurrence (active→running) at the storage layer.
	// The claim — not leadership — is the correctness boundary: even if two
	// scheduler instances dispatch the same occurrence (e.g. during a
	// leader-election split-brain window), the CAS in ClaimRun lets exactly one
	// win, so the task body runs at most once per occurrence. state.NextRunAt and
	// state.RunAt fence the occurrence. A lost claim (already running, advanced, or no longer
	// active) is a clean no-op for this caller.
	//
	// The run is registered as live before the claim can make it visible, so
	// stale recovery in this process never mistakes it for an abandoned run.
	s.liveRuns.Store(runID, struct{}{})
	defer s.liveRuns.Delete(runID)
	claimCtx, claimCancel := s.storageCtx(ctx)
	claimed, err := s.storage.ClaimRun(claimCtx, state.ID, horai.RunClaim{
		NextRunAt: state.NextRunAt, RunAt: state.RunAt,
		StartedAt: startTime.Unix(), RunID: runID, LeaseUntil: startTime.Unix() + leaseSeconds(s.runLease()),
	})
	claimCancel()
	if err != nil {
		s.metrics.StorageError("claim_run")
		s.logger.ErrorContext(ctx, "failed to claim task run, aborting execution",
			slog.String("task_id", state.ID),
			slog.Any("error", err))
		return
	}
	if !claimed {
		s.logger.DebugContext(ctx, "task run not claimed (already running, advanced, or not active), skipping execution",
			slog.String("task_id", state.ID))
		return
	}
	// The claim stored the run's first lease. Keep it alive until the result is
	// recorded: deferred, so it covers the bookkeeping below and stops on every
	// return path. A claim slow enough to need its lease renewed at once must
	// confirm the run is still ours before the body starts.
	stopHeartbeat, owned := s.startRunHeartbeat(ctx, state.ID, runID, startTime)
	defer stopHeartbeat()
	if !owned {
		s.logger.WarnContext(ctx, "task run ownership not confirmed after a slow claim, skipping execution",
			slog.String("task_id", state.ID),
			slog.String("run_id", runID))
		return
	}

	// Reflect the claimed transition locally for the remainder of execution.
	state.Status = horai.TaskStatusRunning
	state.RunStartedAt = startTime.Unix()
	state.LastRunID = runID
	state.UpdatedAt = startTime.Unix()

	// Create execution context with timeout if specified
	execCtx, cancel := corectx.ApplyTimeout(ctx, task.config.Timeout)
	defer cancel()

	// Execute the task
	s.metrics.TaskDispatched(state.ID, task.config.Priority)
	s.metrics.RunningChanged(int(s.executions.Add(1)))
	defer func() { s.metrics.RunningChanged(int(s.executions.Add(-1))) }()
	execErr := s.invoke(execCtx, task.config.Func)

	endTime := s.opts.clock.Now()
	duration := endTime.Sub(startTime)
	success := execErr == nil
	s.metrics.TaskFinished(state.ID, task.config.Priority, duration, execErr)

	// The bookkeeping below must outlive cancellation of ctx. Stop cancels the
	// lifecycle context and only then waits on the WaitGroup, so if these
	// writes inherited that cancellation every graceful shutdown would leave
	// its in-flight tasks stuck in horai.TaskStatusRunning, to be resurrected later
	// by stale recovery with a bogus failure count. WithoutCancel detaches
	// them; the storage timeout keeps them bounded, and Stop still waits for
	// them because the goroutine has not returned yet. History is recorded
	// first, under a deadline of its own, so a slow history backend cannot
	// spend the deadline of the state writes that follow.
	detached := context.WithoutCancel(ctx)

	// Record history unless disabled
	if !state.DisableHistory {
		history := &horai.TaskHistory{
			ID:         generateID(),
			TaskID:     state.ID,
			RunID:      runID,
			StartedAt:  startTime.Unix(),
			EndedAt:    endTime.Unix(),
			DurationMs: duration.Milliseconds(),
			Success:    success,
		}

		if execErr != nil {
			history.Error = execErr.Error()
		}

		histCtx, histCancel := s.storageCtx(detached)
		if histErr := s.history.AddHistory(histCtx, history); histErr != nil {
			s.logger.ErrorContext(histCtx, "failed to record task history",
				slog.String("task_id", state.ID),
				slog.Any("error", histErr))
		}
		histCancel()
	}

	recCtx, recCancel := s.storageCtx(detached)
	defer recCancel()

	// Re-read fresh state to minimize race with concurrent updates (e.g., Pause, SetInterval)
	freshState, err := s.storage.GetTask(recCtx, state.ID)
	if err != nil || freshState == nil {
		s.logger.ErrorContext(recCtx, "failed to get fresh task state after execution",
			slog.String("task_id", state.ID),
			slog.Any("error", err))
		return
	}

	// Fence on the run ID stamped by ClaimRun. If it no longer names our run,
	// this occurrence was taken over while we were executing — stale recovery
	// reset the task and another claim won it. Writing our result now would
	// stamp the live run as finished (status back to active, RunStartedAt
	// cleared), letting a third dispatch claim it alongside the one still in
	// flight. The history entry above is per-run and stays; only the shared
	// state is off limits.
	if freshState.LastRunID != runID {
		s.logger.WarnContext(recCtx, "task state was reclaimed during execution, discarding result",
			slog.String("task_id", state.ID),
			slog.String("run_id", runID),
			slog.String("current_run_id", freshState.LastRunID))
		return
	}

	nextRunAt := int64(0)
	if !state.OneShot {
		if next := s.calculateNextRun(recCtx, startTime, state); !next.IsZero() {
			nextRunAt = next.Unix()
		}
	}
	if !success {
		s.logger.ErrorContext(recCtx, "task execution failed", slog.String("task_id", state.ID), slog.Any("error", execErr))
	}
	finished, err := s.storage.FinishRun(recCtx, state.ID, runID, horai.RunResult{
		StartedAt: startTime.Unix(), EndedAt: endTime.Unix(), NextRunAt: nextRunAt,
		RunAt: state.RunAt, Schedule: state.Schedule, Success: success,
	})
	if err != nil {
		s.logger.ErrorContext(recCtx, "failed to finish task run", slog.String("task_id", state.ID), slog.Any("error", err))
		return
	}
	if !finished {
		s.logger.WarnContext(recCtx, "task run no longer owned, discarding result", slog.String("task_id", state.ID), slog.String("run_id", runID))
		return
	}

	s.logger.DebugContext(ctx, "task execution completed",
		slog.String("task_id", state.ID),
		slog.String("run_id", runID),
		slog.Duration("duration", duration),
		slog.Bool("success", success))
}

// calculateNextRun computes the next run time based on the cron schedule.
func (s *Scheduler) calculateNextRun(ctx context.Context, from time.Time, state *horai.TaskState) time.Time {
	s.mu.RLock()
	registered := s.tasks[state.ID]
	s.mu.RUnlock()
	if registered != nil && registered.parsed != nil && registered.config.Schedule == state.Schedule {
		return registered.parsed.Next(from)
	}
	sched, err := schedule.Parse(state.Schedule)
	if err != nil {
		// This shouldn't happen since we validate schedule at registration,
		// but handle gracefully with a 1 hour fallback.
		s.logger.ErrorContext(ctx, "failed to parse cron schedule during next run calculation",
			slog.String("task_id", state.ID),
			slog.String("schedule", state.Schedule),
			slog.Any("error", err))
		return from.Add(time.Hour)
	}

	return sched.Next(from)
}

// cleanup removes old history entries.
func (s *Scheduler) cleanup() {
	ctx, cancel := s.storageCtx(s.stopCtx)
	defer cancel()

	if err := s.history.CleanupHistory(ctx, s.opts.clock.Now().Add(-s.opts.historyRetention).Unix()); err != nil {
		s.logger.ErrorContext(ctx, "failed to cleanup history", slog.Any("error", err))
	}
}

func (s *Scheduler) invoke(ctx context.Context, fn schedtask.Func) (err error) {
	defer func() {
		if p := recover(); p != nil {
			s.logger.ErrorContext(ctx, "task panicked", slog.String("panic_type", fmt.Sprintf("%T", p)), slog.String("stack", string(debug.Stack())))
			err = errors.New("task panicked")
		}
	}()
	return fn(ctx)
}

// advanceSchedule completes a periodic task when no future occurrence exists.
func (s *Scheduler) advanceSchedule(ctx context.Context, from time.Time, state *horai.TaskState) {
	next := s.calculateNextRun(ctx, from, state)
	if next.IsZero() {
		state.NextRunAt = 0
		state.Status = horai.TaskStatusCompleted
		return
	}
	state.NextRunAt = next.Unix()
}
