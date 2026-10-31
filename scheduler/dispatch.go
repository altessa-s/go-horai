// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"cmp"
	"context"
	"log/slog"
	"maps"
	"slices"
	"time"

	corectx "github.com/altessa-s/go-atlas-core/context"
	horai "github.com/altessa-s/go-horai"
)

// storageCtx caps a scheduler-owned [horai.Store] call at [WithStorageTimeout].
// The base context keeps its own cancellation — so [Scheduler.Stop] still
// aborts in-flight calls — while gaining a hard per-operation deadline that
// the scheduler's lifecycle context does not provide on its own.
func (s *Scheduler) storageCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return corectx.WithMaxTimeout(ctx, s.opts.storageTimeout)
}

// loadTasks verifies that the storage backend is reachable and logs every
// persisted task state for diagnostics. It does not populate the in-memory
// dispatch map — tasks must be re-registered with their functions via
// [Scheduler.Register] after Start returns.
func (s *Scheduler) loadTasks(ctx context.Context) error {
	opCtx, cancel := s.storageCtx(ctx)
	defer cancel()

	for state, err := range s.storage.Tasks(opCtx) {
		if err != nil {
			return err
		}
		s.logger.DebugContext(ctx, "found persisted task state",
			slog.String("task_id", state.ID),
			slog.String("status", state.Status.String()),
			slog.String("priority", state.Priority.String()))
	}

	return nil
}

// run is the main scheduler loop.
func (s *Scheduler) run() {
	ticker := time.NewTicker(s.opts.tickInterval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(s.opts.cleanupInterval)
	defer cleanupTicker.Stop()

	staleRecoveryInterval := min(s.opts.staleTaskTimeout, maxStaleRecoveryInterval)
	staleRecoveryTicker := time.NewTicker(staleRecoveryInterval)
	defer staleRecoveryTicker.Stop()

	for {
		select {
		case <-s.stopCtx.Done():
			return

		case <-ticker.C:
			s.tick()

		case <-cleanupTicker.C:
			s.cleanup()

		case <-staleRecoveryTicker.C:
			s.recoverStaleTasks(s.stopCtx, false)
		}
	}
}

// tick processes all tasks and executes those that are due.
// Tasks are processed in priority order: Critical > High > Normal > Low.
//
// A single DueTasks call fetches eligible states, which are then
// cross-referenced with the in-memory registration map. This avoids
// N individual GetTask calls per tick for networked backends.
func (s *Scheduler) tick() {
	started := s.opts.clock.Now()
	defer func() { s.metrics.TickDone(s.since(started)) }()

	// Skip execution if not the leader
	if !s.isLeader() {
		s.logger.DebugContext(s.stopCtx, "skipping tick: not leader")
		return
	}

	// Skip execution if subsystems are not ready
	if !s.isReady() {
		s.logger.DebugContext(s.stopCtx, "skipping tick: subsystems not ready")
		return
	}

	s.mu.RLock()
	tasksCopy := make(map[string]*registeredTask, len(s.tasks))
	maps.Copy(tasksCopy, s.tasks)
	s.mu.RUnlock()

	now := s.opts.clock.Now()

	dueStates, err := s.fetchDueStates(now.Unix(), len(tasksCopy))
	if err != nil {
		s.logger.ErrorContext(s.stopCtx, "failed to iterate due task states",
			slog.Any("error", err))
		return
	}

	// Collect pending tasks from the fetched states. horai.Store has already
	// filtered on status and due time, so only process-local conditions are
	// re-checked here.
	pending := make([]pendingTask, 0, len(dueStates))

	for _, state := range dueStates {
		// Only consider tasks that are registered in this process.
		task, registered := tasksCopy[state.ID]
		if !registered {
			continue
		}

		// Skip if a dispatch for this task is already queued or running. This
		// covers the window before the goroutine reaches executeTask, which is
		// unbounded in static semaphore mode where it blocks on the semaphore.
		if task.dispatched.Load() {
			continue
		}

		// Backstop the storage's due-time filter. ClaimRun's CAS re-checks
		// status, so a stale status cannot cause a spurious run — but nothing
		// downstream re-checks NextRunAt, so an implementation that got the
		// bound wrong would fire tasks early and silently.
		if now.Unix() < state.NextRunAt {
			continue
		}

		// A state moved to another occurrence by a different registration is
		// not this registration's to run; filtering it here keeps it from
		// taking concurrency slots on every tick. executeTask re-checks.
		if !task.matches(state) {
			continue
		}

		// An earlier run is still unfinished — the task was paused and resumed
		// while it ran — so ClaimRun would reject it; do not let it take a slot.
		if state.RunStartedAt != 0 {
			continue
		}

		if state.SkipNextRun {
			s.applySkip(now, state.ID, horai.FenceOf(state))
			continue
		}

		pending = append(pending, pendingTask{task: task, state: state})
	}

	// Sort by priority (highest first) using cmp.Compare
	slices.SortFunc(pending, func(a, b pendingTask) int {
		// Higher priority value = more important, so sort descending
		return cmp.Compare(b.task.config.Priority, a.task.config.Priority)
	})

	// Dispatch pending tasks respecting concurrency limits
	s.dispatchPending(pending)
}

// fetchDueStates materializes the task states eligible for dispatch at now in
// a single storage round-trip, bounded by [WithStorageTimeout]. The filter is
// pushed down to the backend, so the tick's cost tracks the number of tasks
// actually due rather than the size of the whole collection.
func (s *Scheduler) fetchDueStates(now int64, capacity int) ([]*horai.TaskState, error) {
	opCtx, cancel := s.storageCtx(s.stopCtx)
	defer cancel()

	states := make([]*horai.TaskState, 0, capacity)
	for state, err := range s.storage.DueTasks(opCtx, now) {
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	return states, nil
}

// applySkip consumes the SkipNextRun flag of a due task, advancing it to its
// following occurrence without invoking the task function. One-shot tasks are
// transitioned to [horai.TaskStatusCompleted] instead. The state is re-read first so
// a concurrent Pause or schedule change is not overwritten, and the write is
// fenced on due, the state observed by the due scan: a task paused or claimed
// in between is left alone.
func (s *Scheduler) applySkip(now time.Time, id string, due horai.TaskFence) {
	ctx, cancel := s.storageCtx(s.stopCtx)
	defer cancel()

	freshState, err := s.storage.GetTask(ctx, id)
	if err != nil || freshState == nil || !freshState.SkipNextRun || horai.FenceOf(freshState) != due {
		// State changed concurrently or the read failed: leave it alone.
		return
	}

	freshState.SkipNextRun = false
	if freshState.OneShot {
		// One-shot task: skipping means completed (no next run).
		freshState.Status = horai.TaskStatusCompleted
		freshState.NextRunAt = 0
	} else {
		freshState.NextRunAt = s.calculateNextRun(ctx, now, freshState).Unix()
	}
	freshState.UpdatedAt = now.Unix()

	replaced, err := s.storage.ReplaceTaskIf(ctx, freshState, due)
	if err != nil {
		s.logger.ErrorContext(ctx, "failed to update skipped task",
			slog.String("task_id", id),
			slog.Any("error", err))
	}
	if !replaced {
		// A concurrent claim, pause or schedule change won; it owns the state.
		return
	}
	s.metrics.TaskSkipped(id)
	s.logger.InfoContext(ctx, "task run skipped", slog.String("task_id", id))
}
