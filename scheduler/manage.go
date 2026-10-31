// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"iter"
	"time"

	horai "github.com/altessa-s/go-horai"
)

// MaxUpdateAttempts bounds the optimistic read-modify-write loop of the task
// management methods before they give up with [horai.ErrConcurrentUpdate].
const MaxUpdateAttempts = 10

// updateTask applies mutate to the current state of task id and writes the
// result with [horai.Store.ReplaceTaskIf] fenced on the state it read, re-reading
// and retrying when a concurrent write wins. An error from mutate aborts
// without writing.
func (s *Scheduler) updateTask(ctx context.Context, id string, mutate func(*horai.TaskState) error) error {
	for range MaxUpdateAttempts {
		state, err := s.storage.GetTask(ctx, id)
		if err != nil {
			return err
		}
		if state == nil {
			return horai.ErrTaskNotFound
		}

		fence := horai.FenceOf(state)
		if err = mutate(state); err != nil {
			return err
		}

		replaced, err := s.storage.ReplaceTaskIf(ctx, state, fence)
		if err != nil {
			return err
		}
		if replaced {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return horai.ErrConcurrentUpdate
}

// Pause transitions a task to [horai.TaskStatusPaused], preventing future
// executions until [Scheduler.Resume] is called. A currently running
// execution will complete normally.
//
// Returns [horai.ErrTaskNotFound] if no task with the given ID exists in storage,
// [horai.ErrTaskCompleted] if the task is a completed one-shot, or
// [horai.ErrTaskUnmanaged] if the task has its Unmanaged flag set.
func (s *Scheduler) Pause(ctx context.Context, id string) error {
	return s.updateTask(ctx, id, func(state *horai.TaskState) error {
		if state.Status == horai.TaskStatusCompleted {
			return horai.ErrTaskCompleted
		}

		if state.Unmanaged {
			return horai.ErrTaskUnmanaged
		}

		state.Status = horai.TaskStatusPaused
		state.UpdatedAt = s.opts.clock.Now().Unix()

		return nil
	})
}

// Resume transitions a [horai.TaskStatusPaused] task back to [horai.TaskStatusActive]
// and computes the next run time from the current moment. For one-shot tasks
// whose scheduled time has already passed, the next run is set to now.
//
// Returns [horai.ErrTaskNotFound] if no task with the given ID exists in storage,
// [horai.ErrTaskCompleted] if the task is a completed one-shot, or [horai.ErrTaskNotPaused]
// if the task's current status is not [horai.TaskStatusPaused].
func (s *Scheduler) Resume(ctx context.Context, id string) error {
	return s.updateTask(ctx, id, func(state *horai.TaskState) error {
		if state.Status == horai.TaskStatusCompleted {
			return horai.ErrTaskCompleted
		}

		if state.Status != horai.TaskStatusPaused {
			return horai.ErrTaskNotPaused
		}

		now := s.opts.clock.Now()
		state.Status = horai.TaskStatusActive
		if state.OneShot {
			// One-shot: keep existing NextRunAt or set to now if already past
			if state.NextRunAt < now.Unix() {
				state.NextRunAt = now.Unix()
			}
		} else {
			state.NextRunAt = s.calculateNextRun(ctx, now, state).Unix()
		}
		state.UpdatedAt = now.Unix()

		return nil
	})
}

// Disable transitions a task to [horai.TaskStatusDisabled], preventing both
// scheduled and manual execution (via [Scheduler.Trigger]). Use
// [Scheduler.Enable] to re-activate. Unlike [Scheduler.Pause],
// disabling is allowed in any status except [horai.TaskStatusCompleted]: a completed
// one-shot task is terminal, and disabling then enabling it must not run it
// again.
//
// Returns [horai.ErrTaskNotFound] if no task with the given ID exists in storage,
// [horai.ErrTaskCompleted] if the task is a completed one-shot, or
// [horai.ErrTaskUnmanaged] if the task has its Unmanaged flag set.
func (s *Scheduler) Disable(ctx context.Context, id string) error {
	return s.updateTask(ctx, id, func(state *horai.TaskState) error {
		if state.Status == horai.TaskStatusCompleted {
			return horai.ErrTaskCompleted
		}

		if state.Unmanaged {
			return horai.ErrTaskUnmanaged
		}

		state.Status = horai.TaskStatusDisabled
		state.UpdatedAt = s.opts.clock.Now().Unix()

		return nil
	})
}

// Enable transitions a [horai.TaskStatusDisabled] task back to [horai.TaskStatusActive]
// and recomputes the next run time from the current moment. For one-shot tasks
// whose scheduled time has already passed, the next run is set to now.
//
// Returns [horai.ErrTaskNotFound] if no task with the given ID exists in storage,
// [horai.ErrTaskCompleted] if the task is a completed one-shot, or
// [horai.ErrTaskNotDisabled] if the task's current status is not [horai.TaskStatusDisabled].
func (s *Scheduler) Enable(ctx context.Context, id string) error {
	return s.updateTask(ctx, id, func(state *horai.TaskState) error {
		if state.Status == horai.TaskStatusCompleted {
			return horai.ErrTaskCompleted
		}

		if state.Status != horai.TaskStatusDisabled {
			return horai.ErrTaskNotDisabled
		}

		now := s.opts.clock.Now()
		state.Status = horai.TaskStatusActive
		if state.OneShot {
			// One-shot: keep existing NextRunAt or set to now if already past
			if state.NextRunAt < now.Unix() {
				state.NextRunAt = now.Unix()
			}
		} else {
			state.NextRunAt = s.calculateNextRun(ctx, now, state).Unix()
		}
		state.UpdatedAt = now.Unix()

		return nil
	})
}

// SkipNextRun marks the next scheduled execution of a task to be skipped. When
// the scheduler's tick loop encounters a due task with the skip flag set, it
// clears the flag and advances NextRunAt to the following scheduled time without
// invoking the task function. For one-shot tasks, skipping transitions the task
// to [horai.TaskStatusCompleted].
//
// Returns [horai.ErrTaskNotFound] if no task with the given ID exists in storage.
func (s *Scheduler) SkipNextRun(ctx context.Context, id string) error {
	return s.updateTask(ctx, id, func(state *horai.TaskState) error {
		state.SkipNextRun = true
		state.UpdatedAt = s.opts.clock.Now().Unix()

		return nil
	})
}

// Task retrieves the current persistent state of a task from [horai.Store].
// Returns (nil, nil) if no task with the given ID exists.
func (s *Scheduler) Task(ctx context.Context, id string) (*horai.TaskState, error) {
	return s.storage.GetTask(ctx, id)
}

// Tasks returns an iterator over [horai.TaskSummary] values for all registered tasks.
// The iteration order depends on the underlying [horai.Store] implementation. Use
// slices.Collect to materialize the results as a slice when random access is
// needed.
func (s *Scheduler) Tasks(ctx context.Context) iter.Seq2[*horai.TaskSummary, error] {
	return func(yield func(*horai.TaskSummary, error) bool) {
		for state, err := range s.storage.Tasks(ctx) {
			if err != nil {
				yield(nil, err)
				return
			}
			summary := state.TaskSummary
			if !yield(&summary, nil) {
				return
			}
		}
	}
}

// History returns an iterator over [horai.TaskHistory] entries for the given task ID,
// ordered by start time descending (most recent first). Use slices.Collect to
// materialize the results as a slice when random access is needed.
func (s *Scheduler) History(ctx context.Context, id string) iter.Seq2[*horai.TaskHistory, error] {
	return s.history.History(ctx, id)
}

// Trigger manually triggers immediate execution of a task in a background
// goroutine. The task's regular schedule is not affected; it will still fire at
// its next scheduled time. The triggered execution respects concurrency limits
// and priority-based slot allocation.
//
// Returns [horai.ErrTaskNotRegistered] if the task ID is not in the in-memory map,
// [horai.ErrTaskNotFound] if the task state does not exist in [horai.Store],
// [horai.ErrTaskCompleted] if the task is a completed one-shot,
// [horai.ErrTaskDisabled] if the task's status is [horai.TaskStatusDisabled], or
// [horai.ErrTaskAlreadyDispatched] if a dispatch for this task is already queued or
// running.
func (s *Scheduler) Trigger(ctx context.Context, id string) error {
	if !s.isRunning.Load() {
		return horai.ErrNotReady
	}
	if !s.isReady() {
		return horai.ErrNotReady
	}

	s.mu.RLock()
	task, ok := s.tasks[id]
	s.mu.RUnlock()

	if !ok {
		return horai.ErrTaskNotRegistered
	}

	opCtx, cancel := s.storageCtx(ctx)
	defer cancel()
	state, err := s.storage.GetTask(opCtx, id)
	if err != nil {
		return err
	}

	if state == nil {
		return horai.ErrTaskNotFound
	}

	if state.Status == horai.TaskStatusCompleted {
		return horai.ErrTaskCompleted
	}

	if state.Status == horai.TaskStatusDisabled {
		return horai.ErrTaskDisabled
	}

	// Execute in background. A manual trigger takes the same single dispatch
	// slot as a scheduled one, so it cannot stack a second waiter onto a task
	// that is already queued or running.
	if !task.claim() {
		return horai.ErrTaskAlreadyDispatched
	}
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.stopping || !s.isRunning.Load() {
		task.release()
		return horai.ErrNotReady
	}
	s.wg.Go(func() {
		defer task.release()
		s.runTaskWithSemaphore(task, state)
	})

	return nil
}

// startedWithin reports whether the task started a run — finished
// (LastRunAt) or still in flight (RunStartedAt) — within grace before now.
// A non-positive grace never counts a run as recent.
func startedWithin(st *horai.TaskState, now time.Time, grace time.Duration) bool {
	last := max(st.LastRunAt, st.RunStartedAt)
	if grace <= 0 || last == 0 {
		return false
	}
	return now.Sub(time.Unix(last, 0)) < grace
}
