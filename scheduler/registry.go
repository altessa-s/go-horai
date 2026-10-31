// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"time"

	"github.com/altessa-s/go-horai/schedule"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// Register adds or updates a task in the scheduler. The task is persisted to
// [horai.Store] and registered in the in-memory dispatch map. If a task with the
// same ID already exists, its configuration is merged: the schedule, priority,
// and description are updated, while execution history fields (LastRunAt,
// Failures, etc.) are preserved.
//
// cfg must have a non-empty ID and a non-nil Func. Exactly one of Schedule or
// RunAt must be set; providing both returns [horai.ErrScheduleConflict]. RunAt must
// be after the Unix epoch. Schedule
// strings are validated against the cron parser on registration. If Priority is
// unset, it defaults to [schedtask.PriorityNormal].
//
// Re-registering a [horai.TaskStatusCompleted] one-shot task with the RunAt it
// completed for keeps it completed, so every instance or restart that registers
// the same one-shot task does not run it again. Only a different RunAt resets
// it to [horai.TaskStatusActive] and schedules a new run. Tasks completed before
// RunAt was persisted count as registered for their last run's start time: a
// RunAt after it schedules a new run, an earlier or equal one does not.
func (s *Scheduler) Register(ctx context.Context, cfg schedtask.Config) error {
	if err := validateTaskConfig(cfg); err != nil {
		return err
	}
	if cfg.Priority == schedtask.PriorityUnspecified {
		cfg.Priority = schedtask.PriorityNormal
	}
	cfg.Meta = maps.Clone(cfg.Meta)
	isOneShot := !cfg.RunAt.IsZero()

	// Validate schedule for periodic tasks
	var sched schedule.Schedule
	if !isOneShot {
		var err error
		sched, err = schedule.Parse(cfg.Schedule)
		if err != nil {
			return err
		}
	}

	lock := s.registrationLock(cfg.ID)
	lock.Lock()
	defer lock.Unlock()

	now := s.opts.clock.Now()
	nextRun := firstRun(cfg, sched, now)

	nowUnix := now.Unix()
	var runAt int64
	if isOneShot {
		runAt = cfg.RunAt.Unix()
	}

	// Create or update storage state
	state := &horai.TaskState{
		ID:             cfg.ID,
		Description:    cfg.Description,
		Status:         horai.TaskStatusActive,
		Priority:       cfg.Priority,
		Schedule:       cfg.Schedule,
		NextRunAt:      nextRun.Unix(),
		DisableHistory: cfg.DisableHistory,
		Unmanaged:      cfg.Unmanaged,
		OneShot:        isOneShot,
		Failures:       0,
		RunAt:          runAt,
		Meta:           cfg.Meta,
		CreatedAt:      nowUnix,
		UpdatedAt:      nowUnix,
	}

	if err := s.saveRegistered(ctx, state, cfg, nextRun, now, runAt); err != nil {
		return err
	}

	s.mu.Lock()
	s.tasks[cfg.ID] = &registeredTask{
		config: cfg,
		parsed: sched,
	}
	n := len(s.tasks)
	s.mu.Unlock()
	s.metrics.RegisteredChanged(n)

	logAttrs := []any{
		slog.String("task_id", cfg.ID),
		slog.String("priority", cfg.Priority.String()),
	}
	if isOneShot {
		logAttrs = append(logAttrs, slog.Bool("one_shot", true), slog.Time("run_at", cfg.RunAt))
	} else {
		logAttrs = append(logAttrs, slog.String("schedule", cfg.Schedule), slog.Bool("run_on_start", cfg.RunOnStart))
	}
	s.logger.InfoContext(ctx, "task registered", logAttrs...)

	return nil
}

// validateTaskConfig checks the task configuration [Scheduler.Register]
// accepts: an ID, a function, and exactly one of a schedule or a RunAt after
// the Unix epoch.
func validateTaskConfig(cfg schedtask.Config) error {
	if cfg.Timeout < 0 || cfg.Priority < schedtask.PriorityUnspecified || cfg.Priority > schedtask.PriorityCritical {
		return errors.New("invalid task options")
	}
	if cfg.ID == "" {
		return errors.New("task ID cannot be empty")
	}

	if cfg.Func == nil {
		return errors.New("task function cannot be nil")
	}

	isOneShot := !cfg.RunAt.IsZero()

	if isOneShot && cfg.Schedule != "" {
		return horai.ErrScheduleConflict
	}

	// A one-shot occurrence is identified by its RunAt in Unix seconds, and
	// zero stands for "periodic"; an epoch or earlier RunAt would be ambiguous.
	if isOneShot && cfg.RunAt.Unix() <= 0 {
		return errors.New("task RunAt must be after the Unix epoch")
	}

	if !isOneShot && cfg.Schedule == "" {
		return errors.New("task schedule is required (e.g., '@every 5m', '0 */5 * * * *')")
	}
	return nil
}

// firstRun returns the first run of a registered task: a one-shot task's
// RunAt, or now when it has passed; a periodic task's next scheduled time
// after now, or now with RunOnStart.
func firstRun(cfg schedtask.Config, sched schedule.Schedule, now time.Time) time.Time {
	if !cfg.RunAt.IsZero() {
		if cfg.RunAt.Before(now) {
			return now
		}
		return cfg.RunAt
	}
	if cfg.RunOnStart {
		return now
	}
	return sched.Next(now)
}

// saveRegistered stores base, the state registered for cfg: it creates the
// task when absent, or merges base with the existing state, and writes it
// fenced on that state, so a concurrent claim, finish or management change is
// never overwritten. It retries a lost write up to [MaxUpdateAttempts] times
// and then returns [horai.ErrConcurrentUpdate].
func (s *Scheduler) saveRegistered(
	ctx context.Context, base *horai.TaskState, cfg schedtask.Config, nextRun, now time.Time, runAt int64,
) error {
	for range MaxUpdateAttempts {
		existing, err := s.storage.GetTask(ctx, cfg.ID)
		if err != nil {
			return coreerrs.WrapOperation(err, "check existing task")
		}

		var saved bool
		if existing == nil {
			// Insert-if-absent: another instance may create (and claim) the
			// task between the read above and this write. Losing that race
			// falls through to the fenced merge on the next iteration.
			if saved, err = s.storage.CreateTask(ctx, base); err != nil {
				return coreerrs.WrapOperation(err, "save task state")
			}
		} else {
			// Description is always updated from config (allows changing
			// description without recreating task)
			state := s.mergeRegistered(base, existing, cfg, nextRun, now, runAt)
			if saved, err = s.storage.ReplaceTaskIf(ctx, state, horai.FenceOf(existing)); err != nil {
				return coreerrs.WrapOperation(err, "save task state")
			}
		}
		if saved {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return horai.ErrConcurrentUpdate
}

// mergeRegistered returns base, the state registered for cfg, merged with the
// task's existing stored state: execution history and an in-flight run are
// kept, the next run follows a changed schedule or RunOnStart, and the status
// is kept or reset as [Scheduler.Register] documents.
func (s *Scheduler) mergeRegistered(
	base, existing *horai.TaskState, cfg schedtask.Config, nextRun, now time.Time, runAt int64,
) *horai.TaskState {
	isOneShot := !cfg.RunAt.IsZero()
	nowUnix := now.Unix()
	state := new(*base)
	// Preserve certain fields from existing state
	state.CreatedAt = existing.CreatedAt
	state.LastRunAt = existing.LastRunAt
	state.LastRunID = existing.LastRunID
	state.RunStartedAt = existing.RunStartedAt   // keeps an in-flight run finishable
	state.RunLeaseUntil = existing.RunLeaseUntil // keeps an in-flight run's lease
	state.RunLeaseID = existing.RunLeaseID
	state.Failures = existing.Failures
	if isOneShot {
		// One-shot: always use computed nextRun
		state.NextRunAt = nextRun.Unix()
	} else {
		// Update next run time if schedule changed
		if existing.Schedule != cfg.Schedule {
			state.NextRunAt = nextRun.Unix()
		} else {
			state.NextRunAt = existing.NextRunAt
			if cfg.RunOnStart && !startedWithin(existing, now, s.opts.runOnStartGrace) {
				// The stored schedule would otherwise keep RunOnStart
				// from ever applying to a task that already exists.
				state.NextRunAt = min(state.NextRunAt, nowUnix)
			}
		}
	}
	switch {
	case existing.Status == horai.TaskStatusCompleted && isOneShot && existing.OneShot && completedFor(existing, runAt):
		// Same occurrence as the one already executed: stay completed.
		state.Status = horai.TaskStatusCompleted
		state.NextRunAt = existing.NextRunAt
	case existing.Status == horai.TaskStatusCompleted:
		// A new occurrence (or a changed kind of task) runs again.
		state.Status = horai.TaskStatusActive
	case existing.Status != horai.TaskStatusActive:
		// Preserve status if not active
		state.Status = existing.Status
	}
	return state
}

// completedFor reports whether the completed one-shot task existing already
// executed the occurrence registered for runAt (Unix seconds).
func completedFor(existing *horai.TaskState, runAt int64) bool { return existing.RunAt == runAt }

// Unregister removes a task from both the in-memory dispatch map and the
// [horai.Store] backend. History deletion is best effort, including a separate
// [WithHistoryStore] backend. Any currently running instance of
// the task will complete normally; only future dispatches are prevented.
//
// A task persisted by an earlier process but not registered in this one is
// removed from storage too, so a service can drop tasks it no longer
// schedules without registering a stub first. As with a registered task,
// another live replica that still registers the ID is not notified.
//
// Deleting history kept apart is best effort: it is bounded by
// [WithStorageTimeout], and a failure is logged without failing Unregister,
// leaving the entries to the history backend's retention — indefinitely when
// that backend has none.
//
// Returns [horai.ErrTaskNotRegistered] if the task ID is neither registered in this
// process nor present in storage.
func (s *Scheduler) Unregister(ctx context.Context, id string) error {
	lock := s.registrationLock(id)
	lock.Lock()
	defer lock.Unlock()
	opCtx, opCancel := s.storageCtx(ctx)
	defer opCancel()
	state, err := s.storage.GetTask(opCtx, id)
	if err != nil {
		return err
	}
	s.mu.RLock()
	_, registered := s.tasks[id]
	s.mu.RUnlock()
	if state == nil && !registered {
		return horai.ErrTaskNotRegistered
	}
	if err := s.storage.DeleteTask(opCtx, id); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.tasks, id)
	n := len(s.tasks)
	s.mu.Unlock()
	s.metrics.RegisteredChanged(n)
	delCtx, cancel := s.storageCtx(ctx)
	defer cancel()
	if err := s.history.DeleteHistory(delCtx, id); err != nil {
		s.logger.WarnContext(ctx, "failed to delete task history", slog.String("task_id", id), slog.Any("error", err))
	}
	return nil
}
