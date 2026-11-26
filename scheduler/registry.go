// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/altessa-s/go-horai/schedule"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// Register creates a task in [horai.Store] and installs its function in the
// in-memory dispatch map. If the ID already exists in either place, Register
// returns [horai.ErrTaskAlreadyRegistered]. Use [Scheduler.Bind] to install a
// function for a persisted task on a new scheduler instance.
//
// cfg must have a non-empty ID and a non-nil Func. Exactly one of Schedule or
// RunAt must be set. RunAt must be after the Unix epoch, and Schedule must have
// a future activation. If Priority is unset, it defaults to [schedtask.PriorityNormal].
func (s *Scheduler) Register(ctx context.Context, cfg schedtask.Config) error {
	sched, err := parseTaskConfig(cfg)
	if err != nil {
		return err
	}

	if cfg.Priority == schedtask.PriorityUnspecified {
		cfg.Priority = schedtask.PriorityNormal
	}

	cfg.Meta = maps.Clone(cfg.Meta)
	isOneShot := !cfg.RunAt.IsZero()

	lock := s.registrationLock(cfg.ID)
	lock.Lock()
	defer lock.Unlock()

	if s.isRegistered(cfg.ID) {
		return fmt.Errorf("task %q: %w", cfg.ID, horai.ErrTaskAlreadyRegistered)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	now := s.opts.clock.Now()
	if !isOneShot && sched.Next(now).IsZero() {
		return fmt.Errorf("%w: schedule has no future activation", schedule.ErrInvalid)
	}

	nextRun := firstRun(cfg, sched, now)

	nowUnix := now.Unix()

	var runAt int64
	if isOneShot {
		runAt = cfg.RunAt.Unix()
	}

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

	created, err := s.storage.CreateTask(ctx, state)
	if err != nil {
		return coreerrs.WrapOperation(err, "create task state")
	}

	if !created {
		return fmt.Errorf("task %q: %w", cfg.ID, horai.ErrTaskAlreadyRegistered)
	}

	s.installTask(cfg, sched)

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

// Bind installs the function and execution timeout for an existing task on this
// scheduler instance. Use it after a restart or on another replica sharing the
// store. The supplied Schedule or RunAt must match the persisted task. Bind
// uses the persisted priority and leaves the stored state, including the next
// run, unchanged. RunOnStart applies only when Register creates the task.
//
// Bind returns [horai.ErrTaskNotFound] for an absent task,
// [horai.ErrTaskAlreadyRegistered] for an ID already bound in this instance,
// and [horai.ErrInvalidArgument] for a mismatched schedule or RunAt.
func (s *Scheduler) Bind(ctx context.Context, cfg schedtask.Config) error {
	sched, err := parseTaskConfig(cfg)
	if err != nil {
		return err
	}

	lock := s.registrationLock(cfg.ID)
	lock.Lock()
	defer lock.Unlock()

	if s.isRegistered(cfg.ID) {
		return fmt.Errorf("task %q: %w", cfg.ID, horai.ErrTaskAlreadyRegistered)
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	state, err := s.storage.GetTask(ctx, cfg.ID)
	if err != nil {
		return coreerrs.WrapOperation(err, "read task state")
	}

	if state == nil {
		return fmt.Errorf("task %q: %w", cfg.ID, horai.ErrTaskNotFound)
	}

	var runAt int64
	if !cfg.RunAt.IsZero() {
		runAt = cfg.RunAt.Unix()
	}

	if state.OneShot != !cfg.RunAt.IsZero() || state.RunAt != runAt || state.Schedule != cfg.Schedule {
		return fmt.Errorf("task %q: schedule or RunAt differs from stored task: %w", cfg.ID, horai.ErrInvalidArgument)
	}

	cfg.Priority = state.Priority
	if cfg.Priority == schedtask.PriorityUnspecified {
		cfg.Priority = schedtask.PriorityNormal
	}

	cfg.Meta = maps.Clone(cfg.Meta)
	s.installTask(cfg, sched)

	return nil
}

// parseTaskConfig validates cfg and parses its periodic schedule.
func parseTaskConfig(cfg schedtask.Config) (schedule.Schedule, error) {
	if err := validateTaskConfig(cfg); err != nil {
		return nil, err
	}

	if cfg.Schedule != "" {
		return schedule.Parse(cfg.Schedule)
	}

	return nil, nil //nolint:nilnil // one-shot tasks have no cron schedule
}

func (s *Scheduler) isRegistered(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, registered := s.tasks[id]

	return registered
}

func (s *Scheduler) installTask(cfg schedtask.Config, sched schedule.Schedule) {
	s.mu.Lock()
	s.tasks[cfg.ID] = &registeredTask{config: cfg, parsed: sched}
	n := len(s.tasks)
	s.mu.Unlock()
	s.metrics.RegisteredChanged(n)
}

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

	if state == nil && !s.isRegistered(id) {
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
