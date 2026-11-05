// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"

	schedtask "github.com/altessa-s/go-horai/task"
)

// blockingOneShot returns a one-shot task config due now whose body counts its
// runs and blocks until release is closed.
func blockingOneShot(runAt time.Time, runs *atomic.Int32, release <-chan struct{}) schedtask.Config {
	return schedtask.Config{
		ID: "task", RunAt: runAt,
		Func: func(ctx context.Context) error {
			runs.Add(1)

			select {
			case <-release:
			case <-ctx.Done():
			}

			return nil
		},
	}
}

func requireCompleted(t *testing.T, s *scheduler.Scheduler) {
	t.Helper()
	require.Eventually(t, func() bool {
		st, err := s.Task(t.Context(), "task")
		return err == nil && st != nil && st.Status == horai.TaskStatusCompleted
	}, 3*time.Second, 10*time.Millisecond, "the one-shot task never completed")
	st, err := s.Task(t.Context(), "task")
	require.NoError(t, err)
	require.Zero(t, st.NextRunAt)
	require.Zero(t, st.RunStartedAt)
}

// TestOneShotFinishedAfterManagementIsTerminal changes the status of a one-shot
// task while its only run executes. Whatever the interleaving, the finished run
// completes the task: it must not be left due (active with NextRunAt zero) and
// run a second time, nor be rescheduled by a later resume or enable.
func TestOneShotFinishedAfterManagementIsTerminal(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			during func(*scheduler.Scheduler, context.Context) error
			after  func(*scheduler.Scheduler, context.Context, string) error
		}{
			{
				name: "pause_resume",
				during: func(s *scheduler.Scheduler, ctx context.Context) error {
					if err := s.Pause(ctx, "task"); err != nil {
						return err
					}

					return s.Resume(ctx, "task")
				},
			},
			{
				name:   "paused",
				during: func(s *scheduler.Scheduler, ctx context.Context) error { return s.Pause(ctx, "task") },
				after:  (*scheduler.Scheduler).Resume,
			},
			{
				name:   "disabled",
				during: func(s *scheduler.Scheduler, ctx context.Context) error { return s.Disable(ctx, "task") },
				after:  (*scheduler.Scheduler).Enable,
			},
		} {
			runCase(t, tc.name, func(t *testing.T) {
				mem := mustNewMemory(t, 10)
				s := startForRecovery(t, mem, scheduler.WithTickInterval(20*time.Millisecond))

				var runs atomic.Int32

				release := make(chan struct{})
				require.NoError(t, s.Register(t.Context(), blockingOneShot(time.Now(), &runs, release)))
				require.Eventually(t, func() bool { return runs.Load() == 1 }, 2*time.Second, 10*time.Millisecond)

				require.NoError(t, tc.during(s, t.Context()))
				close(release)
				requireCompleted(t, s)

				if tc.after != nil {
					require.ErrorIs(t, tc.after(s, t.Context(), "task"), horai.ErrTaskCompleted)
				}

				require.Never(t, func() bool { return runs.Load() > 1 }, time.Second, 20*time.Millisecond,
					"a finished one-shot task ran again")
			})
		}
	})
}

// TestReRegisterCompletedOneShot checks that registering a completed one-shot
// task again — as every instance and every restart does — keeps it completed
// for the same RunAt, and that only a different RunAt schedules a new run.
func TestReRegisterCompletedOneShot(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem := mustNewMemory(t, 10)
		s := startForRecovery(t, mem, scheduler.WithTickInterval(20*time.Millisecond))

		var runs atomic.Int32

		released := make(chan struct{})
		close(released)

		runAt := time.Now().Add(-time.Minute)

		require.NoError(t, s.Register(t.Context(), blockingOneShot(runAt, &runs, released)))
		requireCompleted(t, s)
		require.Equal(t, int32(1), runs.Load())

		// Same RunAt, from this or another instance: stays completed.
		require.NoError(t, s.Register(t.Context(), blockingOneShot(runAt, &runs, released)))
		other := startForRecovery(t, mem, scheduler.WithTickInterval(20*time.Millisecond))
		require.NoError(t, other.Register(t.Context(), blockingOneShot(runAt, &runs, released)))
		st, err := s.Task(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, horai.TaskStatusCompleted, st.Status)
		require.Equal(t, runAt.Unix(), st.RunAt)
		require.Never(t, func() bool { return runs.Load() > 1 }, time.Second, 20*time.Millisecond,
			"re-registering with the same RunAt ran the task again")

		// A new RunAt is a new occurrence.
		require.NoError(t, s.Register(t.Context(), blockingOneShot(runAt.Add(time.Second), &runs, released)))
		require.Eventually(t, func() bool { return runs.Load() == 2 }, 3*time.Second, 10*time.Millisecond,
			"a new RunAt must schedule a new run")
		requireCompleted(t, s)
	})
}

// TestReRegisterDifferentCompletedOccurrence covers tasks completed with a different RunAt
// persisted: registration now requires an exact RunAt match.
func TestReRegisterDifferentCompletedOccurrence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		lastRun := time.Now().Add(-time.Hour).Truncate(time.Second)

		for _, tc := range []struct {
			name   string
			runAt  time.Time
			status horai.TaskStatus
		}{
			{"earlier_run_at", lastRun.Add(-time.Minute), horai.TaskStatusActive},
			{"equal_run_at", lastRun, horai.TaskStatusActive},
			{"later_run_at", lastRun.Add(time.Minute), horai.TaskStatusActive},
		} {
			runCase(t, tc.name, func(t *testing.T) {
				mem := mustNewMemory(t, 10)
				require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
					ID: "task", Status: horai.TaskStatusCompleted, OneShot: true, LastRunAt: lastRun.Unix()}))

				require.NoError(t, engine(t, mem).Register(t.Context(), schedtask.Config{
					ID: "task", RunAt: tc.runAt, Func: func(context.Context) error { return nil },
				}))
				st, err := mem.GetTask(t.Context(), "task")
				require.NoError(t, err)
				require.Equal(t, tc.status, st.Status)
				require.Equal(t, tc.runAt.Unix(), st.RunAt, "registration records the RunAt from now on")
			})
		}
	})
}

// TestDisableCompletedOneShotIsRejected checks that a completed one-shot task
// cannot be disabled and re-enabled into running a second time.
func TestDisableCompletedOneShotIsRejected(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem := mustNewMemory(t, 10)
		s := startForRecovery(t, mem, scheduler.WithTickInterval(20*time.Millisecond))

		var runs atomic.Int32

		released := make(chan struct{})
		close(released)
		require.NoError(t, s.Register(t.Context(), blockingOneShot(time.Now(), &runs, released)))
		requireCompleted(t, s)

		require.ErrorIs(t, s.Disable(t.Context(), "task"), horai.ErrTaskCompleted)
		require.ErrorIs(t, s.Enable(t.Context(), "task"), horai.ErrTaskCompleted)
		requireCompleted(t, s)
		require.Never(t, func() bool { return runs.Load() > 1 }, 500*time.Millisecond, 20*time.Millisecond)
	})
}

// TestDisableRacingFinishIsRejected lets a Disable lose its fenced write to
// the run's FinishRun: the retry re-reads the now completed task and gives up.
func TestDisableRacingFinishIsRejected(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem := mustNewMemory(t, 10)
		now := time.Now().Unix()
		require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
			ID: "task", Status: horai.TaskStatusRunning, OneShot: true,
			LastRunID: "node/run", RunStartedAt: now, RunAt: now,
		}))
		store := &finishBeforeReplace{Store: mem}
		s := engine(t, store)

		require.ErrorIs(t, s.Disable(t.Context(), "task"), horai.ErrTaskCompleted)
		got, err := mem.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, horai.TaskStatusCompleted, got.Status)
	})
}

// finishBeforeReplace finishes the run right before the first ReplaceTaskIf,
// so that write loses its fence.
type finishBeforeReplace struct {
	*memory.Store
	once sync.Once
}

func (s *finishBeforeReplace) ReplaceTaskIf(ctx context.Context, state *horai.TaskState, expect horai.TaskFence) (bool, error) {
	var err error

	s.once.Do(func() {
		_, err = s.Store.FinishRun(ctx, state.ID, "node/run",
			horai.RunResult{StartedAt: state.RunStartedAt, EndedAt: state.RunStartedAt, RunAt: state.RunAt, Success: true})
	})

	if err != nil {
		return false, err
	}

	return s.Store.ReplaceTaskIf(ctx, state, expect)
}

// TestReRegisterNewRunAtDuringRun re-registers a one-shot task for a new RunAt
// while its run for the old one executes. The finishing run must not complete
// the new occurrence: it runs afterwards, and registering it again later does
// not run it a third time.
func TestReRegisterNewRunAtDuringRun(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem := mustNewMemory(t, 10)
		s := startForRecovery(t, mem, scheduler.WithTickInterval(20*time.Millisecond))

		var runs atomic.Int32

		release := make(chan struct{})
		first := time.Now().Add(-time.Minute)
		second := first.Add(time.Second)

		require.NoError(t, s.Register(t.Context(), blockingOneShot(first, &runs, release)))
		require.Eventually(t, func() bool { return runs.Load() == 1 }, 2*time.Second, 10*time.Millisecond)

		require.NoError(t, s.Register(t.Context(), blockingOneShot(second, &runs, release)))
		close(release)

		require.Eventually(t, func() bool { return runs.Load() == 2 }, 3*time.Second, 10*time.Millisecond,
			"the occurrence registered during the run was lost")
		requireCompleted(t, s)
		st, err := s.Task(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, second.Unix(), st.RunAt)

		require.NoError(t, s.Register(t.Context(), blockingOneShot(second, &runs, release)))
		require.Never(t, func() bool { return runs.Load() > 2 }, time.Second, 20*time.Millisecond)
	})
}

// TestRegisterRejectsEpochRunAt keeps one-shot occurrence identities distinct
// from the zero RunAt of periodic tasks.
func TestRegisterRejectsEpochRunAt(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := engine(t, mustNewMemory(t, 10))
		for _, runAt := range []time.Time{time.Unix(0, 0), time.Unix(-5, 0)} {
			require.Error(t, s.Register(t.Context(), schedtask.Config{
				ID: "task", RunAt: runAt, Func: func(context.Context) error { return nil },
			}))
		}

		require.NoError(t, s.Register(t.Context(), schedtask.Config{
			ID: "task", RunAt: time.Unix(1, 0), Func: func(context.Context) error { return nil },
		}))
	})
}
