// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/horaitest"
	"github.com/altessa-s/go-horai/schedule"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/task"
)

func TestFinishPreservesReregisteredSchedule(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := engine(t, store(t))
		started, finish := make(chan struct{}), make(chan struct{})
		cfg := task.Config{ID: "changing", Schedule: "@every 1h", Func: func(context.Context) error { close(started); <-finish; return nil }}
		require.NoError(t, s.Register(t.Context(), cfg))
		require.NoError(t, s.Start(t.Context()))
		require.NoError(t, s.Trigger(t.Context(), cfg.ID))
		<-started
		time.Sleep(time.Hour)

		cfg.Schedule = "@every 2h"
		require.NoError(t, s.Register(t.Context(), cfg))
		registered, err := s.Task(t.Context(), cfg.ID)
		require.NoError(t, err)
		time.Sleep(30 * time.Minute)
		close(finish)
		synctest.Wait()

		finished, err := s.Task(t.Context(), cfg.ID)
		require.NoError(t, err)
		require.Equal(t, registered.NextRunAt, finished.NextRunAt)
		require.Zero(t, finished.RunStartedAt)
		require.NoError(t, s.Stop(t.Context()))
	})
}

func TestDynamicSmallLimitsAndDecrease(t *testing.T) {
	t.Parallel()

	for _, target := range []int32{1, 2} {
		for _, priority := range []task.Priority{task.PriorityNormal, task.PriorityLow} {
			t.Run(fmt.Sprintf("%d/%s", target, priority), func(t *testing.T) {
				t.Parallel()
				synctest.Test(t, func(t *testing.T) {
					var limit, calls atomic.Int32
					limit.Store(target)

					s := engine(t, store(t), scheduler.WithConcurrencyLimit(func() int { return int(limit.Load()) }))
					require.NoError(t, s.Start(t.Context()))

					for i := range 3 {
						if i == 2 {
							limit.Store(target)
						}

						require.NoError(t, s.Register(t.Context(), task.Config{ID: fmt.Sprint(i), RunAt: time.Now(), Priority: priority, Func: func(context.Context) error { calls.Add(1); return nil }}))
						time.Sleep(2 * time.Second)
						synctest.Wait()
						require.Equal(t, int32(i+1), calls.Load())
						limit.Store(5)
					}

					require.NoError(t, s.Stop(t.Context()))
				})
			})
		}
	}
}

func TestDynamicTriggerRejectsAtCapacity(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := engine(t, store(t), scheduler.WithConcurrencyLimit(func() int { return 1 }))
		started, finish := make(chan struct{}), make(chan struct{})

		for _, id := range []string{"first", "second", "critical"} {
			priority := task.PriorityNormal
			if id == "critical" {
				priority = task.PriorityCritical
			}

			require.NoError(t, s.Register(t.Context(), task.Config{ID: id, Schedule: "@hourly", Priority: priority, Func: func(context.Context) error { started <- struct{}{}; <-finish; return nil }}))
		}

		require.NoError(t, s.Start(t.Context()))
		require.NoError(t, s.Trigger(t.Context(), "first"))
		<-started
		require.ErrorIs(t, s.Trigger(t.Context(), "second"), horai.ErrConcurrencyLimit)
		require.NoError(t, s.Trigger(t.Context(), "critical"))
		<-started
		close(finish)
		synctest.Wait()
		// Rejection releases the task's dispatch claim.
		require.NoError(t, s.Trigger(t.Context(), "second"))
		<-started
		synctest.Wait()
		require.NoError(t, s.Stop(t.Context()))
	})
}

func TestImpossibleRegistration(t *testing.T) {
	t.Parallel()

	s := engine(t, store(t))
	for _, immediate := range []bool{false, true} {
		require.ErrorIs(t, s.Register(t.Context(), task.Config{ID: "never", Schedule: "0 0 0 31 2 *", RunOnStart: immediate, Func: func(context.Context) error { return nil }}), schedule.ErrInvalid)
		state, err := s.Task(t.Context(), "never")
		require.NoError(t, err)
		require.Nil(t, state)
	}
}

func TestExhaustedScheduleCompletes(t *testing.T) {
	t.Parallel()

	for _, operation := range []string{"finish", "resume", "enable", "skip"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				// The next leap day after 2096 is beyond cron's five-year search horizon.
				clock := horaitest.NewClock(time.Date(2096, 2, 28, 0, 0, 0, 0, time.UTC))
				s := engine(t, store(t), scheduler.WithClock(clock))

				var calls atomic.Int32

				require.NoError(t, s.Register(t.Context(), task.Config{ID: "leap", Schedule: "0 0 0 29 2 *", Func: func(context.Context) error { calls.Add(1); return nil }}))

				switch operation {
				case "resume":
					require.NoError(t, s.Pause(t.Context(), "leap"))
				case "enable":
					require.NoError(t, s.Disable(t.Context(), "leap"))
				case "skip":
					require.NoError(t, s.SkipNextRun(t.Context(), "leap"))
				}

				clock.Advance(24 * time.Hour)

				switch operation {
				case "resume":
					require.NoError(t, s.Resume(t.Context(), "leap"))
				case "enable":
					require.NoError(t, s.Enable(t.Context(), "leap"))
				}

				require.NoError(t, s.Start(t.Context()))
				time.Sleep(3 * time.Second)
				synctest.Wait()

				state, err := s.Task(t.Context(), "leap")
				require.NoError(t, err)
				require.Equal(t, horai.TaskStatusCompleted, state.Status)
				require.Zero(t, state.NextRunAt)

				want := int32(0)
				if operation == "finish" {
					want = 1
				}

				require.Equal(t, want, calls.Load())
				require.NoError(t, s.Stop(t.Context()))
			})
		})
	}
}

type retryDeletionStore struct {
	horai.Store
	fail bool
}

func (s *retryDeletionStore) DeleteTask(ctx context.Context, id string) error {
	if s.fail {
		return errors.New("history cleanup failed")
	}

	return s.Store.DeleteTask(ctx, id)
}
func TestUnregisterRetriesCombinedDeletion(t *testing.T) {
	t.Parallel()

	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			t.Parallel()
			st := &retryDeletionStore{Store: store(t), fail: true}
			original := engine(t, st)
			require.NoError(t, original.Register(t.Context(), task.Config{ID: "remove", Schedule: "@hourly", Func: func(context.Context) error { return nil }}))

			current := original
			if !local {
				current = engine(t, st)
			}

			require.Error(t, current.Unregister(t.Context(), "remove"))
			state, err := st.GetTask(t.Context(), "remove")
			require.NoError(t, err)
			require.NotNil(t, state)

			st.fail = false

			require.NoError(t, current.Unregister(t.Context(), "remove"))
			require.ErrorIs(t, current.Unregister(t.Context(), "remove"), horai.ErrTaskNotRegistered)
		})
	}
}

func TestReregisterExhaustedScheduleStaysCompleted(t *testing.T) {
	t.Parallel()

	for _, immediate := range []bool{false, true} {
		t.Run(fmt.Sprint(immediate), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				clock := horaitest.NewClock(time.Date(2096, 2, 28, 0, 0, 0, 0, time.UTC))
				st := store(t)
				s := engine(t, st, scheduler.WithClock(clock))

				var calls atomic.Int32

				cfg := task.Config{ID: "leap", Schedule: "0 0 0 29 2 *", Func: func(context.Context) error { calls.Add(1); return nil }}
				require.NoError(t, s.Register(t.Context(), cfg))
				clock.Advance(24 * time.Hour)
				require.NoError(t, s.Start(t.Context()))
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.NoError(t, s.Stop(t.Context()))
				require.Equal(t, int32(1), calls.Load())

				clock.Advance(time.Date(2101, 1, 1, 0, 0, 0, 0, time.UTC).Sub(clock.Now()))

				parsed, err := schedule.Parse(cfg.Schedule)
				require.NoError(t, err)
				require.Equal(t, time.Date(2104, 2, 29, 0, 0, 0, 0, time.UTC), parsed.Next(clock.Now()))
				restarted := engine(t, st, scheduler.WithClock(clock))
				cfg.RunOnStart = immediate
				require.NoError(t, restarted.Register(t.Context(), cfg))
				state, err := restarted.Task(t.Context(), cfg.ID)
				require.NoError(t, err)
				require.Equal(t, horai.TaskStatusCompleted, state.Status)
				require.Zero(t, state.NextRunAt)
				require.NoError(t, restarted.Start(t.Context()))
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.Equal(t, int32(1), calls.Load())
				require.NoError(t, restarted.Stop(t.Context()))

				cfg.Schedule, cfg.RunOnStart = "@hourly", false
				require.NoError(t, restarted.Register(t.Context(), cfg))
				state, err = restarted.Task(t.Context(), cfg.ID)
				require.NoError(t, err)
				require.Equal(t, horai.TaskStatusActive, state.Status)
				require.Equal(t, clock.Now().Add(time.Hour).Unix(), state.NextRunAt)
			})
		})
	}
}
