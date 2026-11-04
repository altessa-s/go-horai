// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-atlas-core/runtime/leakcheck"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/horaitest"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/task"
)

func TestMain(m *testing.M) {
	code := m.Run()

	if err := leakcheck.Check(); err != nil {
		fmt.Fprintln(os.Stderr, err)

		code = 1
	}

	os.Exit(code)
}
func store(t testing.TB) *memory.Store {
	t.Helper()

	s, err := memory.New()
	require.NoError(t, err)

	return s
}
func engine(t testing.TB, st horai.Store, opts ...scheduler.Option) *scheduler.Scheduler {
	t.Helper()

	s, err := scheduler.New(st, opts...)
	require.NoError(t, err)

	return s
}
func TestUnfinishedRecovery(t *testing.T) {
	t.Parallel()

	for _, status := range []horai.TaskStatus{horai.TaskStatusRunning, horai.TaskStatusPaused, horai.TaskStatusDisabled} {
		t.Run(status.String(), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				st := store(t)
				now := time.Now().Unix()

				s := engine(t, st, scheduler.WithInstanceID("stable"))
				for _, owner := range []string{"stable", "foreign"} {
					ok, err := st.CreateTask(t.Context(), &horai.TaskState{ID: owner, Status: status, Schedule: "@hourly", LastRunID: owner + "/run", RunStartedAt: now, RunLeaseUntil: now + 3600, RunLeaseID: owner + "/run"})
					require.NoError(t, err)
					require.True(t, ok)
				}

				require.NoError(t, s.Start(t.Context()))
				own, err := st.GetTask(t.Context(), "stable")
				require.NoError(t, err)
				require.Zero(t, own.RunStartedAt)
				require.Equal(t, int32(1), own.Failures)

				if status != horai.TaskStatusRunning {
					require.Equal(t, status, own.Status)
				}

				other, err := st.GetTask(t.Context(), "foreign")
				require.NoError(t, err)
				require.Equal(t, now, other.RunStartedAt)
				require.NoError(t, s.Stop(t.Context()))
			})
		})
	}
}
func TestLiveRunAndHeartbeat(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		s := engine(t, st, scheduler.WithInstanceID("stable"), scheduler.WithStaleTaskTimeout(5*time.Second))

		var calls atomic.Int32

		require.NoError(t, s.Register(t.Context(), task.Config{ID: "live", RunOnStart: true, Schedule: "@hourly", Func: func(ctx context.Context) error { calls.Add(1); <-ctx.Done(); return ctx.Err() }}))
		require.NoError(t, s.Start(t.Context()))
		time.Sleep(time.Second)
		synctest.Wait()

		first, err := s.Task(t.Context(), "live")
		require.NoError(t, err)
		require.NoError(t, s.Pause(t.Context(), "live"))
		time.Sleep(15 * time.Second)
		synctest.Wait()

		live, err := s.Task(t.Context(), "live")
		require.NoError(t, err)
		require.Equal(t, first.LastRunID, live.LastRunID)
		require.NotZero(t, live.RunStartedAt)
		require.Greater(t, live.RunLeaseUntil, first.RunLeaseUntil)
		require.Equal(t, int32(1), calls.Load())
		require.NoError(t, s.Stop(t.Context()))
		done, err := s.Task(t.Context(), "live")
		require.NoError(t, err)
		require.Zero(t, done.RunStartedAt)
		require.Equal(t, horai.TaskStatusPaused, done.Status)
	})
}
func TestTwoEnginesOneOccurrence(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		a, b := engine(t, st), engine(t, st)

		var calls atomic.Int32

		cfg := task.Config{ID: "once", RunAt: time.Now(), Func: func(context.Context) error { calls.Add(1); return nil }}
		require.NoError(t, a.Register(t.Context(), cfg))
		require.NoError(t, b.Register(t.Context(), cfg))
		require.NoError(t, a.Start(t.Context()))
		require.NoError(t, b.Start(t.Context()))
		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load())
		require.NoError(t, a.Register(t.Context(), cfg))
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(1), calls.Load())
		require.NoError(t, a.Stop(t.Context()))
		require.NoError(t, b.Stop(t.Context()))
	})
}
func TestRegisterDoesNotBlockTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		base := store(t)
		gate := make(chan struct{})
		entered := make(chan struct{})
		st := &horaitest.FaultStore{Store: base, BeforeCreate: func(ctx context.Context, v *horai.TaskState) error {
			if v.ID == "blocked" {
				close(entered)

				select {
				case <-gate:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		}}
		s := engine(t, st)

		var calls atomic.Int32

		require.NoError(t, s.Register(t.Context(), task.Config{ID: "due", Schedule: "@every 1s", Func: func(context.Context) error { calls.Add(1); return nil }}))
		require.NoError(t, s.Start(t.Context()))

		result := make(chan error, 1)
		go func() {
			result <- s.Register(t.Context(), task.Config{ID: "blocked", Schedule: "@hourly", Func: func(context.Context) error { return nil }})
		}()

		<-entered
		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.Positive(t, calls.Load())
		close(gate)
		require.NoError(t, <-result)
		require.NoError(t, s.Stop(t.Context()))
	})
}
func TestNewValidation(t *testing.T) {
	t.Parallel()

	_, err := scheduler.New(nil)
	require.Error(t, err)

	for _, opt := range []scheduler.Option{scheduler.WithTickInterval(0), scheduler.WithStorageTimeout(-1), scheduler.WithCleanupInterval(0), scheduler.WithStaleTaskTimeout(0), scheduler.WithMaxConcurrentTasks(-1)} {
		_, err := scheduler.New(store(t), opt)
		require.Error(t, err)
	}
}
func TestManagementAndLists(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		metrics := new(horaitest.Metrics)

		s := engine(t, st, scheduler.WithMetrics(metrics))
		for _, id := range []string{"a", "b"} {
			require.NoError(t, s.Register(t.Context(), task.Config{ID: id, Schedule: "@every 1s", Func: func(context.Context) error { return nil }}))
		}

		require.NoError(t, s.Pause(t.Context(), "a"))
		require.NoError(t, s.Resume(t.Context(), "a"))
		require.NoError(t, s.Disable(t.Context(), "a"))
		require.ErrorIs(t, s.Resume(t.Context(), "a"), horai.ErrTaskNotPaused)
		require.NoError(t, s.Enable(t.Context(), "a"))
		require.NoError(t, s.SkipNextRun(t.Context(), "a"))
		page, err := s.ListTasks(t.Context(), scheduler.TaskListRequest{Limit: 1})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		require.NotNil(t, page.Next)
		next, err := s.ListTasks(t.Context(), scheduler.TaskListRequest{Limit: 1, After: page.Next})
		require.NoError(t, err)
		require.Equal(t, "b", next.Items[0].ID)
		require.NoError(t, s.Start(t.Context()))
		require.True(t, s.Status().Running)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		require.NoError(t, s.Stop(t.Context()))
		h, err := s.ListHistory(t.Context(), scheduler.HistoryListRequest{TaskID: "b", Limit: 1})
		require.NoError(t, err)
		require.Len(t, h.Items, 1)
		require.NotEmpty(t, metrics.Events())
		require.NoError(t, s.Unregister(t.Context(), "a"))
		require.ErrorIs(t, s.Unregister(t.Context(), "a"), horai.ErrTaskNotRegistered)
	})
}
func ExampleNew() {
	st, _ := memory.New()
	s, err := scheduler.New(st)
	fmt.Println(err, s.Status().Running) // Output: <nil> false
}

// A high-priority waiter must observe a reserved slot released after it queued.
func TestReservedSlotsRelease(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		s := engine(t, st, scheduler.WithMaxConcurrentTasks(2), scheduler.WithReservedHighPrioritySlots(1))

		var calls atomic.Int32

		for _, id := range []string{"first", "second"} {
			require.NoError(t, s.Register(t.Context(), task.Config{ID: id, Priority: task.PriorityHigh, RunAt: time.Now(), Func: func(context.Context) error { calls.Add(1); time.Sleep(2 * time.Second); return nil }}))
		}

		require.NoError(t, s.Start(t.Context()))
		time.Sleep(7 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(2), calls.Load())

		status := s.Status()
		require.Zero(t, status.RunningTasks)
		require.Equal(t, 2, status.AvailableSlots)
		require.NoError(t, s.Stop(t.Context()))
	})
}

func BenchmarkRegister(b *testing.B) {
	st := store(b)
	s := engine(b, st)

	cfg := task.Config{ID: "bench", Schedule: "@every 1m", Func: func(context.Context) error { return nil }}
	for b.Loop() {
		require.NoError(b, s.Register(b.Context(), cfg))
	}
}

func TestInjectedClockCleanupAndSeparateHistory(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		history := store(t)
		clock := horaitest.NewClock(time.Unix(10000, 0))
		metrics := &horaitest.Metrics{}
		s := engine(t, st, scheduler.WithClock(clock), scheduler.WithMetrics(metrics), scheduler.WithHistoryStore(history), scheduler.WithCleanupInterval(time.Second), scheduler.WithHistoryRetention(time.Hour))
		require.NoError(t, history.AddHistory(t.Context(), &horai.TaskHistory{ID: "old", TaskID: "task", StartedAt: 1, EndedAt: 6399}))
		require.NoError(t, history.AddHistory(t.Context(), &horai.TaskHistory{ID: "boundary", TaskID: "task", StartedAt: 2, EndedAt: 6400}))
		require.NoError(t, s.Start(t.Context()))
		time.Sleep(time.Second)
		synctest.Wait()

		rows, err := history.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 10}, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, "boundary", rows[0].ID)
		require.NoError(t, s.Stop(t.Context()))
	})
}
