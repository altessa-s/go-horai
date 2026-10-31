// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/horaitest"
	"github.com/altessa-s/go-horai/query"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/task"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
)

func TestSmallStaticLimits(t *testing.T) {
	t.Parallel()
	for _, limit := range []int{1, 2} {
		for _, priority := range []task.Priority{task.PriorityNormal, task.PriorityLow} {
			synctest.Test(t, func(t *testing.T) {
				s := engine(t, store(t), scheduler.WithMaxConcurrentTasks(limit))
				var calls atomic.Int32
				require.NoError(t, s.Register(t.Context(), task.Config{ID: "small", RunAt: time.Now(), Priority: priority, Func: func(context.Context) error { calls.Add(1); return nil }}))
				require.NoError(t, s.Start(t.Context()))
				time.Sleep(3 * time.Second)
				synctest.Wait()
				require.Equal(t, int32(1), calls.Load())
				require.NoError(t, s.Stop(t.Context()))
			})
		}
	}
	for _, limit := range []int{1, 2} {
		for _, reserved := range []int{limit, limit + 1} {
			_, err := scheduler.New(store(t), scheduler.WithMaxConcurrentTasks(limit), scheduler.WithReservedHighPrioritySlots(reserved))
			require.ErrorIs(t, err, coreerrs.ErrInvalidArgument)
			require.ErrorContains(t, err, "reservedHighPrioritySlots")
		}
	}
	_, err := scheduler.New(store(t), scheduler.WithMaxConcurrentTasks(scheduler.MaxConcurrentTasks+1))
	require.ErrorIs(t, err, coreerrs.ErrInvalidArgument)
	require.ErrorContains(t, err, "maxConcurrentTasks")
}

func TestReadinessCanObserveStatus(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var s *scheduler.Scheduler
		s = engine(t, store(t), scheduler.WithReadiness(func() bool { return s.Status().Running }))
		require.NoError(t, s.Register(t.Context(), task.Config{ID: "ready", Schedule: "@hourly", Func: func(context.Context) error { return nil }}))
		require.NoError(t, s.Start(t.Context()))
		require.True(t, s.Status().Ready)
		require.NoError(t, s.Trigger(t.Context(), "ready"))
		synctest.Wait()
		require.NoError(t, s.Stop(t.Context()))
	})
}

func TestSlowTriggerDoesNotBlockStop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var block atomic.Bool
		entered := make(chan struct{})
		release := make(chan struct{})
		st := &horaitest.FaultStore{Store: store(t), BeforeGet: func(ctx context.Context, _ string) error {
			if block.Load() {
				close(entered)
				<-release
				require.NotZero(t, ctx)
			}
			return nil
		}}
		s := engine(t, st)
		require.NoError(t, s.Register(t.Context(), task.Config{ID: "slow", Schedule: "@hourly", Func: func(context.Context) error { return nil }}))
		require.NoError(t, s.Start(t.Context()))
		block.Store(true)
		done := make(chan error, 1)
		go func() { done <- s.Trigger(t.Context(), "slow") }()
		<-entered
		require.NoError(t, s.Stop(t.Context()))
		close(release)
		require.ErrorIs(t, <-done, horai.ErrNotReady)
	})
}

type slowStartup struct {
	horai.Store
	entered chan struct{}
}

func (s *slowStartup) Tasks(ctx context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) { close(s.entered); <-ctx.Done(); yield(nil, ctx.Err()) }
}
func TestStopCancelsStartupStorage(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := &slowStartup{Store: store(t), entered: make(chan struct{})}
		s := engine(t, st)
		done := make(chan error, 1)
		go func() { done <- s.Start(t.Context()) }()
		<-st.entered
		require.NoError(t, s.Stop(t.Context()))
		require.ErrorIs(t, <-done, context.Canceled)
	})
}

type failingHistory struct{ horai.HistoryStore }

func (failingHistory) DeleteHistory(context.Context, string) error {
	return errors.New("history unavailable")
}
func TestUnregisterHistoryFailureIsBestEffort(t *testing.T) {
	t.Parallel()
	st := store(t)
	s := engine(t, st, scheduler.WithHistoryStore(failingHistory{st}))
	require.NoError(t, s.Register(t.Context(), task.Config{ID: "remove", Schedule: "@hourly", Func: func(context.Context) error { return nil }}))
	require.NoError(t, s.Unregister(t.Context(), "remove"))
	state, err := st.GetTask(t.Context(), "remove")
	require.NoError(t, err)
	require.Nil(t, state)
	require.ErrorIs(t, s.Unregister(t.Context(), "remove"), horai.ErrTaskNotRegistered)
}

func TestPanicLogExcludesValue(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var logs bytes.Buffer
		s := engine(t, store(t), scheduler.WithLogger(slog.New(slog.NewTextHandler(&logs, nil))))
		require.NoError(t, s.Register(t.Context(), task.Config{ID: "panic", RunAt: time.Now(), Func: func(context.Context) error { panic("secret-canary") }}))
		require.NoError(t, s.Start(t.Context()))
		time.Sleep(3 * time.Second)
		require.NoError(t, s.Stop(t.Context()))
		require.Contains(t, logs.String(), "panic_type=string")
		require.Contains(t, logs.String(), "stack=")
		require.NotContains(t, logs.String(), "secret-canary")
	})
}

type failedLists struct {
	horai.Store
	err error
}

func (s failedLists) ListTasks(context.Context, horai.TaskPage, query.Expr) ([]*horai.TaskState, error) {
	return nil, s.err
}
func (s failedLists) ListHistory(context.Context, string, horai.HistoryPage, query.Expr) ([]*horai.TaskHistory, error) {
	return nil, s.err
}
func (s failedLists) Tasks(context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) { yield(nil, s.err) }
}
func TestListFailuresAndHistoryCursor(t *testing.T) {
	t.Parallel()
	failure := errors.New("list failed")
	s := engine(t, failedLists{store(t), failure})
	_, err := s.ListTasks(t.Context(), scheduler.TaskListRequest{})
	require.ErrorIs(t, err, failure)
	_, err = s.ListHistory(t.Context(), scheduler.HistoryListRequest{})
	require.ErrorIs(t, err, failure)
	for _, err := range s.Tasks(t.Context()) {
		require.ErrorIs(t, err, failure)
	}
	_, err = s.ListHistory(t.Context(), scheduler.HistoryListRequest{Filter: query.Cmp{Field: "unknown", Op: query.Eq, Value: "x"}})
	require.ErrorIs(t, err, query.ErrInvalid)
	st := store(t)
	s = engine(t, st)
	for _, id := range []string{"a", "b", "c"} {
		require.NoError(t, st.AddHistory(t.Context(), &horai.TaskHistory{ID: id, TaskID: "task", StartedAt: 10}))
	}
	first, err := s.ListHistory(t.Context(), scheduler.HistoryListRequest{TaskID: "task", Limit: 1})
	require.NoError(t, err)
	require.NotNil(t, first.Next)
	second, err := s.ListHistory(t.Context(), scheduler.HistoryListRequest{TaskID: "task", Limit: 1, After: first.Next})
	require.NoError(t, err)
	require.Equal(t, "b", second.Items[0].ID)
	require.Equal(t, scheduler.DefaultPageSize, scheduler.ClampLimit(-1))
	require.Equal(t, scheduler.MaxPageSize, scheduler.ClampLimit(scheduler.MaxPageSize+1))
}

type lostRegistration struct {
	horai.Store
	err error
}

func (s lostRegistration) CreateTask(context.Context, *horai.TaskState) (bool, error) {
	return false, s.err
}
func TestRegistrationLostWrites(t *testing.T) {
	t.Parallel()
	cfg := task.Config{ID: "lost", Schedule: "@hourly", Func: func(context.Context) error { return nil }}
	s := engine(t, lostRegistration{Store: store(t)})
	require.ErrorIs(t, s.Register(t.Context(), cfg), horai.ErrConcurrentUpdate)
	failure := errors.New("write failed")
	s = engine(t, lostRegistration{store(t), failure})
	require.ErrorIs(t, s.Register(t.Context(), cfg), failure)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, engine(t, lostRegistration{Store: store(t)}).Register(ctx, cfg), context.Canceled)
}
