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

// takeoverOnRecoveryRead finishes the stale run and lets a new run claim the
// task right before stale recovery re-reads it, reproducing the window between
// the collection pass and the reset write.
type takeoverOnRecoveryRead struct {
	*memory.Store
	once sync.Once
}

func (s *takeoverOnRecoveryRead) GetTask(ctx context.Context, id string) (*horai.TaskState, error) {
	var err error

	s.once.Do(func() {
		var state *horai.TaskState
		if state, err = s.Store.GetTask(ctx, id); err != nil {
			return
		}

		if _, err = s.Store.FinishRun(ctx, id, state.LastRunID, horai.RunResult{
			StartedAt: state.RunStartedAt, EndedAt: time.Now().Unix(), NextRunAt: state.NextRunAt, Schedule: state.Schedule, Success: true,
		}); err != nil {
			return
		}

		_, err = s.Store.ClaimRun(ctx, id, horai.RunClaim{NextRunAt: state.NextRunAt, StartedAt: time.Now().Unix(), RunID: "new-owner"})
	})

	if err != nil {
		return nil, err
	}

	return s.Store.GetTask(ctx, id)
}

func TestStaleRecoveryCannotOverwriteConcurrentClaim(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem, err := memory.New(memory.WithMaxHistoryPerTask(10))
		require.NoError(t, err)
		require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
			ID: "task", Status: horai.TaskStatusRunning, Schedule: "@every 1h", NextRunAt: 1,
			LastRunID:    "stale-owner",
			RunStartedAt: 1,
		}))

		s := engine(t, &takeoverOnRecoveryRead{Store: mem}, scheduler.WithTickInterval(time.Hour))
		require.NoError(t, s.Start(t.Context()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
			defer cancel()

			require.NoError(t, s.Stop(ctx))
		})

		state, err := mem.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, "new-owner", state.LastRunID)
		require.Equal(t, horai.TaskStatusRunning, state.Status)
		require.NotZero(t, state.RunStartedAt)
		require.Zero(t, state.Failures)
	})
}

// pauseOnSkipRead pauses the task right before applySkip re-reads it,
// reproducing a Pause that lands between the due scan and the skip write.
type pauseOnSkipRead struct {
	*memory.Store
	armed atomic.Bool
	fired chan struct{}
}

func (s *pauseOnSkipRead) GetTask(ctx context.Context, id string) (*horai.TaskState, error) {
	if s.armed.CompareAndSwap(true, false) {
		state, err := s.Store.GetTask(ctx, id)
		if err != nil {
			return nil, err
		}

		state.Status = horai.TaskStatusPaused
		if err := seedState(s.Store, ctx, state); err != nil {
			return nil, err
		}

		close(s.fired)
	}

	return s.Store.GetTask(ctx, id)
}

func TestSkipCannotOverwriteConcurrentPause(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem, err := memory.New(memory.WithMaxHistoryPerTask(10))
		require.NoError(t, err)

		store := &pauseOnSkipRead{Store: mem, fired: make(chan struct{})}

		s := engine(t, store, scheduler.WithTickInterval(20*time.Millisecond))
		require.NoError(t, s.Start(t.Context()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
			defer cancel()

			require.NoError(t, s.Stop(ctx))
		})
		require.NoError(t, s.Register(t.Context(), schedtask.Config{
			ID: "task", Schedule: "@every 1h", Func: func(context.Context) error { return nil },
		}))

		state, err := mem.GetTask(t.Context(), "task")
		require.NoError(t, err)

		state.SkipNextRun = true
		state.NextRunAt = 1 // due now
		require.NoError(t, seedState(mem, t.Context(), state))
		store.armed.Store(true)

		select {
		case <-store.fired:
		case <-time.After(3 * time.Second):
			require.FailNow(t, "skip was not attempted")
		}
		// Let the in-flight skip finish its (rejected) write.
		require.Never(t, func() bool {
			got, err := mem.GetTask(t.Context(), "task")
			return err != nil || !got.SkipNextRun || got.NextRunAt != 1
		}, 200*time.Millisecond, 10*time.Millisecond)

		got, err := mem.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, horai.TaskStatusPaused, got.Status)
	})
}
