// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"

	schedtask "github.com/altessa-s/go-horai/task"
)

// conflictingStore performs a concurrent metadata write right before each of
// the first `conflicts` ReplaceTaskIf calls, so those calls lose the race.
type conflictingStore struct {
	*memory.Store
	conflicts atomic.Int32
	attempts  atomic.Int32
}

func (s *conflictingStore) ReplaceTaskIf(ctx context.Context, state *horai.TaskState, expect horai.TaskFence) (bool, error) {
	s.attempts.Add(1)

	if s.conflicts.Add(-1) >= 0 {
		cur, err := s.Store.GetTask(ctx, state.ID)
		if err != nil {
			return false, err
		}

		cur.Meta = map[string]string{"concurrent": "kept"}
		if err := seedState(s.Store, ctx, cur); err != nil {
			return false, err
		}
	}

	return s.Store.ReplaceTaskIf(ctx, state, expect)
}

func newConflictScheduler(t *testing.T, conflicts int32) (*scheduler.Scheduler, *conflictingStore) {
	t.Helper()

	mem, err := memory.New(memory.WithMaxHistoryPerTask(10))
	require.NoError(t, err)
	require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
		ID: "task", Status: horai.TaskStatusActive, Schedule: "@every 1h", NextRunAt: 1 << 40}))
	store := &conflictingStore{Store: mem}
	store.conflicts.Store(conflicts)

	return engine(t, store), store
}

func TestPauseRetriesAndKeepsConcurrentWrite(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, store := newConflictScheduler(t, 2)

		require.NoError(t, s.Pause(t.Context(), "task"))
		require.Equal(t, int32(3), store.attempts.Load())

		got, err := store.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, horai.TaskStatusPaused, got.Status)
		require.Equal(t, "kept", got.Meta["concurrent"])
	})
}

func TestManagementGivesUpAfterMaxAttempts(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, store := newConflictScheduler(t, scheduler.MaxUpdateAttempts)

		require.ErrorIs(t, s.SkipNextRun(t.Context(), "task"), horai.ErrConcurrentUpdate)
		require.Equal(t, int32(scheduler.MaxUpdateAttempts), store.attempts.Load())

		got, err := store.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.False(t, got.SkipNextRun)
	})
}

func TestRegisterKeepsInFlightRunFinishable(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem, err := memory.New(memory.WithMaxHistoryPerTask(10))
		require.NoError(t, err)
		require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
			ID: "task", Status: horai.TaskStatusRunning, Schedule: "@every 1h", NextRunAt: 1 << 40,
			LastRunID:    "run",
			RunStartedAt: 5,
		}))
		s := engine(t, mem)

		require.NoError(t, s.Register(t.Context(), schedtask.Config{
			ID: "task", Schedule: "@every 1h", Func: func(context.Context) error { return nil },
		}))

		ok, err := mem.FinishRun(t.Context(), "task", "run",
			horai.RunResult{StartedAt: 5, EndedAt: 6, NextRunAt: 1 << 40, Schedule: "@every 1h", Success: true})
		require.NoError(t, err)
		require.True(t, ok)
	})
}
