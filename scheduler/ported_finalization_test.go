// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"

	schedtask "github.com/altessa-s/go-horai/task"
)

type takeoverOnFinish struct {
	*memory.Store
	done chan bool
}

func (s *takeoverOnFinish) FinishRun(ctx context.Context, id, runID string, result horai.RunResult) (bool, error) {
	state, err := s.Store.GetTask(ctx, id)
	if err != nil {
		return false, err
	}
	// Stand in for stale recovery: it reactivates the task and clears the
	// unfinished run, which ClaimRun requires before another claim.
	state.Status = horai.TaskStatusActive

	state.RunStartedAt = 0
	if err = seedState(s.Store, ctx, state); err != nil {
		return false, err
	}

	if _, err = s.Store.ClaimRun(ctx, id, horai.RunClaim{NextRunAt: state.NextRunAt, StartedAt: time.Now().Unix(), RunID: "new-owner"}); err != nil {
		return false, err
	}

	ok, err := s.Store.FinishRun(ctx, id, runID, result)
	s.done <- ok

	return ok, err
}

func TestFinalizationCannotOverwriteConcurrentClaim(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		mem, err := memory.New(memory.WithMaxHistoryPerTask(10))
		require.NoError(t, err)

		store := &takeoverOnFinish{Store: mem, done: make(chan bool, 1)}
		s := engine(t, store, scheduler.WithTickInterval(time.Hour))
		require.NoError(t, s.Start(t.Context()))
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
			defer cancel()

			require.NoError(t, s.Stop(ctx))
		})
		require.NoError(t, s.Register(t.Context(), schedtask.Config{ID: "task", Schedule: "@every 1h", Func: func(context.Context) error { return nil }}))
		require.NoError(t, s.Trigger(t.Context(), "task"))

		select {
		case applied := <-store.done:
			require.False(t, applied)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "task did not finish")
		}

		state, err := mem.GetTask(t.Context(), "task")
		require.NoError(t, err)
		require.Equal(t, "new-owner", state.LastRunID)
		require.Equal(t, horai.TaskStatusRunning, state.Status)
		require.NotZero(t, state.RunStartedAt)
	})
}
