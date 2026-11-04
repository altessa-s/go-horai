// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"

	schedtask "github.com/altessa-s/go-horai/task"
)

func TestScheduler_NoReadinessProbe_DispatchesImmediately(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s, ctx := startScheduler(t)
		execCount := registerCountingTask(t, ctx, s, "no-probe-task", "@every 1s")

		// Wait for at least one execution
		time.Sleep(300 * time.Millisecond)

		require.GreaterOrEqual(t, execCount.Load(), int32(1), "expected at least 1 execution without probe")
	})
}

func TestScheduler_ReadinessProbe_False_SkipsTick(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		storage := mustNewMemory(t, 100)
		s := engine(t, storage,
			scheduler.WithTickInterval(50*time.Millisecond),
			scheduler.WithReadiness(func() bool { return false }),
		)

		ctx := t.Context()

		require.NoError(t, s.Start(ctx))
		defer func() {
			stopCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			_ = s.Stop(stopCtx)
		}()

		var execCount atomic.Int32

		err := s.Register(ctx, schedtask.Config{
			ID:         "blocked-task",
			Schedule:   "@every 1s",
			RunOnStart: true,
			Func: func(_ context.Context) error {
				execCount.Add(1)
				return nil
			},
		})
		require.NoError(t, err)

		time.Sleep(300 * time.Millisecond)

		require.Equal(t, int32(0), execCount.Load(), "expected 0 executions with probe=false")
	})
}

func TestScheduler_ReadinessProbe_FalseToTrue_Transition(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ready atomic.Bool

		s, ctx := startScheduler(t,
			scheduler.WithReadiness(func() bool { return ready.Load() }),
		)
		execCount := registerCountingTask(t, ctx, s, "transition-task", "@every 1s")

		// Probe returns false — no executions
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, int32(0), execCount.Load(), "expected 0 executions before ready")

		// Flip to ready
		ready.Store(true)

		// Wait for at least one execution
		time.Sleep(300 * time.Millisecond)
		require.GreaterOrEqual(t, execCount.Load(), int32(1), "expected at least 1 execution after ready")
	})
}

func TestScheduler_Trigger_NotReady_ReturnsErrNotReady(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		storage := mustNewMemory(t, 100)
		s := engine(t, storage,
			scheduler.WithTickInterval(50*time.Millisecond),
			scheduler.WithReadiness(func() bool { return false }),
		)

		ctx := t.Context()

		require.NoError(t, s.Start(ctx))
		defer func() {
			stopCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()

			_ = s.Stop(stopCtx)
		}()

		err := s.Register(ctx, schedtask.Config{
			ID:       "trigger-test",
			Schedule: "@every 1h",
			Func:     func(_ context.Context) error { return nil },
		})
		require.NoError(t, err)

		err = s.Trigger(ctx, "trigger-test")
		require.ErrorIs(t, err, horai.ErrNotReady)
	})
}

func TestScheduler_ReadinessProbe_RunOnStart_FiresAfterReady(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ready atomic.Bool

		s, ctx := startScheduler(t,
			scheduler.WithReadiness(func() bool { return ready.Load() }),
		)
		execCount := registerCountingTask(t, ctx, s, "run-on-start-task", "@every 1h")

		// Not ready — should not fire RunOnStart
		time.Sleep(300 * time.Millisecond)
		require.Equal(t, int32(0), execCount.Load(), "expected 0 executions before ready")

		// Signal ready — RunOnStart task should fire on first tick
		ready.Store(true)

		time.Sleep(300 * time.Millisecond)
		require.GreaterOrEqual(t, execCount.Load(), int32(1), "expected RunOnStart task to fire after ready")
	})
}

func TestScheduler_IsReady_NoProbe(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		storage := mustNewMemory(t, 100)
		s := engine(t, storage)

		require.True(t, s.Status().Ready, "expected IsReady()=true when no probe is configured")
	})
}

func TestScheduler_IsReady_WithProbe(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		var ready atomic.Bool

		storage := mustNewMemory(t, 100)
		s := engine(t, storage,
			scheduler.WithReadiness(func() bool { return ready.Load() }),
		)

		require.False(t, s.Status().Ready, "expected IsReady()=false when probe returns false")

		ready.Store(true)

		require.True(t, s.Status().Ready, "expected IsReady()=true when probe returns true")
	})
}
