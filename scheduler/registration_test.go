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
	"github.com/altessa-s/go-horai/task"
)

func TestRegisterRejectsDuplicateID(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		st := store(t)
		first := engine(t, st)

		var firstCalls, secondCalls atomic.Int32

		cfg := task.Config{
			ID: "duplicate", Schedule: "@hourly", Description: "original",
			Func: func(context.Context) error { firstCalls.Add(1); return nil },
		}
		require.NoError(t, first.Register(t.Context(), cfg))
		before, err := st.GetTask(t.Context(), cfg.ID)
		require.NoError(t, err)

		cfg.Schedule = "@daily"
		cfg.Description = "duplicate"
		cfg.Func = func(context.Context) error { secondCalls.Add(1); return nil }
		require.ErrorIs(t, first.Register(t.Context(), cfg), horai.ErrTaskAlreadyRegistered)
		after, err := st.GetTask(t.Context(), cfg.ID)
		require.NoError(t, err)
		require.Equal(t, before, after)

		require.NoError(t, first.Start(t.Context()))
		defer func() { require.NoError(t, first.Stop(t.Context())) }()

		require.NoError(t, first.Trigger(t.Context(), cfg.ID))
		require.Eventually(t, func() bool { return firstCalls.Load() == 1 }, time.Second, time.Millisecond)
		require.Zero(t, secondCalls.Load())
	})
}

func TestConcurrentRegisterRejectsDuplicateID(t *testing.T) {
	t.Parallel()
	s := engine(t, store(t))
	cfg := task.Config{ID: "race", Schedule: "@hourly", Func: func(context.Context) error { return nil }}
	start := make(chan struct{})
	results := make(chan error, 2)

	for range 2 {
		go func() {
			<-start

			results <- s.Register(t.Context(), cfg)
		}()
	}

	close(start)

	a, b := <-results, <-results
	if a != nil {
		a, b = b, a
	}

	require.NoError(t, a)
	require.ErrorIs(t, b, horai.ErrTaskAlreadyRegistered)
}

func TestConcurrentRegisterAcrossSchedulers(t *testing.T) {
	t.Parallel()

	for _, seeded := range []bool{false, true} {
		name := "new task"
		if seeded {
			name = "persisted task"
		}

		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				st := store(t)
				first, second := engine(t, st), engine(t, st)

				cfg := task.Config{ID: "shared", Schedule: "@every 1h", Func: func(context.Context) error { return nil }}
				if seeded {
					require.NoError(t, engine(t, st).Register(t.Context(), cfg))
				}

				start := make(chan struct{})
				results := make(chan error, 2)

				var calls [2]atomic.Int32

				for i, registrar := range []task.Registrar{first, second} {
					go func() {
						local := cfg
						local.Func = func(context.Context) error { calls[i].Add(1); return nil }

						<-start

						results <- registrar.Register(t.Context(), local)
					}()
				}

				close(start)

				a, b := <-results, <-results
				require.NoError(t, a)
				require.NoError(t, b)

				require.NoError(t, first.Start(t.Context()))
				defer func() { require.NoError(t, first.Stop(t.Context())) }()

				require.NoError(t, second.Start(t.Context()))
				defer func() { require.NoError(t, second.Stop(t.Context())) }()

				require.NoError(t, first.Trigger(t.Context(), cfg.ID))
				synctest.Wait()
				require.Equal(t, int32(1), calls[0].Load())
				require.Zero(t, calls[1].Load())

				require.NoError(t, second.Trigger(t.Context(), cfg.ID))
				synctest.Wait()
				require.Equal(t, int32(1), calls[0].Load())
				require.Equal(t, int32(1), calls[1].Load())
			})
		})
	}
}

func TestRegisterRejectsIDWithLocalHandlerOnly(t *testing.T) {
	t.Parallel()
	st := store(t)
	s := engine(t, st)
	cfg := task.Config{ID: "local", Schedule: "@hourly", Func: func(context.Context) error { return nil }}
	require.NoError(t, s.Register(t.Context(), cfg))
	require.NoError(t, st.DeleteTask(t.Context(), cfg.ID))

	require.ErrorIs(t, s.Register(t.Context(), cfg), horai.ErrTaskAlreadyRegistered)
	state, err := st.GetTask(t.Context(), cfg.ID)
	require.NoError(t, err)
	require.Nil(t, state)
}
