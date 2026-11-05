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

	"github.com/altessa-s/go-horai/scheduler"

	schedtask "github.com/altessa-s/go-horai/task"
)

// The context given to Start bounds only the start: once it ends, the
// scheduler keeps running tasks until Stop.
func TestStart_ContextBoundsOnlyTheStart(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		storage := mustNewMemory(t, 10)
		s := engine(t, storage, scheduler.WithTickInterval(10*time.Millisecond))
		ran := make(chan struct{}, 1)

		require.NoError(t, s.Register(t.Context(), schedtask.Config{
			ID: "task", Schedule: "@every 1h", RunOnStart: true,
			Func: func(context.Context) error {
				select {
				case ran <- struct{}{}:
				default:
				}

				return nil
			},
		}))

		start, cancel := context.WithCancel(t.Context())
		require.NoError(t, s.Start(start))
		cancel()
		t.Cleanup(func() { _ = s.Stop(context.Background()) })

		select {
		case <-ran:
		case <-time.After(5 * time.Second):
			t.Fatal("the scheduler stopped with its start context")
		}
	})
}

// A start whose context has already ended fails without starting.
func TestStart_EndedContextFails(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		s := engine(t, mustNewMemory(t, 10))
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, s.Start(ctx), context.Canceled)
	})
}
