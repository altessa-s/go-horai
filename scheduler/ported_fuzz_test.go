// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"

	schedtask "github.com/altessa-s/go-horai/task"
)

func FuzzScheduler_Register(f *testing.F) {
	f.Add("task-1", "@every 1s", "description")
	f.Add("", "@every 5m", "")
	f.Add("my-task", "invalid", "test")
	f.Add("task", "0 */5 * * * *", "every 5 min")

	f.Fuzz(func(t *testing.T, id, schedule, description string) {
		storage := mustNewMemory(t, 10)
		s := engine(t, storage, scheduler.WithTickInterval(time.Hour))
		ctx := t.Context()
		require.NoError(t, s.Start(ctx))
		defer func() {
			stopCtx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			_ = s.Stop(stopCtx)
		}()

		// Should not panic
		_ = s.Register(ctx, schedtask.Config{
			ID:          id,
			Schedule:    schedule,
			Description: description,
			Func:        func(_ context.Context) error { return nil },
		})
	})
}

func FuzzMemoryStorage_CreateAndGet(f *testing.F) {
	f.Add("task-1", "@every 1h", int32(1))
	f.Add("", "@every 5m", int32(0))
	f.Add("task-xyz", "0 0 * * * *", int32(3))

	f.Fuzz(func(t *testing.T, id, schedule string, status int32) {
		storage := mustNewMemory(t, 10)
		ctx := t.Context()

		state := &horai.TaskState{
			ID:       id,
			Status:   horai.TaskStatus(status),
			Schedule: schedule,
		}

		// Should not panic
		_, err := storage.CreateTask(ctx, state)
		require.NoError(t, err)

		got, err := storage.GetTask(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, id, got.ID)
	})
}

func FuzzTaskStatus_String(f *testing.F) {
	f.Add(int32(0))
	f.Add(int32(1))
	f.Add(int32(2))
	f.Add(int32(3))
	f.Add(int32(4))
	f.Add(int32(99))

	f.Fuzz(func(t *testing.T, s int32) {
		status := horai.TaskStatus(s)
		// Should not panic
		result := status.String()
		require.NotEmpty(t, result, "String() returned empty")
	})
}
