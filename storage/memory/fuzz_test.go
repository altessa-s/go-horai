// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/storage/memory"

	scheduler "github.com/altessa-s/go-horai"
)

func FuzzStorage_CreateAndGet(f *testing.F) {
	f.Add("task-1", int32(1), "@every 1h")
	f.Add("", int32(0), "")
	f.Add("task-abc", int32(4), "0 0 * * * *")

	f.Fuzz(func(t *testing.T, id string, status int32, schedule string) {
		s, err := memory.New(memory.WithMaxHistoryPerTask(10))
		require.NoError(t, err)
		ctx := t.Context()

		state := &scheduler.TaskState{
			ID:       id,
			Status:   scheduler.TaskStatus(status),
			Schedule: schedule,
		}

		// Should not panic
		_, err = s.CreateTask(ctx, state)
		require.NoError(t, err)

		got, err := s.GetTask(ctx, id)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, id, got.ID)
	})
}

func FuzzStorage_AddHistory(f *testing.F) {
	f.Add("h1", "task-1", "r1", int64(100), true, "")
	f.Add("", "", "", int64(0), false, "some error")

	f.Fuzz(func(t *testing.T, id, taskID, runID string, startedAt int64, success bool, errMsg string) {
		s, err := memory.New(memory.WithMaxHistoryPerTask(5))
		require.NoError(t, err)
		ctx := t.Context()

		h := &scheduler.TaskHistory{
			ID:        id,
			TaskID:    taskID,
			RunID:     runID,
			StartedAt: startedAt,
			Success:   success,
			Error:     errMsg,
		}

		// Should not panic
		_ = s.AddHistory(ctx, h)

		for range s.History(ctx, taskID) {
		}
	})
}
