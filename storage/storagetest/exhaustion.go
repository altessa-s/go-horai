// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

// ExhaustedFinish verifies completion and the changed-schedule guard together.
func ExhaustedFinish(t *testing.T, s horai.Store) {
	t.Helper()
	for _, status := range []horai.TaskStatus{horai.TaskStatusRunning, horai.TaskStatusPaused, horai.TaskStatusDisabled} {
		for _, changed := range []bool{false, true} {
			id := fmt.Sprintf("%s/%t", status, changed)
			schedule := "@daily"
			if changed {
				schedule = "@hourly"
			}
			created, err := s.CreateTask(t.Context(), &horai.TaskState{ID: id, Status: status, Schedule: schedule, NextRunAt: 100,
				LastRunID: "run", RunStartedAt: 1})
			require.NoError(t, err)
			require.True(t, created)
			finished, err := s.FinishRun(t.Context(), id, "run", horai.RunResult{Schedule: "@daily", StartedAt: 1, EndedAt: 2, Success: true})
			require.NoError(t, err)
			require.True(t, finished)
			state, err := s.GetTask(t.Context(), id)
			require.NoError(t, err)
			if changed {
				require.Equal(t, int64(100), state.NextRunAt)
				require.NotEqual(t, horai.TaskStatusCompleted, state.Status)
			} else {
				require.Zero(t, state.NextRunAt)
				require.Equal(t, horai.TaskStatusCompleted, state.Status)
			}
			require.Zero(t, state.RunStartedAt)
		}
	}
}
