// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/task"
)

func TestModelRoundTrip(t *testing.T) {
	t.Parallel()

	state := &horai.TaskState{ID: "case:a_1", Description: "large", Status: horai.TaskStatusPaused, Priority: task.PriorityHigh,
		Schedule: "@every 1s", LastRunAt: 1, NextRunAt: 2, LastRunID: "instance/run", RunStartedAt: 3, RunLeaseUntil: 4, RunLeaseID: "instance/run", RunAt: 5,
		Failures: 6, SkipNextRun: true, DisableHistory: true, Unmanaged: true, OneShot: true, Meta: map[string]string{"key": "value"}, CreatedAt: 7, UpdatedAt: 8, Revision: 9}
	data := newTaskDocument(state)
	require.Equal(t, state, data.toTaskState())
	state.Meta["key"] = "mutated"
	require.Equal(t, "value", data.Meta["key"])
	result := data.toTaskState()
	result.Meta["key"] = "other"
	require.Equal(t, "value", data.Meta["key"])
	require.Equal(t, &horai.TaskState{}, newTaskDocument(&horai.TaskState{}).toTaskState())

	h := &horai.TaskHistory{ID: "id", TaskID: "task", RunID: "run", Error: "error", StartedAt: 10, EndedAt: 11, DurationMs: 12, Success: true}
	require.Equal(t, h, newHistoryDocument(h).toTaskHistory())
}
func TestNilConstructor(t *testing.T) { _, err := New(nil); require.Error(t, err) }
