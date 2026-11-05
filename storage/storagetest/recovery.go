// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,lll,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

// UnfinishedRuns verifies status-independent enumeration and ignores lease expiry.
func UnfinishedRuns(t *testing.T, s horai.Store) {
	t.Helper()

	for _, status := range []horai.TaskStatus{horai.TaskStatusActive, horai.TaskStatusRunning, horai.TaskStatusPaused, horai.TaskStatusDisabled, horai.TaskStatusCompleted} {
		id := status.String()
		require.NoError(t, seed(t.Context(), s, &horai.TaskState{ID: id, Status: status, RunStartedAt: 100, LastRunID: id + "/run", RunLeaseID: id + "/run", RunLeaseUntil: 9999999999}))
	}

	require.NoError(t, seed(t.Context(), s, &horai.TaskState{ID: "finished", Status: horai.TaskStatusActive}))

	for _, v := range []*horai.TaskState{
		{ID: "no-lease", RunStartedAt: 10, LastRunID: "owner/run"},
		{ID: "foreign-lease", RunStartedAt: 10, LastRunID: "owner/run", RunLeaseID: "other/run", RunLeaseUntil: 9999999999},
		{ID: "negative", RunStartedAt: -1, LastRunID: "owner/run"},
	} {
		require.NoError(t, seed(t.Context(), s, v))
	}

	require.ElementsMatch(t, []string{"active", "running", "paused", "disabled", "completed", "no-lease", "foreign-lease", "negative"}, seqIDs(t, s.UnfinishedRuns(t.Context()), taskID))
}

// CleanupHistoryCutoff verifies the exact exclusive boundary.
func CleanupHistoryCutoff(t *testing.T, s horai.HistoryStore) {
	t.Helper()

	for _, h := range []horai.TaskHistory{{ID: "before", TaskID: "task", EndedAt: 99}, {ID: "equal", TaskID: "task", EndedAt: 100}, {ID: "after", TaskID: "task", EndedAt: 101}} {
		require.NoError(t, s.AddHistory(t.Context(), &h))
	}

	require.NoError(t, s.CleanupHistory(t.Context(), 100))
	require.ElementsMatch(t, []string{"equal", "after"}, seqIDs(t, s.History(t.Context(), "task"), historyID))
}

// LargeValues verifies metadata and long descriptions round-trip without truncation.
func LargeValues(t *testing.T, s horai.Store) {
	t.Helper()

	v := &horai.TaskState{ID: "large", Description: strings.Repeat("text ", 10000), Meta: map[string]string{"large": strings.Repeat("value", 10000)}}
	require.NoError(t, seed(t.Context(), s, v))
	got, err := s.GetTask(t.Context(), v.ID)
	require.NoError(t, err)
	require.Equal(t, v.Meta, got.Meta)
	require.Equal(t, v.Description, got.Description)
}

// ListTasksFilterParity compares the adapter's results to the domain evaluator.
func ListTasksFilterParity(t *testing.T, s horai.Store) {
	t.Helper()

	rows := []*horai.TaskState{{ID: "a", Description: "Job ", Status: horai.TaskStatusActive}, {ID: "b", Description: "job", Status: horai.TaskStatusPaused}, {ID: "c"}}
	for _, v := range rows {
		require.NoError(t, seed(t.Context(), s, v))
	}

	filters := []query.Expr{query.Cmp{Field: "description", Op: query.Eq, Value: "Job "}, query.Cmp{Field: "description", Op: query.Contains, Value: "ob"}, query.Not{Expr: query.Cmp{Field: "status", Op: query.In, Value: []int64{1, 2}}}, query.Or{query.Cmp{Field: "id", Op: query.Prefix, Value: "a"}, query.Cmp{Field: "description", Op: query.Eq, Value: ""}}}
	for _, f := range filters {
		var want []string

		for _, v := range rows {
			if query.Match(f, v.QueryRecord()) {
				want = append(want, v.ID)
			}
		}

		got, err := s.ListTasks(t.Context(), horai.TaskPage{Limit: 100}, f)
		require.NoError(t, err)
		require.ElementsMatch(t, want, sliceIDs(got, taskID))
	}
}
