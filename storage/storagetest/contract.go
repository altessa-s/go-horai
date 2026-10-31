// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,lll,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"fmt"
	"iter"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
	"github.com/altessa-s/go-horai/task"
)

// ClaimRun verifies that the claim is an atomic compare-and-swap on status and
// the occurrence fence: exactly one of many concurrent claims wins, and stale
// or paused claims lose. The supplied store must be isolated per invocation.
//
//nolint:mnd // Fixed timestamps and counts describe the storage contract.
func ClaimRun(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	put := func(id string, status horai.TaskStatus) {
		require.NoError(t, seed(ctx, store, &horai.TaskState{
			ID: id, Status: status, Schedule: "@every 1m", NextRunAt: 300}))
	}

	put("claim_race", horai.TaskStatusActive)
	var (
		wins atomic.Int32
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)
	for i := range 16 {
		wg.Go(func() {
			ok, err := store.ClaimRun(ctx, "claim_race", horai.RunClaim{NextRunAt: 300, StartedAt: 100, RunID: fmt.Sprintf("run-%d", i)})
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
				return
			}
			if ok {
				wins.Add(1)
			}
		})
	}
	wg.Wait()
	require.Empty(t, errs, "a losing concurrent claim must report false, not an error")
	require.Equal(t, int32(1), wins.Load(), "exactly one concurrent claim must win")
	got, err := store.GetTask(ctx, "claim_race")
	require.NoError(t, err)
	require.Equal(t, horai.TaskStatusRunning, got.Status)
	require.Equal(t, int64(100), got.RunStartedAt)

	put("claim_stale", horai.TaskStatusActive)
	ok, err := store.ClaimRun(ctx, "claim_stale", horai.RunClaim{NextRunAt: 299, StartedAt: 100, RunID: "run"})
	require.NoError(t, err)
	require.False(t, ok, "a stale occurrence fence must lose")

	put("claim_paused", horai.TaskStatusPaused)
	ok, err = store.ClaimRun(ctx, "claim_paused", horai.RunClaim{NextRunAt: 300, StartedAt: 100, RunID: "run"})
	require.NoError(t, err)
	require.False(t, ok, "a paused task must not be claimed")

	put("claim_zero_fence", horai.TaskStatusActive)
	ok, err = store.ClaimRun(ctx, "claim_zero_fence", horai.RunClaim{StartedAt: 100, RunID: "run"})
	require.NoError(t, err)
	require.False(t, ok, "a zero fence claims only a zero occurrence")

	ok, err = store.ClaimRun(ctx, "claim_missing", horai.RunClaim{StartedAt: 100, RunID: "run"})
	require.NoError(t, err)
	require.False(t, ok)
}

// DueTasks verifies that exactly the active tasks with NextRunAt <= now are
// returned, ordered by ID, and that Tasks returns every task (its order is
// implementation-defined).
// The supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func DueTasks(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	for _, s := range []horai.TaskSummary{
		{ID: "due_c", Status: horai.TaskStatusActive, NextRunAt: 100},
		{ID: "due_a", Status: horai.TaskStatusActive, NextRunAt: 200},
		{ID: "due_b", Status: horai.TaskStatusPaused, NextRunAt: 100},
		{ID: "due_d", Status: horai.TaskStatusActive, NextRunAt: 201},
		{ID: "due_e", Status: horai.TaskStatusRunning, NextRunAt: 50},
	} {
		require.NoError(t, seed(ctx, store, &horai.TaskState{TaskSummary: s}))
	}
	require.Equal(t, []string{"due_a", "due_c"}, seqIDs(t, store.DueTasks(ctx, 200), taskID))
	require.ElementsMatch(t, []string{"due_a", "due_b", "due_c", "due_d", "due_e"}, seqIDs(t, store.Tasks(ctx), taskID))
}

// DueTasksZeroNextRun verifies that an active task whose NextRunAt is zero is
// due at every now >= 0 and not before, whichever write stored it (UpsertTask,
// CreateTask or ReplaceTaskIf), while a negative NextRunAt is due at -1; that a
// zero task in another status is not due; and that a zero task claimed with
// its observed (zero) NextRunAt leaves the due set and returns only at the
// NextRunAt its finished run stored.
// The supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func DueTasksZeroNextRun(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	summary := func(id string, status horai.TaskStatus, next int64) *horai.TaskState {
		return &horai.TaskState{ID: id, Status: status, Schedule: "@every 1m", NextRunAt: next}
	}
	require.NoError(t, seed(ctx, store, summary("zero_upsert", horai.TaskStatusActive, 0)))
	require.NoError(t, seed(ctx, store, summary("zero_paused", horai.TaskStatusPaused, 0)))
	require.NoError(t, seed(ctx, store, summary("zero_later", horai.TaskStatusActive, 300)))
	require.NoError(t, seed(ctx, store, summary("neg", horai.TaskStatusActive, -5)))
	created, err := store.CreateTask(ctx, summary("zero_create", horai.TaskStatusActive, 0))
	require.NoError(t, err)
	require.True(t, created)
	require.NoError(t, seed(ctx, store, summary("zero_replace", horai.TaskStatusActive, 300)))
	stored, err := store.GetTask(ctx, "zero_replace")
	require.NoError(t, err)
	replaced, err := store.ReplaceTaskIf(ctx, summary("zero_replace", horai.TaskStatusActive, 0), horai.FenceOf(stored))
	require.NoError(t, err)
	require.True(t, replaced)

	require.Equal(t, []string{"neg"}, seqIDs(t, store.DueTasks(ctx, -1), taskID), "zero is after now = -1")
	zeros := []string{"neg", "zero_create", "zero_replace", "zero_upsert"}
	require.Equal(t, zeros, seqIDs(t, store.DueTasks(ctx, 0), taskID))
	require.Equal(t, zeros, seqIDs(t, store.DueTasks(ctx, 200), taskID))

	claimed, err := store.ClaimRun(ctx, "zero_upsert", horai.RunClaim{NextRunAt: 0, StartedAt: 100, RunID: "owner/run"})
	require.NoError(t, err)
	require.True(t, claimed, "an active task with a zero NextRunAt must be claimable")
	require.Equal(t, []string{"neg", "zero_create", "zero_replace"}, seqIDs(t, store.DueTasks(ctx, 200), taskID))

	finished, err := store.FinishRun(ctx, "zero_upsert", "owner/run",
		horai.RunResult{StartedAt: 100, EndedAt: 110, NextRunAt: 360, Schedule: "@every 1m", Success: true})
	require.NoError(t, err)
	require.True(t, finished)
	stale, err := store.ClaimRun(ctx, "zero_upsert", horai.RunClaim{NextRunAt: 0, StartedAt: 120, RunID: "late/run"})
	require.NoError(t, err)
	require.False(t, stale, "a dispatch that read the zero before the run finished must not run the task again")
	require.Equal(t, []string{"neg", "zero_create", "zero_replace"}, seqIDs(t, store.DueTasks(ctx, 200), taskID))
	require.Equal(t, []string{"neg", "zero_create", "zero_later", "zero_replace"}, seqIDs(t, store.DueTasks(ctx, 359), taskID))
	require.Equal(t, []string{"neg", "zero_create", "zero_later", "zero_replace", "zero_upsert"},
		seqIDs(t, store.DueTasks(ctx, 360), taskID))
}

// Identity verifies that task IDs and run-ownership fences compare exactly:
// IDs differing only by case or a trailing space are distinct rows with
// separate history, and a run ID differing only by case does not own a run.
// The supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func Identity(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	ids := []string{"task", "task ", "Task"}
	for i, id := range ids {
		require.NoError(t, seed(ctx, store, &horai.TaskState{
			ID: id, Status: horai.TaskStatusActive, Description: fmt.Sprint(i), Meta: map[string]string{"emoji": "🚀", "k": "$revision"}}))
		require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: "h" + fmt.Sprint(i), TaskID: id, StartedAt: 1, EndedAt: 2}))
	}
	for i, id := range ids {
		got, err := store.GetTask(ctx, id)
		require.NoError(t, err)
		require.Equal(t, id, got.ID)
		require.Equal(t, fmt.Sprint(i), got.Description)
		require.Equal(t, map[string]string{"emoji": "🚀", "k": "$revision"}, got.Meta)
		require.Len(t, seqIDs(t, store.History(ctx, id), historyID), 1)
	}
	require.NoError(t, store.DeleteTask(ctx, "task "))
	got, err := store.GetTask(ctx, "task")
	require.NoError(t, err)
	require.NotNil(t, got, "deleting 'task ' must not delete 'task'")
	require.Len(t, seqIDs(t, store.History(ctx, "task"), historyID), 1)

	require.NoError(t, seed(ctx, store, &horai.TaskState{
		ID: "owned", Status: horai.TaskStatusRunning, LastRunID: "owner", RunStartedAt: 100}))
	ok, err := store.FinishRun(ctx, "owned", "OWNER", horai.RunResult{StartedAt: 100, EndedAt: 110, Success: true})
	require.NoError(t, err)
	require.False(t, ok, "run ownership must be case-sensitive")
}

// TaskPage verifies ListTasks (ID order, AfterID cursor, filters, with
// size() counting code points) and ListHistory (StartedAt then ID descending, compound cursor, filters).
// The supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps and sizes describe the storage contract.
func TaskPage(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	ids := []string{"B", "a", "a_1", "a1", "b", "z", "Alpha"}
	for i, id := range ids {
		require.NoError(t, seed(ctx, store, &horai.TaskState{
			ID: id, Status: horai.TaskStatusActive, Priority: task.Priority(i % 3), OneShot: i%2 == 0,
			Description: map[bool]string{true: "Alpha job", false: "alpha job"}[i%2 == 0]}))
	}
	want := slices.Sorted(slices.Values(ids))

	var walked []string
	pg := horai.TaskPage{Limit: 2}
	for {
		page, err := store.ListTasks(ctx, pg, nil)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), 3)
		n := min(len(page), 2)
		for _, s := range page[:n] {
			walked = append(walked, s.ID)
		}
		if len(page) <= 2 {
			break
		}
		pg.AfterID = page[n-1].ID
	}
	require.Equal(t, want, walked, "pages must walk every task once in byte-wise ID order")

	for _, tc := range []struct {
		expr query.Expr
		want []string
	}{
		{query.And{query.Cmp{Field: "status", Op: query.Eq, Value: int64(1)}, query.Cmp{Field: "priority", Op: query.Gt, Value: int64(0)}}, []string{"a", "a_1", "b", "z"}},
		{query.Cmp{Field: "oneShot", Op: query.Eq, Value: true}, []string{"Alpha", "B", "a_1", "b"}},
		{query.Cmp{Field: "id", Op: query.Prefix, Value: "a"}, []string{"a", "a1", "a_1"}},
		{query.Cmp{Field: "description", Op: query.Eq, Value: "Alpha job"}, []string{"Alpha", "B", "a_1", "b"}},
	} {
		page, err := store.ListTasks(ctx, horai.TaskPage{Limit: 100}, tc.expr)
		require.NoError(t, err, tc.expr)
		require.Equal(t, slices.Sorted(slices.Values(tc.want)), sliceIDs(page, taskID), tc.expr)
	}

	// History: ties on StartedAt are broken by ID descending.
	for _, h := range []horai.TaskHistory{
		{ID: "h1", StartedAt: 10, Success: true}, {ID: "h2", StartedAt: 20}, {ID: "h3", StartedAt: 20, Success: true},
		{ID: "h4", StartedAt: 30, Error: "boom"}, {ID: "h5", StartedAt: 5, Success: true},
	} {
		h.TaskID, h.EndedAt = "a", h.StartedAt+1
		require.NoError(t, store.AddHistory(ctx, &h))
	}
	require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: "other", TaskID: "b", StartedAt: 25}))

	var hist []string
	hpg := horai.HistoryPage{Limit: 2}
	for {
		page, err := store.ListHistory(ctx, "a", hpg, nil)
		require.NoError(t, err)
		n := min(len(page), 2)
		for _, h := range page[:n] {
			hist = append(hist, h.ID)
		}
		if len(page) <= 2 {
			break
		}
		hpg.AfterID, hpg.AfterStartedAt = page[n-1].ID, page[n-1].StartedAt
	}
	require.Equal(t, []string{"h4", "h3", "h2", "h1", "h5"}, hist)

	page, err := store.ListHistory(ctx, "a", horai.HistoryPage{Limit: 100},
		query.And{query.Cmp{Field: "success", Op: query.Eq, Value: true}, query.Cmp{Field: "startedAt", Op: query.Ge, Value: int64(10)}})
	require.NoError(t, err)
	require.Equal(t, []string{"h3", "h1"}, sliceIDs(page, historyID))
}

// ZeroValueFilters verifies that filters see a zero-valued field as that zero
// value however the backend stores it — MongoDB omits zero fields from the
// document — and that endsWith is exact about trailing spaces. The supplied
// store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func ZeroValueFilters(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	for _, s := range []horai.TaskSummary{
		{ID: "z"},
		{ID: "n", Description: "job", OneShot: true, NextRunAt: 7, LastRunAt: 3, SkipNextRun: true},
		{ID: "m", Description: "job ", NextRunAt: 10},
	} {
		s.Status = horai.TaskStatusActive
		require.NoError(t, seed(ctx, store, &horai.TaskState{TaskSummary: s}))
	}
	for _, tc := range []struct {
		expr query.Expr
		want []string
	}{
		{query.Cmp{Field: "description", Op: query.Eq, Value: ""}, []string{"z"}},
		{query.Cmp{Field: "description", Op: query.Ne, Value: ""}, []string{"m", "n"}},
		{query.Not{Expr: query.Cmp{Field: "description", Op: query.Eq, Value: ""}}, []string{"m", "n"}},
		{query.Cmp{Field: "description", Op: query.Prefix, Value: ""}, []string{"m", "n", "z"}},
		{query.Cmp{Field: "oneShot", Op: query.Eq, Value: false}, []string{"m", "z"}},
		{query.Cmp{Field: "oneShot", Op: query.Ne, Value: true}, []string{"m", "z"}},
		{query.Not{Expr: query.Cmp{Field: "oneShot", Op: query.Eq, Value: true}}, []string{"m", "z"}},
		{query.Or{query.Cmp{Field: "oneShot", Op: query.Eq, Value: true}, query.Cmp{Field: "nextRunAt", Op: query.Eq, Value: int64(0)}}, []string{"n", "z"}},
		{query.Cmp{Field: "nextRunAt", Op: query.Eq, Value: int64(0)}, []string{"z"}},
		{query.Cmp{Field: "nextRunAt", Op: query.Ne, Value: int64(0)}, []string{"m", "n"}},
		{query.Cmp{Field: "nextRunAt", Op: query.Lt, Value: int64(5)}, []string{"z"}},
		{query.Cmp{Field: "nextRunAt", Op: query.In, Value: []int64{0, 7}}, []string{"n", "z"}},
		{query.Not{Expr: query.Cmp{Field: "nextRunAt", Op: query.In, Value: []int64{0}}}, []string{"m", "n"}},
		{query.And{query.Cmp{Field: "skipNextRun", Op: query.Eq, Value: false}, query.Cmp{Field: "lastRunAt", Op: query.Eq, Value: int64(0)}}, []string{"m", "z"}},
	} {
		page, err := store.ListTasks(ctx, horai.TaskPage{Limit: 100}, tc.expr)
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, sliceIDs(page, taskID), tc.expr)
	}

	for _, h := range []horai.TaskHistory{{ID: "h0", StartedAt: 10}, {ID: "h1", StartedAt: 20, Error: "boom"}} {
		h.TaskID, h.EndedAt = "z", h.StartedAt+1
		require.NoError(t, store.AddHistory(ctx, &h))
	}
	for _, tc := range []struct {
		expr query.Expr
		want []string
	}{
		{query.Cmp{Field: "error", Op: query.Eq, Value: ""}, []string{"h0"}},
		{query.Cmp{Field: "error", Op: query.Ne, Value: ""}, []string{"h1"}},
	} {
		page, err := store.ListHistory(ctx, "z", horai.HistoryPage{Limit: 100},
			tc.expr)
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, sliceIDs(page, historyID), tc.expr)
	}
}

// StringAnchors verifies that endsWith and a `$` in matches() anchor at the
// very end of the text, as CEL does: a description ending in a newline does
// not end with what precedes it. The supplied store must be empty and
// isolated per invocation.
//
//nolint:mnd // Fixed page size describes the storage contract.
func StringAnchors(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	for id, description := range map[string]string{"nl": "job\n", "plain": "job", "inner": "x\njob"} {
		require.NoError(t, seed(ctx, store, &horai.TaskState{
			ID: id, Status: horai.TaskStatusActive, Description: description}))
	}
	for _, tc := range []struct {
		expr query.Expr
		want []string
	}{
		{query.Cmp{Field: "description", Op: query.Prefix, Value: "job"}, []string{"nl", "plain"}},
	} {
		page, err := store.ListTasks(ctx, horai.TaskPage{Limit: 100}, tc.expr)
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, sliceIDs(page, taskID), tc.expr)
	}
}

// History verifies History ordering, CleanupHistory retention and that
// DeleteTask removes a task's history. The supplied store must be empty and
// isolated per invocation.
//
//nolint:mnd // Fixed ages describe the storage contract.
func History(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().Unix()
	require.NoError(t, seed(ctx, store, &horai.TaskState{ID: "hist", Status: horai.TaskStatusActive}))
	for i, age := range []int64{7200, 60, 3600} {
		require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{
			ID: fmt.Sprint("e", i), TaskID: "hist", StartedAt: now - age - 1, EndedAt: now - age, Success: true,
		}))
	}
	require.Equal(t, []string{"e1", "e2", "e0"}, seqIDs(t, store.History(ctx, "hist"), historyID))

	require.NoError(t, store.CleanupHistory(ctx, now-5400))
	require.Equal(t, []string{"e1", "e2"}, seqIDs(t, store.History(ctx, "hist"), historyID))

	require.NoError(t, store.DeleteTask(ctx, "hist"))
	got, err := store.GetTask(ctx, "hist")
	require.NoError(t, err)
	require.Nil(t, got)
	require.Empty(t, seqIDs(t, store.History(ctx, "hist"), historyID))
	require.NoError(t, store.DeleteTask(ctx, "hist"), "deleting a missing task is not an error")
}

// UnknownFilterField verifies that ListTasks and ListHistory reject
// a filter naming a field outside [horai.TaskFilterFields] or
// [horai.HistoryFilterFields] with [query.ErrInvalid] whatever
// is stored: nothing, or a task and an entry that would satisfy the filter's
// allowed branch. The supplied store must be empty and isolated per
// invocation.
//
//nolint:mnd // Fixed sizes and timestamps describe the storage contract.
func UnknownFilterField(t *testing.T, store horai.Store) {
	t.Helper()
	ctx := t.Context()
	pg := horai.TaskPage{Limit: 10}
	check := func(stage string) {
		t.Helper()
		for _, expr := range unknownFieldFilters {
			_, err := store.ListTasks(ctx, pg, expr)
			require.ErrorIs(t, err, query.ErrInvalid, "%s: %s", stage, expr)
		}
	}
	check("no tasks")
	require.NoError(t, seed(ctx, store, &horai.TaskState{ID: "a", Status: horai.TaskStatusActive}))
	check("stored task")
	HistoryUnknownFilterField(t, store)
}

func taskID(s *horai.TaskState) string      { return s.ID }
func historyID(h *horai.TaskHistory) string { return h.ID }

// seqIDs drains seq, failing on any yielded error, and returns the IDs.
func seqIDs[T any](t *testing.T, seq iter.Seq2[*T, error], id func(*T) string) []string {
	t.Helper()
	var ids []string
	for v, err := range seq {
		require.NoError(t, err)
		ids = append(ids, id(v))
	}
	return ids
}

func sliceIDs[T any](items []*T, id func(*T) string) []string {
	ids := make([]string, 0, len(items))
	for _, v := range items {
		ids = append(ids, id(v))
	}
	return ids
}
