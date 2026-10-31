// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,lll,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

// historyContracts lists the [horai.HistoryStore] contracts; [RunHistory]
// executes each, including the mandatory exclusive-cutoff cleanup contract.
var historyContracts = []struct {
	name  string
	check func(*testing.T, horai.HistoryStore)
}{
	{"CleanupHistoryCutoff", CleanupHistoryCutoff},
	{"HistoryCleanup", HistoryCleanup},
	{"HistoryOrder", HistoryOrder},
	{"HistoryPage", HistoryPage},
	{"HistoryDelete", HistoryDelete},
	{"HistoryUnknownFilterField", HistoryUnknownFilterField},
	{"HistoryIntegerBoundary", HistoryIntegerBoundary},
}

// RunHistory executes every [horai.HistoryStore] contract as a parallel
// subtest named after it, each on a fresh store from newStore, which must
// return an empty store isolated from every other store it returns. A backend
// must implement every contract, including CleanupHistory.
func RunHistory(t *testing.T, newStore func(testing.TB) horai.HistoryStore) {
	t.Helper()
	for _, c := range historyContracts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.check(t, newStore(t))
		})
	}
}

// HistoryOrder verifies that History lists one task's entries by StartedAt
// descending, and that task IDs differing only by case or a trailing space
// have separate history. The supplied store must be empty and isolated per
// invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func HistoryOrder(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	ctx := t.Context()
	for _, h := range []horai.TaskHistory{
		{ID: "h1", StartedAt: 10}, {ID: "h3", StartedAt: 25}, {ID: "h2", StartedAt: 20}, {ID: "h4", StartedAt: 30},
	} {
		h.TaskID, h.EndedAt = "a", h.StartedAt+1
		require.NoError(t, store.AddHistory(ctx, &h))
	}
	require.Equal(t, []string{"h4", "h3", "h2", "h1"}, seqIDs(t, store.History(ctx, "a"), historyID))

	ids := []string{"task", "task ", "Task"}
	for i, id := range ids {
		require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: fmt.Sprint("t", i), TaskID: id, StartedAt: 1, EndedAt: 2}))
	}
	for i, id := range ids {
		require.Equal(t, []string{fmt.Sprint("t", i)}, seqIDs(t, store.History(ctx, id), historyID), "task %q", id)
	}
	require.Empty(t, seqIDs(t, store.History(ctx, "missing"), historyID))
}

// HistoryPage verifies that ListHistory walks one task's history
// by StartedAt then ID descending through the compound cursor, round-trips
// every field, and evaluates filters on every [horai.HistoryFilterFields]
// name, treating an empty error as the zero value. The supplied store must be
// empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps and sizes describe the storage contract.
func HistoryPage(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	ctx := t.Context()
	entries := []horai.TaskHistory{
		{ID: "h1", RunID: "r1", StartedAt: 10, DurationMs: 100, Success: true},
		{ID: "h2", RunID: "r2", StartedAt: 20, DurationMs: 200, Error: "boom"},
		{ID: "h3", RunID: "r3", StartedAt: 20, DurationMs: 300, Success: true},
		{ID: "h4", RunID: "r4", StartedAt: 30, DurationMs: 400, Error: "bang ✓"},
		{ID: "h5", RunID: "r5", StartedAt: 5, DurationMs: 500, Success: true},
	}
	for _, h := range entries {
		h.TaskID, h.EndedAt = "a", h.StartedAt+1
		require.NoError(t, store.AddHistory(ctx, &h))
	}
	require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: "other", TaskID: "b", StartedAt: 25, EndedAt: 26}))

	var walked []*horai.TaskHistory
	hpg := horai.HistoryPage{Limit: 2}
	for {
		page, err := store.ListHistory(ctx, "a", hpg, nil)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page), 3)
		n := min(len(page), 2)
		walked = append(walked, page[:n]...)
		if len(page) <= 2 {
			break
		}
		hpg.AfterID, hpg.AfterStartedAt = page[n-1].ID, page[n-1].StartedAt
	}
	require.Equal(t, []string{"h4", "h3", "h2", "h1", "h5"}, sliceIDs(walked, historyID))
	want := entries[3]
	want.TaskID, want.EndedAt = "a", want.StartedAt+1
	require.Equal(t, want, *walked[0], "every field must round-trip")

	for _, tc := range []struct {
		expr query.Expr
		want []string
	}{
		{query.And{query.Cmp{Field: "success", Op: query.Eq, Value: true}, query.Cmp{Field: "startedAt", Op: query.Ge, Value: int64(10)}}, []string{"h3", "h1"}},
		{query.Not{Expr: query.Cmp{Field: "success", Op: query.Eq, Value: true}}, []string{"h4", "h2"}},
		{query.Or{query.Cmp{Field: "id", Op: query.Eq, Value: "h2"}, query.Cmp{Field: "runId", Op: query.Eq, Value: "r5"}}, []string{"h2", "h5"}},
		{query.And{query.Cmp{Field: "taskId", Op: query.Eq, Value: "a"}, query.Cmp{Field: "endedAt", Op: query.Eq, Value: int64(21)}, query.Cmp{Field: "durationMs", Op: query.Gt, Value: int64(250)}}, []string{"h3"}},
		{query.Cmp{Field: "error", Op: query.Eq, Value: ""}, []string{"h3", "h1", "h5"}},
		{query.Cmp{Field: "error", Op: query.Ne, Value: ""}, []string{"h4", "h2"}},
		// The ClickHouse translator measures a string in bytes, the others in
		// code points; "bang ✓" is longer than 5 either way.
		{query.Cmp{Field: "error", Op: query.Prefix, Value: "bang"}, []string{"h4"}},
	} {
		page, err := store.ListHistory(ctx, "a", horai.HistoryPage{Limit: 100},
			tc.expr)
		require.NoError(t, err, tc.expr)
		require.Equal(t, tc.want, sliceIDs(page, historyID), tc.expr)
	}
}

// HistoryDelete verifies mandatory history deletion for a [horai.HistoryStore].
// DeleteHistory removes exactly one task's history; deleting a task without
// history is not an error. The
// supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func HistoryDelete(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	deleter := store
	ctx := t.Context()
	for _, id := range []string{"a", "A", "b"} {
		require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: id + "1", TaskID: id, StartedAt: 1, EndedAt: 2}))
	}
	require.NoError(t, deleter.DeleteHistory(ctx, "a"))
	require.Empty(t, seqIDs(t, store.History(ctx, "a"), historyID))
	require.Equal(t, []string{"A1"}, seqIDs(t, store.History(ctx, "A"), historyID))
	require.Equal(t, []string{"b1"}, seqIDs(t, store.History(ctx, "b"), historyID))
	require.NoError(t, deleter.DeleteHistory(ctx, "missing"))
}

// HistoryCleanup verifies that CleanupHistory removes the entries that ended
// before the retention window and keeps the rest. [RunHistory] leaves it out;
// a backend whose CleanupHistory enforces the retention runs it on its own.
// The supplied store must be empty and isolated per invocation.
//
//nolint:mnd // Fixed ages describe the storage contract.
func HistoryCleanup(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	ctx := t.Context()
	now := time.Now().Unix()
	for i, age := range []int64{7200, 60, 3600} {
		require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{
			ID: fmt.Sprint("e", i), TaskID: "hist", StartedAt: now - age - 1, EndedAt: now - age, Success: true,
		}))
	}
	require.NoError(t, store.CleanupHistory(ctx, now-5400))
	require.Equal(t, []string{"e1", "e2"}, seqIDs(t, store.History(ctx, "hist"), historyID))
}

// unknownFieldFilters name a field outside the allowlist, alone and behind a
// branch an evaluator short-circuits past.
var unknownFieldFilters = []query.Expr{query.Cmp{Field: "secret", Op: query.Eq, Value: int64(1)}, query.Or{query.Cmp{Field: "id", Op: query.Ne, Value: ""}, query.Cmp{Field: "secret", Op: query.Eq, Value: int64(1)}}}

// HistoryUnknownFilterField verifies that ListHistory rejects a filter
// naming a field outside [horai.HistoryFilterFields] with
// [query.ErrInvalid] whatever the stored history: with none, and with
// an entry that would satisfy the filter's allowed branch. The supplied store
// must be empty and isolated per invocation.
//
//nolint:mnd // Fixed sizes and timestamps describe the storage contract.
func HistoryUnknownFilterField(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	ctx := t.Context()
	pg := horai.HistoryPage{Limit: 10}
	check := func(stage string) {
		t.Helper()
		for _, expr := range unknownFieldFilters {
			_, err := store.ListHistory(ctx, "a", pg, expr)
			require.ErrorIs(t, err, query.ErrInvalid, "%s: %s", stage, expr)
		}
	}
	check("empty history")
	require.NoError(t, store.AddHistory(ctx, &horai.TaskHistory{ID: "h1", TaskID: "a", StartedAt: 1, EndedAt: 2}))
	check("stored history")
}

// HistoryIntegerBoundary verifies exact integer filtering at floating-point
// index boundaries. A backend may narrow candidates only without losing matches.
func HistoryIntegerBoundary(t *testing.T, store horai.HistoryStore) {
	t.Helper()
	const boundary int64 = 1 << 53
	for _, h := range []horai.TaskHistory{
		{ID: "positive", TaskID: "boundary", StartedAt: 1, EndedAt: 2, DurationMs: boundary + 1},
		{ID: "negative", TaskID: "boundary", StartedAt: 1, EndedAt: 2, DurationMs: -boundary - 1},
	} {
		require.NoError(t, store.AddHistory(t.Context(), &h))
	}
	for _, tc := range []struct {
		op    query.Op
		bound int64
		want  string
	}{
		{query.Gt, boundary, "positive"},
		{query.Lt, -boundary, "negative"},
	} {
		rows, err := store.ListHistory(t.Context(), "boundary", horai.HistoryPage{Limit: 10},
			query.Cmp{Field: "durationMs", Op: tc.op, Value: tc.bound})
		require.NoError(t, err)
		require.Len(t, rows, 1)
		require.Equal(t, tc.want, rows[0].ID)
	}
}
