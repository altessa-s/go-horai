// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,lll,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

// ReplaceTaskIf verifies that a fenced replace succeeds only while every fence
// field still matches, including zero-valued (absent) fields. The supplied
// store must be isolated per invocation.
//
//nolint:mnd // Fixed timestamps describe the storage contract.
func ReplaceTaskIf(t *testing.T, store horai.Store) {
	t.Helper()
	running := func(id string) *horai.TaskState {
		return &horai.TaskState{
			ID: id, Status: horai.TaskStatusRunning, Schedule: "@every 1m", NextRunAt: 300, LastRunID: "owner", RunStartedAt: 100}
	}
	reset := func(state *horai.TaskState) *horai.TaskState {
		next := *state
		next.Status = horai.TaskStatusActive
		next.RunStartedAt = 0
		next.Failures = 1
		return &next
	}

	for _, tc := range []struct {
		name    string
		mutate  func(*horai.TaskFence)
		wantHit bool
	}{
		{"match", func(*horai.TaskFence) {}, true},
		{"status_changed", func(f *horai.TaskFence) { f.Status = horai.TaskStatusActive }, false},
		{"next_run_changed", func(f *horai.TaskFence) { f.NextRunAt = 301 }, false},
		{"run_id_changed", func(f *horai.TaskFence) { f.LastRunID = "other" }, false},
		{"run_started_changed", func(f *horai.TaskFence) { f.RunStartedAt = 101 }, false},
		{"revision_changed", func(f *horai.TaskFence) { f.Revision++ }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := running("replace_" + tc.name)
			require.NoError(t, seed(t.Context(), store, state))

			stored, err := store.GetTask(t.Context(), state.ID)
			require.NoError(t, err)
			fence := horai.FenceOf(stored)
			tc.mutate(&fence)
			ok, err := store.ReplaceTaskIf(t.Context(), reset(stored), fence)
			require.NoError(t, err)
			require.Equal(t, tc.wantHit, ok)

			got, err := store.GetTask(t.Context(), state.ID)
			require.NoError(t, err)
			if tc.wantHit {
				require.Equal(t, horai.TaskStatusActive, got.Status)
				require.Zero(t, got.RunStartedAt)
				require.Equal(t, int32(1), got.Failures)
			} else {
				require.Equal(t, horai.TaskStatusRunning, got.Status)
				require.Equal(t, int64(100), got.RunStartedAt)
			}
		})
	}

	t.Run("zero_fields", func(t *testing.T) {
		t.Parallel()
		state := &horai.TaskState{
			ID: "replace_zero", Status: horai.TaskStatusActive, OneShot: true}
		require.NoError(t, seed(t.Context(), store, state))

		stored, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		next := *stored
		next.Status = horai.TaskStatusCompleted
		ok, err := store.ReplaceTaskIf(t.Context(), &next, horai.FenceOf(stored))
		require.NoError(t, err)
		require.True(t, ok)
	})

	t.Run("concurrent_upsert", func(t *testing.T) {
		t.Parallel()
		state := running("replace_concurrent_upsert")
		require.NoError(t, seed(t.Context(), store, state))
		stored, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		fence := horai.FenceOf(stored)

		// A write that leaves every run-ownership field untouched.
		changed := *stored
		changed.Meta = map[string]string{"keep": "value"}
		require.NoError(t, seed(t.Context(), store, &changed))

		ok, err := store.ReplaceTaskIf(t.Context(), reset(stored), fence)
		require.NoError(t, err)
		require.False(t, ok)
		got, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		require.Equal(t, "value", got.Meta["keep"])
	})

	t.Run("revision_increments", func(t *testing.T) {
		t.Parallel()
		state := &horai.TaskState{
			ID: "replace_revision", Status: horai.TaskStatusActive, Schedule: "@every 1m", NextRunAt: 300, Revision: 99} // caller-supplied revisions are ignored
		rev := func() int64 {
			got, err := store.GetTask(t.Context(), state.ID)
			require.NoError(t, err)
			return got.Revision
		}
		require.NoError(t, seed(t.Context(), store, state))
		require.Equal(t, int64(1), rev())
		require.NoError(t, seed(t.Context(), store, state))
		require.Equal(t, int64(2), rev())

		ok, err := store.ClaimRun(t.Context(), state.ID, horai.RunClaim{NextRunAt: 300, StartedAt: 100, RunID: "owner"})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(3), rev())

		ok, err = store.FinishRun(t.Context(), state.ID, "owner",
			horai.RunResult{StartedAt: 100, EndedAt: 110, NextRunAt: 400, Schedule: "@every 1m", Success: true})
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(4), rev())

		stored, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		ok, err = store.ReplaceTaskIf(t.Context(), stored, horai.FenceOf(stored))
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, int64(5), rev())
	})

	t.Run("dollar_strings_round_trip", func(t *testing.T) {
		t.Parallel()
		state := &horai.TaskState{
			ID: "replace_dollar", Status: horai.TaskStatusActive, Description: "$status", Meta: map[string]string{"k": "$revision"}}
		require.NoError(t, seed(t.Context(), store, state))
		got, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		require.Equal(t, "$status", got.Description)
		require.Equal(t, "$revision", got.Meta["k"])
	})

	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		state := running("replace_missing")
		ok, err := store.ReplaceTaskIf(t.Context(), state, horai.FenceOf(state))
		require.NoError(t, err)
		require.False(t, ok)
	})
}
