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

// FinishRun verifies ownership, idempotent completion and preservation of
// configuration changes. The supplied store must be isolated per invocation.
//
//nolint:mnd // Fixed timestamps and counts describe the storage contract.
func FinishRun(t *testing.T, store horai.Store) {
	t.Helper()
	result := horai.RunResult{StartedAt: 100, EndedAt: 110, NextRunAt: 200, Schedule: "@every 1m", Success: true}
	for _, tc := range []struct {
		name       string
		status     horai.TaskStatus
		oneShot    bool
		schedule   string
		wantStatus horai.TaskStatus
		wantNext   int64
	}{
		{"normal", horai.TaskStatusRunning, false, "@every 1m", horai.TaskStatusActive, 200},
		{"paused", horai.TaskStatusPaused, false, "@every 1m", horai.TaskStatusPaused, 200},
		{"disabled", horai.TaskStatusDisabled, false, "@every 1m", horai.TaskStatusDisabled, 200},
		{"changed_schedule", horai.TaskStatusRunning, false, "@every 2m", horai.TaskStatusActive, 300},
		{"one_shot", horai.TaskStatusRunning, true, "", horai.TaskStatusCompleted, 0},
		// A one-shot run that finished is terminal whatever management did
		// meanwhile: paused, disabled, or paused and resumed back to active.
		{"one_shot_paused", horai.TaskStatusPaused, true, "", horai.TaskStatusCompleted, 0},
		{"one_shot_disabled", horai.TaskStatusDisabled, true, "", horai.TaskStatusCompleted, 0},
		{"one_shot_resumed", horai.TaskStatusActive, true, "", horai.TaskStatusCompleted, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			state := &horai.TaskState{
				ID: tc.name, Status: tc.status, Schedule: tc.schedule, NextRunAt: 300, Failures: 2, OneShot: tc.oneShot, LastRunID: "owner", RunStartedAt: 100, RunLeaseUntil: 400, RunLeaseID: "owner", Meta: map[string]string{"keep": "value"}}
			require.NoError(t, seed(t.Context(), store, state))
			ok, err := store.FinishRun(t.Context(), tc.name, "stale", result)
			require.NoError(t, err)
			require.False(t, ok)
			ok, err = store.FinishRun(t.Context(), tc.name, "owner", result)
			require.NoError(t, err)
			require.True(t, ok)
			got, err := store.GetTask(t.Context(), tc.name)
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, got.Status)
			require.Equal(t, tc.wantNext, got.NextRunAt)
			require.Equal(t, tc.schedule, got.Schedule)
			require.Equal(t, state.Meta, got.Meta)
			require.Zero(t, got.RunStartedAt)
			require.Zero(t, got.RunLeaseUntil, "a finished run's lease must not outlive it")
			require.Empty(t, got.RunLeaseID)
			require.Zero(t, got.Failures)
			require.Equal(t, int64(100), got.LastRunAt)
			ok, err = store.FinishRun(t.Context(), tc.name, "owner", result)
			require.NoError(t, err)
			require.False(t, ok, "a run can finish only once")
		})
	}
	// A one-shot task re-registered for another occurrence while the run
	// executed keeps that occurrence: the run did not execute it.
	for _, tc := range []struct {
		name       string
		status     horai.TaskStatus
		wantStatus horai.TaskStatus
	}{
		{"one_shot_rescheduled", horai.TaskStatusRunning, horai.TaskStatusActive},
		{"one_shot_rescheduled_paused", horai.TaskStatusPaused, horai.TaskStatusPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NoError(t, seed(t.Context(), store, &horai.TaskState{
				ID: tc.name, Status: tc.status, NextRunAt: 500, OneShot: true,
				LastRunID: "owner", RunStartedAt: 100, RunAt: 500,
			}))
			executed := horai.RunResult{StartedAt: 100, EndedAt: 110, RunAt: 90, Success: true}
			ok, err := store.FinishRun(t.Context(), tc.name, "owner", executed)
			require.NoError(t, err)
			require.True(t, ok)
			got, err := store.GetTask(t.Context(), tc.name)
			require.NoError(t, err)
			require.Equal(t, tc.wantStatus, got.Status)
			require.Equal(t, int64(500), got.NextRunAt, "the newly registered occurrence must be kept")
			require.Equal(t, int64(500), got.RunAt)
			require.Zero(t, got.RunStartedAt)
			require.Equal(t, int64(100), got.LastRunAt)
		})
	}
	t.Run("failed_run", func(t *testing.T) {
		t.Parallel()
		state := &horai.TaskState{
			ID: "failed", Status: horai.TaskStatusRunning, Failures: 2,
			LastRunID: "owner", RunStartedAt: 100,
		}
		require.NoError(t, seed(t.Context(), store, state))
		failure := result
		failure.Success = false
		ok, err := store.FinishRun(t.Context(), state.ID, "owner", failure)
		require.NoError(t, err)
		require.True(t, ok)
		got, err := store.GetTask(t.Context(), state.ID)
		require.NoError(t, err)
		require.Equal(t, int32(3), got.Failures)
		ok, err = store.FinishRun(t.Context(), state.ID, "owner", failure)
		require.NoError(t, err)
		require.False(t, ok)
	})
	t.Run("missing", func(t *testing.T) {
		t.Parallel()
		ok, err := store.FinishRun(t.Context(), "missing", "owner", result)
		require.NoError(t, err)
		require.False(t, ok)
	})
}

// BenchmarkFinishRun measures the conditional write with a fresh claim per run.
//
//nolint:mnd // Fixed timestamps keep the benchmark independent of the wall clock.
func BenchmarkFinishRun(b *testing.B, store horai.Store) {
	b.Helper()
	state := &horai.TaskState{
		ID: "bench", Status: horai.TaskStatusRunning,
		LastRunID: "owner", RunStartedAt: 100,
	}
	result := horai.RunResult{StartedAt: 100, EndedAt: 110, Success: true}
	for b.Loop() {
		b.StopTimer()
		require.NoError(b, seed(b.Context(), store, state))
		b.StartTimer()
		ok, err := store.FinishRun(b.Context(), state.ID, "owner", result)
		require.NoError(b, err)
		require.True(b, ok)
	}
}
