// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"
)

// legacyClaim claims task like a release without run leases: it writes only
// the status and the run stamps, leaving any stored lease fields untouched.
func legacyClaim(t *testing.T, mem *memory.Store, runID string, startedAt int64) {
	t.Helper()
	st, err := mem.GetTask(t.Context(), "task")
	require.NoError(t, err)
	st.Status = horai.TaskStatusRunning
	st.RunStartedAt = startedAt
	st.LastRunID = runID
	st.UpdatedAt = startedAt
	require.NoError(t, seedState(mem, t.Context(), st))
}

// TestLegacyClaimAfterLeasedRunIsNotRecovered finishes a leased run and lets a
// release without leases claim the next occurrence: the finished run's lease
// must not make the live legacy run look expired.
func TestLegacyClaimAfterLeasedRunIsNotRecovered(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {

		mem := mustNewMemory(t, 10)
		now := time.Now().Unix()
		require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
			ID: "task", Status: horai.TaskStatusActive, Schedule: "@every 1h", NextRunAt: now - 30}))
		claimed, err := mem.ClaimRun(t.Context(), "task",
			horai.RunClaim{NextRunAt: now - 30, StartedAt: now - 30, RunID: "fixed/run", LeaseUntil: now - 10})
		require.NoError(t, err)
		require.True(t, claimed)
		finished, err := mem.FinishRun(t.Context(), "task", "fixed/run",
			horai.RunResult{StartedAt: now - 30, EndedAt: now - 20, NextRunAt: now + 3600, Schedule: "@every 1h", Success: true})
		require.NoError(t, err)
		require.True(t, finished)

		legacyClaim(t, mem, "legacy/run", now)
		startForRecovery(t, mem)

		requireStillRunning(t, mem, "legacy/run")

	})
}

// TestInheritedLeaseIsNotTrusted covers a legacy claim made while an earlier
// run's lease was still valid; that lease has since expired, but the legacy run
// is within the recovering instance's own (30m) fallback window.
func TestInheritedLeaseIsNotTrusted(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {

		for _, tc := range []struct{ name, leaseID string }{
			// Left by a lease-aware build that did not clear leases on finish.
			{"bound_to_earlier_run", "earlier/run"},
			// Left by a lease-aware build that did not bind leases.
			{"unbound", ""},
		} {
			runCase(t, tc.name, func(t *testing.T) {

				mem := mustNewMemory(t, 10)
				now := time.Now().Unix()
				require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
					ID: "task", Status: horai.TaskStatusRunning, Schedule: "@every 1h", NextRunAt: now - 60,
					LastRunID:     "legacy/run",
					RunStartedAt:  now - 60,
					RunLeaseUntil: now - 5,
					RunLeaseID:    tc.leaseID,
					UpdatedAt:     now - 60,
				}))

				startForRecovery(t, mem)

				requireStillRunning(t, mem, "legacy/run")
			})
		}

	})
}

// TestUnboundLeaseUsesLaterDeadline pins the conservative rule for a lease not
// bound to any run: the run is recovered only once both the lease and the
// local fallback have expired.
func TestUnboundLeaseUsesLaterDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {

		for _, tc := range []struct {
			name       string
			leaseUntil int64
			recovered  bool
		}{
			{"both_expired", -10, true},
			{"lease_valid", 3600, false},
		} {
			runCase(t, tc.name, func(t *testing.T) {

				mem := mustNewMemory(t, 10)
				now := time.Now().Unix()
				require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
					ID: "task", Status: horai.TaskStatusRunning, Schedule: "@every 1h", NextRunAt: now - 3600,
					LastRunID:     "node-b/run",
					RunStartedAt:  now - 3600,
					RunLeaseUntil: now + tc.leaseUntil,
					UpdatedAt:     now - 3600,
				}))

				startForRecovery(t, mem, scheduler.WithStaleTaskTimeout(time.Second))

				if tc.recovered {
					requireRecovered(t, mem, horai.TaskStatusActive)
					return
				}
				requireStillRunning(t, mem, "node-b/run")
			})
		}

	})
}

// TestRenewalBindsLease renews a run whose stored lease is bound to an earlier
// run: the renewed lease must protect it beyond the observer's local fallback.
func TestRenewalBindsLease(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {

		mem := mustNewMemory(t, 10)
		now := time.Now().Unix()
		require.NoError(t, seedState(mem, t.Context(), &horai.TaskState{
			ID: "task", Status: horai.TaskStatusRunning, Schedule: "@every 1h", NextRunAt: now - 3600,
			LastRunID:     "node-b/run",
			RunStartedAt:  now - 3600,
			RunLeaseUntil: now - 10,
			RunLeaseID:    "node-a/run",
			UpdatedAt:     now - 3600,
		}))
		renewed, err := mem.RenewRun(t.Context(), "task", "node-b/run", now+3600)
		require.NoError(t, err)
		require.True(t, renewed)

		startForRecovery(t, mem, scheduler.WithStaleTaskTimeout(time.Second))

		requireStillRunning(t, mem, "node-b/run")

	})
}
