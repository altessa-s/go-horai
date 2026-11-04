// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,lll,mnd // Contract fixtures keep their inputs and expected values explicit.
package storagetest

import (
	"testing"

	"github.com/altessa-s/go-horai"
)

// contracts lists the storage contracts; [Run] executes each.
var contracts = []struct {
	name  string
	check func(*testing.T, horai.Store)
}{
	{"UnfinishedRuns", UnfinishedRuns},
	{"ConcurrentClaim", ConcurrentClaim},
	{"LargeValues", LargeValues},
	{"ListTasksFilterParity", ListTasksFilterParity},
	{"FinishRun", FinishRun},
	{"ExhaustedFinish", ExhaustedFinish},
	{"ReplaceTaskIf", ReplaceTaskIf},
	{"CreateTask", CreateTask},
	{"RenewRun", RenewRun},
	{"ClaimRunRequiresFinishedRun", ClaimRunRequiresFinishedRun},
	{"ClaimRunFencesOccurrence", ClaimRunFencesOccurrence},
	{"RunIDRoundTrip", RunIDRoundTrip},
	{"ClaimRunRejectsInvalidClaim", ClaimRunRejectsInvalidClaim},
	{"OwnedRunAnyNonZeroStart", OwnedRunAnyNonZeroStart},
	{"ClaimRun", ClaimRun},
	{"DueTasks", DueTasks},
	{"DueTasksZeroNextRun", DueTasksZeroNextRun},
	{"Identity", Identity},
	{"TaskPage", TaskPage},
	{"ZeroValueFilters", ZeroValueFilters},
	{"StringAnchors", StringAnchors},
	{"History", History},
	{"UnknownFilterField", UnknownFilterField},
}

// Run executes every storage contract as a parallel subtest named after it, each
// on a fresh store from newStore, which must return an empty store isolated from
// every other store it returns. A backend wires the whole suite with one call,
// so a new contract cannot be left out of a single backend.
func Run(t *testing.T, newStore func(testing.TB) horai.Store) {
	t.Helper()

	for _, c := range contracts {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.check(t, newStore(t))
		})
	}

	RunHistory(t, func(tb testing.TB) horai.HistoryStore { tb.Helper(); return newStore(tb) })
}
