// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:mnd // Bounded test data and explicit contract fixture values.
package storagetest

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

// ConcurrentClaim requires exactly one winner among simultaneous claims of one occurrence.
func ConcurrentClaim(t *testing.T, store horai.Store) {
	t.Helper()
	created, err := store.CreateTask(t.Context(), &horai.TaskState{ID: "contended", Status: horai.TaskStatusActive, NextRunAt: 10})
	require.NoError(t, err)
	require.True(t, created)
	const contenders = 16
	start := make(chan struct{})
	type result struct {
		won bool
		err error
	}
	results := make(chan result, contenders)
	var wg sync.WaitGroup
	for i := range contenders {
		wg.Go(func() {
			<-start
			won, claimErr := store.ClaimRun(t.Context(), "contended", horai.RunClaim{NextRunAt: 10, StartedAt: 11, RunID: fmt.Sprintf("node/%d", i), LeaseUntil: 20})
			results <- result{won, claimErr}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	for r := range results {
		require.NoError(t, r.err)
		if r.won {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	state, err := store.GetTask(t.Context(), "contended")
	require.NoError(t, err)
	require.Equal(t, int64(11), state.RunStartedAt)
}
