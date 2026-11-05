// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:goconst,mnd // Benchmark fixtures use fixed identifiers and timestamps.
package storagetest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

// Bench measures storage operations. Every sub-benchmark receives an isolated empty store.
func Bench(b *testing.B, newStore func(testing.TB) horai.Store) {
	b.Helper()
	b.Run("FinishRun", func(b *testing.B) { b.ReportAllocs(); BenchmarkFinishRun(b, newStore(b)) })
	b.Run("ClaimRun", func(b *testing.B) {
		s := newStore(b)
		state := &horai.TaskState{ID: "bench", Status: horai.TaskStatusActive}

		b.ReportAllocs()

		for b.Loop() {
			b.StopTimer()
			require.NoError(b, seed(b.Context(), s, state))
			b.StartTimer()
			ok, err := s.ClaimRun(b.Context(), state.ID, horai.RunClaim{RunID: "owner/run", StartedAt: 100})
			require.NoError(b, err)
			require.True(b, ok)
		}
	})
	b.Run("RenewRun", func(b *testing.B) {
		s := newStore(b)
		require.NoError(b, seed(b.Context(), s, &horai.TaskState{ID: "bench", Status: horai.TaskStatusRunning, LastRunID: "owner/run", RunStartedAt: 100}))
		b.ReportAllocs()

		for b.Loop() {
			ok, err := s.RenewRun(b.Context(), "bench", "owner/run", 200)
			require.NoError(b, err)
			require.True(b, ok)
		}
	})
	b.Run("DueTasks/10k", func(b *testing.B) {
		s := newStore(b)
		for i := range 10000 {
			require.NoError(b, seed(b.Context(), s, &horai.TaskState{ID: fmt.Sprint(i), Status: horai.TaskStatusActive, NextRunAt: 100}))
		}

		b.ReportAllocs()

		for b.Loop() {
			count := 0

			for _, err := range s.DueTasks(b.Context(), 100) {
				require.NoError(b, err)

				count++
			}

			require.Equal(b, 10000, count)
		}
	})
	b.Run("ListTasks/filter", func(b *testing.B) {
		s := newStore(b)
		require.NoError(b, seed(b.Context(), s, &horai.TaskState{ID: "bench", Status: horai.TaskStatusActive}))

		filter := query.Cmp{Field: "id", Op: query.Prefix, Value: "ben"}

		b.ReportAllocs()

		for b.Loop() {
			rows, err := s.ListTasks(b.Context(), horai.TaskPage{Limit: 50}, filter)
			require.NoError(b, err)
			require.Len(b, rows, 1)
		}
	})
	b.Run("AddHistory", func(b *testing.B) {
		s := newStore(b)
		b.ReportAllocs()

		i := 0
		for b.Loop() {
			i++
			require.NoError(b, s.AddHistory(b.Context(), &horai.TaskHistory{ID: fmt.Sprint(i), TaskID: "bench", StartedAt: 100, EndedAt: 101}))
		}
	})
}
