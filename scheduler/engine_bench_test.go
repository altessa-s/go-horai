// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/task"
)

func benchEngine(b *testing.B, n, duePercent int, options ...Option) (*Scheduler, *memory.Store, []string) {
	b.Helper()
	st, err := memory.New()
	require.NoError(b, err)
	s, err := New(st, options...)
	require.NoError(b, err)
	ctx, cancel := context.WithCancel(b.Context())
	s.stopCtx = ctx
	b.Cleanup(func() { cancel(); s.wg.Wait() })
	ids := make([]string, 0, n*duePercent/100)
	for i := range n {
		id := fmt.Sprintf("task-%06d", i)
		due := i < n*duePercent/100
		require.NoError(b, s.Register(ctx, task.Config{ID: id, Schedule: "@hourly", RunOnStart: due, Func: func(context.Context) error { return nil }}))
		if due {
			ids = append(ids, id)
		}
	}
	return s, st, ids
}
func resetDue(b *testing.B, st *memory.Store, ids []string) {
	b.Helper()
	for _, id := range ids {
		v, err := st.GetTask(b.Context(), id)
		require.NoError(b, err)
		f := horai.FenceOf(v)
		v.Status = horai.TaskStatusActive
		v.NextRunAt = time.Now().Unix()
		ok, err := st.ReplaceTaskIf(b.Context(), v, f)
		require.NoError(b, err)
		require.True(b, ok)
	}
}
func BenchmarkTick(b *testing.B) {
	for _, n := range []int{100, 10000} {
		for _, pct := range []int{1, 100} {
			b.Run(fmt.Sprintf("tasks=%d/due=%d%%", n, pct), func(b *testing.B) {
				s, st, ids := benchEngine(b, n, pct)
				b.ReportAllocs()
				for b.Loop() {
					b.StopTimer()
					resetDue(b, st, ids)
					b.StartTimer()
					s.tick()
					s.wg.Wait()
				}
			})
		}
	}
}
func BenchmarkDispatch(b *testing.B) {
	for _, mode := range []string{"static", "dynamic", "unlimited"} {
		b.Run(mode, func(b *testing.B) {
			var opts []Option
			switch mode {
			case "static":
				opts = []Option{WithMaxConcurrentTasks(4), WithReservedHighPrioritySlots(0)}
			case "dynamic":
				opts = []Option{WithConcurrencyLimit(func() int { return 4 }), WithReservedHighPrioritySlots(0)}
			}
			s, st, ids := benchEngine(b, 100, 100, opts...)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				resetDue(b, st, ids)
				b.StartTimer()
				s.tick()
				s.wg.Wait()
			}
		})
	}
}
func BenchmarkRunLifecycle(b *testing.B) {
	s, st, ids := benchEngine(b, 1, 100)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		resetDue(b, st, ids)
		b.StartTimer()
		s.tick()
		s.wg.Wait()
	}
}
func BenchmarkListTasks(b *testing.B) {
	s, _, _ := benchEngine(b, 10000, 0)
	b.ReportAllocs()
	for b.Loop() {
		_, err := s.ListTasks(b.Context(), TaskListRequest{Limit: 100})
		require.NoError(b, err)
	}
}
