// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/scheduler"

	schedtask "github.com/altessa-s/go-horai/task"
)

// TestRunOnStartForStoredTask pins RunOnStart for a task that already exists
// in storage — a restarted instance registering it again: it is due at once
// unless it started within the grace window.
func TestRunOnStartForStoredTask(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {

		const schedule = "@every 1h"
		tests := []struct {
			name       string
			lastRun    time.Duration // before now; 0 means never ran
			inFlight   bool          // the recent start is a run still in progress
			grace      time.Duration
			runOnStart bool
			wantNow    bool
		}{
			{name: "never ran", runOnStart: true, grace: scheduler.DefaultRunOnStartGrace, wantNow: true},
			{name: "ran long ago", lastRun: 30 * time.Minute, runOnStart: true, grace: scheduler.DefaultRunOnStartGrace, wantNow: true},
			{name: "ran within grace", lastRun: time.Minute, runOnStart: true, grace: scheduler.DefaultRunOnStartGrace},
			{name: "run in flight within grace", lastRun: time.Minute, inFlight: true, runOnStart: true, grace: scheduler.DefaultRunOnStartGrace},
			{name: "minimal grace runs on every start", lastRun: time.Minute, runOnStart: true, grace: time.Nanosecond, wantNow: true},
			{name: "flag off keeps the stored schedule", lastRun: 30 * time.Minute, grace: scheduler.DefaultRunOnStartGrace},
		}
		for _, tc := range tests {
			runCase(t, tc.name, func(t *testing.T) {

				ctx := t.Context()
				storage := mustNewMemory(t, 10)
				now := time.Now()
				stored := now.Add(time.Hour).Unix()

				state := &horai.TaskState{
					ID: "task", Status: horai.TaskStatusActive, Priority: schedtask.PriorityNormal,
					Schedule: schedule, NextRunAt: stored}
				if tc.lastRun > 0 {
					started := now.Add(-tc.lastRun).Unix()
					if tc.inFlight {
						state.RunStartedAt = started
					} else {
						state.LastRunAt = started
					}
				}
				created, err := storage.CreateTask(ctx, state)
				require.NoError(t, err)
				require.True(t, created)

				s := engine(t, storage, scheduler.WithRunOnStartGrace(tc.grace))
				require.NoError(t, s.Register(ctx, schedtask.Config{
					ID: "task", Schedule: schedule, RunOnStart: tc.runOnStart,
					Func: func(context.Context) error { return nil },
				}))

				got, err := storage.GetTask(ctx, "task")
				require.NoError(t, err)
				if tc.wantNow {
					require.LessOrEqual(t, got.NextRunAt, time.Now().Unix(), "due at once")
				} else {
					require.Equal(t, stored, got.NextRunAt, "stored schedule kept")
				}
			})
		}

	})
}
