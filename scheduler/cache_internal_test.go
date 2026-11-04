// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/task"
)

func TestScheduleCacheFollowsRegistration(t *testing.T) {
	t.Parallel()

	st, err := memory.New()
	require.NoError(t, err)
	s, err := New(st)
	require.NoError(t, err)

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for _, spec := range []string{"@every 1m", "@every 2m"} {
		require.NoError(t, s.Register(t.Context(), task.Config{ID: "id", Schedule: spec, Func: func(context.Context) error { return nil }}))
		require.Len(t, s.tasks, 1)
	}

	require.Equal(t, now.Add(2*time.Minute), s.calculateNextRun(t.Context(), now, &horai.TaskState{ID: "id", Schedule: "@every 2m"}))
	// A different persisted schedule must never reuse the local registration's parse.
	require.Equal(t, now.Add(3*time.Minute), s.calculateNextRun(t.Context(), now, &horai.TaskState{ID: "id", Schedule: "@every 3m"}))
	require.NoError(t, s.Unregister(t.Context(), "id"))
	require.Empty(t, s.tasks)
}
