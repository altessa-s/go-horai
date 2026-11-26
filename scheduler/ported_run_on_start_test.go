// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/task"
)

func TestBindPreservesStoredSchedule(t *testing.T) {
	t.Parallel()
	st := store(t)
	s, restarted := engine(t, st), engine(t, st)
	cfg := task.Config{ID: "periodic", Schedule: "@hourly", Func: func(context.Context) error { return nil }}
	require.ErrorIs(t, restarted.Bind(t.Context(), cfg), horai.ErrTaskNotFound)
	require.NoError(t, s.Register(t.Context(), cfg))
	before, err := st.GetTask(t.Context(), cfg.ID)
	require.NoError(t, err)
	require.Greater(t, before.NextRunAt, time.Now().Unix())

	mismatch := cfg
	mismatch.Schedule = "@daily"
	require.ErrorIs(t, restarted.Bind(t.Context(), mismatch), horai.ErrInvalidArgument)

	cfg.RunOnStart = true
	require.NoError(t, restarted.Bind(t.Context(), cfg))
	require.ErrorIs(t, restarted.Bind(t.Context(), cfg), horai.ErrTaskAlreadyRegistered)
	after, err := st.GetTask(t.Context(), cfg.ID)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
