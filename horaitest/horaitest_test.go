// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horaitest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/horaitest"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/task"
)

func TestFixtures(t *testing.T) {
	t.Parallel()

	c := horaitest.NewClock(time.Unix(100, 0))
	c.Advance(time.Second)
	require.Equal(t, int64(101), c.Now().Unix())

	m := &horaitest.Metrics{}
	m.TaskDispatched("id", task.PriorityHigh)
	m.TaskFinished("id", task.PriorityHigh, time.Second, nil)
	m.TaskSkipped("id")
	m.DispatchLag("id", time.Second)
	m.StaleRecovered()
	m.TickDone(time.Second)
	m.RunningChanged(1)
	m.RegisteredChanged(2)
	m.StorageError("get")
	events := m.Events()
	require.Len(t, events, 9)
	events[0].ID = "mutated"
	require.Equal(t, "id", m.Events()[0].ID)

	st, err := memory.New()
	require.NoError(t, err)

	fault := errors.New("injected")
	s := &horaitest.FaultStore{Store: st, BeforeCreate: func(context.Context, *horai.TaskState) error { return fault }, BeforeGet: func(context.Context, string) error { return fault }}
	_, err = s.CreateTask(t.Context(), &horai.TaskState{ID: "id"})
	require.ErrorIs(t, err, fault)
	_, err = s.GetTask(t.Context(), "id")
	require.ErrorIs(t, err, fault)

	s.BeforeCreate = func(context.Context, *horai.TaskState) error { return nil }
	s.BeforeGet = func(context.Context, string) error { return nil }
	ok, err := s.CreateTask(t.Context(), &horai.TaskState{ID: "id"})
	require.NoError(t, err)
	require.True(t, ok)
	got, err := s.GetTask(t.Context(), "id")
	require.NoError(t, err)
	require.Equal(t, "id", got.ID)
}
