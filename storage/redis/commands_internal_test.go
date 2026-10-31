// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"

	rdb "github.com/redis/go-redis/v9"
)

type commandHook struct {
	commands [][]any
	err      error
}

func (h *commandHook) DialHook(next rdb.DialHook) rdb.DialHook {
	return func(ctx context.Context, n, a string) (net.Conn, error) { return next(ctx, n, a) }
}
func (h *commandHook) ProcessHook(rdb.ProcessHook) rdb.ProcessHook {
	return func(_ context.Context, c rdb.Cmder) error {
		h.commands = append(h.commands, c.Args())
		if h.err != nil {
			c.SetErr(h.err)
			return h.err
		}
		switch c := c.(type) {
		case *rdb.Cmd:
			c.SetVal(int64(1))
		case *rdb.JSONCmd:
			c.SetVal(`[{"id":"task","revision":3}]`)
		case *rdb.FTSearchCmd:
			c.SetVal(rdb.FTSearchResult{})
		case *rdb.IntCmd:
			c.SetVal(1)
		}
		return nil
	}
}
func (h *commandHook) ProcessPipelineHook(rdb.ProcessPipelineHook) rdb.ProcessPipelineHook {
	return func(ctx context.Context, cmds []rdb.Cmder) error {
		for _, cmd := range cmds {
			if err := h.ProcessHook(nil)(ctx, cmd); err != nil {
				return err
			}
		}
		return nil
	}
}
func TestCommandsAndErrors(t *testing.T) {
	t.Parallel()
	client := rdb.NewClient(&rdb.Options{})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	hook := &commandHook{}
	client.AddHook(hook)
	s, err := New(client, WithKeyPrefix("prefix:"))
	require.NoError(t, err)
	require.Equal(t, "prefix:task:task", s.taskKey("task"))
	require.Equal(t, "prefix:history:task:h", s.historyKey("task", "h"))
	require.Equal(t, "prefix:idx:tasks:v2", s.taskIndexName())
	require.Equal(t, "prefix:idx:history", s.historyIndexName())
	require.Equal(t, "a:b", (&Store{}).key("a", "b"))
	state, err := s.GetTask(t.Context(), "task")
	require.NoError(t, err)
	require.Equal(t, int64(3), state.Revision)
	for _, call := range []func() (bool, error){
		func() (bool, error) { return s.CreateTask(t.Context(), &horai.TaskState{ID: "task"}) },
		func() (bool, error) {
			return s.ReplaceTaskIf(t.Context(), &horai.TaskState{ID: "task"}, horai.TaskFence{})
		},
		func() (bool, error) {
			return s.ClaimRun(t.Context(), "task", horai.RunClaim{RunID: "node/run", StartedAt: 10, LeaseUntil: 20})
		},
		func() (bool, error) {
			return s.FinishRun(t.Context(), "task", "node/run", horai.RunResult{Success: true})
		},
		func() (bool, error) { return s.RenewRun(t.Context(), "task", "node/run", 30) },
	} {
		ok, err := call()
		require.NoError(t, err)
		require.True(t, ok)
		last := hook.commands[len(hook.commands)-1]
		require.Equal(t, "prefix:task:task", last[3])
	}
	failure := errors.New("redis unavailable")
	hook.err = failure
	_, err = s.GetTask(t.Context(), "task")
	require.ErrorIs(t, err, failure)
	_, err = s.CreateTask(t.Context(), &horai.TaskState{ID: "task"})
	require.ErrorIs(t, err, failure)
	_, err = s.ReplaceTaskIf(t.Context(), &horai.TaskState{ID: "task"}, horai.TaskFence{})
	require.ErrorIs(t, err, failure)
	_, err = s.ClaimRun(t.Context(), "task", horai.RunClaim{RunID: "node/run", StartedAt: 10})
	require.ErrorIs(t, err, failure)
	_, err = s.FinishRun(t.Context(), "task", "node/run", horai.RunResult{})
	require.ErrorIs(t, err, failure)
	_, err = s.RenewRun(t.Context(), "task", "node/run", 20)
	require.ErrorIs(t, err, failure)
	require.ErrorIs(t, s.AddHistory(t.Context(), &horai.TaskHistory{ID: "h", TaskID: "task"}), failure)
	for _, err := range s.Tasks(t.Context()) {
		require.ErrorIs(t, err, failure)
	}
	for _, err := range s.DueTasks(t.Context(), 10) {
		require.ErrorIs(t, err, failure)
	}
	for _, err := range s.UnfinishedRuns(t.Context()) {
		require.ErrorIs(t, err, failure)
	}
	hook.err = rdb.Nil
	state, err = s.GetTask(t.Context(), "missing")
	require.NoError(t, err)
	require.Nil(t, state)
}
func TestJSONMappingErrors(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"[]", "null", "invalid"} {
		_, err := unmarshalJSONResult[taskData](raw)
		require.Error(t, err)
	}
	for _, fields := range []map[string]string{nil, {"$": "invalid"}} {
		_, err := parseDocJSON[taskData](rdb.Document{Fields: fields})
		require.Error(t, err)
	}
	doc, err := parseDocJSON[taskData](rdb.Document{Fields: map[string]string{"$": `{"id":"task"}`}})
	require.NoError(t, err)
	require.Equal(t, "task", doc.ID)
}
