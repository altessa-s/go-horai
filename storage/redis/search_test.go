// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"

	store "github.com/altessa-s/go-horai/storage/redis"
)

type replyStep struct {
	args  []any
	value any
	err   error
}
type sequenceHook struct {
	t     *testing.T
	steps []replyStep
}

func (h *sequenceHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		require.NotEmpty(h.t, h.steps)
		step := h.steps[0]
		h.steps = h.steps[1:]
		require.Equal(h.t, step.args, cmd.Args())
		cmd.SetErr(step.err)
		if step.err != nil {
			return step.err
		}
		switch cmd := cmd.(type) {
		case *redis.Cmd:
			cmd.SetVal(step.value)
		case *redis.JSONCmd:
			cmd.SetVal(step.value.(string))
		default:
			h.t.Fatalf("unexpected command %T", cmd)
		}
		return nil
	}
}
func (h *sequenceHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *sequenceHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestSearchLoadsBodies(t *testing.T) {
	t.Parallel()
	for _, cluster := range []bool{false, true} {
		for _, protocol := range []int{2, 3} {
			for _, operation := range []string{"tasks", "due", "unfinished", "list_tasks", "history", "list_history"} {
				t.Run(fmt.Sprintf("cluster=%t/resp%d/%s", cluster, protocol, operation), func(t *testing.T) {
					t.Parallel()
					var client redis.UniversalClient
					if cluster {
						client = redis.NewClusterClient(&redis.ClusterOptions{Protocol: protocol})
					} else {
						client = redis.NewClient(&redis.Options{Protocol: protocol})
					}
					t.Cleanup(func() { require.NoError(t, client.Close()) })
					s, err := store.New(client, store.WithKeyPrefix("test"))
					require.NoError(t, err)
					index, query, sort, direction := "test:idx:tasks:v2", "*", "id", "ASC"
					switch operation {
					case "due":
						query = "@status:[1 1] @nextRunAt:[-inf 10]"
					case "unfinished":
						query = "(@runStartedAt:[-inf (0] | @runStartedAt:[(0 +inf])"
					case "history":
						index, query, sort, direction = "test:idx:history", "@taskId:{task}", "startedAt", "DESC"
					case "list_history":
						index, query, sort, direction = "test:idx:history", "(@taskId:{task})", "startedAt", "DESC"
					}
					var reply any = []any{int64(3), "gone", "{one}:a", "{two}:b"}
					if protocol == 3 {
						reply = map[any]any{"total_results": int64(3), "results": []any{map[any]any{"id": "gone", "extra_attributes": nil, "values": []any{}}, map[any]any{"id": "{one}:a"}, map[any]any{"id": "{two}:b"}}}
					}
					hook := &sequenceHook{t: t, steps: []replyStep{
						{args: []any{"FT.SEARCH", index, query, "NOCONTENT", "SORTBY", sort, direction, "LIMIT", 0, 1000, "DIALECT", 2}, value: reply},
						{args: []any{"JSON.GET", "gone", "$"}, err: redis.Nil},
						{args: []any{"JSON.GET", "{one}:a", "$"}, value: `[{"id":"a","task_id":"task","started_at":20}]`},
						{args: []any{"JSON.GET", "{two}:b", "$"}, value: `[{"id":"b","task_id":"task","started_at":10}]`},
					}}
					client.AddHook(hook)
					var ids []string
					switch operation {
					case "tasks":
						for v, e := range s.Tasks(t.Context()) {
							require.NoError(t, e)
							ids = append(ids, v.ID)
						}
					case "due":
						for v, e := range s.DueTasks(t.Context(), 10) {
							require.NoError(t, e)
							ids = append(ids, v.ID)
						}
					case "unfinished":
						for v, e := range s.UnfinishedRuns(t.Context()) {
							require.NoError(t, e)
							ids = append(ids, v.ID)
						}
					case "list_tasks":
						values, e := s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
						require.NoError(t, e)
						for _, v := range values {
							ids = append(ids, v.ID)
						}
					case "history":
						for v, e := range s.History(t.Context(), "task") {
							require.NoError(t, e)
							ids = append(ids, v.ID)
						}
					case "list_history":
						values, e := s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 1}, nil)
						require.NoError(t, e)
						for _, v := range values {
							ids = append(ids, v.ID)
						}
					}
					require.Equal(t, []string{"a", "b"}, ids)
					require.Empty(t, hook.steps)
				})
			}
		}
	}
}

func TestSearchBodyErrors(t *testing.T) {
	t.Parallel()
	failure := errors.New("load failed")
	for _, tc := range []struct {
		name, body string
		err        error
	}{{name: "load", err: failure}, {name: "malformed", body: "invalid"}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := redis.NewClient(&redis.Options{})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			hook := &sequenceHook{t: t, steps: []replyStep{
				{args: []any{"FT.SEARCH", "test:idx:tasks:v2", "*", "NOCONTENT", "SORTBY", "id", "ASC", "LIMIT", 0, 1000, "DIALECT", 2}, value: []any{int64(1), "key"}},
				{args: []any{"JSON.GET", "key", "$"}, value: tc.body, err: tc.err},
			}}
			client.AddHook(hook)
			s, err := store.New(client, store.WithKeyPrefix("test"))
			require.NoError(t, err)
			_, err = s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
			require.Error(t, err)
			if tc.err != nil {
				require.ErrorIs(t, err, failure)
			}
			require.Empty(t, hook.steps)
		})
	}
}

func TestSearchDeletedPagePreservesLookahead(t *testing.T) {
	t.Parallel()
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	first := []any{int64(1002)}
	for i := range 1000 {
		first = append(first, fmt.Sprintf("gone:%d", i))
	}
	search := func(offset int, reply any) replyStep {
		return replyStep{args: []any{"FT.SEARCH", "test:idx:tasks:v2", "*", "NOCONTENT", "SORTBY", "id", "ASC", "LIMIT", offset, 1000, "DIALECT", 2}, value: reply}
	}
	steps := []replyStep{search(0, first)}
	for i := range 1000 {
		steps = append(steps, replyStep{args: []any{"JSON.GET", fmt.Sprintf("gone:%d", i), "$"}, err: redis.Nil})
	}
	steps = append(steps, search(1000, []any{int64(1002), "a", "b"}),
		replyStep{args: []any{"JSON.GET", "a", "$"}, value: `[{"id":"a"}]`},
		replyStep{args: []any{"JSON.GET", "b", "$"}, value: `[{"id":"b"}]`})
	hook := &sequenceHook{t: t, steps: steps}
	client.AddHook(hook)
	s, err := store.New(client, store.WithKeyPrefix("test"))
	require.NoError(t, err)
	values, err := s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
	require.NoError(t, err)
	require.Len(t, values, 2)
	require.Equal(t, "a", values[0].ID)
	require.Equal(t, "b", values[1].ID)
	require.Empty(t, hook.steps)
}
