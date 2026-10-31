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
		case *redis.FTInfoCmd:
			cmd.SetVal(step.value.(redis.FTInfoResult))
		case *redis.FTSearchCmd:
			cmd.SetVal(step.value.(redis.FTSearchResult))
		case *redis.Cmd:
			cmd.SetVal(step.value)
		case *redis.IntCmd:
			cmd.SetVal(step.value.(int64))
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
					index, query, direction := "test:idx:tasks:v2", "*", "ASC"
					switch operation {
					case "due":
						query = "@status:[1 1] @nextRunAt:[-inf 10]"
					case "unfinished":
						query = "(@runStartedAt:[-inf (0] | @runStartedAt:[(0 +inf])"
					case "history":
						index, query, direction = "test:idx:history:v2", "@taskId:{task}", "DESC"
					case "list_history":
						index, query, direction = "test:idx:history:v2", "(@taskId:{task})", "DESC"
					}
					history := direction == "DESC"
					reply := aggregateReply(protocol, history, []string{"gone", "{one}:a", "{two}:b"}, []string{"0", "a", "b"}, []int64{30, 20, 10})
					hook := &sequenceHook{t: t, steps: []replyStep{
						{args: aggregateArgs(index, query, history, ""), value: reply},
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
				{args: aggregateArgs("test:idx:tasks:v2", "*", false, ""), value: aggregateReply(2, false, []string{"key"}, []string{"key"}, nil)},
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

func aggregateArgs(index, query string, history bool, filter string) []any {
	key := `format("%s%s", "test:task:", @id)`
	if index == "test:idx:history:v2" {
		key = `format("%s%s:%s", "test:history:", @taskId, @id)`
	}
	args := []any{"FT.AGGREGATE", index, query, "APPLY", key, "AS", "__key"}
	if filter != "" {
		args = append(args, "FILTER", filter)
	}
	if history {
		args = append(args, "SORTBY", 6, "@startedAt", "DESC", "@id", "DESC", "@__key", "ASC")
	} else {
		args = append(args, "SORTBY", 4, "@id", "ASC", "@__key", "ASC")
	}
	return append(args, "LIMIT", 0, 1000, "DIALECT", 2)
}

func aggregateReply(protocol int, history bool, keys, ids []string, times []int64) any {
	rows := []any{}
	for i, key := range keys {
		row := []any{"__key", key, "id", ids[i]}
		attrs := map[any]any{"__key": key, "id": ids[i]}
		if history {
			row = append(row, "startedAt", fmt.Sprint(times[i]))
			attrs["startedAt"] = fmt.Sprint(times[i])
		}
		if protocol == 2 {
			rows = append(rows, row)
		} else {
			rows = append(rows, map[any]any{"extra_attributes": attrs})
		}
	}
	if protocol == 2 {
		return append([]any{int64(len(keys))}, rows...)
	}
	return map[any]any{"results": rows}
}

func TestSearchCompactedIndexPreservesLookahead(t *testing.T) {
	t.Parallel()
	for _, history := range []bool{false, true} {
		t.Run(fmt.Sprint(history), func(t *testing.T) {
			t.Parallel()
			client := redis.NewClient(&redis.Options{})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			keys, ids, times := []string{}, []string{}, []int64{}
			for i := range 1000 {
				key := fmt.Sprintf("gone:%04d", i)
				keys = append(keys, key)
				ids = append(ids, key)
				times = append(times, 30)
			}
			index, query := "test:idx:tasks:v2", "*"
			filter := `(@id > "gone:0999" || (@id == "gone:0999" && @__key > "gone:0999"))`
			if history {
				index, query = "test:idx:history:v2", "(@taskId:{task})"
				// All deleted entries have the same timestamp; the ID tie breaker must advance too.
				for i := range ids {
					ids[i] = fmt.Sprintf("gone:%04d", 999-i)
				}
				filter = `(@startedAt < 30 || (@startedAt == 30 && (@id < "gone:0000" || (@id == "gone:0000" && @__key > "gone:0999"))))`
			}
			steps := []replyStep{{args: aggregateArgs(index, query, history, ""), value: aggregateReply(2, history, keys, ids, times)}}
			for _, key := range keys {
				steps = append(steps, replyStep{args: []any{"JSON.GET", key, "$"}, err: redis.Nil})
			}
			// The compacted index contains only these two rows, both now at offset zero.
			steps = append(steps, replyStep{args: aggregateArgs(index, query, history, filter), value: aggregateReply(2, history, []string{"x", "y"}, []string{"x", "y"}, []int64{20, 10})},
				replyStep{args: []any{"JSON.GET", "x", "$"}, value: `[{"id":"x","task_id":"task","started_at":20}]`},
				replyStep{args: []any{"JSON.GET", "y", "$"}, value: `[{"id":"y","task_id":"task","started_at":10}]`})
			hook := &sequenceHook{t: t, steps: steps}
			client.AddHook(hook)
			s, err := store.New(client, store.WithKeyPrefix("test"))
			require.NoError(t, err)
			if history {
				values, err := s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 1}, nil)
				require.NoError(t, err)
				require.Len(t, values, 2)
			} else {
				values, err := s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
				require.NoError(t, err)
				require.Len(t, values, 2)
			}
			require.Empty(t, hook.steps)
		})
	}
}

func TestDeleteHistoryFailureLeavesTaskRetryable(t *testing.T) {
	t.Parallel()
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	failure := errors.New("history unavailable")
	search := replyStep{args: aggregateArgs("test:idx:history:v2", "*", false, ""), value: aggregateReply(2, false, []string{"history-key"}, []string{"h"}, nil)}
	body := replyStep{args: []any{"JSON.GET", "history-key", "$"}, value: `[{"id":"h","task_id":"task"}]`}
	hook := &sequenceHook{t: t, steps: []replyStep{
		search, body, {args: []any{"del", "history-key"}, err: failure},
		search, body, {args: []any{"del", "history-key"}, value: int64(1)},
		{args: []any{"del", "test:task:task"}, value: int64(1)},
	}}
	client.AddHook(hook)
	s, err := store.New(client, store.WithKeyPrefix("test"))
	require.NoError(t, err)
	require.ErrorIs(t, s.DeleteTask(t.Context(), "task"), failure)
	require.NoError(t, s.DeleteTask(t.Context(), "task"))
	require.Empty(t, hook.steps)
}

func TestKeysetCursorEscaping(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ id, literal string }{
		{"z\n\té", "\"z\\\n\té\""},
		{`z\"`, `format("%s%s%s", "z", "\\", "\"")`},
	} {
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			client := redis.NewClient(&redis.Options{})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			keys, ids := make([]string, 1000), make([]string, 1000)
			for i := range keys {
				keys[i] = fmt.Sprintf("key:%04d", i)
				ids[i] = fmt.Sprintf("id:%04d", i)
			}
			ids[999] = tc.id
			steps := []replyStep{{args: aggregateArgs("test:idx:tasks:v2", "*", false, ""), value: aggregateReply(2, false, keys, ids, nil)}}
			for _, key := range keys {
				steps = append(steps, replyStep{args: []any{"JSON.GET", key, "$"}, err: redis.Nil})
			}
			filter := fmt.Sprintf(`(@id > %s || (@id == %s && @__key > "key:0999"))`, tc.literal, tc.literal)
			steps = append(steps, replyStep{args: aggregateArgs("test:idx:tasks:v2", "*", false, filter), value: []any{int64(0)}})
			hook := &sequenceHook{t: t, steps: steps}
			client.AddHook(hook)
			s, err := store.New(client, store.WithKeyPrefix("test"))
			require.NoError(t, err)
			_, err = s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
			require.NoError(t, err)
			require.Empty(t, hook.steps)
		})
	}
}
