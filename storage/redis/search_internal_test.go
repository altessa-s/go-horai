// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

type searchHook struct {
	commandHook
	reply any
}

func (h *searchHook) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		h.commands = append(h.commands, cmd.Args())
		cmd.(*redis.Cmd).SetVal(h.reply)
		return nil
	}
}

func TestSearchReplyShapes(t *testing.T) {
	t.Parallel()
	for name, fields := range map[string]map[string]string{
		"json": {"$": `{"id":"h","task_id":"task","started_at":10}`},
		"hash": {"id": "h", "taskId": "task", "startedAt": "10"},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			pairs := []any{}
			attrs := map[any]any{}
			for key, value := range fields {
				pairs = append(pairs, key, value)
				attrs[key] = value
			}
			for shape, raw := range map[string]any{
				"resp2_array": []any{int64(1), "key", pairs},
				"resp2_map":   []any{int64(1), "key", attrs},
				"resp3_map":   map[any]any{"total_results": int64(1), "results": []any{map[any]any{"id": "key", "extra_attributes": attrs}}},
				"resp3_array": map[any]any{"total_results": int64(1), "results": []any{map[any]any{"id": "key", "extra_attributes": pairs}}},
			} {
				t.Run(shape, func(t *testing.T) {
					t.Parallel()
					result, err := parseSearchReply(raw, false)
					require.NoError(t, err)
					require.Equal(t, redis.FTSearchResult{Total: 1, Docs: []redis.Document{{ID: "key", Fields: fields}}}, result)
					if name != "json" {
						return
					}
					for _, cluster := range []bool{false, true} {
						var client redis.UniversalClient
						if cluster {
							client = redis.NewClusterClient(&redis.ClusterOptions{})
						} else {
							client = redis.NewClient(&redis.Options{})
						}
						t.Cleanup(func() { require.NoError(t, client.Close()) })
						hook := &searchHook{reply: raw}
						client.AddHook(hook)
						store, err := New(client)
						require.NoError(t, err)
						tasks, err := store.ListTasks(t.Context(), horai.TaskPage{Limit: 10}, nil)
						require.NoError(t, err)
						require.Len(t, tasks, 1)
						require.Equal(t, "h", tasks[0].ID)
						history, err := store.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 10}, nil)
						require.NoError(t, err)
						require.Len(t, history, 1)
						require.Equal(t, "h", history[0].ID)
						for task, err := range store.DueTasks(t.Context(), 10) {
							require.NoError(t, err)
							require.Equal(t, "h", task.ID)
						}
					}
				})
			}
		})
	}
}

func TestSearchReplyEmptyAndNoContent(t *testing.T) {
	t.Parallel()
	for _, raw := range []any{[]any{int64(0)}, map[any]any{"total_results": int64(0), "results": []any{}}} {
		got, err := parseSearchReply(raw, false)
		require.NoError(t, err)
		require.Zero(t, got.Total)
		require.Empty(t, got.Docs)
	}
	for _, raw := range []any{[]any{int64(2), "a", "b"}, map[any]any{"total_results": int64(2), "results": []any{map[any]any{"id": "a"}, map[any]any{"id": "b"}}}} {
		got, err := parseSearchReply(raw, true)
		require.NoError(t, err)
		require.Equal(t, []redis.Document{{ID: "a"}, {ID: "b"}}, got.Docs)
	}
}

func TestSearchReplyMalformed(t *testing.T) {
	t.Parallel()
	for _, raw := range []any{
		nil, []any{}, []any{"bad"}, []any{int64(1), 7}, []any{int64(1), "key"},
		[]any{int64(1), "key", []any{"$"}}, []any{int64(1), "key", []any{1, "value"}},
		[]any{int64(1), "key", map[any]any{"$": 1}},
		map[any]any{1: 2}, map[any]any{"total_results": "bad"},
		map[any]any{"total_results": int64(1), "results": "bad"},
		map[any]any{"total_results": int64(1), "results": []any{nil}},
		map[any]any{"total_results": int64(1), "results": []any{map[any]any{"id": 1}}},
	} {
		_, err := parseSearchReply(raw, false)
		require.Error(t, err)
	}
}

func TestSearchReplyRESP3Attributes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		item map[any]any
		want map[string]string
	}{
		{"attributes_with_empty_values", map[any]any{"extra_attributes": map[any]any{"$": `{"id":"task"}`}, "values": []any{}}, map[string]string{"$": `{"id":"task"}`}},
		{"attributes_take_precedence", map[any]any{"extra_attributes": map[any]any{"id": "task"}, "values": []any{"id", "other"}}, map[string]string{"id": "task"}},
		{"empty_attributes", map[any]any{"extra_attributes": map[any]any{}, "values": []any{}}, map[string]string{}},
		{"nil_attributes", map[any]any{"extra_attributes": nil, "values": []any{}}, map[string]string{}},
		{"missing_attributes", map[any]any{"values": []any{}}, map[string]string{}},
		{"missing_content", map[any]any{}, nil},
		{"nil_content", map[any]any{"extra_attributes": nil, "values": nil}, nil},
		{"values_pairs", map[any]any{"values": []any{"id", "task"}}, map[string]string{"id": "task"}},
		{"values_map", map[any]any{"extra_attributes": nil, "values": map[any]any{"id": "task"}}, map[string]string{"id": "task"}},
		{"empty_attributes_with_values", map[any]any{"extra_attributes": map[any]any{}, "values": []any{"id", "task"}}, map[string]string{"id": "task"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.item["id"] = "key"
			raw := map[any]any{
				"total_results": int64(1),
				"results":       []any{tc.item},
				"format":        "STRING",
			}
			got, err := parseSearchReply(raw, false)
			require.NoError(t, err)
			require.Equal(t, redis.FTSearchResult{Total: 1, Docs: []redis.Document{{ID: "key", Fields: tc.want}}}, got)
			got, err = parseSearchReply(raw, true)
			require.NoError(t, err)
			require.Equal(t, redis.FTSearchResult{Total: 1, Docs: []redis.Document{{ID: "key"}}}, got)
		})
	}
}
