// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestSearchReplyIDs(t *testing.T) {
	t.Parallel()

	for _, raw := range []any{
		[]any{int64(2), "a", "b"},
		map[any]any{"total_results": int64(2), "results": []any{map[any]any{"id": "a", "extra_attributes": nil, "values": []any{}}, map[string]any{"id": "b"}}},
	} {
		got, err := parseSearchReply(raw)
		require.NoError(t, err)
		require.Equal(t, redis.FTSearchResult{Total: 2, Docs: []redis.Document{{ID: "a"}, {ID: "b"}}}, got)
	}

	for _, raw := range []any{[]any{int64(0)}, map[string]any{"total_results": int64(0), "results": []any{}}} {
		got, err := parseSearchReply(raw)
		require.NoError(t, err)
		require.Empty(t, got.Docs)
	}
}

func TestSearchReplyMalformed(t *testing.T) {
	t.Parallel()

	for _, raw := range []any{
		nil, []any{}, []any{"bad"}, []any{int64(-1)}, []any{int64(1), 7},
		map[any]any{1: 2}, map[any]any{"total_results": "bad"},
		map[any]any{"total_results": int64(1), "results": "bad"},
		map[any]any{"total_results": int64(1), "results": []any{nil}},
		map[any]any{"total_results": int64(1), "results": []any{map[any]any{"id": 1}}},
	} {
		_, err := parseSearchReply(raw)
		require.Error(t, err)
	}
}
