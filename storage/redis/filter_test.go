// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/query"

	rdb "github.com/redis/go-redis/v9"
)

func TestFilterPlan(t *testing.T) {
	e := query.And{query.Cmp{Field: "status", Op: query.Eq, Value: int64(2)}, query.Cmp{Field: "id", Op: query.Contains, Value: "a*"}}
	p, err := newFilterPlan(e, query.TaskFields())
	require.NoError(t, err)
	require.Equal(t, "@status:[2 2]", p.query)
	require.True(t, p.match(query.Record{"status": int64(2), "id": "a*"}))
	require.False(t, p.match(query.Record{"status": int64(2), "id": "abc"}))
	require.Empty(t, pushdown(query.Cmp{Field: "startedAt", Op: query.Eq, Value: int64(1 << 54)}))
	for _, op := range []query.Op{query.Ne, query.In, query.Contains, query.Prefix} {
		require.Empty(t, pushdown(query.Cmp{Field: "status", Op: op, Value: int64(1)}))
	}
	_, err = newFilterPlan(query.Cmp{Field: "secret", Op: query.Eq, Value: ""}, query.TaskFields())
	require.Error(t, err)
	var client *rdb.Client
	_, err = New(client)
	require.Error(t, err)
}
func FuzzFilter(f *testing.F) {
	f.Add("a:b", int64(0))
	f.Fuzz(func(t *testing.T, id string, n int64) {
		e := query.Cmp{Field: "failures", Op: query.Eq, Value: n}
		p, err := newFilterPlan(e, query.TaskFields())
		require.NoError(t, err)
		require.True(t, p.match(query.Record{"failures": n}))
		_ = escapeTag(id)
	})
}
