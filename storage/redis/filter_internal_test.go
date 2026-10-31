// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/internal/testhelpers"
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
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		fields := append(query.TaskFields(), query.HistoryFields()...)
		e := testhelpers.QueryTree(data, fields)
		plan, err := newFilterPlan(e, fields)
		require.NoError(t, err)
		require.NotContains(t, plan.query, "value:")
		require.Equal(t, strings.Count(plan.query, "["), strings.Count(plan.query, "]"))
		if plan.query != "" {
			require.Regexp(t, `^(@(status|priority|failures|startedAt|endedAt|durationMs):\[[()0-9+inf -]+\] ?)+$`, plan.query)
		}
		for _, r := range []query.Record{nil, {"status": int64(2), "id": "value:"}} {
			require.Equal(t, query.Match(e, r), plan.match(r))
		}
		escaped := escapeTag(string(data))
		require.NotRegexp(t, regexp.MustCompile(`(^|[^\\])\{`), escaped)
	})
}
