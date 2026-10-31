// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/query"
)

func TestTranslateBoundValues(t *testing.T) {
	for _, e := range []query.Expr{nil, query.And{}, query.Or{}, query.Not{Expr: query.Cmp{Field: "id", Op: query.Eq, Value: "quote'--"}}, query.Cmp{Field: "id", Op: query.In, Value: []string{"a", "b"}}, query.Cmp{Field: "startedAt", Op: query.In, Value: []int64{1, 2}}, query.Cmp{Field: "success", Op: query.In, Value: []bool{false, true}}, query.Cmp{Field: "id", Op: query.Prefix, Value: "%_!"}, query.Cmp{Field: "id", Op: query.Contains, Value: "%_!"}} {
		sql, args, err := translate(e, query.HistoryFields())
		require.NoError(t, err)
		require.NotEmpty(t, sql)
		require.NotContains(t, sql, "quote'--")
		_ = args
	}
	_, _, err := translate(query.Cmp{Field: "id; DROP TABLE tasks", Op: query.Eq, Value: "x"}, query.HistoryFields())
	require.Error(t, err)
}
func FuzzTranslation(f *testing.F) {
	f.Add("a'b%_!", true)
	f.Fuzz(func(t *testing.T, value string, prefix bool) {
		op := query.Contains
		if prefix {
			op = query.Prefix
		}
		_, args, err := translate(query.Cmp{Field: "id", Op: op, Value: value}, query.HistoryFields())
		require.NoError(t, err)
		require.Len(t, args, 1)
	})
}
