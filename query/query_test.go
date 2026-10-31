// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/query"
)

func TestComparisons(t *testing.T) {
	t.Parallel()
	cases := []struct {
		expr query.Expr
		want bool
	}{
		{nil, true}, {query.And{}, true}, {query.Or{}, false}, {query.Not{Expr: nil}, false},
		{query.Cmp{Field: "description", Op: query.Eq, Value: "Job "}, true}, {query.Cmp{Field: "description", Op: query.Ne, Value: "job "}, true},
		{query.Cmp{Field: "description", Op: query.Prefix, Value: "Jo"}, true}, {query.Cmp{Field: "description", Op: query.Contains, Value: "ob "}, true},
		{query.Cmp{Field: "description", Op: query.Lt, Value: "Z"}, true}, {query.Cmp{Field: "description", Op: query.Gt, Value: "A"}, true},
		{query.Cmp{Field: "status", Op: query.Eq, Value: int64(2)}, true}, {query.Cmp{Field: "status", Op: query.Ne, Value: int64(1)}, true},
		{query.Cmp{Field: "status", Op: query.Lt, Value: int64(3)}, true}, {query.Cmp{Field: "status", Op: query.Le, Value: int64(2)}, true},
		{query.Cmp{Field: "status", Op: query.Gt, Value: int64(1)}, true}, {query.Cmp{Field: "status", Op: query.Ge, Value: int64(3)}, false},
		{query.Cmp{Field: "status", Op: query.In, Value: []int64{1, 2}}, true}, {query.Cmp{Field: "description", Op: query.In, Value: []string{"Job "}}, true},
		{query.Cmp{Field: "oneShot", Op: query.Eq, Value: false}, true}, {query.Cmp{Field: "oneShot", Op: query.Ne, Value: true}, true}, {query.Cmp{Field: "oneShot", Op: query.In, Value: []bool{true}}, false},
		{query.Cmp{Field: "nextRunAt", Op: query.Eq, Value: int64(0)}, true}, {query.Cmp{Field: "id", Op: query.Eq, Value: ""}, true},
		{query.And{query.Or{query.Not{Expr: nil}, query.And{}}, query.And{}}, true},
	}
	for _, tc := range cases {
		require.NoError(t, query.Validate(tc.expr, query.TaskFields()))
		require.Equal(t, tc.want, query.Match(tc.expr, query.Record{"description": "Job ", "status": int64(2)}))
	}
}
func TestValidation(t *testing.T) {
	t.Parallel()
	for _, e := range []query.Expr{
		query.Cmp{Field: "unknown", Op: query.Eq, Value: ""}, query.Cmp{Field: "status", Op: query.Eq, Value: "1"}, query.Cmp{Field: "status", Op: "bad", Value: int64(1)}, query.Cmp{Field: "status", Op: query.In, Value: []int64{}}, query.Cmp{Field: "oneShot", Op: query.Lt, Value: true}, query.Cmp{Field: "status", Op: query.Contains, Value: int64(1)}, query.Or{nil, query.Cmp{Field: "secret", Op: query.Eq, Value: "x"}},
	} {
		require.ErrorIs(t, query.Validate(e, query.TaskFields()), query.ErrInvalid)
		require.False(t, query.Match(e, nil))
	}
	var deep query.Expr
	for range 34 {
		deep = query.Not{Expr: deep}
	}
	require.ErrorIs(t, query.Validate(deep, query.TaskFields()), query.ErrInvalid)
	require.ErrorIs(t, query.Validate(query.And(make([]query.Expr, 1025)), query.TaskFields()), query.ErrInvalid)
	fields := query.TaskFields()
	fields[0] = "mutated"
	require.Equal(t, query.Field("id"), query.TaskFields()[0])
}
func FuzzQuery(f *testing.F) {
	f.Add("status", int64(2), uint8(0))
	f.Add("id", int64(-1), uint8(5))
	f.Fuzz(func(t *testing.T, field string, n int64, op uint8) {
		ops := []query.Op{query.Eq, query.Ne, query.Lt, query.Le, query.Gt, query.Ge}
		e := query.Cmp{Field: query.Field(field), Op: ops[int(op)%len(ops)], Value: n}
		if query.Validate(e, query.TaskFields()) == nil {
			r := query.Record{query.Field(field): n}
			require.NotEqual(t, query.Match(e, r), query.Match(query.Not{Expr: e}, r))
			require.Equal(t, query.Match(e, r), query.Match(query.And{e, e}, r))
		}
	})
}
