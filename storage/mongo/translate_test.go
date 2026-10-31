// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/altessa-s/go-horai/query"
)

func TestTranslateComparisonOperators(t *testing.T) {
	for _, tc := range []struct {
		op   query.Op
		want string
	}{{query.Eq, "$eq"}, {query.Ne, "$ne"}, {query.Le, "$lte"}, {query.Ge, "$gte"}, {query.Lt, "$lt"}, {query.Gt, "$gt"}} {
		got, err := translate(query.Cmp{Field: "failures", Op: tc.op, Value: int64(2)}, query.TaskFields())
		require.NoError(t, err)
		require.Equal(t, bson.M{"$expr": bson.M{tc.want: bson.A{"$failures", bson.M{"$literal": int64(2)}}}}, got)
	}
	got, err := translate(query.Cmp{Field: "id", Op: query.Eq, Value: "$revision"}, query.TaskFields())
	require.NoError(t, err)
	require.Equal(t, bson.M{"$expr": bson.M{"$eq": bson.A{"$_id", bson.M{"$literal": "$revision"}}}}, got)
	_, err = translate(query.Or{nil, query.Cmp{Field: "secret", Op: query.Eq, Value: "x"}}, query.TaskFields())
	require.Error(t, err)
}
func FuzzTranslation(f *testing.F) {
	f.Add("a.*$", true)
	f.Fuzz(func(t *testing.T, value string, prefix bool) {
		op := query.Contains
		if prefix {
			op = query.Prefix
		}
		_, err := translate(query.Cmp{Field: "id", Op: op, Value: value}, query.TaskFields())
		require.NoError(t, err)
	})
}
