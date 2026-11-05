// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/altessa-s/go-horai/internal/testhelpers"
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
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
	f.Fuzz(func(t *testing.T, data []byte) {
		fields := append(query.TaskFields(), query.HistoryFields()...)
		e := testhelpers.QueryTree(data, fields)
		doc, err := translate(e, fields)
		require.NoError(t, err)

		var inspect func(any)

		inspect = func(v any) {
			switch x := v.(type) {
			case bson.M:
				for k, v := range x {
					require.Contains(t, []string{"$expr", "$and", "$or", "$not", "$eq", "$ne", "$lt", "$lte", "$gt", "$gte", "$in", "$literal", "$regexMatch", "input", "regex"}, k)

					if k != "$literal" {
						inspect(v)
					}
				}
			case bson.A:
				for _, v := range x {
					inspect(v)
				}
			}
		}
		inspect(doc)
		_, err = bson.Marshal(doc)
		require.NoError(t, err)
	})
}
func TestColumnRejectsUnknown(t *testing.T) {
	t.Parallel()

	_, err := column("$unknown")
	require.ErrorIs(t, err, query.ErrInvalid)
}
func TestFiltersAndIndexes(t *testing.T) {
	t.Parallel()
	require.Equal(t, bson.M{"$in": bson.A{int64(0), nil}}, zeroOrMissing(int64(0)))
	require.Equal(t, int64(5), zeroOrMissing(int64(5)))
	require.Equal(t, "run", zeroOrMissing("run"))
	require.NotEmpty(t, ownedRunFilter("id", "node/run"))
	require.Len(t, taskIndexModels(), 3)
	require.Len(t, historyIndexModels(), 2)

	for _, field := range append(query.TaskFields(), query.HistoryFields()...) {
		col, err := column(field)
		require.NoError(t, err)
		require.NotEmpty(t, col)
	}
}
