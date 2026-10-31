// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/internal/testhelpers"
	"github.com/altessa-s/go-horai/query"
)

func TestTranslateBoundValues(t *testing.T) {
	for _, e := range []query.Expr{nil, query.And{}, query.Or{}, query.Not{Expr: query.Cmp{Field: "id", Op: query.Eq, Value: "quote'--"}}, query.Cmp{Field: "id", Op: query.In, Value: []string{"a", "b"}}, query.Cmp{Field: "startedAt", Op: query.In, Value: []int64{1, 2}}, query.Cmp{Field: "success", Op: query.In, Value: []bool{false, true}}, query.Cmp{Field: "id", Op: query.Prefix, Value: "%_!"}, query.Cmp{Field: "id", Op: query.Contains, Value: "%_!"}} {
		sql, args, err := translate(e, query.HistoryFields(), Postgres)
		require.NoError(t, err)
		require.NotEmpty(t, sql)
		require.NotContains(t, sql, "quote'--")
		_ = args
	}
	_, _, err := translate(query.Cmp{Field: "id; DROP TABLE tasks", Op: query.Eq, Value: "x"}, query.HistoryFields(), Postgres)
	require.Error(t, err)
}
func FuzzTranslation(f *testing.F) {
	for _, seed := range [][]byte{{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, {4, 0, 8}, {4, 1, 7}, {3, 6, 2}, {2, 5, 3}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fields := append(query.TaskFields(), query.HistoryFields()...)
		e := testhelpers.QueryTree(data, fields)
		for _, d := range []Dialect{Postgres, MySQL} {
			sql, args, err := translate(e, fields, d)
			require.NoError(t, err)
			require.NotContains(t, sql, "value:")
			placeholders := regexp.MustCompile(`\$[0-9]+|\?`)
			require.Len(t, placeholders.FindAllString(sql, -1), len(args))
			stripped := placeholders.ReplaceAllString(sql, "")
			for _, word := range regexp.MustCompile(`[A-Za-z_]+`).FindAllString(stripped, -1) {
				require.Contains(t, []string{"TRUE", "FALSE", "AND", "OR", "NOT", "IN", "LIKE", "ESCAPE", "startsWith", "position", "id", "description", "status", "priority", "schedule", "last_run_at", "next_run_at", "skip_next_run", "disable_history", "unmanaged", "one_shot", "failures", "task_id", "run_id", "started_at", "ended_at", "duration_ms", "success", "error"}, word)
			}
			require.Equal(t, strings.Count(sql, "("), strings.Count(sql, ")"))
		}
	})
}
func TestColumnRejectsUnknown(t *testing.T) {
	t.Parallel()
	_, err := column("unknown")
	require.ErrorIs(t, err, query.ErrInvalid)
}
