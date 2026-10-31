// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb

import (
	"fmt"
	"strings"

	"github.com/altessa-s/go-horai/query"
)

func column(f query.Field) string {
	switch f {
	case "taskId":
		return "task_id"
	case "runId":
		return "run_id"
	case "startedAt":
		return "started_at"
	case "endedAt":
		return "ended_at"
	case "durationMs":
		return "duration_ms"
	case "lastRunAt":
		return "last_run_at"
	case "nextRunAt":
		return "next_run_at"
	case "skipNextRun":
		return "skip_next_run"
	case "disableHistory":
		return "disable_history"
	case "oneShot":
		return "one_shot"
	}
	return string(f)
}
func translate(e query.Expr, fields []query.Field, d Dialect) (string, []any, error) {
	if err := query.Validate(e, fields); err != nil {
		return "", nil, err
	}
	var args []any
	bind := func(v any) string {
		args = append(args, v)
		if d == Postgres {
			return fmt.Sprintf("$%d", len(args))
		}
		return "?"
	}
	var walk func(query.Expr) string
	walk = func(e query.Expr) string {
		join := func(es []query.Expr, op, empty string) string {
			if len(es) == 0 {
				return empty
			}
			ss := make([]string, len(es))
			for i, v := range es {
				ss[i] = walk(v)
			}
			return "(" + strings.Join(ss, op) + ")"
		}
		switch v := e.(type) {
		case nil:
			return "TRUE"
		case query.And:
			return join(v, " AND ", "TRUE")
		case query.Or:
			return join(v, " OR ", "FALSE")
		case query.Not:
			return "(NOT " + walk(v.Expr) + ")"
		case query.Cmp:
			return comparison(v, bind)
		}
		panic("validated expression")
	}
	sql := walk(e)
	return sql, args, nil
}

func comparison(v query.Cmp, bind func(any) string) string {
	col := column(v.Field)
	if v.Op == query.In {
		var values []string
		switch vs := v.Value.(type) {
		case []string:
			for _, x := range vs {
				values = append(values, bind(x))
			}
		case []int64:
			for _, x := range vs {
				values = append(values, bind(x))
			}
		case []bool:
			for _, x := range vs {
				values = append(values, bind(x))
			}
		}
		return "(" + col + " IN (" + strings.Join(values, ",") + "))"
	}
	if v.Op == query.Prefix || v.Op == query.Contains {
		value, _ := v.Value.(string) // Validated above.
		// An explicit escape character avoids SQL-mode-dependent backslash handling.
		value = strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(value) + "%"
		if v.Op == query.Contains {
			value = "%" + value
		}
		return "(" + col + " LIKE " + bind(value) + " ESCAPE '!')"
	}
	op := map[query.Op]string{query.Eq: "=", query.Ne: "<>", query.Lt: "<", query.Le: "<=", query.Gt: ">", query.Ge: ">="}[v.Op]
	return "(" + col + " " + op + " " + bind(v.Value) + ")"
}
