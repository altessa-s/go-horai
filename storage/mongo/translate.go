// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"regexp"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/altessa-s/go-horai/query"
)

func column(f query.Field) string {
	switch f {
	case "id":
		return fieldId
	case "taskId":
		return fieldTaskId
	case "runId":
		return "run_id"
	case "startedAt":
		return fieldStartedAt
	case "endedAt":
		return fieldEndedAt
	case "durationMs":
		return "duration_ms"
	case "lastRunAt":
		return "last_run_at"
	case "nextRunAt":
		return fieldNextRunAt
	case "skipNextRun":
		return "skip_next_run"
	case "disableHistory":
		return "disable_history"
	case "oneShot":
		return "one_shot"
	}
	return string(f)
}

func translate(e query.Expr, fields []query.Field) (bson.M, error) {
	if err := query.Validate(e, fields); err != nil {
		return nil, err
	}
	var walk func(query.Expr) any
	walk = func(e query.Expr) any {
		switch v := e.(type) {
		case nil:
			return true
		case query.And:
			a := bson.A{}
			for _, c := range v {
				a = append(a, walk(c))
			}
			return bson.M{opAnd: a}
		case query.Or:
			a := bson.A{}
			for _, c := range v {
				a = append(a, walk(c))
			}
			return bson.M{"$or": a}
		case query.Not:
			return bson.M{"$not": bson.A{walk(v.Expr)}}
		case query.Cmp:
			col := "$" + column(v.Field)
			if v.Op == query.Prefix || v.Op == query.Contains {
				value, _ := v.Value.(string) // Validated above.
				pattern := regexp.QuoteMeta(value)
				if v.Op == query.Prefix {
					pattern = "^" + pattern
				}
				return bson.M{"$regexMatch": bson.M{"input": col, "regex": bson.M{opLiteral: pattern}}}
			}
			op := string(v.Op)
			if v.Op == query.Le {
				op = "lte"
			}
			if v.Op == query.Ge {
				op = "gte"
			}
			return bson.M{"$" + op: bson.A{col, bson.M{opLiteral: v.Value}}}
		}
		panic("validated expression")
	}
	return bson.M{"$expr": walk(e)}, nil
}
