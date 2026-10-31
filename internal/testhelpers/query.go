// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//nolint:mnd // Bounded test data and explicit contract fixture values.
package testhelpers

import (
	"encoding/hex"

	"github.com/altessa-s/go-horai/query"
)

// QueryTree builds bounded expression trees covering every operator and field type.
// String values carry a marker so translators can assert they never enter SQL text.
func QueryTree(data []byte, fields []query.Field) query.Expr {
	offset := 0
	next := func() byte {
		if len(data) == 0 {
			return 0
		}
		b := data[offset%len(data)]
		offset++
		return b
	}
	var build func(int) query.Expr
	build = func(depth int) query.Expr {
		kind := next() % 5
		if depth < 3 {
			switch kind {
			case 0:
				return query.And{build(depth + 1), build(depth + 1)}
			case 1:
				return query.Or{build(depth + 1), build(depth + 1)}
			case 2:
				return query.Not{Expr: build(depth + 1)}
			}
		}
		field := fields[int(next())%len(fields)]
		ops := []query.Op{query.Eq, query.Ne, query.Lt, query.Le, query.Gt, query.Ge, query.In}
		var value any
		switch field {
		case "id", "description", "schedule", "taskId", "runId", "error":
			ops = append(ops, query.Prefix, query.Contains)
			value = "value:" + hex.EncodeToString(data[:min(len(data), 32)])
		case "skipNextRun", "disableHistory", "unmanaged", "oneShot", "success":
			ops = []query.Op{query.Eq, query.Ne, query.In}
			value = next()%2 == 0
		default:
			value = int64(int8(next()))
		}
		op := ops[int(next())%len(ops)]
		if op == query.In {
			switch v := value.(type) {
			case string:
				value = []string{v, "value:second"}
			case int64:
				value = []int64{v, v + 1}
			case bool:
				value = []bool{v, !v}
			}
		}
		return query.Cmp{Field: field, Op: op, Value: value}
	}
	return build(0)
}
