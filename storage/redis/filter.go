// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"fmt"
	"strings"

	"github.com/altessa-s/go-horai/query"
)

type filterPlan struct {
	node  query.Expr
	query string
}

func newFilterPlan(e query.Expr, fields []query.Field) (*filterPlan, error) {
	if err := query.Validate(e, fields); err != nil {
		return nil, err
	}

	return &filterPlan{node: e, query: pushdown(e)}, nil
}
func (p *filterPlan) match(r query.Record) bool { return query.Match(p.node, r) }

// Push only exact numeric conjuncts; retain full client evaluation for string and boolean parity.
func pushdown(e query.Expr) string {
	if a, ok := e.(query.And); ok {
		var parts []string

		for _, c := range a {
			if s := pushdown(c); s != "" {
				parts = append(parts, s)
			}
		}

		return strings.Join(parts, " ")
	}

	c, ok := e.(query.Cmp)
	if !ok {
		return ""
	}

	switch c.Field {
	case "status", "priority", "failures", startedAtField, "endedAt", "durationMs":
	default:
		return ""
	}

	n, ok := c.Value.(int64)
	if !ok {
		return ""
	}
	// RediSearch numbers are doubles. At the exact-integer boundary an
	// adjacent integer can round to the bound and be excluded by a strict
	// comparison, so keep boundary comparisons in the client too.
	if n >= maxExactInteger || n <= -maxExactInteger {
		return ""
	}

	switch c.Op {
	case query.Eq:
		return fmt.Sprintf("@%s:[%d %d]", c.Field, n, n)
	case query.Lt:
		return fmt.Sprintf("@%s:[-inf (%d]", c.Field, n)
	case query.Le:
		return fmt.Sprintf("@%s:[-inf %d]", c.Field, n)
	case query.Gt:
		return fmt.Sprintf("@%s:[(%d +inf]", c.Field, n)
	case query.Ge:
		return fmt.Sprintf("@%s:[%d +inf]", c.Field, n)
	default:
		return ""
	}
}
func escapeTag(s string) string {
	var b strings.Builder

	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			b.WriteByte('\\')
		}

		b.WriteRune(r)
	}

	return b.String()
}

const maxExactInteger = 1 << 53

const startedAtField = "startedAt"
