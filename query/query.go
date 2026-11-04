// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package query

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// ErrInvalid marks an unknown field, operator, type, or excessive expression.
var ErrInvalid = errors.New("invalid filter")

// Field identifies a closed, documented task or history field.
type Field string

// Op is a comparison operation.
type Op string

const (
	// Eq matches a field equal to the value.
	Eq Op = "eq"
	// Ne matches a field not equal to the value.
	Ne Op = "ne"
	// Lt matches a field less than the value.
	Lt Op = "lt"
	// Le matches a field less than or equal to the value.
	Le Op = "le"
	// Gt matches a field greater than the value.
	Gt Op = "gt"
	// Ge matches a field greater than or equal to the value.
	Ge Op = "ge"
	// In matches a field equal to one element of the value slice.
	In Op = "in"
	// Prefix matches a string field that starts with the value.
	Prefix Op = "prefix"
	// Contains matches a string field that contains the value.
	Contains Op = "contains"
)

// Expr is a typed filter. Nil matches every record. No text parser is provided.
type Expr interface{ expression() }

// And requires every child to match. Empty And is true.
type And []Expr

// Or requires at least one child to match. Empty Or is false.
type Or []Expr

// Not negates its child.
type Not struct{ Expr Expr }

// Cmp compares a field to a string, int64, or bool. In uses []string, []int64, or []bool.
type Cmp struct {
	Field Field
	Op    Op
	Value any
}

func (And) expression() {}
func (Or) expression()  {}
func (Not) expression() {}
func (Cmp) expression() {}

const (
	stringKind = "string"
	numberKind = "number"
	boolKind   = "bool"
)

// Record carries canonical field values: strings, int64 numbers, and booleans.
type Record map[Field]any

// TaskFields returns a fresh task field allowlist.
func TaskFields() []Field {
	return []Field{"id", "description", "status", "priority", "schedule", "lastRunAt", "nextRunAt",
		"skipNextRun", "disableHistory", "unmanaged", "oneShot", "failures"}
}

// HistoryFields returns a fresh history field allowlist.
func HistoryFields() []Field {
	return []Field{"id", "taskId", "runId", "startedAt", "endedAt", "durationMs", "success", "error"}
}
func kind(f Field) string {
	switch f {
	case "id", "description", "schedule", "taskId", "runId", "error":
		return stringKind
	case "status", "priority", "lastRunAt", "nextRunAt",
		"failures", "startedAt", "endedAt", "durationMs":
		return numberKind
	case "skipNextRun", "disableHistory", "unmanaged", "oneShot", "success":
		return boolKind
	}

	return ""
}

// Validate checks the entire expression, including short-circuited branches.
// Expressions are limited to depth 32 and 1024 nodes to bound evaluation work.
func Validate(e Expr, fields []Field) error { n := 0; return validate(e, fields, 0, &n) }
func validate(e Expr, fields []Field, depth int, n *int) error {
	*n++
	if depth > 32 || *n > 1024 {
		return fmt.Errorf("%w: expression limit", ErrInvalid)
	}

	children := func(es []Expr) error {
		for _, v := range es {
			if err := validate(v, fields, depth+1, n); err != nil {
				return err
			}
		}

		return nil
	}

	switch v := e.(type) {
	case nil:
		return nil
	case And:
		return children(v)
	case Or:
		return children(v)
	case Not:
		return validate(v.Expr, fields, depth+1, n)
	case Cmp:
		return validateComparison(v, fields)
	default:
		return fmt.Errorf("%w: expression type", ErrInvalid)
	}
}

func validateComparison(v Cmp, fields []Field) error {
	if !slices.Contains(fields, v.Field) || kind(v.Field) == "" {
		return fmt.Errorf("%w: field %q", ErrInvalid, v.Field)
	}

	if !slices.Contains([]Op{Eq, Ne, Lt, Le, Gt, Ge, In, Prefix, Contains}, v.Op) {
		return fmt.Errorf("%w: operator %q", ErrInvalid, v.Op)
	}

	k := kind(v.Field)
	if (v.Op == Prefix || v.Op == Contains) && k != stringKind {
		return fmt.Errorf("%w: string operator", ErrInvalid)
	}

	if k == boolKind && v.Op != Eq && v.Op != Ne && v.Op != In {
		return fmt.Errorf("%w: boolean operator", ErrInvalid)
	}

	valid := false

	if v.Op == In {
		switch x := v.Value.(type) {
		case []string:
			valid = k == stringKind && len(x) > 0
		case []int64:
			valid = k == numberKind && len(x) > 0
		case []bool:
			valid = k == boolKind && len(x) > 0
		}
	} else {
		switch v.Value.(type) {
		case string:
			valid = k == stringKind
		case int64:
			valid = k == numberKind
		case bool:
			valid = k == boolKind
		}
	}

	if !valid {
		return fmt.Errorf("%w: value type for %q", ErrInvalid, v.Field)
	}

	return nil
}

// Match evaluates a validated expression. Malformed expressions return false.
func Match(e Expr, r Record) bool {
	fields := append(TaskFields(), HistoryFields()...)
	if Validate(e, fields) != nil {
		return false
	}

	return match(e, r)
}
func match(e Expr, r Record) bool {
	switch v := e.(type) {
	case nil:
		return true
	case And:
		for _, c := range v {
			if !match(c, r) {
				return false
			}
		}

		return true
	case Or:
		for _, c := range v {
			if match(c, r) {
				return true
			}
		}

		return false
	case Not:
		return !match(v.Expr, r)
	case Cmp:
		return matchComparison(v, r)
	}

	return false
}

func matchComparison(v Cmp, r Record) bool {
	a, ok := r[v.Field]
	if !ok {
		switch kind(v.Field) {
		case stringKind:
			a = ""
		case numberKind:
			a = int64(0)
		case boolKind:
			a = false
		}
	}

	if v.Op == In {
		return member(v.Value, a)
	}

	c := 0

	switch x := a.(type) {
	case string:
		y, ok := v.Value.(string)
		if !ok {
			return false
		}

		if v.Op == Prefix {
			return strings.HasPrefix(x, y)
		}

		if v.Op == Contains {
			return strings.Contains(x, y)
		}

		c = strings.Compare(x, y)
	case int64:
		y, ok := v.Value.(int64)
		if !ok {
			return false
		}

		if x < y {
			c = -1
		} else if x > y {
			c = 1
		}
	case bool:
		y, ok := v.Value.(bool)
		if !ok {
			return false
		}

		if x != y {
			c = 1
		}
	default:
		return false
	}

	switch v.Op {
	case Eq:
		return c == 0
	case Ne:
		return c != 0
	case Lt:
		return c < 0
	case Le:
		return c <= 0
	case Gt:
		return c > 0
	case Ge:
		return c >= 0
	case In, Prefix, Contains:
		return false
	}

	return false
}

func member(values, a any) bool {
	switch vs := values.(type) {
	case []string:
		x, ok := a.(string)
		return ok && slices.Contains(vs, x)
	case []int64:
		x, ok := a.(int64)
		return ok && slices.Contains(vs, x)
	case []bool:
		x, ok := a.(bool)
		return ok && slices.Contains(vs, x)
	}

	return false
}
