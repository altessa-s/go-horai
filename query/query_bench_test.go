// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package query_test

import (
	"testing"

	"github.com/altessa-s/go-horai/query"
)

func BenchmarkMatch(b *testing.B) {
	e := query.Cmp{Field: "status", Op: query.Eq, Value: int64(1)}
	r := query.Record{"status": int64(1)}

	b.ReportAllocs()

	for b.Loop() {
		if !query.Match(e, r) {
			b.Fatal("match")
		}
	}
}
func BenchmarkValidate(b *testing.B) {
	e := query.Cmp{Field: "status", Op: query.Eq, Value: int64(1)}

	fields := query.TaskFields()
	for b.Loop() {
		if err := query.Validate(e, fields); err != nil {
			b.Fatal(err)
		}
	}
}
