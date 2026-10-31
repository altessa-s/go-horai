// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package query_test

import (
	"fmt"

	"github.com/altessa-s/go-horai/query"
)

func ExampleMatch() {
	filter := query.And{query.Cmp{Field: "id", Op: query.Prefix, Value: "daily/"}, query.Cmp{Field: "failures", Op: query.Eq, Value: int64(0)}}
	fmt.Println(query.Match(filter, query.Record{"id": "daily/report", "failures": int64(0)})) // Output: true
}
