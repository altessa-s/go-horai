// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package schedule_test

import (
	"fmt"
	"time"

	"github.com/altessa-s/go-horai/schedule"
)

func ExampleParse() {
	s, err := schedule.Parse("@hourly")
	if err != nil {
		panic(err)
	}

	fmt.Println(s.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)).Format(time.RFC3339)) // Output: 2026-01-01T01:00:00Z
}
