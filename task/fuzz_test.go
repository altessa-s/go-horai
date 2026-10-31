// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package task_test

import (
	"testing"

	"github.com/altessa-s/go-horai/task"
)

func FuzzTaskPriority_String(f *testing.F) {
	f.Add(int32(0))
	f.Add(int32(1))
	f.Add(int32(4))
	f.Add(int32(-1))
	f.Add(int32(100))

	f.Fuzz(func(t *testing.T, val int32) {
		p := task.Priority(val)
		_ = p.String()
	})
}
