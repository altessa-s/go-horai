// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import "time"

// NextRun returns when the task of st runs next: its stored NextRunAt while
// the task is active, zero when st is nil (unknown task), the task is
// running, paused, disabled or completed, or no run is planned.
func (s *TaskState) NextRun() time.Time {
	if s == nil || s.Status != TaskStatusActive || s.NextRunAt <= 0 {
		return time.Time{}
	}
	return time.Unix(s.NextRunAt, 0).UTC()
}
