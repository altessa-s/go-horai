// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package task_test

import (
	"context"
	"testing"

	"github.com/altessa-s/go-horai/schedule"
	"github.com/altessa-s/go-horai/task"
)

func BenchmarkPriority_String(b *testing.B) {
	p := task.PriorityNormal
	for b.Loop() {
		_ = p.String()
	}
}

func BenchmarkConfig_Construction(b *testing.B) {
	for b.Loop() {
		cfg := task.Config{
			ID:       "bench-task",
			Schedule: "@every 5m",
			Func:     func(ctx context.Context) error { return nil },
			Priority: task.PriorityNormal,
			Meta: map[string]string{
				"team":  "platform",
				"owner": "bench",
			},
		}
		_ = cfg
	}
}

func BenchmarkParseSchedule(b *testing.B) {
	for _, spec := range []string{"@every 5m", "0 */5 * * * MON-FRI", "CRON_TZ=Europe/Moscow 0 0 3 * * *"} {
		b.Run(spec, func(b *testing.B) {
			for b.Loop() {
				if _, err := schedule.Parse(spec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
