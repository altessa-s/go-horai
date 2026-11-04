// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package task_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/task"
)

func TestPriority_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		priority task.Priority
		expected string
	}{
		{"unspecified", task.PriorityUnspecified, "unknown"},
		{"low", task.PriorityLow, "low"},
		{"normal", task.PriorityNormal, "normal"},
		{"high", task.PriorityHigh, "high"},
		{"critical", task.PriorityCritical, "critical"},
		{"negative", task.Priority(-1), "unknown"},
		{"out of range", task.Priority(99), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.expected, tt.priority.String(), "Priority(%d).String()", tt.priority)
		})
	}
}

func TestPriority_Values(t *testing.T) {
	t.Parallel()
	require.Equal(t, task.Priority(0), task.PriorityUnspecified)
	require.Equal(t, task.Priority(1), task.PriorityLow)
	require.Equal(t, task.Priority(2), task.PriorityNormal)
	require.Equal(t, task.Priority(3), task.PriorityHigh)
	require.Equal(t, task.Priority(4), task.PriorityCritical)
}

func TestConfig_ZeroValue(t *testing.T) {
	t.Parallel()

	var cfg task.Config

	require.Equal(t, "", cfg.ID)
	require.Equal(t, task.PriorityUnspecified, cfg.Priority)
	require.Equal(t, 0, int(cfg.Timeout))
	require.False(t, cfg.RunOnStart)
	require.Nil(t, cfg.Func)
	require.Nil(t, cfg.Meta)
}
