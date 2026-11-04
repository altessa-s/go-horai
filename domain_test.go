// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

func TestDomain(t *testing.T) {
	t.Parallel()

	var absent *horai.TaskState
	require.True(t, absent.NextRun().IsZero())

	for _, status := range []horai.TaskStatus{horai.TaskStatusActive, horai.TaskStatusRunning, horai.TaskStatusPaused, horai.TaskStatusDisabled, horai.TaskStatusCompleted, horai.TaskStatusUnspecified} {
		s := &horai.TaskState{ID: "task", Status: status, NextRunAt: 10, Revision: 3}
		require.NotEmpty(t, status.String())
		require.Equal(t, status, horai.FenceOf(s).Status)

		if status == horai.TaskStatusActive {
			require.Equal(t, time.Unix(10, 0).UTC(), s.NextRun())
		} else {
			require.True(t, s.NextRun().IsZero())
		}

		require.Equal(t, "task", s.QueryRecord()["id"])
	}

	for _, c := range []horai.RunClaim{{}, {StartedAt: 1}, {StartedAt: 1, RunID: "run", LeaseUntil: -1}} {
		require.ErrorIs(t, c.Validate(), horai.ErrInvalidRunClaim)
	}

	require.NoError(t, (horai.RunClaim{StartedAt: 1, RunID: "run"}).Validate())
	require.Equal(t, "task", (&horai.TaskHistory{TaskID: "task"}).QueryRecord()["taskId"])
}
