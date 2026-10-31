// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import "github.com/altessa-s/go-horai/query"

// QueryRecord converts a task state to a map for filter evaluation. Field names use
// proto camelCase to match CEL expressions.
func (s *TaskState) QueryRecord() query.Record {
	return query.Record{
		"id":             s.ID,
		"description":    s.Description,
		"status":         int64(s.Status),
		"priority":       int64(s.Priority),
		"schedule":       s.Schedule,
		"lastRunAt":      s.LastRunAt,
		"nextRunAt":      s.NextRunAt,
		"skipNextRun":    s.SkipNextRun,
		"disableHistory": s.DisableHistory,
		"unmanaged":      s.Unmanaged,
		"oneShot":        s.OneShot,
		"failures":       int64(s.Failures),
	}
}

// QueryRecord converts a history entry to a map for filter evaluation.
func (h *TaskHistory) QueryRecord() query.Record {
	return query.Record{
		"id":         h.ID,
		"taskId":     h.TaskID,
		"runId":      h.RunID,
		"startedAt":  h.StartedAt,
		"endedAt":    h.EndedAt,
		"durationMs": h.DurationMs,
		"success":    h.Success,
		"error":      h.Error,
	}
}
