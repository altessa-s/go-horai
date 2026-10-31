// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"maps"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/task"
)

type taskData struct {
	ID             string            `json:"id"`
	Description    string            `json:"description"`
	Status         int32             `json:"status"`
	Priority       int32             `json:"priority"`
	Schedule       string            `json:"schedule"`
	LastRunAt      int64             `json:"last_run_at"`
	NextRunAt      int64             `json:"next_run_at"`
	LastRunID      string            `json:"last_run_id"`
	RunStartedAt   int64             `json:"run_started_at"`
	RunLeaseUntil  int64             `json:"run_lease_until"`
	RunLeaseID     string            `json:"run_lease_id"`
	RunAt          int64             `json:"run_at"`
	Failures       int32             `json:"failures"`
	SkipNextRun    bool              `json:"skip_next_run"`
	DisableHistory bool              `json:"disable_history"`
	Unmanaged      bool              `json:"unmanaged"`
	OneShot        bool              `json:"one_shot"`
	Meta           map[string]string `json:"meta"`
	CreatedAt      int64             `json:"created_at"`
	UpdatedAt      int64             `json:"updated_at"`
	Revision       int64             `json:"revision"`
}

type historyData struct {
	ID         string `json:"id"`
	TaskID     string `json:"task_id"`
	RunID      string `json:"run_id"`
	Error      string `json:"error"`
	StartedAt  int64  `json:"started_at"`
	EndedAt    int64  `json:"ended_at"`
	DurationMs int64  `json:"duration_ms"`
	Success    bool   `json:"success"`
}

func newTaskData(state *horai.TaskState) *taskData {
	return &taskData{ID: state.ID,
		Description:    state.Description,
		Status:         int32(state.Status),
		Priority:       int32(state.Priority),
		Schedule:       state.Schedule,
		LastRunAt:      state.LastRunAt,
		NextRunAt:      state.NextRunAt,
		LastRunID:      state.LastRunID,
		RunStartedAt:   state.RunStartedAt,
		RunLeaseUntil:  state.RunLeaseUntil,
		RunLeaseID:     state.RunLeaseID,
		RunAt:          state.RunAt,
		Failures:       state.Failures,
		SkipNextRun:    state.SkipNextRun,
		DisableHistory: state.DisableHistory,
		Unmanaged:      state.Unmanaged,
		OneShot:        state.OneShot,
		Meta:           maps.Clone(state.Meta),
		CreatedAt:      state.CreatedAt,
		UpdatedAt:      state.UpdatedAt,
		Revision:       state.Revision}
}
func (d *taskData) toTaskState() *horai.TaskState {
	return &horai.TaskState{ID: d.ID,
		Description:    d.Description,
		Status:         horai.TaskStatus(d.Status),
		Priority:       task.Priority(d.Priority),
		Schedule:       d.Schedule,
		LastRunAt:      d.LastRunAt,
		NextRunAt:      d.NextRunAt,
		SkipNextRun:    d.SkipNextRun,
		DisableHistory: d.DisableHistory,
		Unmanaged:      d.Unmanaged,
		OneShot:        d.OneShot,
		Failures:       d.Failures,
		LastRunID:      d.LastRunID,
		RunStartedAt:   d.RunStartedAt,
		RunLeaseUntil:  d.RunLeaseUntil,
		RunLeaseID:     d.RunLeaseID,
		RunAt:          d.RunAt,
		Meta:           maps.Clone(d.Meta),
		CreatedAt:      d.CreatedAt,
		UpdatedAt:      d.UpdatedAt,
		Revision:       d.Revision}
}
func newHistoryData(h *horai.TaskHistory) *historyData {
	return &historyData{ID: h.ID,
		TaskID:     h.TaskID,
		RunID:      h.RunID,
		Error:      h.Error,
		StartedAt:  h.StartedAt,
		EndedAt:    h.EndedAt,
		DurationMs: h.DurationMs,
		Success:    h.Success}
}
func (d *historyData) toTaskHistory() *horai.TaskHistory {
	return &horai.TaskHistory{ID: d.ID,
		TaskID:     d.TaskID,
		RunID:      d.RunID,
		Error:      d.Error,
		StartedAt:  d.StartedAt,
		EndedAt:    d.EndedAt,
		DurationMs: d.DurationMs,
		Success:    d.Success}
}
