// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"maps"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/task"
)

type taskDocument struct {
	ID             string            `bson:"_id"`
	Description    string            `bson:"description"`
	Status         int32             `bson:"status"`
	Priority       int32             `bson:"priority"`
	Schedule       string            `bson:"schedule"`
	LastRunAt      int64             `bson:"last_run_at"`
	NextRunAt      int64             `bson:"next_run_at"`
	LastRunID      string            `bson:"last_run_id"`
	RunStartedAt   int64             `bson:"run_started_at"`
	RunLeaseUntil  int64             `bson:"run_lease_until"`
	RunLeaseID     string            `bson:"run_lease_id"`
	RunAt          int64             `bson:"run_at"`
	Failures       int32             `bson:"failures"`
	SkipNextRun    bool              `bson:"skip_next_run"`
	DisableHistory bool              `bson:"disable_history"`
	Unmanaged      bool              `bson:"unmanaged"`
	OneShot        bool              `bson:"one_shot"`
	Meta           map[string]string `bson:"meta"`
	CreatedAt      int64             `bson:"created_at"`
	UpdatedAt      int64             `bson:"updated_at"`
	Revision       int64             `bson:"revision"`
}

type historyDocument struct {
	ID         string `bson:"_id"`
	TaskID     string `bson:"task_id"`
	RunID      string `bson:"run_id"`
	Error      string `bson:"error"`
	StartedAt  int64  `bson:"started_at"`
	EndedAt    int64  `bson:"ended_at"`
	DurationMs int64  `bson:"duration_ms"`
	Success    bool   `bson:"success"`
}

func newTaskDocument(state *horai.TaskState) *taskDocument {
	return &taskDocument{ID: state.ID,
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
func (d *taskDocument) toTaskState() *horai.TaskState {
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
func newHistoryDocument(h *horai.TaskHistory) *historyDocument {
	return &historyDocument{ID: h.ID,
		TaskID:     h.TaskID,
		RunID:      h.RunID,
		Error:      h.Error,
		StartedAt:  h.StartedAt,
		EndedAt:    h.EndedAt,
		DurationMs: h.DurationMs,
		Success:    h.Success}
}
func (d *historyDocument) toTaskHistory() *horai.TaskHistory {
	return &horai.TaskHistory{ID: d.ID,
		TaskID:     d.TaskID,
		RunID:      d.RunID,
		Error:      d.Error,
		StartedAt:  d.StartedAt,
		EndedAt:    d.EndedAt,
		DurationMs: d.DurationMs,
		Success:    d.Success}
}
