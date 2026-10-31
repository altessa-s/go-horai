// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/altessa-s/go-horai"

	store "github.com/altessa-s/go-horai/storage/mongo"
)

// A disconnected SDK client exercises adapter error propagation without servers
// or network access. Disconnect joins the SDK's background activity before use.
func TestDisconnectedClientErrors(t *testing.T) {
	t.Parallel()
	client, err := mongo.Connect()
	require.NoError(t, err)
	require.NoError(t, client.Disconnect(t.Context()))
	s, err := store.New(client.Database("horai"))
	require.NoError(t, err)
	require.ErrorIs(t, s.EnsureIndexes(t.Context()), mongo.ErrClientDisconnected)
	_, err = s.GetTask(t.Context(), "task")
	require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	for _, call := range []func() (bool, error){
		func() (bool, error) { return s.CreateTask(t.Context(), &horai.TaskState{ID: "task"}) },
		func() (bool, error) {
			return s.ReplaceTaskIf(t.Context(), &horai.TaskState{ID: "task"}, horai.TaskFence{})
		},
		func() (bool, error) {
			return s.ClaimRun(t.Context(), "task", horai.RunClaim{StartedAt: 1, RunID: "node/run"})
		},
		func() (bool, error) { return s.FinishRun(t.Context(), "task", "node/run", horai.RunResult{}) },
		func() (bool, error) { return s.RenewRun(t.Context(), "task", "node/run", 10) },
	} {
		won, err := call()
		require.False(t, won)
		require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	}
	for _, call := range []func() error{
		func() error { return s.AddHistory(t.Context(), &horai.TaskHistory{}) },
		func() error { return s.DeleteHistory(t.Context(), "task") },
		func() error { return s.DeleteTask(t.Context(), "task") },
		func() error { return s.CleanupHistory(t.Context(), 10) },
	} {
		require.ErrorIs(t, call(), mongo.ErrClientDisconnected)
	}
	for _, err := range s.Tasks(t.Context()) {
		require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	}
	for _, err := range s.DueTasks(t.Context(), 10) {
		require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	}
	for _, err := range s.UnfinishedRuns(t.Context()) {
		require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	}
	for _, err := range s.History(t.Context(), "task") {
		require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	}
	_, err = s.ListTasks(t.Context(), horai.TaskPage{Limit: 10, AfterID: "after"}, nil)
	require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	_, err = s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: 10, AfterID: "after", AfterStartedAt: 10}, nil)
	require.ErrorIs(t, err, mongo.ErrClientDisconnected)
	_, err = s.ClaimRun(t.Context(), "task", horai.RunClaim{})
	require.ErrorIs(t, err, horai.ErrInvalidRunClaim)
	won, err := s.RenewRun(t.Context(), "task", "", 10)
	require.NoError(t, err)
	require.False(t, won)
	_, err = store.New(client.Database("horai"), store.WithTasksCollection("same"), store.WithHistoryCollection("same"))
	require.Error(t, err)
}
