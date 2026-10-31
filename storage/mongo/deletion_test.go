// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/drivertest"

	store "github.com/altessa-s/go-horai/storage/mongo"
)

func TestDeleteHistoryFailureLeavesTaskRetryable(t *testing.T) {
	t.Parallel()
	deployment := drivertest.NewMockDeployment(bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 13}, {Key: "errmsg", Value: "denied"}})
	var collections []string
	opts := options.Client().SetRetryWrites(false).SetMonitor(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "delete" {
			collections = append(collections, e.Command.Lookup("delete").StringValue())
		}
	}})
	opts.Deployment = deployment
	client, err := mongo.Connect(opts)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Disconnect(context.Background())) })
	s, err := store.New(client.Database("test"), store.WithTasksCollection("tasks"), store.WithHistoryCollection("history"))
	require.NoError(t, err)
	require.Error(t, s.DeleteTask(t.Context(), "task"))
	require.Equal(t, []string{"history"}, collections)
	success := bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}}
	deployment.AddResponses(success, success)
	require.NoError(t, s.DeleteTask(t.Context(), "task"))
	require.Equal(t, []string{"history", "history", "tasks"}, collections)
}
