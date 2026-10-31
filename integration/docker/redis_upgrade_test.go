// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"

	redisstore "github.com/altessa-s/go-horai/storage/redis"
)

func TestRedisHistoryIndexUpgrade(t *testing.T) {
	t.Parallel()
	addr := os.Getenv("HORAI_REDIS_ADDR")
	if addr == "" {
		addr = endpoint(t, "redis/redis-stack-server:7.4.0-v3", "6379/tcp", nil)
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	prefix := namespace()
	s, err := redisstore.New(client, redisstore.WithKeyPrefix(prefix))
	require.NoError(t, err)
	// Leave the task index initialized, as on a history-only schema upgrade.
	require.NoError(t, s.EnsureIndexes(t.Context()))
	require.NoError(t, client.FTDropIndex(t.Context(), prefix+":idx:history:v2").Err())
	created, err := s.CreateTask(t.Context(), &horai.TaskState{ID: "Task", Status: horai.TaskStatusActive})
	require.NoError(t, err)
	require.True(t, created)
	const count = 20000
	keys := make([]string, count)
	pipe := client.Pipeline()
	for i := range count {
		id := fmt.Sprintf("history-%05d", i)
		keys[i] = prefix + ":history:Task:" + id
		pipe.JSONSet(t.Context(), keys[i], "$", map[string]any{"id": id, "task_id": "Task", "started_at": i, "ended_at": i + 1})
	}
	_, err = pipe.Exec(t.Context())
	require.NoError(t, err)
	require.NoError(t, s.EnsureIndexes(t.Context()))
	// No eventual assertion: initialization must make all old history visible.
	seen := make(map[string]bool, count)
	for row, err := range s.History(t.Context(), "Task") {
		require.NoError(t, err)
		require.False(t, seen[row.ID])
		seen[row.ID] = true
	}
	require.Len(t, seen, count)
	require.NoError(t, s.DeleteTask(t.Context(), "Task"))
	remaining, err := client.Exists(t.Context(), keys...).Result()
	require.NoError(t, err)
	require.Zero(t, remaining)
	state, err := s.GetTask(t.Context(), "Task")
	require.NoError(t, err)
	require.Nil(t, state)
}
