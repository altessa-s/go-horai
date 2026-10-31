// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

//go:build integration_docker

package docker_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"

	redisstore "github.com/altessa-s/go-horai/storage/redis"
)

// compactAfterAggregate removes the first page after Redis has returned its
// indexed cursor values but before the store can fetch any JSON bodies.
type compactAfterAggregate struct {
	client *redis.Client
	keys   []string
	fired  atomic.Bool
}

func (h *compactAfterAggregate) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) { return next(ctx, network, addr) }
}
func (h *compactAfterAggregate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *compactAfterAggregate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if err := next(ctx, cmd); err != nil {
			return err
		}
		if cmd.Name() == "ft.aggregate" && h.fired.CompareAndSwap(false, true) {
			pipe := h.client.Pipeline()
			for _, key := range h.keys {
				pipe.Del(ctx, key)
			}
			_, err := pipe.Exec(ctx)
			return err
		}
		return nil
	}
}

func TestRedisCompactedKeyset(t *testing.T) {
	t.Parallel()
	addr := os.Getenv("HORAI_REDIS_ADDR")
	if addr == "" {
		addr = endpoint(t, "redis/redis-stack-server:7.4.0-v3", "6379/tcp", nil)
	}
	compactedKeyset(t, addr, false)
}

func compactedKeyset(t *testing.T, addr string, cluster bool) {
	t.Helper()
	seed := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { require.NoError(t, seed.Close()) })
	for _, protocol := range []int{2, 3} {
		for _, history := range []bool{false, true} {
			t.Run(fmt.Sprintf("resp%d/history=%t", protocol, history), func(t *testing.T) {
				var client redis.UniversalClient = redis.NewClient(&redis.Options{Addr: addr, Protocol: protocol})
				if cluster {
					require.NoError(t, client.Close())
					client = redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{addr}, Protocol: protocol,
						ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
							return []redis.ClusterSlot{{Start: 0, End: 16383, Nodes: []redis.ClusterNode{{Addr: addr}}}}, nil
						},
					})
				}
				t.Cleanup(func() { require.NoError(t, client.Close()) })
				prefix := namespace()
				s, err := redisstore.New(client, redisstore.WithKeyPrefix(prefix))
				require.NoError(t, err)
				require.NoError(t, s.EnsureIndexes(t.Context()))
				var deleted []string
				pipe := seed.Pipeline()
				for i := range 1002 {
					id := fmt.Sprintf("Id:%04d", i)
					key := prefix + ":task:" + id
					var body any = map[string]any{"id": id, "next_run_at": 1}
					if history {
						key = prefix + ":history:Task:" + id
						body = map[string]any{"id": id, "task_id": "Task", "started_at": i / 2}
					}
					pipe.JSONSet(t.Context(), key, "$", body)
					if (!history && i < 1000) || (history && i >= 2) {
						deleted = append(deleted, key)
					}
				}
				_, err = pipe.Exec(t.Context())
				require.NoError(t, err)
				if history {
					// Reverse ID order and timestamp ties expose incorrect server
					// ordering, including the seek after a full 1000-row page.
					var ids []string
					for row, err := range s.History(t.Context(), "Task") {
						require.NoError(t, err)
						ids = append(ids, row.ID)
					}
					want := make([]string, 1002)
					for i := range want {
						want[i] = fmt.Sprintf("Id:%04d", len(want)-1-i)
					}
					require.Equal(t, want, ids)
				}
				hook := &compactAfterAggregate{client: seed, keys: deleted}
				client.AddHook(hook)
				if history {
					rows, err := s.ListHistory(t.Context(), "Task", horai.HistoryPage{Limit: 1}, nil)
					require.NoError(t, err)
					require.Len(t, rows, 2)
					require.Equal(t, "Id:0001", rows[0].ID)
					require.Equal(t, "Id:0000", rows[1].ID)
				} else {
					rows, err := s.ListTasks(t.Context(), horai.TaskPage{Limit: 1}, nil)
					require.NoError(t, err)
					require.Len(t, rows, 2)
					require.Equal(t, "Id:1000", rows[0].ID)
					require.Equal(t, "Id:1001", rows[1].ID)
				}
				require.True(t, hook.fired.Load())
			})
		}
	}
}
