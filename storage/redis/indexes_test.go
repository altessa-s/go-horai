// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	store "github.com/altessa-s/go-horai/storage/redis"
)

func TestEnsureIndexesWaitsForHistory(t *testing.T) {
	t.Parallel()
	for _, cancel := range []bool{false, true} {
		t.Run(map[bool]string{false: "ready", true: "canceled"}[cancel], func(t *testing.T) {
			t.Parallel()
			client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
			t.Cleanup(func() { require.NoError(t, client.Close()) })
			synctest.Test(t, func(t *testing.T) {
				hook := &sequenceHook{t: t, steps: []replyStep{
					{args: []any{"FT.INFO", "scheduler:idx:tasks:v2"}, value: redis.FTInfoResult{}},
					{args: []any{"FT.INFO", "scheduler:idx:history:v2"}, value: redis.FTInfoResult{Indexing: 1}},
					{args: []any{"FT.INFO", "scheduler:idx:history:v2"}, value: redis.FTInfoResult{Indexing: 1}},
				}}
				if !cancel {
					hook.steps = append(hook.steps,
						replyStep{args: []any{"FT.INFO", "scheduler:idx:history:v2"}, value: redis.FTInfoResult{}},
						replyStep{args: []any{"FT.INFO", "scheduler:idx:tasks:v2"}, value: redis.FTInfoResult{}},
						replyStep{args: []any{"FT.SEARCH", "scheduler:idx:tasks:v2", "-@nextRunAt:[-inf +inf]", "NOCONTENT", "SORTBY", "id", "ASC", "LIMIT", 0, 1000, "DIALECT", 2}, value: redis.FTSearchResult{}},
					)
				}
				client.AddHook(hook)
				s, err := store.New(client)
				require.NoError(t, err)
				ctx, stop := context.WithCancel(t.Context())
				defer stop()
				done := make(chan error, 1)
				go func() { done <- s.EnsureIndexes(ctx) }()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("initialization returned before indexing completed: %v", err)
				default:
				}
				if cancel {
					stop()
					require.ErrorIs(t, <-done, context.Canceled)
				} else {
					time.Sleep(50 * time.Millisecond)
					require.NoError(t, <-done)
				}
				require.Empty(t, hook.steps)
			})
		})
	}
}
