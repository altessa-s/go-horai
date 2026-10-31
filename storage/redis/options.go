// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import "time"

//go:generate optgen generate --type=options
//go:generate sh -c "cat ../../docs/generated-header.txt options_gen.go > .header.tmp && mv .header.tmp options_gen.go"

const (
	// DefaultKeyPrefix is the default Redis key prefix prepended to all keys
	// managed by [Store]. Override with [WithKeyPrefix].
	DefaultKeyPrefix = "scheduler"

	// DefaultHistoryTTL is the default time-to-live for history entry keys.
	// A value of 0 disables automatic key expiration; history is then only
	// removed by [Store.CleanupHistory] or per-task trimming controlled by
	// [DefaultMaxHistoryPerTask]. Override with [WithHistoryTTL].
	DefaultHistoryTTL = 0

	// DefaultMaxHistoryPerTask is the default upper bound on the number of
	// history entries retained per task. When [Store.AddHistory] inserts a
	// new entry and the count exceeds this limit, the oldest entries are
	// deleted on a best-effort basis. Override with [WithMaxHistoryPerTask].
	DefaultMaxHistoryPerTask = 1000
)

type options struct {
	keyPrefix         string        `optgen:"default=DefaultKeyPrefix"`
	historyTTL        time.Duration `optgen:"default=DefaultHistoryTTL" optval:"nonpositive"`
	maxHistoryPerTask int           `optgen:"default=DefaultMaxHistoryPerTask"`
}
