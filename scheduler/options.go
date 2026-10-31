// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"log/slog"
	"time"

	horai "github.com/altessa-s/go-horai"
)

//go:generate optgen generate --type=options
//go:generate sh -c "sed -n '1,4p' options.go > .header.tmp && cat options_gen.go >> .header.tmp && mv .header.tmp options_gen.go"

const (
	// DefaultTickInterval is the default interval between scheduler loop iterations.
	// See [WithTickInterval].
	DefaultTickInterval = 1 * time.Second
	// DefaultHistoryRetention is the default duration for which completed task
	// execution history is retained before being purged. See [WithHistoryRetention].
	DefaultHistoryRetention = 7 * 24 * time.Hour
	// DefaultMaxConcurrentTasks is the default upper bound on concurrent task
	// executions. Zero means unlimited. See [WithMaxConcurrentTasks].
	DefaultMaxConcurrentTasks = 0
	// DefaultReservedHighPrioritySlots is the default number of concurrency slots
	// reserved exclusively for [schedtask.PriorityHigh] tasks. Normal and Low priority
	// tasks cannot consume these slots. See [WithReservedHighPrioritySlots].
	DefaultReservedHighPrioritySlots = 2
	// DefaultStaleTaskTimeout is the default lease of a task run. The instance
	// executing a run renews its lease every third of this duration (at least
	// [minRunLease]); a run whose lease has expired — its instance crashed or
	// lost its storage — is reset to [horai.TaskStatusActive] by stale recovery on any
	// instance. See [WithStaleTaskTimeout].
	DefaultStaleTaskTimeout = 30 * time.Minute
	// DefaultCleanupInterval is the default interval between periodic history
	// cleanup runs. See [WithCleanupInterval].
	DefaultCleanupInterval = 1 * time.Hour
	// DefaultStorageTimeout is the default per-operation deadline applied to
	// every [horai.Store] call the scheduler makes on its own behalf.
	// See [WithStorageTimeout].
	DefaultStorageTimeout = 10 * time.Second
	// DefaultRunOnStartGrace is the default window within which a recent run
	// suppresses a RunOnStart run. See [WithRunOnStartGrace].
	DefaultRunOnStartGrace = 5 * time.Minute
	// maxStaleRecoveryInterval caps the stale recovery ticker interval.
	maxStaleRecoveryInterval = 5 * time.Minute
	// minRunLease is the shortest run lease an instance persists, whatever
	// [WithStaleTaskTimeout] says. Leases are stored in whole seconds and renewed
	// every third of their length, so the floor keeps renewal latency headroom
	// above a second and bounds the heartbeat write rate.
	minRunLease = 5 * time.Second
)

type options struct {
	tickInterval              time.Duration `optgen:"default=DefaultTickInterval" optval:"nonpositive"`
	historyRetention          time.Duration `optgen:"default=DefaultHistoryRetention" optval:"nonpositive"`
	staleTaskTimeout          time.Duration `optgen:"default=DefaultStaleTaskTimeout" optval:"nonpositive"`
	cleanupInterval           time.Duration `optgen:"default=DefaultCleanupInterval" optval:"nonpositive"`
	storageTimeout            time.Duration `optgen:"default=DefaultStorageTimeout" optval:"nonpositive"`
	runOnStartGrace           time.Duration `optgen:"default=DefaultRunOnStartGrace" optval:"nonpositive"`
	instanceID                string
	maxConcurrentTasks        int `optgen:"default=DefaultMaxConcurrentTasks"`
	reservedHighPrioritySlots int `optgen:"default=DefaultReservedHighPrioritySlots"`
	logger                    *slog.Logger
	leaderElector             horai.LeaderElector
	concurrencyLimit          func() int
	readiness                 horai.ReadinessFunc
	historyStore              horai.HistoryStore
	clock                     horai.Clock
	metrics                   horai.Metrics
}
