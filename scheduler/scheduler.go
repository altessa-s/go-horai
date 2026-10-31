// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/altessa-s/go-atlas-core/types/nilcheck"

	"github.com/altessa-s/go-horai/schedule"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// Scheduler manages periodic and one-shot task execution with persistent state.
// It provides task registration, status tracking, pause/resume/disable
// capabilities, execution history recording, and stale-task recovery.
//
// Each registered task runs in its own goroutine; the number of concurrent
// executions is governed by either a static limit ([WithMaxConcurrentTasks])
// or a dynamic limit function ([WithConcurrencyLimit]). Long-running tasks
// do not block other tasks from being dispatched.
//
// Priority levels control dispatch order when concurrency is contended:
//   - [schedtask.PriorityCritical]: bypass all concurrency limits, execute immediately
//   - [schedtask.PriorityHigh]: use reserved slots + shared pool
//   - [schedtask.PriorityNormal]: use shared pool only (default)
//   - [schedtask.PriorityLow]: execute only when no higher-priority tasks are waiting
//
// For distributed environments, use [WithLeaderElector] to ensure only the
// leader node executes tasks.
//
// All exported methods are safe for concurrent use.
type Scheduler struct {
	storage horai.Store
	history horai.HistoryStore // storage unless WithHistoryStorage is set
	opts    *options
	logger  *slog.Logger

	mu    sync.RWMutex
	tasks map[string]*registeredTask

	isRunning     atomic.Bool
	stopCtx       context.Context
	stopCtxCancel context.CancelFunc
	wg            sync.WaitGroup

	// semaphore limits concurrent task executions for Normal/Low priority.
	// nil means unlimited concurrency.
	semaphore chan struct{}
	// semaphoreUsed tracks current semaphore usage atomically for metrics.
	semaphoreUsed atomic.Int32

	// highPrioritySemaphore provides reserved slots for High priority tasks.
	// High priority tasks can use both this and the main semaphore.
	highPrioritySemaphore chan struct{}
	// highPrioritySemaphoreUsed tracks current high priority semaphore usage atomically.
	highPrioritySemaphoreUsed atomic.Int32

	// runningCount tracks the number of currently executing non-critical tasks
	// in dynamic concurrency mode and static unlimited mode. Used for
	// tick-gated dispatch and runningTasksCount observability.
	runningCount atomic.Int32

	// leaderElector provides distributed leader election support.
	// If set, tasks only execute when this node is the leader.
	leaderElector horai.LeaderElector

	// readiness is evaluated at the start of each tick. When non-nil and
	// returning false, the tick is skipped — the loop keeps running but no
	// tasks are dispatched. See [WithReadiness].
	readiness func() bool

	// metrics holds Prometheus metrics for the scheduler.
	metrics      horai.Metrics
	registration [64]sync.Mutex
	lifecycle    sync.Mutex
	stopping     bool
	stopped      chan struct{}
	executions   atomic.Int32

	// liveRuns holds the IDs of the runs this instance is executing right now,
	// from just before ClaimRun until executeTask returns. Stale recovery uses
	// it to tell a live run of this instance from one it abandoned.
	statusMu sync.Mutex
	slots    slotStrategy
	liveRuns sync.Map // map[string]struct{}
}

// registeredTask holds the runtime state of a registered task.
type registeredTask struct {
	config schedtask.Config
	parsed schedule.Schedule

	// dispatched is true from the moment a dispatch goroutine is spawned until
	// that goroutine returns. It covers the queued-but-not-yet-started window
	// that running does not: in static semaphore mode a dispatched goroutine
	// blocks on the semaphore before reaching executeTask, so without this flag
	// every subsequent tick would spawn another goroutine for the same task and
	// the blocked set would grow without bound.
	dispatched atomic.Bool

	// running is true only while the task function is actually executing. It is
	// read by recoverStaleTasks to distinguish a task executing in this process
	// from one left behind by a crashed instance.
	running atomic.Bool
}

// claim reserves the single in-flight dispatch slot for the task. It returns
// false when a dispatch is already queued or running, in which case the caller
// must not spawn a goroutine.
func (t *registeredTask) claim() bool { return t.dispatched.CompareAndSwap(false, true) }

// matches reports whether state is the occurrence this registration was made
// for: the same kind of task and, for a one-shot task, the same RunAt. Another
// registration — a concurrent local re-registration, or another instance's —
// may have moved the persisted task to a different occurrence; running this
// registration's function for it would execute the wrong configuration.
func (t *registeredTask) matches(state *horai.TaskState) bool {
	oneShot := !t.config.RunAt.IsZero()
	if oneShot != state.OneShot {
		return false
	}
	return !oneShot || t.config.RunAt.Unix() == state.RunAt
}

// release frees the dispatch slot reserved by claim.
func (t *registeredTask) release() { t.dispatched.Store(false) }

// pendingTask holds task info for priority sorting.
type pendingTask struct {
	task  *registeredTask
	state *horai.TaskState
}

// New creates a new [Scheduler] backed by the provided [horai.Store] implementation.
// The scheduler is created in a stopped state; call [Scheduler.Start] to begin
// the main loop and [Scheduler.Stop] to shut it down gracefully.
//
// When [WithConcurrencyLimit] is provided, the scheduler operates in dynamic
// concurrency mode and does not allocate semaphores. Otherwise, if
// [WithMaxConcurrentTasks] is positive, static semaphore-based concurrency
// limiting is used.
//
// Example:
//
//	s := scheduler.New(storage, scheduler.WithTickInterval(time.Second))
//	if err := s.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
func New(storage horai.Store, opts ...Option) (*Scheduler, error) {
	o := newOptions(opts...)
	if o.instanceID == "" {
		o.instanceID = generateID()
	}
	if nilcheck.IsNil(storage) {
		return nil, errors.New("store is required")
	}
	if o.tickInterval <= 0 ||
		o.staleTaskTimeout <= 0 ||
		o.cleanupInterval <= 0 ||
		o.storageTimeout <= 0 ||
		o.historyRetention < 0 ||
		o.runOnStartGrace < 0 ||
		o.maxConcurrentTasks < 0 ||
		o.reservedHighPrioritySlots < 0 || (o.maxConcurrentTasks > 0 && o.reservedHighPrioritySlots > o.maxConcurrentTasks) {
		return nil, errors.New("invalid scheduler options")
	}
	if nilcheck.IsNil(o.clock) {
		o.clock = realClock{}
	}
	if nilcheck.IsNil(o.metrics) {
		o.metrics = noopMetrics{}
	}
	if o.logger == nil {
		o.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if nilcheck.IsNil(o.historyStore) {
		o.historyStore = storage
	}

	s := &Scheduler{
		storage:       storage,
		history:       cmp.Or[horai.HistoryStore](o.historyStore, storage),
		opts:          o,
		logger:        o.logger,
		tasks:         make(map[string]*registeredTask),
		leaderElector: o.leaderElector,
		readiness:     o.readiness,
	}

	s.metrics = o.metrics
	s.slots = unlimitedSlots{}

	// Dynamic concurrency mode: no semaphores needed, limits are evaluated per tick.
	if o.concurrencyLimit != nil {
		s.slots = dynamicSlots{}
		return s, nil
	}

	// Static mode: initialize semaphores for concurrency limiting if configured.
	if o.maxConcurrentTasks > 0 {
		s.slots = staticSlots{}
		// Main semaphore for Normal/Low tasks
		// Reserve some slots for High priority
		normalSlots := max(o.maxConcurrentTasks-o.reservedHighPrioritySlots, 0)
		s.semaphore = make(chan struct{}, normalSlots)

		// Reserved semaphore for High priority tasks
		if o.reservedHighPrioritySlots > 0 {
			s.highPrioritySemaphore = make(chan struct{}, o.reservedHighPrioritySlots)
		}
	}

	return s, nil
}

// Start begins the scheduler's main loop. It loads existing task states from
// storage, recovers runs left unfinished by a previous crash — those owned by
// this instance ID ([WithInstanceID]) that are not executing, and those whose
// lease has expired — and starts the tick, cleanup, and stale-recovery
// goroutines. Runs still held by other live instances are left alone.
//
// Start must be called exactly once. Calling Start on an already-running
// scheduler returns an error. The provided context bounds only the start (the
// initial load and recovery): a start whose context ends fails without
// starting, and its cancellation or deadline never stops a started scheduler,
// whose lifecycle keeps only the context's values and is ended by
// [Scheduler.Stop].
func (s *Scheduler) Start(ctx context.Context) error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.stopping {
		return errors.New("scheduler has stopped")
	}
	if !s.isRunning.CompareAndSwap(false, true) {
		return errors.New("scheduler already running")
	}

	// The lifecycle keeps ctx's values but not its cancellation or deadline:
	// ctx bounds only the start, and Stop ends the scheduler.
	s.stopCtx, s.stopCtxCancel = context.WithCancel(context.WithoutCancel(ctx))

	// Load existing tasks from storage
	if err := s.loadTasks(ctx); err != nil {
		s.stopCtxCancel()
		s.isRunning.Store(false)
		return coreerrs.WrapOperation(err, "load tasks")
	}

	// Recover any tasks left in Running status from a previous crash
	s.recoverStaleTasks(ctx, true)

	// A start whose context ended (canceled or past its budget) does not
	// start the loops, whatever the storage made of the context.
	if err := ctx.Err(); err != nil {
		s.stopCtxCancel()
		s.isRunning.Store(false)
		return coreerrs.WrapOperation(err, "start scheduler")
	}

	s.wg.Go(s.run)

	logAttrs := []any{
		slog.Duration("tick_interval", s.opts.tickInterval),
		slog.Duration("stale_task_timeout", s.opts.staleTaskTimeout),
	}
	switch {
	case s.opts.concurrencyLimit != nil:
		logAttrs = append(logAttrs,
			slog.String("concurrency_mode", "dynamic"),
			slog.Int("current_limit", s.opts.concurrencyLimit()),
			slog.Int("reserved_high_priority_slots", s.opts.reservedHighPrioritySlots))
	case s.opts.maxConcurrentTasks > 0:
		logAttrs = append(logAttrs,
			slog.Int("max_concurrent_tasks", s.opts.maxConcurrentTasks),
			slog.Int("reserved_high_priority_slots", s.opts.reservedHighPrioritySlots))
	default:
		logAttrs = append(logAttrs, slog.String("max_concurrent_tasks", "unlimited"))
	}
	if nilcheck.IsNotNil(s.leaderElector) {
		logAttrs = append(logAttrs, slog.String("leader_election", "enabled"))
	}
	if s.readiness != nil {
		logAttrs = append(logAttrs, slog.String("readiness_probe", "enabled"))
	}
	s.logger.InfoContext(ctx, "scheduler started", logAttrs...)

	return nil
}

// Stop gracefully stops the scheduler by canceling the internal context and
// waiting for all in-flight task goroutines to finish. If the provided context
// expires before all tasks complete, Stop returns the context error and some
// tasks may still be running in the background.
//
// Calling Stop on an already-stopped scheduler is a no-op and returns nil.
// Stop must be called after [Scheduler.Start].
func (s *Scheduler) Stop(ctx context.Context) error {
	s.lifecycle.Lock()
	if !s.isRunning.Load() && !s.stopping {
		s.lifecycle.Unlock()
		return nil
	}
	if !s.stopping {
		s.stopping = true
		s.isRunning.Store(false)
		s.stopCtxCancel()
		s.stopped = make(chan struct{})
		go func() { s.wg.Wait(); close(s.stopped) }()
	}
	done := s.stopped
	s.lifecycle.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// isLeader reports whether this scheduler instance is the elected leader.
// Always returns true when no [WithLeaderElector] option was provided.
// Safe for concurrent use.
func (s *Scheduler) isLeader() bool {
	if nilcheck.IsNil(s.leaderElector) {
		return true
	}
	return s.leaderElector.IsLeader()
}

// isReady reports whether the scheduler's readiness probe (if configured)
// returns true. When no [WithReadiness] was provided, isReady always
// returns true. Safe for concurrent use.
func (s *Scheduler) isReady() bool {
	if s.readiness == nil {
		return true
	}
	return s.readiness()
}

// runningTasksCount returns the number of currently executing non-critical tasks.
//
// In dynamic concurrency mode and static unlimited mode, it returns the atomic
// running count. In static semaphore mode, it returns the sum of shared-pool
// and reserved high-priority semaphore usage. [schedtask.PriorityCritical] tasks are
// never counted because they bypass concurrency limits.
//
// Safe for concurrent use.
func (s *Scheduler) runningTasksCount() int {
	if s.opts.concurrencyLimit != nil || s.semaphore == nil {
		return int(s.runningCount.Load())
	}
	count := int(s.semaphoreUsed.Load())
	count += int(s.highPrioritySemaphoreUsed.Load())
	return count
}

// availableSlots returns the number of concurrency slots available for task
// dispatch, including both shared-pool and reserved high-priority slots.
//
// In dynamic mode, it evaluates the concurrency limit function and subtracts
// the current running count. In static mode, it sums the free capacity of both
// semaphores. Returns -1 when concurrency limiting is not enabled (unlimited).
//
// Safe for concurrent use.
func (s *Scheduler) availableSlots() int {
	if s.opts.concurrencyLimit != nil {
		limit := s.opts.concurrencyLimit()
		if limit <= 0 {
			return -1
		}
		return max(0, limit-int(s.runningCount.Load()))
	}
	if s.semaphore == nil {
		return -1 // Unlimited
	}
	available := cap(s.semaphore) - int(s.semaphoreUsed.Load())
	if s.highPrioritySemaphore != nil {
		available += cap(s.highPrioritySemaphore) - int(s.highPrioritySemaphoreUsed.Load())
	}
	return available
}

// availableHighPrioritySlots returns the number of reserved slots currently
// available for [schedtask.PriorityHigh] tasks.
//
// In dynamic concurrency mode, reserved slots are enforced at dispatch time
// rather than tracked via a semaphore, so this method returns -1. It also
// returns -1 when concurrency limiting is not enabled or no reserved slots are
// configured.
//
// Safe for concurrent use.
func (s *Scheduler) availableHighPrioritySlots() int {
	if s.opts.concurrencyLimit != nil {
		return -1
	}
	if s.highPrioritySemaphore == nil {
		return -1
	}
	return cap(s.highPrioritySemaphore) - int(s.highPrioritySemaphoreUsed.Load())
}

// Status is one observation of lifecycle, readiness, leadership and slot availability.
type Status struct {
	Running, Leader, Ready                                   bool
	RunningTasks, AvailableSlots, AvailableHighPrioritySlots int
}

// Status reports the engine's current state. External readiness and leadership are sampled once.
func (s *Scheduler) Status() Status {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	return Status{s.isRunning.Load(), s.isLeader(), s.isReady(), s.runningTasksCount(), s.availableSlots(), s.availableHighPrioritySlots()}
}
func (s *Scheduler) since(t time.Time) time.Duration { return s.opts.clock.Now().Sub(t) }
func (s *Scheduler) registrationLock(id string) *sync.Mutex {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return &s.registration[h.Sum32()%uint32(len(s.registration))]
}
func generateID() string { b := make([]byte, 16); _, _ = rand.Read(b); return hex.EncodeToString(b) }

func (s *Scheduler) adjustCount(counter *atomic.Int32, delta int32) {
	s.statusMu.Lock()
	defer s.statusMu.Unlock()
	counter.Add(delta)
}
