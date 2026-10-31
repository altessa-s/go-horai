// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"runtime"
	"sync"

	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// slotStrategy owns admission and accounting. Release is called exactly once
// for each successful acquisition, including cancellation before execution.
type slotStrategy interface {
	acquire(context.Context, schedtask.Priority, bool) (func(), bool)
	status() (running, available, high int)
}
type staticSlots struct{ shared, reserved chan struct{} }
type dynamicSlots struct {
	mu                      sync.Mutex
	running, high, reserved int
	limit                   func() int
}
type unlimitedSlots struct{ dynamicSlots }

func newSlots(o *options) slotStrategy {
	if o.concurrencyLimit != nil {
		return &dynamicSlots{limit: o.concurrencyLimit, reserved: o.reservedHighPrioritySlots}
	}
	if o.maxConcurrentTasks == 0 {
		return &unlimitedSlots{}
	}
	return &staticSlots{
		shared:   make(chan struct{}, max(o.maxConcurrentTasks-o.reservedHighPrioritySlots, 1)),
		reserved: make(chan struct{}, o.reservedHighPrioritySlots),
	}
}
func (s *staticSlots) status() (int, int, int) {
	used, high := len(s.shared), len(s.reserved)
	availableHigh := -1
	if cap(s.reserved) > 0 {
		availableHigh = cap(s.reserved) - high
	}
	return used + high, cap(s.shared) + cap(s.reserved) - used - high, availableHigh
}
func (s *staticSlots) acquire(ctx context.Context, p schedtask.Priority, _ bool) (func(), bool) {
	if p == schedtask.PriorityCritical {
		return func() {}, true
	}
	release := func(c chan struct{}) func() { return func() { <-c } }
	if p == schedtask.PriorityHigh {
		select {
		case s.reserved <- struct{}{}:
			return release(s.reserved), true
		default:
		}
		select {
		case s.reserved <- struct{}{}:
			return release(s.reserved), true
		case s.shared <- struct{}{}:
			return release(s.shared), true
		case <-ctx.Done():
			return nil, false
		}
	}
	if p == schedtask.PriorityLow {
		select {
		case s.shared <- struct{}{}:
			return release(s.shared), true
		default:
			return nil, false
		}
	}
	select {
	case s.shared <- struct{}{}:
		return release(s.shared), true
	case <-ctx.Done():
		return nil, false
	}
}
func (s *dynamicSlots) status() (int, int, int) {
	limit := 0
	if s.limit != nil {
		limit = s.limit()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	available := -1
	if limit > 0 {
		available = max(0, limit-s.running)
	}
	return s.running, available, -1
}
func (s *dynamicSlots) acquire(_ context.Context, p schedtask.Priority, manual bool) (func(), bool) {
	if p == schedtask.PriorityCritical {
		return func() {}, true
	}
	limit := 0
	if s.limit != nil {
		limit = s.limit()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !manual && limit > 0 {
		available := limit - s.running
		if available <= 0 || (p != schedtask.PriorityHigh && available <= max(0, s.reserved-s.high)) {
			return nil, false
		}
	}
	s.running++
	if p == schedtask.PriorityHigh {
		s.high++
	}
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.running--
		if p == schedtask.PriorityHigh {
			s.high--
		}
	}, true
}

func (s *Scheduler) dispatchPending(pending []pendingTask) {
	const perTickMultiplier = 4
	perTickCap := runtime.GOMAXPROCS(0) * perTickMultiplier
	dispatched := 0
	for _, pt := range pending {
		if !pt.task.claim() {
			continue
		}
		if _, unlimited := s.slots.(*unlimitedSlots); unlimited && pt.task.config.Priority != schedtask.PriorityCritical {
			if dispatched >= perTickCap {
				pt.task.release()
				continue
			}
			dispatched++
		}
		if _, static := s.slots.(*staticSlots); static {
			s.wg.Go(func() { defer pt.task.release(); s.runTaskWithSemaphore(pt.task, pt.state) })
			continue
		}
		release, ok := s.slots.acquire(s.stopCtx, pt.task.config.Priority, false)
		if !ok {
			pt.task.release()
			continue
		}
		s.wg.Go(func() { defer pt.task.release(); defer release(); s.executeTask(s.stopCtx, pt.task, pt.state) })
	}
}

// runTaskWithSemaphore also handles manual dispatch, which bypasses the
// tick gate in dynamic mode but still contributes to its running count.
func (s *Scheduler) runTaskWithSemaphore(task *registeredTask, state *horai.TaskState) {
	release, ok := s.slots.acquire(s.stopCtx, task.config.Priority, true)
	if !ok {
		return
	}
	defer release()
	s.executeTask(s.stopCtx, task, state)
}
