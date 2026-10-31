// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"log/slog"
	"runtime"

	horai "github.com/altessa-s/go-horai"
	schedtask "github.com/altessa-s/go-horai/task"
)

// dispatchPending routes pending tasks to the appropriate dispatch strategy.
type slotStrategy interface {
	dispatch(*Scheduler, []pendingTask)
}
type staticSlots struct{}
type dynamicSlots struct{}
type unlimitedSlots struct{}

func (dynamicSlots) dispatch(s *Scheduler, pending []pendingTask)   { s.dispatchDynamic(pending) }
func (unlimitedSlots) dispatch(s *Scheduler, pending []pendingTask) { s.dispatchUnlimited(pending) }
func (s *Scheduler) dispatchPending(pending []pendingTask)          { s.slots.dispatch(s, pending) }
func (staticSlots) dispatch(s *Scheduler, pending []pendingTask) {
	// Static limited mode: dispatch all tasks, let semaphores handle concurrency.
	// claim bounds the blocked set at one goroutine per registered task — the
	// semaphore acquire inside runTaskWithSemaphore blocks, so without it every
	// tick would stack another waiter onto the same still-queued task.
	for _, pt := range pending {
		if !pt.task.claim() {
			continue
		}
		s.wg.Go(func() {
			defer pt.task.release()
			s.runTaskWithSemaphore(pt.task, pt.state)
		})
	}
}

// dispatchUnlimited dispatches tasks in static unlimited mode (no semaphore,
// no concurrencyLimit). It caps the number of goroutines spawned per tick
// to runtime.GOMAXPROCS * 4 to prevent goroutine storms when many tasks become
// due simultaneously. Tasks that don't fit in this tick are deferred to the
// next one. Critical-priority tasks always bypass this cap.
func (s *Scheduler) dispatchUnlimited(pending []pendingTask) {
	const perTickMultiplier = 4
	perTickCap := runtime.GOMAXPROCS(0) * perTickMultiplier
	dispatched := 0

	for _, pt := range pending {
		if !pt.task.claim() {
			continue
		}

		// Critical bypasses all limits
		if pt.task.config.Priority == schedtask.PriorityCritical {
			s.wg.Go(func() {
				defer pt.task.release()
				s.executeTask(s.stopCtx, pt.task, pt.state)
			})
			continue
		}

		if dispatched >= perTickCap {
			pt.task.release()
			s.logger.DebugContext(s.stopCtx, "task deferred: per-tick dispatch cap reached",
				slog.String("task_id", pt.state.ID),
				slog.Int("cap", perTickCap))
			break
		}

		dispatched++
		s.adjustCount(&s.runningCount, 1)
		s.wg.Go(func() {
			defer pt.task.release()
			defer s.adjustCount(&s.runningCount, -1)
			s.executeTask(s.stopCtx, pt.task, pt.state)
		})
	}
}

// dispatchDynamic dispatches tasks using tick-gated concurrency control.
// It evaluates the concurrency limit function and dispatches only as many
// tasks as the current limit allows, eliminating blocked goroutines.
func (s *Scheduler) dispatchDynamic(pending []pendingTask) {
	limit := s.opts.concurrencyLimit()
	if limit <= 0 {
		// Limit function returned 0 or negative: unlimited mode.
		for _, pt := range pending {
			if !pt.task.claim() {
				continue
			}
			s.adjustCount(&s.runningCount, 1)
			s.wg.Go(func() {
				defer pt.task.release()
				defer s.adjustCount(&s.runningCount, -1)
				s.executeTask(s.stopCtx, pt.task, pt.state)
			})
		}
		return
	}

	running := int(s.runningCount.Load())
	available := limit - running
	reserved := s.opts.reservedHighPrioritySlots

	for _, pt := range pending {
		priority := pt.task.config.Priority

		if !pt.task.claim() {
			continue
		}

		// Critical bypasses all limits
		if priority == schedtask.PriorityCritical {
			s.wg.Go(func() {
				defer pt.task.release()
				s.executeTask(s.stopCtx, pt.task, pt.state)
			})
			continue
		}

		if available <= 0 {
			pt.task.release()
			s.logger.DebugContext(s.stopCtx, "task deferred: no available slots",
				slog.String("task_id", pt.state.ID),
				slog.String("priority", pt.task.config.Priority.String()),
				slog.Int("limit", limit),
				slog.Int("running", running))
			break
		}

		// Normal and Low: don't consume reserved-for-high slots
		if priority != schedtask.PriorityHigh && available <= reserved {
			pt.task.release()
			s.logger.DebugContext(s.stopCtx, "task deferred: only reserved slots available",
				slog.String("task_id", pt.state.ID),
				slog.String("priority", pt.task.config.Priority.String()))
			break
		}

		s.adjustCount(&s.runningCount, 1)
		available--
		if priority == schedtask.PriorityHigh && reserved > 0 {
			reserved--
		}

		s.wg.Go(func() {
			defer pt.task.release()
			defer s.adjustCount(&s.runningCount, -1)
			s.executeTask(s.stopCtx, pt.task, pt.state)
		})
	}
}

// runTaskWithSemaphore acquires the appropriate semaphore based on priority and executes the task.
// Used by both tick-dispatched and manually triggered tasks.
func (s *Scheduler) runTaskWithSemaphore(task *registeredTask, state *horai.TaskState) {
	priority := task.config.Priority

	// Critical tasks bypass all limits
	if priority == schedtask.PriorityCritical {
		s.executeTask(s.stopCtx, task, state)
		return
	}

	// Dynamic concurrency mode: track running count.
	// Capacity gating for tick-dispatched tasks is handled in dispatchDynamic.
	// For manually triggered tasks (Trigger), this provides accurate counting.
	if s.opts.concurrencyLimit != nil {
		s.adjustCount(&s.runningCount, 1)
		defer s.adjustCount(&s.runningCount, -1)
		s.executeTask(s.stopCtx, task, state)
		return
	}

	// Static unlimited mode: track running count for observability.
	if s.semaphore == nil {
		s.adjustCount(&s.runningCount, 1)
		defer s.adjustCount(&s.runningCount, -1)
		s.executeTask(s.stopCtx, task, state)
		return
	}

	// High priority: try reserved slots first, then shared pool
	if priority == schedtask.PriorityHigh && s.highPrioritySemaphore != nil {
		select {
		case s.highPrioritySemaphore <- struct{}{}:
			// Got reserved slot
			s.adjustCount(&s.highPrioritySemaphoreUsed, 1)
			defer func() {
				<-s.highPrioritySemaphore
				s.adjustCount(&s.highPrioritySemaphoreUsed, -1)
			}()
			s.executeTask(s.stopCtx, task, state)
			return
		default:
			// Reserved slots full, try shared pool
		}
	}

	if priority == schedtask.PriorityHigh && s.highPrioritySemaphore != nil {
		select {
		case s.highPrioritySemaphore <- struct{}{}:
			s.adjustCount(&s.highPrioritySemaphoreUsed, 1)
			defer func() { <-s.highPrioritySemaphore; s.adjustCount(&s.highPrioritySemaphoreUsed, -1) }()
		case s.semaphore <- struct{}{}:
			s.adjustCount(&s.semaphoreUsed, 1)
			defer func() { <-s.semaphore; s.adjustCount(&s.semaphoreUsed, -1) }()
		case <-s.stopCtx.Done():
			return
		}
		s.executeTask(s.stopCtx, task, state)
		return
	}

	// Normal/Low priority or High that couldn't get reserved slot: use shared pool
	// Low priority: only run if there are available slots (non-blocking check first)
	if priority == schedtask.PriorityLow {
		select {
		case s.semaphore <- struct{}{}:
			// Got slot
			s.adjustCount(&s.semaphoreUsed, 1)
			defer func() {
				<-s.semaphore
				s.adjustCount(&s.semaphoreUsed, -1)
			}()
			s.executeTask(s.stopCtx, task, state)
		default:
			// No slots available, Low priority task will wait for next tick
			s.logger.DebugContext(s.stopCtx, "low priority task deferred due to no available slots",
				slog.String("task_id", state.ID))
		}
		return
	}

	// Normal and High (fallback): wait for slot
	select {
	case s.semaphore <- struct{}{}:
		// Got slot
		s.adjustCount(&s.semaphoreUsed, 1)
		defer func() {
			<-s.semaphore
			s.adjustCount(&s.semaphoreUsed, -1)
		}()
		s.executeTask(s.stopCtx, task, state)
	case <-s.stopCtx.Done():
		// Scheduler is stopping
		return
	}
}
