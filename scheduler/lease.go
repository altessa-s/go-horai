// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"
)

// runIDSeparator separates the owning instance ID from the random part of a
// run ID. The random part is hex, so the owner is everything before the last
// separator.
const runIDSeparator = "/"

// runOwner returns the instance ID that created runID, or "" for a run ID
// written before run IDs carried their owner.
func runOwner(runID string) string {
	owner, _, found := strings.CutLast(runID, runIDSeparator)
	if !found {
		return ""
	}
	return owner
}

// runLease returns the lease this instance grants its own runs: the stale task
// timeout, floored at [minRunLease].
func (s *Scheduler) runLease() time.Duration {
	return max(s.opts.staleTaskTimeout, minRunLease)
}

// leaseSeconds converts a lease to whole seconds, rounding up so a stored
// lease never ends before the duration it stands for.
func leaseSeconds(lease time.Duration) int64 {
	sec := int64(lease / time.Second)
	if lease%time.Second != 0 {
		sec++ // without adding to lease, which overflows near the maximum duration
	}
	return sec
}

// startRunHeartbeat keeps the lease of run runID of task id alive until the
// returned stop function is called; stop also waits for an in-flight renewal.
// leaseBase is the instant the stored lease was computed from (ClaimRun stored
// leaseBase + lease). Each renewal is issued a third of a lease after the
// previous lease write, so it has two thirds of a lease to land before the
// stored lease runs out.
//
// When the claim itself took a third of a lease or more, the lease is renewed
// synchronously before returning, and owned reports whether that confirmed the
// run is still this instance's: a slow claim response may arrive after the
// lease expired and another instance reclaimed the task, in which case the
// caller must not run the task body. [confirmRunOwnership] decides what counts
// as confirmation.
//
// Renewals outlive cancellation of ctx for the same reason the result
// bookkeeping does: a run still finishing during Stop must not look abandoned
// to other instances. Renewal ends early once the storage reports that the run
// is no longer owned; a failed renewal is retried shortly after.
func (s *Scheduler) startRunHeartbeat(ctx context.Context, id, runID string, leaseBase time.Time) (stop func(), owned bool) {
	lease := s.runLease()
	interval := lease / 3 //nolint:mnd // renew three times per lease
	leaseSec := leaseSeconds(lease)
	hbCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	renew := func() (time.Time, bool, error) { return s.renewRunLease(hbCtx, id, runID, leaseSec) }

	leaseBase, owned = confirmRunOwnership(renew, s.since, leaseBase, interval)
	if !owned {
		cancel()
		return func() {}, false
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		timer := time.NewTimer(leaseBase.Add(interval).Sub(s.opts.clock.Now()))
		defer timer.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-timer.C:
			}
			base, renewed, err := renew()
			switch {
			case hbCtx.Err() != nil:
				return
			case err != nil:
				timer.Reset(min(interval, time.Second)) // transient: retry soon
			case !renewed:
				// Expected when the run finished just before stop; a run that
				// lost ownership while executing is reported when its result is
				// discarded.
				s.logger.DebugContext(hbCtx, "task run no longer owned, stopping lease renewal",
					slog.String("task_id", id),
					slog.String("run_id", runID))
				return
			default:
				leaseBase = base
				timer.Reset(leaseBase.Add(interval).Sub(s.opts.clock.Now()))
			}
		}
	})

	return func() {
		cancel()
		wg.Wait()
	}, true
}

// renewRunLease issues one [horai.Store.RenewRun] for run runID of task id, moving
// its lease to leaseSec seconds from now. It returns the instant the renewal
// was issued — the base of the new lease — and whether the run was still
// owned. Failures are counted and logged unless ctx has been canceled.
func (s *Scheduler) renewRunLease(ctx context.Context, id, runID string, leaseSec int64) (issued time.Time, renewed bool, err error) {
	opCtx, opCancel := s.storageCtx(ctx)
	defer opCancel()
	issued = s.opts.clock.Now()
	renewed, err = s.storage.RenewRun(opCtx, id, runID, issued.Unix()+leaseSec)
	if err != nil && ctx.Err() == nil {
		s.metrics.StorageError("renew_run")
		s.logger.ErrorContext(ctx, "failed to renew task run lease",
			slog.String("task_id", id),
			slog.String("run_id", runID),
			slog.Any("error", err))
	}
	return issued, renewed, err
}

// maxOwnershipConfirmations bounds the synchronous renewals a slow claim may
// take to confirm, with a timely response, that its run is still owned.
const maxOwnershipConfirmations = 3

// confirmRunOwnership is the slow-claim check of [Scheduler.startRunHeartbeat].
// While a third of a lease (interval) or more has passed since leaseBase, the
// stored lease is too short to start the task body on: it renews, and accepts
// only a renewal whose response is itself fresh — one that arrives within
// interval of being issued still leaves two thirds of the new lease. A late
// response proves nothing about the present, so it is retried. At most
// maxOwnershipConfirmations renewals are issued, and the last one must be
// fresh, so the check always ends. It returns the base of the latest lease
// write and whether ownership is confirmed; a failed or rejected renewal means
// it is not. since measures elapsed time (s.since outside tests).
func confirmRunOwnership(
	renew func() (time.Time, bool, error), since func(time.Time) time.Duration, leaseBase time.Time, interval time.Duration,
) (time.Time, bool) {
	for attempt := 0; since(leaseBase) >= interval; attempt++ {
		if attempt == maxOwnershipConfirmations {
			return leaseBase, false
		}
		base, renewed, err := renew()
		if err != nil || !renewed {
			return leaseBase, false
		}
		if attempt >= maxOwnershipConfirmations-1 && since(base) >= interval {
			return leaseBase, false
		}
		leaseBase = base
	}
	return leaseBase, true
}
