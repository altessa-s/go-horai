// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

// LeaderElector is the narrow leader-election contract consumed by the
// scheduler. Any type exposing a concurrency-safe IsLeader method satisfies
// it — for example, *leadelect.Leader from data/leadelect.
type LeaderElector interface {
	// IsLeader reports whether this node currently holds leadership.
	// Implementations must be safe for concurrent use.
	IsLeader() bool
}
