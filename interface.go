// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

// LeaderElector is the narrow leader-election contract consumed by the
// scheduler. Any type exposing a concurrency-safe IsLeader method satisfies
// it. The consumer owns leader election and its lifecycle.
type LeaderElector interface {
	// IsLeader reports whether this node currently holds leadership.
	// Implementations must be safe for concurrent use.
	IsLeader() bool
}
