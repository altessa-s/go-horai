// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package scheduler orchestrates durable task execution over horai ports.
//
// [Scheduler.Register] creates a new task and rejects duplicate IDs.
// [Scheduler.Bind] attaches a function to existing task state on this instance.
package scheduler
