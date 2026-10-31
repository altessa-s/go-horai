// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package clickhouse provides a clickhouse storage adapter.
//
// Store implements only horai.HistoryStore
// Supply it with scheduler.WithHistoryStore while using another backend for task state and ownership
// New(connection, options...) validates configuration without I/O
// Call EnsureSchema during startup
// CleanupHistory synchronously deletes entries strictly before the supplied cutoff; optional table TTL is additional retention.
package clickhouse
