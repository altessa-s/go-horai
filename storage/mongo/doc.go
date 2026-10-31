// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package mongo provides a mongo storage adapter.
//
// Store uses the MongoDB v2 driver
// New(database, options...) validates configuration without I/O
// The application owns the client
// Call EnsureIndexes during startup
// Atomic updates fence run ownership; typed filters compile to BSON expressions with literal values.
package mongo
