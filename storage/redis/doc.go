// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package redis provides a redis storage adapter.
//
// Store requires RedisJSON and RediSearch (Redis Stack)
// New(client, options...) validates configuration without I/O; the application owns the client
// Call EnsureIndexes before queries
// Lua scripts fence run ownership
// Numeric filter conjuncts narrow indexed searches; complete typed filters are evaluated against returned records for parity.
package redis
