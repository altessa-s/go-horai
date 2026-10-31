// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package memory provides a memory storage adapter.
//
// Store keeps task state and history in memory
// New accepts generated options and returns an error for invalid limits
// Reads and iterators return independent snapshots; iterators hold no lock while yielding.
package memory
