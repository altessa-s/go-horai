// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package memory

// DefaultMaxHistoryPerTask bounds retained history in memory.
//
//go:generate optgen generate --type=options
//go:generate sh -c "cat ../../docs/generated-header.txt options_gen.go > .header.tmp && mv .header.tmp options_gen.go"
const DefaultMaxHistoryPerTask = 1000

type options struct {
	maxHistoryPerTask int `optgen:"default=DefaultMaxHistoryPerTask"`
}
