// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"fmt"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
)

// MaxConcurrentTasks bounds static semaphore allocation and counter arithmetic.
const MaxConcurrentTasks = 1 << 20

func validateOptions(o *options) error {
	checks := []struct {
		name    string
		invalid bool
	}{
		{"tickInterval", o.tickInterval <= 0},
		{"staleTaskTimeout", o.staleTaskTimeout <= 0},
		{"cleanupInterval", o.cleanupInterval <= 0},
		{"storageTimeout", o.storageTimeout <= 0},
		{"historyRetention", o.historyRetention < 0},
		{"runOnStartGrace", o.runOnStartGrace < 0},
		{"maxConcurrentTasks", o.maxConcurrentTasks < 0 || o.maxConcurrentTasks > MaxConcurrentTasks},
		{"reservedHighPrioritySlots", o.reservedHighPrioritySlots < 0 || o.reservedHighPrioritySlots > MaxConcurrentTasks ||
			(o.maxConcurrentTasks > 0 && o.reservedHighPrioritySlots >= o.maxConcurrentTasks)},
	}
	for _, c := range checks {
		if c.invalid {
			return fmt.Errorf("%s: %w", c.name, coreerrs.ErrInvalidArgument)
		}
	}
	return nil
}
