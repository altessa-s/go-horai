// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai

import "fmt"

// MaxStorePageLimit bounds storage pages, excluding the single lookahead item.
const MaxStorePageLimit int64 = 10000

// ValidatePageLimit validates a storage page before lookahead arithmetic or I/O.
func ValidatePageLimit(limit int64) error {
	if limit < 1 || limit > MaxStorePageLimit {
		return fmt.Errorf("%w: page limit must be between 1 and %d", ErrInvalidArgument, MaxStorePageLimit)
	}
	return nil
}
