// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package schedule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai/schedule"
)

func FuzzParse(f *testing.F) {
	for _, s := range []string{"", "TZ=UTC", "@hourly", "0 0 0 * * *"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, spec string) {
		s, err := schedule.Parse(spec)
		if err != nil {
			require.ErrorIs(t, err, schedule.ErrInvalid)
			return
		}
		now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
		for range 5 {
			next := s.Next(now)
			require.True(t, next.IsZero() || next.After(now))
			if next.IsZero() {
				break
			}
			now = next
		}
	})
}
