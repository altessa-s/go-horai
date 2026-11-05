// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package clickhouse_test

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"

	store "github.com/altessa-s/go-horai/storage/clickhouse"
)

func TestInvalidPageLimitsBeforeIO(t *testing.T) {
	t.Parallel()

	s := new(store.Store)

	for _, limit := range []int64{-1, 0, horai.MaxStorePageLimit + 1, math.MaxInt64} {
		{
			_, err := s.ListHistory(t.Context(), "task", horai.HistoryPage{Limit: limit}, nil)
			require.ErrorIs(t, err, horai.ErrInvalidArgument)
		}
	}
}
