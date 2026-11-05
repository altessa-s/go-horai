// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/storage/storagetest"
)

func TestContracts(t *testing.T) {
	t.Parallel()
	storagetest.Run(t, func(t testing.TB) horai.Store { t.Helper(); s, err := memory.New(); require.NoError(t, err); return s })
}
func TestHistoryContracts(t *testing.T) {
	t.Parallel()
	storagetest.RunHistory(t, func(t testing.TB) horai.HistoryStore {
		t.Helper()

		s, err := memory.New()
		require.NoError(t, err)

		return s
	})
}
