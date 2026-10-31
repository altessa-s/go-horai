// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horai_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altessa-s/go-horai"
)

func TestPageLimitBounds(t *testing.T) {
	t.Parallel()
	require.NoError(t, horai.ValidatePageLimit(1))
	require.NoError(t, horai.ValidatePageLimit(horai.MaxStorePageLimit))
}
