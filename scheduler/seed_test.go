// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"testing"

	"github.com/altessa-s/go-horai"
)

func seedState(s horai.TaskStore, ctx context.Context, v *horai.TaskState) error {
	old, err := s.GetTask(ctx, v.ID)
	if err != nil {
		return err
	}
	var ok bool
	if old == nil {
		ok, err = s.CreateTask(ctx, v)
	} else {
		ok, err = s.ReplaceTaskIf(ctx, v, horai.FenceOf(old))
	}
	if err != nil {
		return err
	}
	if !ok {
		return horai.ErrConcurrentUpdate
	}
	return nil
}

func runCase(t *testing.T, name string, fn func(*testing.T)) { t.Helper(); t.Log(name); fn(t) }
