// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler_test

import (
	"context"
	"fmt"

	"github.com/altessa-s/go-horai/scheduler"
	"github.com/altessa-s/go-horai/storage/memory"
	"github.com/altessa-s/go-horai/task"
)

func ExampleScheduler_ListTasks() {
	store, err := memory.New()
	if err != nil {
		panic(err)
	}

	engine, err := scheduler.New(store)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	err = engine.Register(ctx, task.Config{ID: "refresh", Schedule: "@hourly", Func: func(context.Context) error { return nil }})
	if err != nil {
		panic(err)
	}

	page, err := engine.ListTasks(ctx, scheduler.TaskListRequest{Limit: 10})
	if err != nil {
		panic(err)
	}

	fmt.Println(page.Items[0].ID)
	// Output: refresh
}
