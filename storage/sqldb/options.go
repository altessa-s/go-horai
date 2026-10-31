// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb

//go:generate optgen generate --type=options
//go:generate sh -c "sed -n '1,4p' options.go > .header.tmp && cat options_gen.go >> .header.tmp && mv .header.tmp options_gen.go"

const (
	// DefaultTasksTable is the table holding [horai.TaskState] rows when no
	// override is provided via [WithTasksTable].
	DefaultTasksTable = "scheduler_tasks"

	// DefaultHistoryTable is the table holding [horai.TaskHistory] rows
	// when no override is provided via [WithHistoryTable].
	DefaultHistoryTable = "scheduler_history"
)

type options struct {
	tasksTable   string `optgen:"default=DefaultTasksTable"`
	historyTable string `optgen:"default=DefaultHistoryTable"`
}
