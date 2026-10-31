// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

//go:generate optgen generate --type=options
//go:generate sh -c "cat ../../docs/generated-header.txt options_gen.go > .header.tmp && mv .header.tmp options_gen.go"

type options struct {
	tasksCollection   string `optgen:"default=DefaultTasksCollection"`
	historyCollection string `optgen:"default=DefaultHistoryCollection"`
}
