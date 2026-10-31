// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package scheduler

import (
	"time"

	"github.com/altessa-s/go-horai/task"
)

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type noopMetrics struct{}

func (noopMetrics) TaskDispatched(string, task.Priority)                     {}
func (noopMetrics) TaskFinished(string, task.Priority, time.Duration, error) {}
func (noopMetrics) TaskSkipped(string)                                       {}
func (noopMetrics) DispatchLag(string, time.Duration)                        {}
func (noopMetrics) StaleRecovered()                                          {}
func (noopMetrics) TickDone(time.Duration)                                   {}
func (noopMetrics) RunningChanged(int)                                       {}
func (noopMetrics) RegisteredChanged(int)                                    {}
func (noopMetrics) StorageError(string)                                      {}
