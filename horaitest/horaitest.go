// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package horaitest

import (
	"context"
	"sync"
	"time"

	"github.com/altessa-s/go-atlas-core/time/clock"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/task"
)

// Clock is the shared core fake clock.
type Clock = clock.Fake

// NewClock returns a clock at now.
func NewClock(now time.Time) *Clock { return clock.NewFake(now) }

// Event is one metrics observation.
type Event struct {
	Kind, ID string
	Priority task.Priority
	Duration time.Duration
	Err      error
	Count    int
}

// Metrics records observations for inspection.
type Metrics struct {
	mu     sync.Mutex
	events []Event
}

func (m *Metrics) add(e Event) { m.mu.Lock(); defer m.mu.Unlock(); m.events = append(m.events, e) }

// Events returns an independent snapshot.
func (m *Metrics) Events() []Event {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]Event(nil), m.events...)
}

// TaskDispatched records a "dispatched" event for the task.
func (m *Metrics) TaskDispatched(id string, p task.Priority) {
	m.add(Event{Kind: "dispatched", ID: id, Priority: p})
}

// TaskFinished records a "finished" event with the run duration and error.
func (m *Metrics) TaskFinished(id string, p task.Priority, d time.Duration, err error) {
	m.add(Event{Kind: "finished", ID: id, Priority: p, Duration: d, Err: err})
}

// TaskSkipped records a "skipped" event for the task.
func (m *Metrics) TaskSkipped(id string) { m.add(Event{Kind: "skipped", ID: id}) }

// DispatchLag records a "lag" event with the dispatch delay.
func (m *Metrics) DispatchLag(id string, d time.Duration) {
	m.add(Event{Kind: "lag", ID: id, Duration: d})
}

// StaleRecovered records a "recovered" event.
func (m *Metrics) StaleRecovered() { m.add(Event{Kind: "recovered"}) }

// TickDone records a "tick" event with the tick duration.
func (m *Metrics) TickDone(d time.Duration) { m.add(Event{Kind: "tick", Duration: d}) }

// RunningChanged records a "running" event with the running count.
func (m *Metrics) RunningChanged(n int) { m.add(Event{Kind: "running", Count: n}) }

// RegisteredChanged records a "registered" event with the registered count.
func (m *Metrics) RegisteredChanged(n int) { m.add(Event{Kind: "registered", Count: n}) }

// StorageError records a "storage-error" event with the failed operation in ID.
func (m *Metrics) StorageError(op string) { m.add(Event{Kind: "storage-error", ID: op}) }

// FaultStore overrides selected storage calls while forwarding the remaining contract.
// Set hooks before concurrent use. A hook can wait for ctx cancellation to model a timeout.
type FaultStore struct {
	horai.Store
	BeforeCreate func(context.Context, *horai.TaskState) error
	BeforeGet    func(context.Context, string) error
}

// CreateTask runs BeforeCreate, when set, and forwards to the wrapped store
// unless the hook fails.
func (s *FaultStore) CreateTask(ctx context.Context, v *horai.TaskState) (bool, error) {
	if s.BeforeCreate != nil {
		if err := s.BeforeCreate(ctx, v); err != nil {
			return false, err
		}
	}

	return s.Store.CreateTask(ctx, v)
}

// GetTask runs BeforeGet, when set, and forwards to the wrapped store unless
// the hook fails.
func (s *FaultStore) GetTask(ctx context.Context, id string) (*horai.TaskState, error) {
	if s.BeforeGet != nil {
		if err := s.BeforeGet(ctx, id); err != nil {
			return nil, err
		}
	}

	return s.Store.GetTask(ctx, id)
}
