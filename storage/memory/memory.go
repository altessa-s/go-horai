// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package memory

import (
	"cmp"
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"sort"
	"sync"

	"github.com/altessa-s/go-horai"
	"github.com/altessa-s/go-horai/query"
)

// Store is an in-memory implementation of [horai.Store] that keeps task
// state and execution history in plain Go maps protected by a [sync.RWMutex].
//
// All methods are safe for concurrent use. Returned [horai.TaskState] and
// [horai.TaskHistory] values are deep copies, so callers may modify them
// without affecting the data held by Store.
type Store struct {
	mu      sync.RWMutex
	tasks   map[string]*horai.TaskState
	history map[string][]*horai.TaskHistory
	maxHist int // Maximum history entries per task
}

// New creates an isolated in-memory store. A positive history limit is required.
func New(opts ...Option) (*Store, error) {
	o := newOptions(opts...)
	if o.maxHistoryPerTask <= 0 {
		return nil, errors.New("history limit must be positive")
	}
	return &Store{tasks: make(map[string]*horai.TaskState), history: make(map[string][]*horai.TaskHistory), maxHist: o.maxHistoryPerTask}, nil
}

// GetTask retrieves the current [horai.TaskState] for the given id.
// It returns (nil, nil) when the task does not exist; callers should check
// for a nil state to distinguish "not found" from an error.
func (m *Store) GetTask(_ context.Context, id string) (*horai.TaskState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	state, ok := m.tasks[id]
	if !ok {
		return nil, nil //nolint:nilnil // nil state with nil error indicates "not found"
	}

	// Return a clone to prevent external mutation
	return cloneTaskState(state), nil
}

// CreateTask stores a deep copy of state with revision one only when no task
// with state.ID exists; otherwise it returns false and leaves the stored task
// untouched.
func (m *Store) CreateTask(_ context.Context, state *horai.TaskState) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.tasks[state.ID]; ok {
		return false, nil
	}
	next := cloneTaskState(state)
	next.Revision = 1
	m.tasks[state.ID] = next
	return true, nil
}

// ReplaceTaskIf replaces the stored state only while it still matches expect.
func (m *Store) ReplaceTaskIf(_ context.Context, state *horai.TaskState, expect horai.TaskFence) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cur, ok := m.tasks[state.ID]
	if !ok || horai.FenceOf(cur) != expect {
		return false, nil
	}
	next := cloneTaskState(state)
	next.Revision = cur.Revision + 1
	m.tasks[state.ID] = next
	return true, nil
}

// ClaimRun applies the claim rule of [horai.Store.ClaimRun]. Because all
// access is serialized by the storage mutex, the read-check-write is a single
// critical section, so two concurrent callers can never both claim the same
// occurrence.
func (m *Store) ClaimRun(_ context.Context, id string, claim horai.RunClaim) (bool, error) {
	if err := claim.Validate(); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.tasks[id]
	if !ok || state.Status != horai.TaskStatusActive || state.RunStartedAt != 0 {
		return false, nil
	}
	if state.NextRunAt != claim.NextRunAt || state.RunAt != claim.RunAt {
		return false, nil
	}

	state.Status = horai.TaskStatusRunning
	state.RunStartedAt = claim.StartedAt
	state.LastRunID = claim.RunID
	state.RunLeaseUntil = claim.LeaseUntil
	state.RunLeaseID = claim.RunID
	state.UpdatedAt = claim.StartedAt
	state.Revision++
	return true, nil
}

// DeleteTask removes the [horai.TaskState] and all associated
// [horai.TaskHistory] entries for the given id. Deleting a non-existent
// task is a no-op and does not return an error.
func (m *Store) DeleteTask(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.history, id)
	delete(m.tasks, id)

	return nil
}

// Tasks returns an [iter.Seq2] iterator that yields every stored
// [horai.TaskState] sorted lexicographically by ID. Each yielded value is
// a deep copy. The read lock is held for the entire iteration, so callers
// should consume the sequence promptly to avoid blocking writers.
func (m *Store) Tasks(_ context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		m.mu.RLock()

		// Collect and sort for consistent ordering
		states := make([]*horai.TaskState, 0, len(m.tasks))
		for _, state := range m.tasks {
			states = append(states, cloneTaskState(state))
		}
		slices.SortFunc(states, func(a, b *horai.TaskState) int {
			return cmp.Compare(a.ID, b.ID)
		})

		m.mu.RUnlock()
		for _, state := range states {
			if !yield(state, nil) {
				return
			}
		}
	}
}

// DueTasks returns an [iter.Seq2] iterator that yields the [horai.TaskState]
// values eligible for dispatch at now — status active and NextRunAt at or
// before now — sorted lexicographically by ID. Each yielded value is a deep
// copy. The predicate is applied while the read lock is held, so unlike
// [Store.Tasks] the caller never sees, or pays to clone, the tasks it would
// immediately discard.
func (m *Store) DueTasks(_ context.Context, now int64) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		m.mu.RLock()

		states := make([]*horai.TaskState, 0, len(m.tasks))
		for _, state := range m.tasks {
			if state.Status != horai.TaskStatusActive || state.NextRunAt > now {
				continue
			}
			states = append(states, cloneTaskState(state))
		}
		slices.SortFunc(states, func(a, b *horai.TaskState) int {
			return cmp.Compare(a.ID, b.ID)
		})

		m.mu.RUnlock()
		for _, state := range states {
			if !yield(state, nil) {
				return
			}
		}
	}
}

// AddHistory appends a [horai.TaskHistory] entry for the task identified by
// history.TaskID. A shallow copy of the struct is stored. If the per-task
// history limit (set via [New]) is exceeded, the oldest entry is evicted.
func (m *Store) AddHistory(_ context.Context, history *horai.TaskHistory) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store a clone
	clone := *history
	m.history[history.TaskID] = append(m.history[history.TaskID], &clone)

	// Trim if over limit
	if len(m.history[history.TaskID]) > m.maxHist {
		m.history[history.TaskID] = m.history[history.TaskID][1:]
	}

	return nil
}

// History returns an [iter.Seq2] iterator that yields [horai.TaskHistory]
// entries for the given task id, ordered by StartedAt descending (most recent
// first). Each yielded value is a shallow copy. If no history exists for the
// task the iterator yields zero elements. The read lock is held for the entire
// iteration.
func (m *Store) History(_ context.Context, id string) iter.Seq2[*horai.TaskHistory, error] {
	return func(yield func(*horai.TaskHistory, error) bool) {
		m.mu.RLock()

		hist, ok := m.history[id]
		if !ok || len(hist) == 0 {
			m.mu.RUnlock()
			return
		}

		// Sort descending by start time
		sorted := make([]*horai.TaskHistory, len(hist))
		for i, h := range hist {
			clone := *h
			sorted[i] = &clone
		}
		slices.SortFunc(sorted, func(a, b *horai.TaskHistory) int {
			return cmp.Compare(b.StartedAt, a.StartedAt)
		})

		m.mu.RUnlock()
		for _, h := range sorted {
			clone := *h
			if !yield(&clone, nil) {
				return
			}
		}
	}
}

// CleanupHistory deletes all [horai.TaskHistory] entries whose EndedAt
// timestamp is older than time.Now minus the given retention duration. The
// operation scans every task's history slice in-place.
func (m *Store) CleanupHistory(_ context.Context, cutoff int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for taskID, hist := range m.history {
		// Use slices.DeleteFunc for efficient in-place filtering
		m.history[taskID] = slices.DeleteFunc(hist, func(h *horai.TaskHistory) bool {
			return h.EndedAt < cutoff
		})
	}

	return nil
}

// cloneTaskState creates a deep copy of a TaskState.
func cloneTaskState(state *horai.TaskState) *horai.TaskState {
	clone := *state
	if state.Meta != nil {
		clone.Meta = maps.Clone(state.Meta)
	}
	return &clone
}

// ListTasks returns up to (pg.Limit+1) task states whose ID is
// lexicographically greater than pg.AfterID, sorted by ID ascending.
// When f is non-nil, only tasks matching the filter are included; a filter
// naming a field outside [horai.TaskFilterFields] fails whatever is stored.
func (m *Store) ListTasks(_ context.Context, pg horai.TaskPage, f query.Expr) ([]*horai.TaskState, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}

	if f != nil {
		if err := query.Validate(f, query.TaskFields()); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Collect and sort by ID
	states := make([]*horai.TaskState, 0, len(m.tasks))
	for _, state := range m.tasks {
		states = append(states, cloneTaskState(state))
	}
	slices.SortFunc(states, func(a, b *horai.TaskState) int {
		return cmp.Compare(a.ID, b.ID)
	})

	// Binary search for the start position after afterID
	startIdx := 0
	if pg.AfterID != "" {
		startIdx = sort.Search(len(states), func(i int) bool {
			return states[i].ID > pg.AfterID
		})
	}

	if f == nil {
		// No filter: return up to limit+1 items directly
		endIdx := min(startIdx+int(pg.Limit)+1, len(states))
		return states[startIdx:endIdx], nil
	}

	// With filter: scan and collect matching items up to limit+1
	var results []*horai.TaskState
	for _, state := range states[startIdx:] {
		match := query.Match(f, state.QueryRecord())
		if match {
			results = append(results, state)
			if int64(len(results)) > pg.Limit {
				break
			}
		}
	}
	return results, nil
}

// ListHistory returns up to (pg.Limit+1) history entries for taskID,
// sorted by StartedAt descending with ID descending as a tie-breaker.
// When f is non-nil, only entries matching the filter are included; a filter
// naming a field outside [horai.HistoryFilterFields] fails whatever is
// stored.
func (m *Store) ListHistory(_ context.Context, taskID string, pg horai.HistoryPage, f query.Expr) ([]*horai.TaskHistory, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}

	if f != nil {
		if err := query.Validate(f, query.HistoryFields()); err != nil {
			return nil, err
		}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()

	hist, ok := m.history[taskID]
	if !ok || len(hist) == 0 {
		return nil, nil
	}

	sorted := sortedHistoryDesc(hist)
	startIdx, ok := historyCursorStart(sorted, pg)
	if !ok {
		return nil, nil
	}

	if f == nil {
		// No filter: return up to limit+1 items directly
		endIdx := min(startIdx+int(pg.Limit)+1, len(sorted))

		result := make([]*horai.TaskHistory, 0, endIdx-startIdx)
		for _, h := range sorted[startIdx:endIdx] {
			clone := *h
			result = append(result, &clone)
		}
		return result, nil
	}

	// With filter: scan and collect matching items up to limit+1
	var results []*horai.TaskHistory
	for _, h := range sorted[startIdx:] {
		match := query.Match(f, h.QueryRecord())
		if !match {
			continue
		}
		clone := *h
		results = append(results, &clone)
		if int64(len(results)) > pg.Limit {
			break
		}
	}
	return results, nil
}

// sortedHistoryDesc returns a copy of hist sorted descending by StartedAt,
// then descending by ID (tie-breaker).
func sortedHistoryDesc(hist []*horai.TaskHistory) []*horai.TaskHistory {
	sorted := slices.Clone(hist)
	slices.SortFunc(sorted, func(a, b *horai.TaskHistory) int {
		if c := cmp.Compare(b.StartedAt, a.StartedAt); c != 0 {
			return c
		}
		return cmp.Compare(b.ID, a.ID)
	})
	return sorted
}

// historyCursorStart finds the cursor position in sorted: the first entry
// strictly past the cursor in descending order. An entry is past the cursor
// if its StartedAt is less than AfterStartedAt, or if StartedAt is equal but
// ID is less. ok is false when no entry lies past the cursor.
func historyCursorStart(sorted []*horai.TaskHistory, pg horai.HistoryPage) (start int, ok bool) {
	if pg.AfterID == "" {
		return 0, true
	}
	for i, h := range sorted {
		if h.StartedAt < pg.AfterStartedAt || (h.StartedAt == pg.AfterStartedAt && h.ID < pg.AfterID) {
			return i, true
		}
	}
	return 0, false
}

// Compile-time interface check
var _ horai.Store = (*Store)(nil)

// ownsUnfinishedRun reports whether runID owns the unfinished run of state,
// the ownership predicate of [horai.Store.RenewRun] and
// [horai.Store.FinishRun]. state may be nil.
func ownsUnfinishedRun(state *horai.TaskState, runID string) bool {
	return state != nil && runID != "" && state.LastRunID == runID && state.RunStartedAt != 0
}

// FinishRun applies the finish transition of [horai.Store.FinishRun]
// under the storage mutex.
func (m *Store) FinishRun(_ context.Context, id, runID string, result horai.RunResult) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.tasks[id]
	if !ownsUnfinishedRun(state, runID) {
		return false, nil
	}
	// A one-shot task still registered for the occurrence this run executed is
	// terminal, even if it was paused, disabled or resumed while it ran.
	executed := state.OneShot && state.RunAt == result.RunAt
	switch {
	case executed || (!state.OneShot && state.Schedule == result.Schedule && result.NextRunAt == 0):
		state.Status = horai.TaskStatusCompleted
		state.NextRunAt = 0
	case state.Status == horai.TaskStatusRunning:
		state.Status = horai.TaskStatusActive
	}
	if !state.OneShot && state.Schedule == result.Schedule {
		state.NextRunAt = result.NextRunAt
	}
	state.LastRunAt = result.StartedAt
	state.RunStartedAt = 0
	state.RunLeaseUntil = 0
	state.RunLeaseID = ""
	state.UpdatedAt = result.EndedAt
	if result.Success {
		state.Failures = 0
	} else {
		state.Failures++
	}
	state.Revision++
	return true, nil
}

// RenewRun extends the lease of the unfinished run runID to leaseUntil under
// the storage mutex; see [horai.Store.RenewRun].
func (m *Store) RenewRun(_ context.Context, id, runID string, leaseUntil int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.tasks[id]
	if !ownsUnfinishedRun(state, runID) {
		return false, nil
	}
	state.RunLeaseUntil = leaseUntil
	state.RunLeaseID = runID
	state.Revision++
	return true, nil
}

// UnfinishedRuns snapshots all unfinished runs regardless of task status.
func (m *Store) UnfinishedRuns(_ context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		m.mu.RLock()
		var states []*horai.TaskState
		for _, s := range m.tasks {
			if s.RunStartedAt != 0 {
				states = append(states, cloneTaskState(s))
			}
		}
		m.mu.RUnlock()
		slices.SortFunc(states, func(a, b *horai.TaskState) int { return cmp.Compare(a.ID, b.ID) })
		for _, s := range states {
			if !yield(s, nil) {
				return
			}
		}
	}
}

// DeleteHistory removes all history for one task.
func (m *Store) DeleteHistory(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.history, id)
	return nil
}
