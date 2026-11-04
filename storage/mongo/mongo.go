// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package mongo

import (
	"context"
	"errors"
	"iter"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/altessa-s/go-horai"

	q "github.com/altessa-s/go-horai/query"
	mongoOptions "go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	// DefaultTasksCollection is the MongoDB collection name used for persisting
	// [horai.TaskState] documents when no override is provided via [WithTasksCollection].
	DefaultTasksCollection = "scheduler_tasks"

	// DefaultHistoryCollection is the MongoDB collection name used for persisting
	// [horai.TaskHistory] documents when no override is provided via [WithHistoryCollection].
	DefaultHistoryCollection = "scheduler_history"
)

// Store implements the [horai.Store] interface using MongoDB as the
// backing store. Task states and execution history are persisted in separate
// collections within the same database.
//
// All methods are safe for concurrent use because they delegate to the
// underlying mongo-driver, which manages its own connection pool.
//
// Create a Store with [New] and optionally call [Store.EnsureIndexes]
// once at startup for optimal query performance.
type Store struct {
	tasks   *mongo.Collection
	history *mongo.Collection
}

// New creates a [Store] backed by the given MongoDB database. By default it
// uses [DefaultTasksCollection] and [DefaultHistoryCollection] as collection
// names; pass [WithTasksCollection] or [WithHistoryCollection] to override them.
//
// The returned Store is ready to use immediately. Call [Store.EnsureIndexes]
// once during application startup to create the indexes required for efficient
// queries.
//
// Example:
//
//	storage, err := schedmongo.New(client.Database("mydb"))
//	s, err := scheduler.New(storage)
func New(db *mongo.Database, opts ...Option) (*Store, error) {
	o := newOptions(opts...)
	if db == nil || o.tasksCollection == "" || o.historyCollection == "" || o.tasksCollection == o.historyCollection {
		return nil, errors.New("invalid mongo store configuration")
	}

	return &Store{
		tasks:   db.Collection(o.tasksCollection),
		history: db.Collection(o.historyCollection),
	}, nil
}

// EnsureIndexes creates or updates MongoDB indexes on the tasks and history
// collections for optimal query performance. It should be called once during
// application startup. The call is idempotent: running it multiple times is
// safe and has no side effects beyond the initial index creation.
//
// The following indexes are created:
//   - tasks: compound (status, next_run_at), descending priority
//   - history: compound (task_id, started_at desc, _id desc), ascending ended_at
//
// Every key must match a bson tag in [taskDocument] / [historyDocument] — an
// index on a field name that no document carries is silently accepted by
// MongoDB and then never used.
//
// History retention is driven by [Store.CleanupHistory], not by a TTL index:
// EndedAt is a Unix timestamp stored as int64, and MongoDB TTL indexes only
// expire documents whose indexed field holds a BSON date. The ascending
// ended_at index exists so that cleanup's range delete is served by an index.
//
// Returns an error if any index creation fails.
//
// Deployments created before this was corrected still carry the earlier
// indexes on the nonexistent fields next_run, start_time, and end_time. They
// are inert but consume write amplification; drop them manually.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	if _, err := s.tasks.Indexes().CreateMany(ctx, taskIndexModels()); err != nil {
		return err
	}

	_, err := s.history.Indexes().CreateMany(ctx, historyIndexModels())

	return err
}

// taskIndexModels returns the indexes maintained on the tasks collection.
// Every key must correspond to a bson tag on [taskDocument]; TestIndexKeys
// enforces that.
func taskIndexModels() []mongo.IndexModel {
	// No _id index is declared. MongoDB maintains a unique one automatically and
	// rejects any attempt to restate it — "the field 'unique' is not valid for
	// an _id index specification" — which made EnsureIndexes fail outright on
	// every call, taking down startup for anyone who checked its error.
	return []mongo.IndexModel{
		{Keys: bson.D{{Key: fieldRunStartedAt, Value: 1}}},
		{
			Keys: bson.D{{Key: fieldStatus, Value: 1}, {Key: fieldNextRunAt, Value: 1}},
		},
		{
			Keys: bson.D{{Key: "priority", Value: -1}},
		},
	}
}

// historyIndexModels returns the indexes maintained on the history collection.
// The compound key mirrors the sort used by both [Store.History] and
// [Store.ListHistory] so each is served by this index; the ascending
// ended_at index serves [Store.CleanupHistory]'s range delete.
func historyIndexModels() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: fieldTaskId, Value: 1},
				{Key: fieldStartedAt, Value: -1},
				{Key: fieldId, Value: -1},
			},
		},
		{
			Keys: bson.D{{Key: fieldEndedAt, Value: 1}},
		},
	}
}

// GetTask retrieves the [horai.TaskState] for the given task ID.
// It returns (nil, nil) when no document matches the ID, allowing callers to
// distinguish "not found" from a storage error.
func (s *Store) GetTask(ctx context.Context, id string) (*horai.TaskState, error) {
	var doc taskDocument

	err := s.tasks.FindOne(ctx, bson.M{fieldId: id}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil //nolint:nilnil // nil state with nil error indicates "not found"
		}

		return nil, err
	}

	return doc.toTaskState(), nil
}

// CreateTask inserts the task document with revision one. The _id primary key
// makes the insert atomic insert-if-absent: a duplicate-key error means the
// task already exists and is reported as (false, nil).
func (s *Store) CreateTask(ctx context.Context, state *horai.TaskState) (bool, error) {
	doc := newTaskDocument(state)

	doc.Revision = 1
	if _, err := s.tasks.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil
		}

		return false, err
	}

	return true, nil
}

// nextRevision is the aggregation expression for the stored revision plus one.
var nextRevision = bson.M{"$add": bson.A{bson.M{opIfNull: bson.A{"$revision", 0}}, 1}}

// ReplaceTaskIf replaces the document via a single conditional ReplaceOne whose
// filter carries the fence, so the compare and the write are atomic. Fields
// stored with omitempty match their zero value when absent.
func (s *Store) ReplaceTaskIf(ctx context.Context, state *horai.TaskState, expect horai.TaskFence) (bool, error) {
	filter := bson.M{
		fieldId:           state.ID,
		fieldStatus:       int32(expect.Status),
		fieldNextRunAt:    zeroOrMissing(expect.NextRunAt),
		fieldLastRunId:    zeroOrMissing(expect.LastRunID),
		fieldRunStartedAt: zeroOrMissing(expect.RunStartedAt),
		fieldRevision:     zeroOrMissing(expect.Revision),
	}
	doc := newTaskDocument(state)
	doc.Revision = expect.Revision + 1

	res, err := s.tasks.ReplaceOne(ctx, filter, doc)
	if err != nil {
		return false, err
	}

	return res.MatchedCount == 1, nil
}

// zeroOrMissing builds a filter value that also matches an absent field when v
// is the zero value, mirroring the omitempty encoding of the task document.
func zeroOrMissing[T comparable](v T) any {
	var zero T
	if v == zero {
		return bson.M{"$in": bson.A{zero, nil}}
	}

	return v
}

// ClaimRun applies the claim rule of [horai.Store.ClaimRun] via a single
// conditional UpdateOne. The filter matches status==active, an absent or zero
// run_started_at (no unfinished run), next_run_at and run_at (an absent field
// matching zero), so MongoDB's atomic document update guarantees that at most
// one concurrent caller flips the document and thus wins the claim.
func (s *Store) ClaimRun(ctx context.Context, id string, claim horai.RunClaim) (bool, error) {
	if err := claim.Validate(); err != nil {
		return false, err
	}

	filter := bson.M{
		fieldId:           id,
		fieldStatus:       int32(horai.TaskStatusActive),
		fieldRunStartedAt: zeroOrMissing(int64(0)),
		fieldNextRunAt:    zeroOrMissing(claim.NextRunAt),
		"run_at":          zeroOrMissing(claim.RunAt),
	}
	update := bson.M{
		opSet: bson.M{
			fieldStatus:        int32(horai.TaskStatusRunning),
			fieldRunStartedAt:  claim.StartedAt,
			fieldLastRunId:     claim.RunID,
			fieldRunLeaseUntil: claim.LeaseUntil,
			fieldRunLeaseId:    claim.RunID,
			"updated_at":       claim.StartedAt,
		},
		"$inc": bson.M{fieldRevision: 1},
	}

	res, err := s.tasks.UpdateOne(ctx, filter, update)
	if err != nil {
		return false, err
	}

	return res.ModifiedCount == 1, nil
}

// DeleteTask removes the task document and all associated history entries for
// the given task ID. Deleting a non-existent task is not considered an error.
// History is deleted first so a cleanup failure leaves the task retryable.
func (s *Store) DeleteTask(ctx context.Context, id string) error {
	// Delete associated history before removing the registration.
	if _, err := s.history.DeleteMany(ctx, bson.M{fieldTaskId: id}); err != nil {
		return err
	}

	_, err := s.tasks.DeleteOne(ctx, bson.M{fieldId: id})

	return err
}

// Tasks returns an iterator over all [horai.TaskState] documents, sorted by
// ID in ascending order. The underlying MongoDB cursor is opened lazily on first
// iteration and closed automatically when the iterator is exhausted or abandoned
// early via a break. If a cursor or decode error occurs, it is yielded as the
// error element and iteration stops.
func (s *Store) Tasks(ctx context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		findOpts := mongoOptions.Find().SetSort(bson.D{{Key: fieldId, Value: 1}})

		cursor, err := s.tasks.Find(ctx, bson.M{}, findOpts)
		if err != nil {
			yield(nil, err)
			return
		}

		defer func() { _ = cursor.Close(ctx) }()

		for cursor.Next(ctx) {
			var doc taskDocument
			if err := cursor.Decode(&doc); err != nil {
				yield(nil, err)
				return
			}

			if !yield(doc.toTaskState(), nil) {
				return
			}
		}

		if err := cursor.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// DueTasks returns an iterator over the task documents eligible for dispatch at
// now: status active and next_run_at at or before now, sorted by ID ascending.
// A zero NextRunAt is stored as an absent field (omitempty), which a range
// never matches, so for now >= 0 an absent next_run_at is matched as the zero
// it stands for. Both branches are served by the compound (status,
// next_run_at) index created in [Store.EnsureIndexes] — an absent field is
// indexed as null — so a tick costs a range scan over the due tasks rather
// than a full collection fetch. The cursor lifecycle matches [Store.Tasks].
func (s *Store) DueTasks(ctx context.Context, now int64) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		due := bson.M{fieldNextRunAt: bson.M{"$lte": now}}

		query := bson.M{fieldStatus: int32(horai.TaskStatusActive)}
		if now >= 0 {
			query["$or"] = bson.A{due, bson.M{fieldNextRunAt: nil}}
		} else {
			query[fieldNextRunAt] = due[fieldNextRunAt]
		}

		findOpts := mongoOptions.Find().SetSort(bson.D{{Key: fieldId, Value: 1}})

		cursor, err := s.tasks.Find(ctx, query, findOpts)
		if err != nil {
			yield(nil, err)
			return
		}

		defer func() { _ = cursor.Close(ctx) }()

		for cursor.Next(ctx) {
			var doc taskDocument
			if err := cursor.Decode(&doc); err != nil {
				yield(nil, err)
				return
			}

			if !yield(doc.toTaskState(), nil) {
				return
			}
		}

		if err := cursor.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// AddHistory inserts a [horai.TaskHistory] document into the history
// collection. Each call creates a new document; duplicates are not checked.
func (s *Store) AddHistory(ctx context.Context, history *horai.TaskHistory) error {
	doc := newHistoryDocument(history)
	_, err := s.history.InsertOne(ctx, doc)

	return err
}

// History returns an iterator over [horai.TaskHistory] entries for the given
// task ID, ordered by StartedAt descending (most recent first) with _id
// descending as tie-breaker — the same total order [Store.ListHistory]
// produces. The cursor lifecycle follows the same semantics as [Store.Tasks]:
// it is closed on exhaustion or early break, and errors are yielded inline.
func (s *Store) History(ctx context.Context, id string) iter.Seq2[*horai.TaskHistory, error] {
	return func(yield func(*horai.TaskHistory, error) bool) {
		findOpts := mongoOptions.Find().SetSort(bson.D{
			{Key: fieldStartedAt, Value: -1},
			{Key: fieldId, Value: -1},
		})

		cursor, err := s.history.Find(ctx, bson.M{fieldTaskId: id}, findOpts)
		if err != nil {
			yield(nil, err)
			return
		}

		defer func() { _ = cursor.Close(ctx) }()

		for cursor.Next(ctx) {
			var doc historyDocument
			if err := cursor.Decode(&doc); err != nil {
				yield(nil, err)
				return
			}

			if !yield(doc.toTaskHistory(), nil) {
				return
			}
		}

		if err := cursor.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// CleanupHistory deletes all history entries whose EndedAt timestamp is older
// than the given retention duration measured from the current wall-clock time.
// For example, a retention of 7*24*time.Hour removes entries that ended more
// than seven days ago. Returns an error if the delete operation fails.
func (s *Store) CleanupHistory(ctx context.Context, cutoff int64) error {
	_, err := s.history.DeleteMany(ctx, bson.M{fieldEndedAt: bson.M{opLt: cutoff}})
	return err
}

// ListTasks returns up to (pg.Limit+1) task states whose ID is
// lexicographically greater than pg.AfterID, sorted by ID ascending. When f
// is non-nil the filter AST is translated to a bson.M and merged into the
// query so MongoDB evaluates it server-side.
func (s *Store) ListTasks(ctx context.Context, pg horai.TaskPage, f q.Expr) ([]*horai.TaskState, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}

	query := bson.M{}
	if pg.AfterID != "" {
		query[fieldId] = bson.M{"$gt": pg.AfterID}
	}

	if f != nil {
		filterBson, err := translate(f, q.TaskFields())
		if err != nil {
			return nil, err
		}
		// Wrap existing query + filter in $and to avoid key conflicts
		query = bson.M{opAnd: bson.A{query, filterBson}}
	}

	findOpts := mongoOptions.Find().
		SetSort(bson.D{{Key: fieldId, Value: 1}}).
		SetLimit(pg.Limit + 1)

	cursor, err := s.tasks.Find(ctx, query, findOpts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var results []*horai.TaskState

	for cursor.Next(ctx) {
		var doc taskDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		results = append(results, doc.toTaskState())
	}

	return results, cursor.Err()
}

// ListHistory returns up to (pg.Limit+1) history entries for taskID,
// sorted by StartedAt descending with ID descending as tie-breaker, starting
// after the cursor position in pg. When f is non-nil the filter AST is
// translated to a bson.M and merged into the query for server-side evaluation.
func (s *Store) ListHistory(ctx context.Context, taskID string, pg horai.HistoryPage, f q.Expr) ([]*horai.TaskHistory, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}

	query := bson.M{fieldTaskId: taskID}
	if pg.AfterID != "" {
		// Compound cursor seek: entries with start_time < cursor, OR same
		// start_time but _id < cursor (descending order).
		query["$or"] = bson.A{
			bson.M{fieldStartedAt: bson.M{opLt: pg.AfterStartedAt}},
			bson.M{fieldStartedAt: pg.AfterStartedAt, fieldId: bson.M{opLt: pg.AfterID}},
		}
	}

	if f != nil {
		filterBson, err := translate(f, q.HistoryFields())
		if err != nil {
			return nil, err
		}
		// Wrap existing query + filter in $and to avoid key conflicts
		query = bson.M{opAnd: bson.A{query, filterBson}}
	}

	findOpts := mongoOptions.Find().
		SetSort(bson.D{{Key: fieldStartedAt, Value: -1}, {Key: fieldId, Value: -1}}).
		SetLimit(pg.Limit + 1)

	cursor, err := s.history.Find(ctx, query, findOpts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()

	var results []*horai.TaskHistory

	for cursor.Next(ctx) {
		var doc historyDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, err
		}

		results = append(results, doc.toTaskHistory())
	}

	return results, cursor.Err()
}

// Compile-time interface check
var _ horai.Store = (*Store)(nil)

// ownedRunFilter matches task id while runID owns its unfinished run, the
// ownership predicate of [horai.Store.RenewRun] and
// [horai.Store.FinishRun]: run_started_at present and non-zero.
func ownedRunFilter(id, runID string) bson.M {
	return bson.M{fieldId: id, fieldLastRunId: runID, fieldRunStartedAt: bson.M{"$nin": bson.A{0, nil}}}
}

// FinishRun applies the finish transition of [horai.Store.FinishRun] as
// a single conditional pipeline update, so the server evaluates ownership and
// the concurrent status and configuration together.
func (s *Store) FinishRun(ctx context.Context, id, runID string, result horai.RunResult) (bool, error) {
	if runID == "" {
		return false, nil
	}

	oneShot := bson.M{opIfNull: bson.A{"$one_shot", false}}
	// executed: a one-shot task still registered for the occurrence this run
	// executed (re-registration may have moved it to another RunAt meanwhile).
	executed := bson.M{opAnd: bson.A{
		oneShot,
		bson.M{opEq: bson.A{bson.M{opIfNull: bson.A{"$run_at", 0}}, result.RunAt}},
	}}
	completed := bson.M{"$or": bson.A{executed, bson.M{opAnd: bson.A{
		bson.M{opEq: bson.A{oneShot, false}},
		bson.M{opEq: bson.A{"$schedule", bson.M{opLiteral: result.Schedule}}},
		result.NextRunAt == 0,
	}}}}

	var failures any = 0
	if !result.Success {
		failures = bson.M{"$add": bson.A{bson.M{opIfNull: bson.A{"$failures", 0}}, 1}}
	}

	update := mongo.Pipeline{bson.D{{Key: opSet, Value: bson.M{
		// A finished one-shot occurrence is terminal whatever the status; otherwise
		// a running task returns to active unless management changed its status.
		fieldStatus: bson.M{opCond: bson.A{completed, horai.TaskStatusCompleted,
			bson.M{opCond: bson.A{
				bson.M{opEq: bson.A{"$status", horai.TaskStatusRunning}}, horai.TaskStatusActive, "$status",
			}},
		}},
		fieldNextRunAt: bson.M{opCond: bson.A{executed, 0,
			bson.M{opCond: bson.A{oneShot, "$next_run_at",
				bson.M{opCond: bson.A{bson.M{opEq: bson.A{"$schedule", bson.M{opLiteral: result.Schedule}}}, result.NextRunAt, "$next_run_at"}},
			}},
		}},
		"last_run_at": result.StartedAt, "updated_at": result.EndedAt,
		fieldRunStartedAt: 0, fieldRunLeaseUntil: 0, fieldRunLeaseId: "", "failures": failures,
		fieldRevision: nextRevision,
	}}}}

	res, err := s.tasks.UpdateOne(ctx, ownedRunFilter(id, runID), update)
	if err != nil {
		return false, err
	}

	return res.MatchedCount == 1, nil
}

// RenewRun extends the lease of the unfinished run runID via a single
// conditional UpdateOne; see [horai.Store.RenewRun].
func (s *Store) RenewRun(ctx context.Context, id, runID string, leaseUntil int64) (bool, error) {
	if runID == "" {
		return false, nil
	}

	res, err := s.tasks.UpdateOne(ctx, ownedRunFilter(id, runID),
		bson.M{opSet: bson.M{fieldRunLeaseUntil: leaseUntil, fieldRunLeaseId: runID}, "$inc": bson.M{fieldRevision: 1}},
	)
	if err != nil {
		return false, err
	}

	return res.MatchedCount == 1, nil
}

// UnfinishedRuns uses the run-start index without restricting status.
func (s *Store) UnfinishedRuns(ctx context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		cur, err := s.tasks.Find(ctx, bson.M{fieldRunStartedAt: bson.M{"$nin": bson.A{0, nil}}}, mongoOptions.Find().SetSort(bson.D{{Key: fieldId, Value: 1}}))
		if err != nil {
			yield(nil, err)
			return
		}
		defer func() { _ = cur.Close(ctx) }()

		for cur.Next(ctx) {
			var d taskDocument
			if err := cur.Decode(&d); err != nil {
				yield(nil, err)
				return
			}

			if !yield(d.toTaskState(), nil) {
				return
			}
		}

		if err := cur.Err(); err != nil {
			yield(nil, err)
		}
	}
}

// DeleteHistory removes a task's history independently of its state.
func (s *Store) DeleteHistory(ctx context.Context, id string) error {
	_, err := s.history.DeleteMany(ctx, bson.M{fieldTaskId: id})
	return err
}
