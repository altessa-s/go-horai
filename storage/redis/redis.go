// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/altessa-s/go-atlas-core/types/nilcheck"

	"github.com/altessa-s/go-horai"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
	q "github.com/altessa-s/go-horai/query"
)

// Key suffixes and constants
const (
	taskKeyPrefix    = "task"
	historyKeyPrefix = "history"
	idxKeyPrefix     = "idx"

	taskIndexSuffix    = "tasks"
	historyIndexSuffix = "history"

	// searchPageSize is how many documents each FT.AGGREGATE round-trip fetches.
	// Reads walk every matching document a page at a time via searchAll rather
	// than issuing one capped request.
	searchPageSize = 1000
)

// Store provides a Redis-backed implementation of [horai.Store]
// using RedisJSON for document storage and RediSearch for indexed querying.
//
// All methods are safe for concurrent use by multiple goroutines.
// Thread safety is delegated to the underlying [redis.UniversalClient].
//
// Create a Store with [New] and call [Store.EnsureIndexes] once at startup.
//
// Key patterns used:
//   - Task state: {prefix}:task:{task_id} (JSON document via JSON.SET)
//   - History entry: {prefix}:history:{task_id}:{history_id} (JSON document via JSON.SET)
//   - Task index: {prefix}:idx:tasks:v2 (RediSearch FT index, created by [Store.EnsureIndexes])
//   - History index: {prefix}:idx:history:v2 (RediSearch FT index, created by [Store.EnsureIndexes])
type Store struct {
	client    redis.UniversalClient
	keyPrefix string
	opts      *options
}

// New creates a new [Store] backed by the given Redis client.
// It accepts zero or more [Option] values to override default configuration.
//
// New returns an error if client is nil. The client is used as-is; callers are
// responsible for its lifecycle (connection pooling, closing, etc.).
//
// After calling New, invoke [Store.EnsureIndexes] to create the
// RediSearch indexes required for query operations.
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	storage, err := redis.New(rdb, redis.WithKeyPrefix("myapp:scheduler"))
//	if err := storage.EnsureIndexes(ctx); err != nil { ... }
func New(client redis.UniversalClient, opts ...Option) (*Store, error) {
	if nilcheck.IsNil(client) {
		return nil, errors.New("redis client must not be nil")
	}

	o := newOptions(opts...)
	if strings.Trim(o.keyPrefix, ":") == "" || o.historyTTL < 0 || o.maxHistoryPerTask <= 0 {
		return nil, errors.New("invalid redis store options")
	}

	// Normalize prefix: remove trailing colon if present
	prefix := strings.TrimSuffix(o.keyPrefix, ":")

	return &Store{
		client:    client,
		keyPrefix: prefix,
		opts:      o,
	}, nil
}

// EnsureIndexes creates the RediSearch indexes required by [Store] for
// task and history queries. It is idempotent: indexes that already exist are
// left as they are. It waits for both indexes to finish indexing existing
// documents, respecting context cancellation. Call it during application startup
// before invoking any query methods ([Store.Tasks], [Store.ListTasks],
// [Store.History], [Store.ListHistory], or [Store.CleanupHistory]).
//
// Returns an error if the index creation command fails for a reason other
// than the index already existing.
//
// EnsureIndexes also migrates task documents stored without next_run_at —
// written before the field was always stored, or by an older binary — by
// setting it to zero, so DueTasks can find them (see backfillNextRunAt). Run
// it again after a rolling upgrade to cover documents older instances wrote
// meanwhile.
func (s *Store) EnsureIndexes(ctx context.Context) error {
	if err := s.ensureTaskIndex(ctx); err != nil {
		return coreerrs.WrapOperation(err, "ensure task index")
	}
	if err := s.ensureHistoryIndex(ctx); err != nil {
		return coreerrs.WrapOperation(err, "ensure history index")
	}
	if err := s.waitForIndex(ctx, s.historyIndexName()); err != nil {
		return coreerrs.WrapOperation(err, "wait for history index")
	}
	if err := s.backfillNextRunAt(ctx); err != nil {
		return coreerrs.WrapOperation(err, "backfill task next_run_at")
	}
	return nil
}

// key builds a Redis key with the configured prefix.
func (s *Store) key(parts ...string) string {
	if s.keyPrefix == "" {
		return strings.Join(parts, ":")
	}
	return s.keyPrefix + ":" + strings.Join(parts, ":")
}

// taskKey returns the Redis key for a task.
func (s *Store) taskKey(id string) string {
	return s.key(taskKeyPrefix, id)
}

// historyKey returns the Redis key for a history entry.
func (s *Store) historyKey(taskID string, historyID string) string {
	return s.key(historyKeyPrefix, taskID, historyID)
}

// taskIndexName returns the RediSearch index name for tasks.
func (s *Store) taskIndexName() string {
	return s.key(idxKeyPrefix, taskIndexSuffix+":"+taskIndexVersion)
}

// historyIndexName returns the RediSearch index name for history.
func (s *Store) historyIndexName() string {
	return s.key(idxKeyPrefix, historyIndexSuffix+":"+historyIndexVersion)
}

// searchAll traverses indexed sort values and IDs using a keyset. Cursor values
// come from the search reply, so deletion before JSON.GET cannot shift the walk.
// Each body is fetched separately to route it to its cluster slot.
func (s *Store) searchAll(
	ctx context.Context, index, query string, sortBy []redis.FTSearchSortBy, noContent bool,
	fn func(key, body string) (bool, error),
) error {
	history := len(sortBy) > 0 && sortBy[0].FieldName == startedAtField
	var cursor *searchCursor
	for {
		rows, err := s.searchKeyset(ctx, index, query, history, cursor)
		if err != nil {
			return err
		}
		for _, row := range rows {
			cursor = &row
			var body string
			if !noContent {
				body, err = s.client.JSONGet(ctx, row.key, "$").Result()
				if errors.Is(err, redis.Nil) {
					continue
				}
				if err != nil {
					return err
				}
			}
			cont, err := fn(row.key, body)
			if err != nil {
				return err
			}
			if !cont {
				return nil
			}
		}
		if len(rows) < searchPageSize {
			return nil
		}
	}
}

// GetTask retrieves the state of a specific task by its unique identifier.
// If the task does not exist in Redis, it returns (nil, nil).
// Any Redis communication error is returned wrapped with the task ID.
func (s *Store) GetTask(ctx context.Context, id string) (*horai.TaskState, error) {
	result, err := s.client.JSONGet(ctx, s.taskKey(id), "$").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil //nolint:nilnil // nil state with nil error indicates "not found"
		}
		return nil, coreerrs.Wrapf(err, "failed to get task %q", id)
	}

	td, err := unmarshalJSONResult[taskData](result)
	if err != nil {
		return nil, coreerrs.Wrapf(err, "failed to unmarshal task %q", id)
	}

	return td.toTaskState(), nil
}

// revisionLua reads the stored revision of KEYS[1] (0 when absent).
const revisionLua = `
local function revision()
  local v = redis.call('JSON.GET', KEYS[1], '$.revision')
  if (not v) or v == '[]' then return 0 end
  return tonumber(string.match(v, '(-?%d+)'))
end
`

// CreateTask atomically inserts a task only if its ID is absent.
func (s *Store) CreateTask(ctx context.Context, state *horai.TaskState) (bool, error) {
	td := newTaskData(state)
	td.Revision = 1
	doc, err := json.Marshal(td)
	if err != nil {
		return false, coreerrs.Wrapf(err, "failed to marshal task %q", state.ID)
	}
	n, err := createTask.Run(ctx, s.client, []string{s.taskKey(state.ID)}, string(doc)).Int64()
	if err != nil {
		return false, coreerrs.Wrapf(err, "failed to create task %q", state.ID)
	}
	return n == 1, nil
}

// replaceTaskIfScript replaces a task JSON document only while its fence fields
// still match. KEYS[1] is the task key; ARGV = [status, nextRunAt, runStartedAt,
// lastRunID, document, revision]. Absent omitempty fields read as their zero
// values. The document already carries revision+1.
// Returns 1 when the document was replaced, 0 otherwise.
const replaceTaskIfScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local function num(path)
  local v = redis.call('JSON.GET', KEYS[1], path)
  if (not v) or v == '[]' then return 0 end
  return tonumber(string.match(v, '(-?%d+)'))
end
if num('$.status') ~= tonumber(ARGV[1]) then return 0 end
if num('$.next_run_at') ~= tonumber(ARGV[2]) then return 0 end
if num('$.run_started_at') ~= tonumber(ARGV[3]) then return 0 end
if num('$.revision') ~= tonumber(ARGV[6]) then return 0 end
local id = ''
local v = redis.call('JSON.GET', KEYS[1], '$.last_run_id')
if v and v ~= '[]' then id = cjson.decode(v)[1] end
if id ~= ARGV[4] then return 0 end
redis.call('JSON.SET', KEYS[1], '$', ARGV[5])
return 1
`

// ReplaceTaskIf replaces the task document via replaceTaskIfScript, so the fence
// comparison and the write execute atomically on the server.
func (s *Store) ReplaceTaskIf(ctx context.Context, state *horai.TaskState, expect horai.TaskFence) (bool, error) {
	td := newTaskData(state)
	td.Revision = expect.Revision + 1
	doc, err := json.Marshal(td)
	if err != nil {
		return false, coreerrs.Wrapf(err, "failed to marshal task %q", state.ID)
	}
	res, err := replaceTaskIf.Run(ctx, s.client, []string{s.taskKey(state.ID)},
		int(expect.Status),
		expect.NextRunAt,
		expect.RunStartedAt,
		expect.LastRunID,
		string(doc),
		expect.Revision,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, coreerrs.Wrapf(err, "failed to replace task %q", state.ID)
	}
	n, _ := res.(int64)
	return n == 1, nil
}

// claimRunScript atomically transitions a task JSON document from active→running
// for a specific occurrence. It runs entirely server-side under Redis's single-
// threaded execution, so the read-check-write is atomic: KEYS[1] is the task key;
// ARGV = [activeStatus, expectedNextRunAt, runningStatus, runStartedAt, jsonRunID,
// expectedRunAt, leaseUntil]; run_at is fenced together with next_run_at (an
// absent field counts as zero), and jsonRunID is stored as both last_run_id and
// run_lease_id.
// A task whose previous run is unfinished (run_started_at non-zero) is not
// claimable. Returns 1 when this caller claimed the run, 0 otherwise.
const claimRunScript = revisionLua + `
local s = redis.call('JSON.GET', KEYS[1], '$.status')
if (not s) or s == '[]' then return 0 end
if tonumber(string.match(s, '(-?%d+)')) ~= tonumber(ARGV[1]) then return 0 end
local rs = redis.call('JSON.GET', KEYS[1], '$.run_started_at')
if rs and rs ~= '[]' and tonumber(string.match(rs, '(-?%d+)')) ~= 0 then return 0 end
local nr = redis.call('JSON.GET', KEYS[1], '$.next_run_at')
local storedNext = 0
if nr and nr ~= '[]' then storedNext = tonumber(string.match(nr, '(-?%d+)')) end
if storedNext ~= tonumber(ARGV[2]) then return 0 end
local ra = redis.call('JSON.GET', KEYS[1], '$.run_at')
local storedRun = 0
if ra and ra ~= '[]' then storedRun = tonumber(string.match(ra, '(-?%d+)')) end
if storedRun ~= tonumber(ARGV[6]) then return 0 end
redis.call('JSON.SET', KEYS[1], '$.status', ARGV[3])
redis.call('JSON.SET', KEYS[1], '$.run_started_at', ARGV[4])
redis.call('JSON.SET', KEYS[1], '$.updated_at', ARGV[4])
redis.call('JSON.SET', KEYS[1], '$.last_run_id', ARGV[5])
redis.call('JSON.SET', KEYS[1], '$.run_lease_until', ARGV[7])
redis.call('JSON.SET', KEYS[1], '$.run_lease_id', ARGV[5])
redis.call('JSON.SET', KEYS[1], '$.revision', tostring(revision() + 1))
return 1
`

// ClaimRun applies the claim rule of [horai.Store.ClaimRun] by executing
// claimRunScript. Redis runs the script atomically, so concurrent
// callers cannot both claim one occurrence.
func (s *Store) ClaimRun(ctx context.Context, id string, claim horai.RunClaim) (bool, error) {
	if err := claim.Validate(); err != nil {
		return false, err
	}
	// JSON.SET takes a JSON value: the run ID must be JSON-encoded, not quoted
	// as a Go literal, or instance IDs with control characters break the write.
	runID, err := json.Marshal(claim.RunID)
	if err != nil {
		return false, coreerrs.WrapOperation(err, "encode run id")
	}
	res, err := claimRun.Run(ctx, s.client, []string{s.taskKey(id)},
		int(horai.TaskStatusActive),
		claim.NextRunAt,
		int(horai.TaskStatusRunning),
		claim.StartedAt,
		string(runID),
		claim.RunAt,
		claim.LeaseUntil,
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, coreerrs.WrapOperation(err, "claim task run")
	}
	n, _ := res.(int64)
	return n == 1, nil
}

// DeleteTask removes a task state and all of its associated history entries
// from Redis. History deletion must succeed before the task is removed.
// If the task does not exist, DeleteTask succeeds silently.
func (s *Store) DeleteTask(ctx context.Context, id string) error {
	if err := s.DeleteHistory(ctx, id); err != nil {
		return err
	}
	return s.client.Del(ctx, s.taskKey(id)).Err()
}

// Tasks returns an iterator over all task states, sorted by ID in ascending
// order. Results are traversed using bounded aggregate keyset pages.
// If the iterator encounters a Redis or
// deserialization error, it yields (nil, err) and stops.
func (s *Store) Tasks(ctx context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		sortBy := []redis.FTSearchSortBy{{FieldName: "id", Asc: true}}
		err := s.searchAll(ctx, s.taskIndexName(), "*", sortBy, false,
			func(key, body string) (bool, error) {
				td, parseErr := unmarshalJSONResult[taskData](body)
				if parseErr != nil {
					return false, coreerrs.WrapOperation(parseErr, "parse task document")
				}
				return yield(td.toTaskState(), nil), nil
			})
		if err != nil {
			yield(nil, coreerrs.WrapOperation(err, "search tasks"))
		}
	}
}

// DueTasks returns an iterator over the task states eligible for dispatch at
// now: status active and nextRunAt at or before now, sorted by ID ascending.
// Both fields are indexed as RediSearch numerics, so the predicate is evaluated
// server-side and a tick transfers only the due documents instead of the whole
// task index. A zero NextRunAt is due too: it is stored explicitly, and
// [Store.EnsureIndexes] backfills documents written without it, because the
// range never matches a field RediSearch did not index.
func (s *Store) DueTasks(ctx context.Context, now int64) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		active := int(horai.TaskStatusActive)
		query := fmt.Sprintf("@status:[%d %d] @nextRunAt:[-inf %d]", active, active, now)

		sortBy := []redis.FTSearchSortBy{{FieldName: "id", Asc: true}}
		err := s.searchAll(ctx, s.taskIndexName(), query, sortBy, false,
			func(key, body string) (bool, error) {
				td, parseErr := unmarshalJSONResult[taskData](body)
				if parseErr != nil {
					return false, coreerrs.WrapOperation(parseErr, "parse task document")
				}
				return yield(td.toTaskState(), nil), nil
			})
		if err != nil {
			yield(nil, coreerrs.WrapOperation(err, "search due tasks"))
		}
	}
}

// unmarshalJSONResult parses the JSON array wrapper returned by JSONGet("$").
// RedisJSON returns `[{...}]` for root path queries.
func unmarshalJSONResult[T any](raw string) (*T, error) {
	var arr []T
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil, err
	}
	if len(arr) == 0 {
		return nil, errors.New("empty JSON result")
	}
	return &arr[0], nil
}

// ListTasks returns up to (pg.Limit+1) task states whose ID is
// lexicographically greater than pg.AfterID, sorted by ID ascending. When f
// is non-nil it is evaluated on the client with CEL semantics, after
// RediSearch has narrowed the scan by the part of f it evaluates exactly (see
// filterPlan). Aggregate keysets preserve traversal across concurrent deletions.
func (s *Store) ListTasks(ctx context.Context, pg horai.TaskPage, f q.Expr) ([]*horai.TaskState, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}
	query := "*"
	var plan *filterPlan
	if f != nil {
		var err error
		plan, err = newFilterPlan(f, q.TaskFields())
		if err != nil {
			return nil, coreerrs.WrapOperation(err, "prepare task filter")
		}
		if plan.query != "" {
			query = plan.query
		}
	}

	var results []*horai.TaskState
	sortBy := []redis.FTSearchSortBy{{FieldName: "id", Asc: true}}
	err := s.searchAll(ctx, s.taskIndexName(), query, sortBy, false,
		func(key, body string) (bool, error) {
			td, parseErr := unmarshalJSONResult[taskData](body)
			if parseErr != nil {
				return false, coreerrs.WrapOperation(parseErr, "parse task document")
			}
			state := td.toTaskState()
			if pg.AfterID != "" && state.ID <= pg.AfterID {
				return true, nil
			}
			if plan != nil {
				if !plan.match(state.QueryRecord()) {
					return true, nil
				}
			}
			results = append(results, state)
			return int64(len(results)) <= pg.Limit, nil
		})
	if err != nil {
		return nil, coreerrs.WrapOperation(err, "search tasks paginated")
	}

	return results, nil
}

// Compile-time interface check
var _ horai.Store = (*Store)(nil)

// The Lua scripts run through [redis.Script]: EVALSHA by digest, falling back
// to EVAL (which also loads the script) when the server answers NOSCRIPT, so
// the script body is not resent on every call.
var (
	createTask      = redis.NewScript(createTaskScript)
	replaceTaskIf   = redis.NewScript(replaceTaskIfScript)
	claimRun        = redis.NewScript(claimRunScript)
	finishRun       = redis.NewScript(finishRunScript)
	renewRun        = redis.NewScript(renewRunScript)
	backfillNextRun = redis.NewScript(backfillNextRunAtScript)
)

// ownedRunLua is the script prefix shared by finishRunScript and
// renewRunScript. It decodes the task document of KEYS[1] into s and ends the
// script with 0 unless ARGV[1] owns its unfinished run — the ownership
// predicate of [horai.Store.RenewRun] and [horai.Store.FinishRun].
const ownedRunLua = `
local raw = redis.call('JSON.GET', KEYS[1], '$')
if not raw then return 0 end
local s = cjson.decode(raw)[1]
if not s or ARGV[1] == '' or s.last_run_id ~= ARGV[1] or not s.run_started_at or s.run_started_at == 0 then return 0 end
`

// finishRunScript applies the finish transition of [horai.Store.FinishRun]
// to KEYS[1] while ARGV[1] owns its unfinished run. ARGV = [runID, startedAt,
// endedAt, nextRunAt, schedule, success ("1" or "0"), runningStatus,
// activeStatus, completedStatus, runAt]. Returns 1 when the result was
// recorded, 0 otherwise.
const finishRunScript = ownedRunLua + `
local status = s.status
local executed = s.one_shot and (s.run_at or 0) == tonumber(ARGV[10])
local nextRun = s.next_run_at or 0
if executed then status = tonumber(ARGV[9]); nextRun = 0
elseif status == tonumber(ARGV[7]) then status = tonumber(ARGV[8]) end
if not s.one_shot and s.schedule == ARGV[5] then
 nextRun = tonumber(ARGV[4])
 if nextRun == 0 then status = tonumber(ARGV[9]) end
end
local failures = 0
if ARGV[6] ~= '1' then failures = (s.failures or 0) + 1 end
redis.call('JSON.SET', KEYS[1], '$.status', tostring(status))
redis.call('JSON.SET', KEYS[1], '$.next_run_at', tostring(nextRun))
redis.call('JSON.SET', KEYS[1], '$.last_run_at', ARGV[2])
redis.call('JSON.SET', KEYS[1], '$.updated_at', ARGV[3])
redis.call('JSON.SET', KEYS[1], '$.run_started_at', '0')
redis.call('JSON.SET', KEYS[1], '$.run_lease_until', '0')
redis.call('JSON.SET', KEYS[1], '$.run_lease_id', '""')
redis.call('JSON.SET', KEYS[1], '$.failures', tostring(failures))
redis.call('JSON.SET', KEYS[1], '$.revision', tostring((s.revision or 0) + 1))
return 1
`

// FinishRun applies the finish transition of [horai.Store.FinishRun] in
// one atomic script, finishRunScript.
func (s *Store) FinishRun(ctx context.Context, id, runID string, result horai.RunResult) (bool, error) {
	success := 0
	if result.Success {
		success = 1
	}
	n, err := finishRun.Run(ctx, s.client, []string{s.taskKey(id)},
		runID, result.StartedAt, result.EndedAt, result.NextRunAt, result.Schedule, success,
		int(horai.TaskStatusRunning), int(horai.TaskStatusActive), int(horai.TaskStatusCompleted), result.RunAt).Int64()
	if err != nil {
		return false, coreerrs.WrapOperation(err, "finish task run")
	}
	return n == 1, nil
}

// renewRunScript sets run_lease_until and run_lease_id and bumps the revision
// only while ARGV[1] still owns an unfinished run. ARGV = [runID, leaseUntil,
// jsonRunID].
const renewRunScript = ownedRunLua + `
redis.call('JSON.SET', KEYS[1], '$.run_lease_until', ARGV[2])
redis.call('JSON.SET', KEYS[1], '$.run_lease_id', ARGV[3])
redis.call('JSON.SET', KEYS[1], '$.revision', tostring((s.revision or 0) + 1))
return 1
`

// RenewRun extends the lease of the unfinished run runID in one atomic script,
// renewRunScript; see [horai.Store.RenewRun].
func (s *Store) RenewRun(ctx context.Context, id, runID string, leaseUntil int64) (bool, error) {
	leaseID, err := json.Marshal(runID)
	if err != nil {
		return false, coreerrs.WrapOperation(err, "encode run id")
	}
	n, err := renewRun.Run(ctx, s.client, []string{s.taskKey(id)}, runID, leaseUntil, string(leaseID)).Int64()
	if err != nil {
		return false, coreerrs.WrapOperation(err, "renew task run")
	}
	return n == 1, nil
}

// UnfinishedRuns queries the run-start index regardless of task status.
func (s *Store) UnfinishedRuns(ctx context.Context) iter.Seq2[*horai.TaskState, error] {
	return func(yield func(*horai.TaskState, error) bool) {
		query := "(@runStartedAt:[-inf (0] | @runStartedAt:[(0 +inf])"

		sortBy := []redis.FTSearchSortBy{{FieldName: "id", Asc: true}}
		err := s.searchAll(ctx, s.taskIndexName(), query, sortBy, false,
			func(key, body string) (bool, error) {
				td, parseErr := unmarshalJSONResult[taskData](body)
				if parseErr != nil {
					return false, coreerrs.WrapOperation(parseErr, "parse task document")
				}
				return yield(td.toTaskState(), nil), nil
			})
		if err != nil {
			yield(nil, coreerrs.WrapOperation(err, "search unfinished runs"))
		}
	}
}

const createTaskScript = `
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('JSON.SET', KEYS[1], '$', ARGV[1])
return 1
`
