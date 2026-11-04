// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
)

// taskIndexVersion changes whenever the task search schema changes. v2 adds
// runStartedAt so existing namespaces receive an independent unfinished-run index.
const taskIndexVersion = "v2"

// historyIndexVersion adds sortable identity fields for document-free traversal.
const historyIndexVersion = "v2"

// indexPollInterval is how often waitForIndex re-checks an index that is
// still indexing existing documents.
const indexPollInterval = 50 * time.Millisecond

// missingNextRunQuery matches task documents without an indexed numeric
// next_run_at.
const missingNextRunQuery = "-@nextRunAt:[-inf +inf]"

// backfillNextRunAtScript sets $.next_run_at to 0 on KEYS[1] while the key
// exists and the field is absent or null. Returns 1 when it wrote, 0
// otherwise.
const backfillNextRunAtScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local t = redis.call('JSON.TYPE', KEYS[1], '$.next_run_at')
if #t == 0 or t[1] == 'null' then
  redis.call('JSON.SET', KEYS[1], '$.next_run_at', '0')
  return 1
end
return 0
`

// errBackfillStalled reports a backfill round that changed nothing and saw
// the same matches as the round before, so another round cannot progress.
var errBackfillStalled = errors.New("next_run_at backfill made no progress")

// backfillNextRunAt stores next_run_at = 0 on every task document that lacks
// it. RediSearch does not index a missing NUMERIC field, so such a document —
// a task whose NextRunAt is zero, written while the field was omitted when
// zero — never matched the DueTasks range although it is due.
//
// A freshly created index indexes existing keys in the background, so the
// search waits for that first. The walk then always re-reads the first page of
// the remaining matches instead of paging by offset: every document it handles
// leaves the match set — migrated, deleted, or rewritten by another writer
// with a numeric next_run_at or with a value RediSearch refuses to index — so a
// match is never skipped because earlier ones disappeared. A round that
// writes nothing and sees the same page as the round before ends the walk with
// errBackfillStalled instead of looping. The revision is not incremented: an
// absent next_run_at and a zero one are the same state, which every fence
// already compares equal.
func (s *Store) backfillNextRunAt(ctx context.Context) error {
	idx := s.taskIndexName()
	if err := s.waitForIndex(ctx, idx); err != nil {
		return err
	}

	byID := []redis.FTSearchSortBy{{FieldName: "id", Asc: true}}

	var idle []string // the page of the previous round, when it wrote nothing

	for {
		page, err := s.client.FTSearchWithArgs(ctx, idx, missingNextRunQuery, &redis.FTSearchOptions{
			NoContent: true, LimitOffset: 0, Limit: searchPageSize, SortBy: byID,
		}).Result()
		if err != nil {
			return err
		}

		if len(page.Docs) == 0 {
			return nil
		}

		ids := make([]string, 0, len(page.Docs))
		wrote := false

		for _, doc := range page.Docs {
			ids = append(ids, doc.ID)

			n, err := backfillNextRun.Run(ctx, s.client, []string{doc.ID}).Int64()
			if err != nil {
				return coreerrs.Wrapf(err, "backfill %q", doc.ID)
			}

			wrote = wrote || n == 1
		}

		switch {
		case wrote:
			idle = nil
		case slices.Equal(ids, idle):
			return errBackfillStalled
		default:
			idle = ids
		}
	}
}

// waitForIndex waits for existing documents to become searchable, including
// when an earlier initialization left the index building in the background.
func (s *Store) waitForIndex(ctx context.Context, idx string) error {
	for {
		info, err := s.client.FTInfo(ctx, idx).Result()
		if err != nil {
			return err
		}

		if info.Indexing == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(indexPollInterval):
		}
	}
}

func (s *Store) ensureTaskIndex(ctx context.Context) error {
	idxName := s.taskIndexName()

	// Check if index already exists
	_, err := s.client.FTInfo(ctx, idxName).Result()
	if err == nil {
		return nil // Index already exists
	}

	return s.client.FTCreate(ctx, idxName,
		&redis.FTCreateOptions{
			OnJSON:   true,
			Prefix:   []any{s.key(taskKeyPrefix, "")},
			NoFreqs:  true,
			NoFields: true,
		},
		&redis.FieldSchema{FieldName: "$.id", As: "id", FieldType: redis.SearchFieldTypeTag, Sortable: true},
		&redis.FieldSchema{FieldName: "$.description", As: "description", FieldType: redis.SearchFieldTypeText},
		&redis.FieldSchema{FieldName: "$.run_started_at", As: "runStartedAt", FieldType: redis.SearchFieldTypeNumeric},
		&redis.FieldSchema{FieldName: "$.status", As: "status", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.priority", As: "priority", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.schedule", As: "schedule", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.last_run_at", As: "lastRunAt", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.next_run_at", As: "nextRunAt", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.failures", As: "failures", FieldType: redis.SearchFieldTypeNumeric},
		&redis.FieldSchema{FieldName: "$.skip_next_run", As: "skipNextRun", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.disable_history", As: "disableHistory", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.unmanaged", As: "unmanaged", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.one_shot", As: "oneShot", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.created_at", As: "createdAt", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.updated_at", As: "updatedAt", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
	).Err()
}

func (s *Store) ensureHistoryIndex(ctx context.Context) error {
	idxName := s.historyIndexName()

	// Check if index already exists
	_, err := s.client.FTInfo(ctx, idxName).Result()
	if err == nil {
		return nil // Index already exists
	}

	return s.client.FTCreate(ctx, idxName,
		&redis.FTCreateOptions{
			OnJSON:   true,
			Prefix:   []any{s.key(historyKeyPrefix, "")},
			NoFreqs:  true,
			NoFields: true,
		},
		&redis.FieldSchema{FieldName: "$.task_id", As: "taskId", FieldType: redis.SearchFieldTypeTag, Sortable: true, UNF: true},
		&redis.FieldSchema{FieldName: "$.id", As: "id", FieldType: redis.SearchFieldTypeTag, Sortable: true, UNF: true},
		&redis.FieldSchema{FieldName: "$.run_id", As: "runId", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.started_at", As: startedAtField, FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.ended_at", As: "endedAt", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		&redis.FieldSchema{FieldName: "$.duration_ms", As: "durationMs", FieldType: redis.SearchFieldTypeNumeric},
		&redis.FieldSchema{FieldName: "$.success", As: "success", FieldType: redis.SearchFieldTypeTag},
		&redis.FieldSchema{FieldName: "$.error", As: "error", FieldType: redis.SearchFieldTypeText},
	).Err()
}
