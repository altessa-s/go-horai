// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"

	"github.com/redis/go-redis/v9"

	"github.com/altessa-s/go-horai"

	coreerrs "github.com/altessa-s/go-atlas-core/errors"
	q "github.com/altessa-s/go-horai/query"
)

// AddHistory records a task execution history entry as a JSON document in Redis.
// If a history TTL is configured via [WithHistoryTTL], an expiration is set on
// the key. If a per-task maximum is configured via [WithMaxHistoryPerTask],
// entries exceeding the limit are trimmed on a best-effort basis (oldest first).
func (s *Store) AddHistory(ctx context.Context, history *horai.TaskHistory) error {
	hd := newHistoryData(history)

	historyKey := s.historyKey(history.TaskID, history.ID)
	if err := s.client.JSONSet(ctx, historyKey, "$", hd).Err(); err != nil {
		return coreerrs.WrapOperation(err, "add history")
	}

	// Set TTL if configured
	if s.opts.historyTTL > 0 {
		s.client.Expire(ctx, historyKey, s.opts.historyTTL)
	}

	// Trim to max entries if configured
	if s.opts.maxHistoryPerTask > 0 {
		s.trimHistory(ctx, history.TaskID)
	}

	return nil
}

// trimHistory removes the oldest history entries beyond the configured maximum.
//
// The first search asks for no documents at all — FT.SEARCH still reports the
// full match count — so a task below the cap costs one round trip. The TAG
// query folds case, though, so that count also includes tasks whose IDs differ
// only by case and cannot say how many entries are this task's. Once it
// exceeds the cap, the candidates are walked newest first with their
// JSON bodies loaded, the first max exact matches are kept and the remaining
// exact matches deleted, so another task's entries are never counted against
// this task nor removed.
func (s *Store) trimHistory(ctx context.Context, taskID string) {
	if ctx.Err() != nil {
		return
	}

	query := fmt.Sprintf("@taskId:{%s}", escapeTag(taskID))

	// Count only: LIMIT 0 0 returns Total without any document payload.
	counted, err := s.search(ctx, s.historyIndexName(), query, nil, 0, 0)
	if err != nil || counted.Total <= s.opts.maxHistoryPerTask {
		return
	}

	kept := 0

	var stale []string

	newestFirst := []redis.FTSearchSortBy{{FieldName: startedAtField, Desc: true}}

	err = s.searchAll(ctx, s.historyIndexName(), query, newestFirst, false,
		func(key, body string) (bool, error) {
			hd, parseErr := unmarshalJSONResult[historyData](body)
			if parseErr != nil {
				return false, parseErr
			}

			switch {
			case hd.TaskID != taskID:
			case kept < s.opts.maxHistoryPerTask:
				kept++
			default:
				stale = append(stale, key)
			}

			return true, nil
		})
	if err != nil || len(stale) == 0 {
		return
	}

	pipe := s.client.Pipeline()
	for _, key := range stale {
		pipe.Del(ctx, key)
	}

	_, _ = pipe.Exec(ctx) //nolint:errcheck // best-effort trim
}

// History returns an iterator over execution history entries for the given task,
// ordered by start time descending (most recent first). If the task has no
// history or does not exist, the iterator yields zero elements. A redis.Nil
// error from the search is treated as empty (not propagated). The TAG query
// folds case, so entries of a task whose ID differs only by case are skipped
// on the client.
func (s *Store) History(ctx context.Context, id string) iter.Seq2[*horai.TaskHistory, error] {
	return func(yield func(*horai.TaskHistory, error) bool) {
		escapedID := escapeTag(id)
		query := fmt.Sprintf("@taskId:{%s}", escapedID)

		sortBy := []redis.FTSearchSortBy{{FieldName: startedAtField, Desc: true}}

		err := s.searchAll(ctx, s.historyIndexName(), query, sortBy, false,
			func(key, body string) (bool, error) {
				hd, parseErr := unmarshalJSONResult[historyData](body)
				if parseErr != nil {
					return false, coreerrs.WrapOperation(parseErr, "parse history document")
				}

				if hd.TaskID != id {
					return true, nil
				}

				return yield(hd.toTaskHistory(), nil), nil
			})
		if err != nil && !errors.Is(err, redis.Nil) {
			yield(nil, coreerrs.WrapOperation(err, "search history"))
		}
	}
}

// CleanupHistory deletes all history entries whose endedAt timestamp is older
// than the specified retention duration relative to the current time. Entries
// are discovered via a RediSearch range query and deleted in a single pipeline.
// Returns nil if no expired entries are found.
func (s *Store) CleanupHistory(ctx context.Context, cutoff int64) error {
	query := fmt.Sprintf("@endedAt:[-inf (%d]", cutoff)

	// Collect first so a traversal error does not leave a partially cleaned history.
	var keys []string

	if err := s.searchAll(ctx, s.historyIndexName(), query, nil, true,
		func(key, body string) (bool, error) {
			keys = append(keys, key)
			return true, nil
		}); err != nil {
		return coreerrs.WrapOperation(err, "search expired history")
	}

	if len(keys) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	for _, key := range keys {
		pipe.Del(ctx, key)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return coreerrs.WrapOperation(err, "delete expired history")
	}

	return nil
}

// ListHistory returns up to (pg.Limit+1) history entries for taskID,
// sorted by StartedAt descending with ID descending as tie-breaker, starting
// after the cursor position in pg. When f is non-nil it is evaluated on the
// client with CEL semantics, after RediSearch has narrowed the scan by the part
// of f it evaluates exactly (see filterPlan).
//
// Keyset traversal orders by StartedAt and ID descending. The TAG
// query on taskId folds case, so entries of a task whose ID differs only by
// case are skipped on the client.
func (s *Store) ListHistory(ctx context.Context, taskID string, pg horai.HistoryPage, f q.Expr) ([]*horai.TaskHistory, error) {
	if err := horai.ValidatePageLimit(pg.Limit); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("@taskId:{%s}", escapeTag(taskID))
	if pg.AfterID != "" {
		query += fmt.Sprintf(" @startedAt:[-inf %d]", pg.AfterStartedAt)
	}

	var plan *filterPlan

	if f != nil {
		var err error

		plan, err = newFilterPlan(f, q.HistoryFields())
		if err != nil {
			return nil, coreerrs.WrapOperation(err, "prepare history filter")
		}

		if plan.query != "" {
			query += " " + plan.query
		}
	}

	var results []*horai.TaskHistory

	sortBy := []redis.FTSearchSortBy{{FieldName: startedAtField, Desc: true}}

	err := s.searchAll(ctx, s.historyIndexName(), "("+query+")", sortBy, false,
		func(key, body string) (bool, error) {
			hd, parseErr := unmarshalJSONResult[historyData](body)
			if parseErr != nil {
				return false, coreerrs.WrapOperation(parseErr, "parse history document")
			}

			if hd.TaskID != taskID {
				return true, nil
			}

			entry := hd.toTaskHistory()
			if pg.AfterID != "" &&
				entry.StartedAt >= pg.AfterStartedAt && (entry.StartedAt != pg.AfterStartedAt || entry.ID >= pg.AfterID) {
				return true, nil
			}
			// Past the page and past the tie group at its boundary: done.
			if int64(len(results)) > pg.Limit && entry.StartedAt < results[pg.Limit].StartedAt {
				return false, nil
			}

			if plan != nil {
				if !plan.match(entry.QueryRecord()) {
					return true, nil
				}
			}

			results = append(results, entry)

			return true, nil
		})
	if err != nil {
		return nil, coreerrs.WrapOperation(err, "search history paginated")
	}

	slices.SortFunc(results, func(a, b *horai.TaskHistory) int {
		if c := cmp.Compare(b.StartedAt, a.StartedAt); c != 0 {
			return c
		}

		return cmp.Compare(b.ID, a.ID)
	})

	if int64(len(results)) > pg.Limit+1 {
		results = results[:pg.Limit+1]
	}

	return results, nil
}

// DeleteHistory removes all history entries belonging exactly to id.
func (s *Store) DeleteHistory(ctx context.Context, id string) error {
	var keys []string

	err := s.searchAll(ctx, s.historyIndexName(), "*", nil, false, func(key, body string) (bool, error) {
		h, err := unmarshalJSONResult[historyData](body)
		if err != nil {
			return false, err
		}

		if h.TaskID == id {
			keys = append(keys, key)
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	for _, key := range keys {
		if err := s.client.Del(ctx, key).Err(); err != nil {
			return err
		}
	}

	return nil
}
