// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

type searchCursor struct {
	key, id string
	started int64
}

// searchKeyset loads raw IDs rather than normalized TAG values. The key is the
// final tie breaker when different tasks have history entries with the same ID.
func (s *Store) searchKeyset(ctx context.Context, index, query string, history bool, after *searchCursor) ([]searchCursor, error) {
	// Read only sortable index values. LOAD opens documents on the search
	// node, which can return null attributes in cluster mode. JSON.GET below
	// is routed independently to the document's slot.
	key := fmt.Sprintf(`format("%%s%%s", %s, @id)`, aggregateLiteral(s.key(taskKeyPrefix, "")))
	if index == s.historyIndexName() {
		key = fmt.Sprintf(`format("%%s%%s:%%s", %s, @taskId, @id)`, aggregateLiteral(s.key(historyKeyPrefix, "")))
	}
	args := []any{"FT.AGGREGATE", index, query,
		"APPLY", key, "AS", "__key"}
	if after != nil {
		op := ">"
		if history {
			op = "<"
		}
		filter := fmt.Sprintf("(@id %s %s || (@id == %s && @__key > %s))",
			op, aggregateLiteral(after.id), aggregateLiteral(after.id), aggregateLiteral(after.key))
		if history {
			filter = fmt.Sprintf("(@startedAt < %d || (@startedAt == %d && %s))", after.started, after.started, filter)
		}
		args = append(args, "FILTER", filter)
	}
	// Sort and seek on the indexed fields themselves, not APPLY aliases.
	sortFields := []any{"@id", sortAscending, "@__key", sortAscending}
	if history {
		sortFields = []any{"@startedAt", sortDescending, "@id", sortDescending, "@__key", sortAscending}
	}
	args = append(args, "SORTBY", len(sortFields))
	args = append(args, sortFields...)
	args = append(args, "LIMIT", 0, searchPageSize, "DIALECT", searchDialect)
	raw, err := s.client.Do(ctx, args...).Result()
	if err != nil {
		return nil, err
	}
	return parseKeyset(raw, history)
}

func parseKeyset(raw any, history bool) ([]searchCursor, error) {
	var values []any
	if array, ok := raw.([]any); ok {
		if len(array) == 0 {
			return nil, fmt.Errorf("empty aggregate reply")
		}
		values = array[1:]
	} else {
		fields, err := searchMap(raw)
		if err != nil {
			return nil, err
		}
		var ok bool
		values, ok = fields["results"].([]any)
		if !ok {
			return nil, fmt.Errorf("invalid aggregate results")
		}
	}
	rows := make([]searchCursor, 0, len(values))
	for _, value := range values {
		fields, err := aggregateFields(value)
		if err != nil {
			return nil, err
		}
		key, keyOK := fields["__key"].(string)
		id, idOK := fields["id"].(string)
		if !keyOK || !idOK {
			return nil, fmt.Errorf("missing aggregate cursor")
		}
		row := searchCursor{key: key, id: id}
		if history {
			var err error
			row.started, err = strconv.ParseInt(fmt.Sprint(fields["startedAt"]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid aggregate time: %w", err)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// aggregateFields decodes RESP2 alternating field pairs or a RESP3 attribute map.
func aggregateFields(value any) (map[string]any, error) {
	if array, ok := value.([]any); ok {
		if len(array)%2 != 0 {
			return nil, fmt.Errorf("invalid aggregate row")
		}
		fields := make(map[string]any, len(array)/2)
		for i := 0; i < len(array); i += 2 {
			name, ok := array[i].(string)
			if !ok {
				return nil, fmt.Errorf("invalid aggregate field")
			}
			fields[name] = array[i+1]
		}
		return fields, nil
	}
	fields, err := searchMap(value)
	if err != nil {
		return nil, err
	}
	if extra, ok := fields["extra_attributes"]; ok {
		return searchMap(extra)
	}
	return fields, nil
}

// aggregateLiteral preserves bytes using RediSearch's punctuation escapes, not
// Go escapes. Separate backslashes prevent recursive unescaping from consuming
// a backslash immediately before punctuation in an ID.
func aggregateLiteral(value string) string {
	quote := func(part string) string {
		part = strings.ReplaceAll(part, `"`, `\"`)
		part = strings.ReplaceAll(part, "\n", "\\\n")
		return `"` + part + `"`
	}
	parts := strings.Split(value, `\`)
	if len(parts) == 1 {
		return quote(value)
	}
	var args []string
	for i, part := range parts {
		if i > 0 {
			args = append(args, `"\\"`)
		}
		args = append(args, quote(part))
	}
	return "format(" + quote(strings.Repeat("%s", len(args))) + ", " + strings.Join(args, ", ") + ")"
}
