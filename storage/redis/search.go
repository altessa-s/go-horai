// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const searchDialect = 2

// search requests IDs only: cluster RediSearch does not return JSON bodies.
func (s *Store) search(
	ctx context.Context, index, query string, sortBy []redis.FTSearchSortBy,
	offset, limit int,
) (redis.FTSearchResult, error) {
	args := make([]any, 0, 4+3*len(sortBy)+6)
	args = append(args, "FT.SEARCH", index, query, "NOCONTENT")
	for _, sort := range sortBy {
		direction := "ASC"
		if sort.Desc {
			direction = "DESC"
		}
		args = append(args, "SORTBY", sort.FieldName, direction)
	}
	args = append(args, "LIMIT", offset, limit, "DIALECT", searchDialect)
	raw, err := s.client.Do(ctx, args...).Result()
	if err != nil {
		return redis.FTSearchResult{}, err
	}
	return parseSearchReply(raw)
}

// parseSearchReply accepts IDs and totals in RESP2 arrays and RESP3 maps.
func parseSearchReply(raw any) (redis.FTSearchResult, error) {
	var result redis.FTSearchResult
	if values, ok := raw.([]any); ok {
		return parseSearchArray(values)
	}
	fields, err := searchMap(raw)
	if err != nil {
		return result, err
	}
	total, ok := fields["total_results"].(int64)
	if !ok || total < 0 {
		return result, fmt.Errorf("invalid search total: %T", fields["total_results"])
	}
	result.Total = int(total)
	docs, ok := fields["results"].([]any)
	if !ok {
		return result, fmt.Errorf("invalid search results: %T", fields["results"])
	}
	for _, value := range docs {
		item, err := searchMap(value)
		if err != nil {
			return result, err
		}
		id, ok := item["id"].(string)
		if !ok {
			return result, fmt.Errorf("invalid search document ID: %T", item["id"])
		}
		doc := redis.Document{ID: id}
		result.Docs = append(result.Docs, doc)
	}
	return result, nil
}

func searchMap(raw any) (map[string]any, error) {
	switch fields := raw.(type) {
	case map[string]any:
		return fields, nil
	case map[any]any:
		result := make(map[string]any, len(fields))
		for key, value := range fields {
			name, ok := key.(string)
			if !ok {
				return nil, fmt.Errorf("invalid search field name: %T", key)
			}
			result[name] = value
		}
		return result, nil
	default:
		return nil, fmt.Errorf("invalid search fields: %T", raw)
	}
}

func parseSearchArray(values []any) (redis.FTSearchResult, error) {
	var result redis.FTSearchResult
	if len(values) == 0 {
		return result, fmt.Errorf("empty search reply")
	}
	total, ok := values[0].(int64)
	if !ok || total < 0 {
		return result, fmt.Errorf("invalid search total: %T", values[0])
	}
	result.Total = int(total)
	for i := 1; i < len(values); i++ {
		id, ok := values[i].(string)
		if !ok {
			return result, fmt.Errorf("invalid search document ID: %T", values[i])
		}
		doc := redis.Document{ID: id}
		result.Docs = append(result.Docs, doc)
	}
	return result, nil
}
