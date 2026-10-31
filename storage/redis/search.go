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

// search uses the raw command reply because typed SDK decoding can discard
// attributes when a cluster returns field/value arrays inside a RESP3 map.
func (s *Store) search(
	ctx context.Context, index, query string, sortBy []redis.FTSearchSortBy,
	noContent bool, ret []redis.FTSearchReturn, offset, limit int,
) (redis.FTSearchResult, error) {
	args := []any{"FT.SEARCH", index, query}
	if noContent {
		args = append(args, "NOCONTENT")
	}
	if ret != nil {
		fields := []any{}
		for _, field := range ret {
			fields = append(fields, field.FieldName)
			if field.As != "" {
				fields = append(fields, "AS", field.As)
			}
		}
		args = append(args, "RETURN", len(fields))
		args = append(args, fields...)
	}
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
	return parseSearchReply(raw, noContent)
}

// parseSearchReply accepts RESP2 arrays and RESP3 maps. Both JSON roots and
// projected/hash fields are retained without interpreting the document schema.
func parseSearchReply(raw any, noContent bool) (redis.FTSearchResult, error) {
	var result redis.FTSearchResult
	if values, ok := raw.([]any); ok {
		return parseSearchArray(values, noContent)
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
		if !noContent {
			doc.Fields, err = searchFields(item["extra_attributes"])
			if err != nil {
				return result, err
			}
		}
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
	case []any:
		if len(fields)%2 != 0 {
			return nil, fmt.Errorf("odd search field/value array")
		}
		result := make(map[string]any, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			name, ok := fields[i].(string)
			if !ok {
				return nil, fmt.Errorf("invalid search field name: %T", fields[i])
			}
			result[name] = fields[i+1]
		}
		return result, nil
	default:
		return nil, fmt.Errorf("invalid search fields: %T", raw)
	}
}

func searchFields(raw any) (map[string]string, error) {
	fields, err := searchMap(raw)
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(fields))
	for key, value := range fields {
		text, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("invalid search field %q value: %T", key, value)
		}
		result[key] = text
	}
	return result, nil
}

func parseSearchArray(values []any, noContent bool) (redis.FTSearchResult, error) {
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
		if !noContent {
			i++
			if i == len(values) {
				return result, fmt.Errorf("missing search document fields")
			}
			fields, err := searchFields(values[i])
			if err != nil {
				return result, err
			}
			doc.Fields = fields
		}
		result.Docs = append(result.Docs, doc)
	}
	return result, nil
}
