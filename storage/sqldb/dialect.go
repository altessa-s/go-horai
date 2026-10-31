// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

package sqldb

import (
	"errors"
	"fmt"

	sqldialect "github.com/altessa-s/go-horai/storage/sqldb/internal/dialect"
)

// Dialect selects the SQL flavor the storage speaks.
type Dialect string

const (
	// Postgres targets PostgreSQL 12 or newer.
	Postgres Dialect = "postgres"
	// MySQL targets MySQL 8.0+ and MariaDB 10.6+.
	MySQL Dialect = "mysql"
)

// Sentinel errors returned by the storage.
var (
	// ErrUnsupportedDialect is returned by [New] for a dialect other than
	// [Postgres] or [MySQL].
	ErrUnsupportedDialect = sqldialect.ErrUnsupportedDialect
	// ErrInvalidTableName is returned by [New] when a table name is not a plain
	// SQL identifier, optionally qualified by one schema name, or a part of it
	// exceeds 63 characters.
	ErrInvalidTableName = sqldialect.ErrInvalidTableName
	// ErrValueTooLong is returned by writes whose ID-like value exceeds its
	// column: task, history and run IDs hold at most [MaxIDLength] characters,
	// a schedule at most [MaxScheduleLength]. Rejecting them up front matters
	// because PostgreSQL silently trims excess trailing spaces, which would
	// make "id" and "id " (past the limit) the same row.
	ErrValueTooLong = errors.New("sqldb: value exceeds its column length")
)

// Column limits for ID-like values, in characters.
const (
	// MaxIDLength bounds task, history and run IDs.
	MaxIDLength = 255
	// MaxScheduleLength bounds a task's schedule expression.
	MaxScheduleLength = 1024
)

type dialect struct {
	sqldialect.Style
	name Dialect
}

func dialectFor(d Dialect) (dialect, error) {
	switch d {
	case Postgres:
		return dialect{sqldialect.Postgres(), d}, nil
	case MySQL:
		return dialect{sqldialect.MySQL(), d}, nil
	default:
		return dialect{}, fmt.Errorf("%w: %q", ErrUnsupportedDialect, d)
	}
}
