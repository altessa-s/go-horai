// Copyright 2021-2026 ALTESSA SOLUTIONS INC.
// Use of this source code is governed by the MIT license that can be found in
// the LICENSE file.

// Package sqldb provides a sqldb storage adapter.
//
// Store uses database/sql with PostgreSQL or MySQL
// The application supplies and owns the driver and connection
// New(db, dialect, options...) validates configuration without I/O
// Call EnsureSchema during startup, or apply SchemaDDL through migrations
// Typed filters compile to bound SQL parameters.
package sqldb
