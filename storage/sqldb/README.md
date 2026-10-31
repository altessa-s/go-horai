# sqldb

`github.com/altessa-s/go-horai/storage/sqldb`

Store uses database/sql with PostgreSQL or MySQL. The application supplies and owns the driver and connection. New(db, dialect, options...) validates configuration without I/O. Call EnsureSchema during startup, or apply SchemaDDL through migrations. Typed filters compile to bound SQL parameters.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.
