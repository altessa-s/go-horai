# mongo

`github.com/altessa-s/go-horai/storage/mongo`

Store uses the MongoDB v2 driver. New(database, options...) validates configuration without I/O. The application owns the client. Call EnsureIndexes during startup. Atomic updates fence run ownership; typed filters compile to BSON expressions with literal values.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.
