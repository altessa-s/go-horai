# clickhouse

`github.com/altessa-s/go-horai/storage/clickhouse`

Store implements only horai.HistoryStore. Supply it with scheduler.WithHistoryStore while using another backend for task state and ownership. New(connection, options...) validates configuration without I/O. Call EnsureSchema during startup. CleanupHistory synchronously deletes entries strictly before the supplied cutoff; optional table TTL is additional retention.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.
