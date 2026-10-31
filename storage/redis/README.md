# redis

`github.com/altessa-s/go-horai/storage/redis`

Store requires RedisJSON and RediSearch (Redis Stack). New(client, options...) validates configuration without I/O; the application owns the client. Call EnsureIndexes before queries. Lua scripts fence run ownership. Numeric filter conjuncts narrow indexed searches; complete typed filters are evaluated against returned records for parity.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.

The task search index is versioned as `{prefix}:idx:tasks:v2` (`taskIndexVersion`); v2 adds `runStartedAt` for unfinished-run recovery.
`EnsureIndexes` creates it beside an older index. The history schema is unchanged and keeps its existing index name.

Searches use `FT.AGGREGATE` keysets over raw IDs (history: start time descending, ID descending), with the Redis key as a final tie breaker. Each page seeks strictly beyond the last indexed tuple, then loads JSON bodies individually so each read routes to its own cluster slot.
Keys deleted before loading are skipped; surviving records count toward pagination lookahead. Deleting earlier entries cannot shift the next page. Search pagination is not a snapshot.

Keyset queries use sortable index values without `LOAD`, including in cluster mode.
History index `history:v2` adds sortable raw identity fields; `EnsureIndexes` creates it independently of older indexes.
