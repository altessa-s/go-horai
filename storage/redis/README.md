# redis

`github.com/altessa-s/go-horai/storage/redis`

Store requires RedisJSON and RediSearch (Redis Stack). New(client, options...) validates configuration without I/O; the application owns the client. Call EnsureIndexes before queries. Lua scripts fence run ownership. Numeric filter conjuncts narrow indexed searches; complete typed filters are evaluated against returned records for parity.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.
