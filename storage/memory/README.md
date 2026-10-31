# memory

`github.com/altessa-s/go-horai/storage/memory`

Store keeps task state and history in memory. New accepts generated options and returns an error for invalid limits. Reads and iterators return independent snapshots; iterators hold no lock while yielding.

The caller owns connections and lifecycle. Operations accept contexts. History cleanup uses an exclusive Unix-second cutoff supplied by the scheduler. Cursors and filters are typed; the library has no query text parser or signed page tokens.

Run unit tests with `go test ./...`. Live backend contracts are in `integration/docker` under the `integration_docker` build tag. They fail when a required server cannot start; they do not silently skip.
