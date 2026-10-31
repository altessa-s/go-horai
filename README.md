# go-horai

```go
import "github.com/altessa-s/go-horai/scheduler"
```

Durable periodic and one-shot scheduling with atomic occurrence claims, renewable leases, fenced management, and execution history.
Consumers own backend clients and lifecycle. Register task functions before or after starting the scheduler; call Stop to cancel and drain runs.

```go
store, err := memory.New()
if err != nil { return err }
engine, err := scheduler.New(store)
if err != nil { return err }
err = engine.Register(ctx, task.Config{
    ID: "refresh", Schedule: "@every 5m",
    Func: func(ctx context.Context) error { return refresh(ctx) },
})
if err != nil { return err }
return engine.Start(ctx)
```

## Packages and modules

| Path | Responsibility | Module |
|---|---|---|
| horai | Entities, ownership contracts, clock and metrics ports | root |
| task | Registration contract and priorities | root |
| schedule | Six-field cron, descriptors and time zones | root |
| query | Typed filters; no text parser | root |
| scheduler | Lifecycle, dispatch, management, leases and recovery | root |
| storage/memory | In-process reference persistence | root |
| storage/sqldb | PostgreSQL and MySQL/MariaDB over caller-owned database/sql | root |
| storage/redis | RedisJSON and RediSearch; Redis Stack required | storage/redis |
| storage/mongo | MongoDB atomic document operations | storage/mongo |
| storage/clickhouse | History-only persistence | storage/clickhouse |
| storage/storagetest, horaitest | Test contracts and fixtures | root |
| integration/docker | Server-backed contracts and end-to-end tests | integration/docker, test-only |

## Ownership

ClaimRun atomically verifies status, unfinished-run state, and occurrence identity. Renew and finish require the owning run ID.
Management and registration use revision fences, preserving concurrent execution. Recovery includes paused and disabled unfinished runs.
A restarted process with the same instance ID recovers its own abandoned runs before lease expiry; active local runs remain protected.
An abandoned run may execute again: task functions should tolerate retries after process failure. Stored data formats are not compatibility APIs.

## Queries and observability

Filters use a bounded AST with string, int64 and boolean values. Lists return typed cursors; transport signing is the consumer's responsibility.
SQL and MongoDB translate filters to backend predicates. Redis pushes exact numeric conjuncts and evaluates the complete filter for parity.
Clock and Metrics are owned ports. Factories, gRPC, CEL, OpenTelemetry, health integration, and environment configuration stay outside the library.

## Development

Dependencies point inward: the domain owns ports, the scheduler orchestrates them,
and storage adapters implement them. Consumers own configuration and startup.
See [Docker tests](integration/docker/README.md) for backend verification.
The proof command is `make vet && make lint && make test-all && make test-integration && make generate`.
