# scheduler

Register task functions, then Start the engine. Stop cancels and drains executions.
Run IDs combine instance identity and random bytes. Leases are renewed until finalization ends.
Recovery queries all unfinished runs, including paused and disabled tasks, and resets only abandoned runs using revision fences.

Static concurrency is bounded by `MaxConcurrentTasks` (1,048,576); zero means unlimited. Default reserved slots are clamped to max minus one,
leaving shared capacity for Normal and Low tasks. An explicitly reserved count at or above a positive maximum is invalid.
Unregister deletes the task first and treats history deletion as best effort: a history failure is logged and retention may clean it later.
Timing and multi-engine integration tests use `testing/synctest` in the unit suite; there is no separate in-process integration-tag suite.
