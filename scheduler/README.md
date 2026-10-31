# scheduler

Register task functions, then Start the engine. Stop cancels and drains executions.
Run IDs combine instance identity and random bytes. Leases are renewed until finalization ends.
Recovery queries all unfinished runs, including paused and disabled tasks, and resets only abandoned runs using revision fences.
