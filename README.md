English | [简体中文](README.zh-CN.md)

# gjob

A lightweight distributed job framework in Go: cron/event triggers, executor
registry and execution receipts.

> Status: **experimental scaffold** (v0.0.0-dev).

## Planned features

- Cron and event triggers
- Executor registration and dispatch over a message bus
- Execution receipts and result persistence hooks
- Single-execution semantics across a cluster (Redis lock based)

## Scheduler

```go
scheduler, _ := gjob.NewScheduler(gjob.SchedulerConfig{Tick: time.Second},
    jobs, receipts, dispatcher.Submit)
go scheduler.Run(ctx)
```

- The **first scan only seeds** the cursors: a restart does not replay history.
- **Misfire**: `skip` ignores a missed slot, `run-once` collapses a backlog into one
  execution, `backfill` replays it — bounded by `MaxTriggersPerTick` so a long outage
  cannot dispatch thousands of jobs in one scan.
- The execution id is derived from `jobName + scheduled instant`, so every node
  computes the same value and only one claim wins.
- A slot that is less than one tick old counts as on time, not as a misfire.

## Receipt stores

| Store | Use |
|---|---|
| `memory/` | contract reference and single-process deployments |
| `redisstore/` | clusters: atomic claim (`SETNX` in Lua), fenced heartbeat/complete, sorted-set lease index for bounded recovery |
| `mysqlstore/` | clusters: claim is an `INSERT` conflict, fencing checked on every write, `lease_until` index for bounded recovery |

Both satisfy the same contract test: unique claim, fencing rejection, lease
recovery.

## Install

```bash
go get github.com/maxid/gjob
```

## License

MIT. See [LICENSE](LICENSE).
