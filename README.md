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

## Install

```bash
go get github.com/maxid/gjob
```

## License

MIT. See [LICENSE](LICENSE).
