[English](README.md) | 简体中文

# gjob

Go 的轻量级分布式任务框架：cron/事件触发、执行器
注册与执行回执。

> 状态：**实验性脚手架**（v0.0.0-dev）。

## 计划中的特性

- Cron 与事件触发
- 执行器注册，并经消息总线派发
- 执行回执与结果持久化钩子
- 集群内的单次执行语义（基于 Redis 锁）

Redis 回执默认使用 `gjob:receipt:`，fencing 计数器使用 `gjob:fencing`。
多个应用共用 Redis 时，用 gns 派生两套 key：

```go
store, _ := redisstore.New(client, redisstore.WithNamespace(set))
// app:job:<execution>、app:job:lease、app:job:fencing
```

`WithPrefix` 仍是显式的底层逃生口。

## 安装

```bash
go get github.com/maxid/gjob
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
