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

## 安装

```bash
go get github.com/maxid/gjob
```

## 许可证

MIT。见 [LICENSE](LICENSE)。
