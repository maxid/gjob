[English](CHANGELOG.md) | 简体中文

# 变更日志

本文件记录本项目的重要变更。

## [Unreleased]

- `redisstore.WithNamespace(string)` 派生回执与 fencing key；`WithPrefix` 现在把 fencing 移到同一前缀下。不再依赖 gns。
- 初始脚手架。
