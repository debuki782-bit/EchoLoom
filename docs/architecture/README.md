# 当前代码架构文档

本目录按运行边界记录当前仓库的结构，不把早期 Mock 骨架和现在使用的 Pilot 混为一条调用链。

- [分层架构图](./current-architecture.md)：服务边界、数据流和 SQLite/媒体存储。
- [代码 Wiki](./code-wiki.md)：源码文件、主要类/方法、输入输出及上下游关系。
- [调用拓扑](./call-topology.md)：入站、出站、跨 Session、登录恢复和定时任务的主要调用顺序。
- [最终架构升级记录](./mvp-upgrade-final-architecture-2026-10-31.md)：开发分支落地项、验证边界和未完成项。

## 运行入口速查

| 用途 | 入口 | 下游 |
|---|---|---|
| 真实 Hook + ADK Pilot | `scripts/live.ps1` 管理进程；实际 Python 入口 `scripts/run-group-pilot.py --run` | `Pilot`、ADK `ScopedAdkRunner`、`HookTextSender` |
| 本地 Mock Gateway | `scripts/start.ps1` / `python -m wechat_agent.app` | `Gateway`、`Repository`、`MockProvider`、`MockTransport` |
| 固定事件回放 | `python -m wechat_agent.replay <fixture>` | Mock `Gateway` 处理链 |
| 开发/检查脚本 | `scripts/dev.ps1`、`scripts/test.ps1` | 项目环境及自动化测试；本 Wiki 不代表已执行这些命令 |

真实 Pilot 的 Hook 回调是 `src/wechat_agent/pilot.py` 中 `create_pilot_app()` 的 `/callback`；`src/wechat_agent/app.py` 是 Mock Gateway，不是 Pilot 的实时入口。
