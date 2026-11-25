# 管理命令状态

当前仅实现 **M2 离线 Mock 路由与表达契约**。真实微信 Hook 不会执行这里的命令；主 Gateway 的现有入口也未启用命令功能。设计边界见 [ADR 0002](adr/0002-mock-command-boundary.md)。

解析器识别的命令：/help、/status、/model show、/model use lite|flash、/budget show、/budget set monthly <decimal>、/asr on|off、/tts on|off、/search on|off、/memory status、/service status、/service restart、/confirm <code>。L2 在 M2 只产生待确认占位，不执行预算修改或重启。

只有可信、稳定的原生消息和本机绑定管理员可执行 Mock 命令。群管理额外要求授权群、真实 mention ID 与已验证命令 span；当前 Hook 不能提供这些字段，所以群命令保持 BLOCKED。非法命令、非管理员和不可信来源会生成结构化不可执行回退；执行器已经失败时不再回退普通 Agent。

项目测试入口：

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_commands.py tests\test_command_mock.py tests\test_command_presentation.py -q
```
