# ADR 0002：Mock 管理命令信任边界

日期：2026-10-29。状态：M2 Mock 实现；真实微信命令保持关闭。

## 决定

- 仅内部构造的 CommandEvidence 能让原生文本进入命令路由：来源认证、原生文本、原生 sender 和稳定来源事件 ID 都必须成立。转发、ASR/OCR/RAG 或模型生成内容不得获得此证据。
- 私聊只允许本机绑定的管理员 ID；群管理默认关闭。Mock 群命令只有在授权群、已验证群身份、结构化 mention 命中机器人 ID 且命令 span 经适配器验证时才可路由。当前真实 Hook 缺原生 mention_ids、稳定 msgsvrid 和可靠群成员证明，因此不能构造这些证据。
- 严格解析有限命令注册表，非法语法、未知参数和非管理员消息返回不可执行的结构化 ChatFallbackContext。原文保留为普通聊天内容，但路由标记、权限结果与 sender_scope 由代码生成，不读取正文中的伪造标签。
- MockCommandApplication 只有内存态效果；同一进程内按账号和事件 ID 去重，内容冲突拒绝。执行失败返回 EXECUTION_FAILED，不交给普通 Agent 重试。L2 只返回待确认占位结果，不生成真实挑战。
- CommandResultProjector 仅放行公开字段，ResponseComposer 固定写入权威状态与数值；MockPresenter 只能提供非关键叙述。模型式叙述不合规时退回确定性模板。

## 限制

可选端口已接入主 Gateway，但 FastAPI 的 HTTP 入口未配置，Mock 状态不持久，未实现确认管理器、真实 Supervisor、真实管理权限配置或任何真实微信命令执行。不得把 Mock 测试通过写为 M3/M5 命令验收通过。生产执行器与配置写入层必须继续隔离 LLM 工具。

## 2026-10-29 补充

`MockCommandResponder` 现在通过 `CommandResponder` 端口接入主 `Gateway` 的可选回复路径。只有调用方传入 `CommandEvidence` 才尝试命令路由；无证据或不获授权时使用普通 Mock Provider。命令执行失败由本地固定结果模板生成草稿，不回退聊天。FastAPI 运行入口没有配置该端口，真实 Hook 仍缺来源认证和稳定事件 ID，因此不能触发管理命令。Mock 结果仍只在进程内保存，不保证异常重启后的执行结果恢复。
