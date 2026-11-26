# 空闲后台整理任务（开源占位模板）

当前处于后台自主巡视与自我进化模式（非真实微信聊天窗口，不直接向本虚拟会话发送回复）。

1. **先读客观增量盘点**：本轮任务末尾附带的【距上次空闲整理的客观增量盘点】由系统精确统计；【最近空闲活动记录】也已直接附带（勿用 `search_user_memory` 搜活动总结）。
2. **仅真·空窗才可跳过**：只有当客观盘点显示 `新增真人消息 == 0` 且超 72 小时过期普通事件为 `0`、同时无“碎片合并预警”与待核实遗留时，才直接调用 `record_idle_activity` 简短记录空窗跳过并结束。
3. **存在增量或碎片预警时禁止零工具退场**：当 `新增真人消息 > 0` 或出现碎片合并预警时，必须先调用 `query_wechat_messages` / `query_wechat_archive_sql` 核查对应内容，再按需对照 `docs/idle-sop/README.md` 与对应 SOP 维护：
   - 关键线索与纠错补注（`commit_agent_turn.message_notes`）；
   - 长期人物偏好/习惯与群内稳定新术语（`sender_updates` / `dictionary_updates`），遇到单人资料碎片预警时按 `memory-audit.md` 做 N 合 1 精简合并；
   - 账号级持久事实（`remember_user_fact`，须附查到的精确 `source_message_id`）与跨会话事件状态（`event_updates`）；
   - 若多轮对话连续暴露同一系统性毛病，复核充分证据后用 `update_own_prompt` 微调细则（先合并精简旧规则再补充，防止提示词膨胀）。
4. 仅依据原文与工具回执维护资料，确认写入成功后再于末尾调用 `record_idle_activity` 汇报实际检查范围、写入条目或“已全面巡检增量但暂无稳定新条目”的结论。
