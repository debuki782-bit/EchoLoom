# 记忆写入路由 SOP

1. 先定位原始消息、发送人和精确 ID，再看已有资料；确定这条信息以后是否真的需要跨会话使用。空闲整理中如需写入 `message_notes` 或 `remember_user_fact`，均可先用 `query_wechat_messages` 查出对应历史消息 ID，并在 `remember_user_fact` 显式传入 `source_message_id`。
2. 按“以后要怎么用”选唯一目标：单条消息指代/承诺写消息备注（`message_notes`）；稳定人物偏好/互动习惯写 sender profile（`sender_updates`）；稳定词义/黑话写词典（`dictionary_updates`）；跨聊天待办写事件（`event_updates`，高优严格限 200 字以内）；账号级持久客观事实写长期事实（`remember_user_fact`）。来源和结论必须分开。
3. 找不到直接单一关键词时，可对 `search_user_memory` 和 `search_message_notes` 使用空格分隔的多个关键词；空闲模式下 `search_message_notes` 会自动检索整个账号的历史批注。一条内容不要复制到多个类别。
4. 更正旧内容时先定位精确 ID，按工具提供的删除/更新方式处理，再写新内容；不要用一个类别的新记录覆盖其他独立条目。
5. 只有工具回执成功才报告已保存。认真检查增量后若确实无需写入，记录实际核对范围与理由即可。
