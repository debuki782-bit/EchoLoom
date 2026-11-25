"""trace_view must merge session traces into a readable, ordered transcript."""

import json

from wechat_agent.trace_view import load_records, render, summarize


def _write(path, records):
    path.mkdir(parents=True, exist_ok=True)
    (path / "trace.jsonl").write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in records) + "\n",
        encoding="utf-8",
    )


def _sample_records():
    return [
        {
            "record": "model_request",
            "request_id": "r1",
            "session_id": "s1",
            "conversation_id": "wxid_a",
            "timestamp": "2026-11-01T08:00:00+00:00",
            "contents": [
                {"role": "user", "parts": [
                    {"text": "本轮聊天记录：\n[*{在吗}*]"},
                    {"text": "【本轮动态召回尾块】当前会话：私聊…"},
                ]},
                {"role": "model", "parts": [{"text": "上一轮回复"}]},
            ],
        },
        {
            "session_id": "s1",
            "conversation_id": "wxid_a",
            "event_id": "e1",
            "author": "model",
            "final": True,
            "timestamp": 1793520001.5,
            "content": [
                {"text": "在的"},
                {"tool_call": {"name": "commit_agent_turn", "args": {"reply_text": "在的"}}},
            ],
        },
        {
            "session_id": "s1",
            "conversation_id": "wxid_a",
            "event_id": "e2",
            "author": "user",
            "timestamp": 1793520002.5,
            "content": [
                {"tool_result": {"name": "commit_agent_turn", "result": {"status": "sent"}}},
            ],
        },
    ]


def test_render_orders_and_labels_turns(tmp_path):
    _write(tmp_path, _sample_records())
    records = load_records(tmp_path)
    assert [item.get("record") or item.get("event_id") for item in records] == [
        "model_request",
        "e1",
        "e2",
    ]
    text = render(records)
    assert "模型请求" in text
    assert "【本轮动态召回尾块】" in text
    assert "模型回复" in text and "在的" in text
    assert "🔧 调用" in text and "commit_agent_turn" in text
    assert "📋 结果" in text and '"status": "sent"' in text
    rows = summarize(tmp_path)
    assert rows[0]["conversation_id"] == "wxid_a"
    assert rows[0]["turns"] == 1
    assert rows[0]["tool_calls"] == 1


def test_render_clips_long_text_unless_full(tmp_path):
    long_text = "细" * 5000
    _write(tmp_path, [
        {
            "record": "model_request",
            "request_id": "r1",
            "session_id": "s",
            "conversation_id": "wxid_b",
            "timestamp": "2026-11-01T08:00:00+00:00",
            "contents": [{"role": "user", "parts": [{"text": long_text}]}],
        }
    ])
    clipped = render(load_records(tmp_path))
    assert "已截断" in clipped
    assert long_text not in clipped
    full = render(load_records(tmp_path), full=True)
    assert long_text in full


def test_conversation_prefix_filters_other_chats(tmp_path):
    records = _sample_records() + [
        {
            "session_id": "s2",
            "conversation_id": "wxid_other",
            "event_id": "e3",
            "author": "model",
            "final": True,
            "timestamp": 1793520003.5,
            "content": [{"text": "别的聊天"}],
        }
    ]
    _write(tmp_path, records)
    only_a = load_records(tmp_path, conversation_prefix="wxid_a")
    assert {item.get("conversation_id") for item in only_a} == {"wxid_a"}
    assert summarize(tmp_path)[0]["conversation_id"] in {"wxid_a", "wxid_other"}
