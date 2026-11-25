import asyncio
from pathlib import Path

from wechat_agent.hook_status_recovery import observation_from_hook_status
from wechat_agent.hook_outbound import HookSendResult
from wechat_agent.pilot import Pilot


ACCOUNT = "wxid_bot_account"
ROOM = "58959893267@chatroom"
SENDER = "wxid_member_123"


def status(**overrides):
    value = {
        "IsLogin": 1,
        "SyncBatchToUsername": ACCOUNT,
        "SyncBatchFromUsername": ROOM,
        "SyncBatchProcessorCalls": 42,
        "SyncBatchItemCount": 1,
        "SyncBatchLastMsgType": 1,
        "SyncBatchContent": f"{SENDER}:\nluna写的东西bug就是多",
        "ServerPort": 30001,
    }
    value.update(overrides)
    return value


def test_hook_status_recovery_parses_group_sender_and_plain_body():
    event = observation_from_hook_status(
        status(), account_id=ACCOUNT, conversation_id=ROOM,
    )
    assert event is not None
    assert event.sender_id == SENDER
    assert event.text == f"{SENDER}:\nluna写的东西bug就是多"
    assert event.conversation_id == ROOM
    assert event.raw_message_json.find('"hook_status_recovery_sequence":42') >= 0


def test_hook_status_recovery_rejects_wrong_account_room_and_non_text():
    assert observation_from_hook_status(
        status(SyncBatchToUsername="wxid_other_account"),
        account_id=ACCOUNT, conversation_id=ROOM,
    ) is None
    assert observation_from_hook_status(
        status(SyncBatchFromUsername="other@chatroom"),
        account_id=ACCOUNT, conversation_id=ROOM,
    ) is None
    assert observation_from_hook_status(
        status(SyncBatchLastMsgType=3),
        account_id=ACCOUNT, conversation_id=ROOM,
    ) is None


def test_recovered_snapshot_is_duplicate_after_agent_restart(tmp_path: Path):
    async def empty_reply(_context):
        return ""

    snapshot = status()
    first = Pilot(
        tmp_path / "pilot.sqlite3", ACCOUNT, ROOM, empty_reply,
        lambda _draft: HookSendResult("queued"),
    )
    second = None
    try:
        assert first.recover_latest_hook_status_message(snapshot) == "queued_test"
        first.db.close()
        second = Pilot(
            tmp_path / "pilot.sqlite3", ACCOUNT, ROOM, empty_reply,
            lambda _draft: HookSendResult("queued"),
        )
        assert second.recover_latest_hook_status_message(snapshot) == "duplicate"
        assert second.scheduler.pending_count() == 1
    finally:
        if second is not None:
            second.db.close()


def test_incomplete_turn_requeues_even_after_send_attempt(tmp_path: Path):
    async def scenario():
        calls = 0
        completed = asyncio.Event()

        async def reply(_context):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("simulated incomplete turn after tool effect")
            completed.set()
            return ""

        def send_state(_scope):
            return {"send_attempt_count": 1, "side_effect_started": True}

        pilot = Pilot(
            tmp_path / "retry.sqlite3", ACCOUNT, ROOM, reply,
            lambda _draft: HookSendResult("queued"), delivery_state=send_state,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        pilot.scheduler.MAX_PROCESS_ATTEMPTS = 3
        event = observation_from_hook_status(
            status(), account_id=ACCOUNT, conversation_id=ROOM,
        )
        assert event is not None
        assert pilot.offer(event) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        try:
            await asyncio.wait_for(completed.wait(), timeout=5)
            assert calls == 2
            assert pilot.scheduler.state_counts().get("model_seen") == 1
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            pilot.db.close()

    asyncio.run(scenario())
