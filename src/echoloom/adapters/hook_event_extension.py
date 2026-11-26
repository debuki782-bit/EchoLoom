"""Receive-only additions for the pinned 4.1.10.27 Hook, behind its callback contract."""
from __future__ import annotations

import json

from ..contacts import is_contact_id, is_room_id

DLL_SHA = "f487e4f56930456a25fb0d7b058c77e2dcd364e268d49bdfff4dc0e3be26ba36"

# Offsets and caller restrictions are tied to DLL_SHA. No native function calls,
# replacements or return-value changes: copy bounded data during natural receipt.
PROBE = r'''
const wx = Process.getModuleByName('Weixin.dll');
function nativeString(p) {
    try {
        const n = p.add(16).readU64().toNumber(), cap = p.add(24).readU64().toNumber();
        if (n < 1 || n > 65535 || cap < n || cap > 1048576) return '';
        return (cap < 16 ? p : p.readPointer()).readUtf8String(n);
    } catch (_) { return ''; }
}
function field(p, offset, bit) {
    try {
        if (!(p.add(0x6c).readU32() & bit)) return '';
        const a = p.add(offset).readPointer();
        if (!a.readPointer().equals(wx.base.add(0x82643a8))) return '';
        return nativeString(a.add(8).readPointer());
    } catch (_) { return ''; }
}
function addMsg(p) {
    if (!p.readPointer().equals(wx.base.add(0x83df408))) return null;
    const bits = p.add(0x6c).readU32();
    if ((bits & 0x1e) !== 0x1e) return null;
    return {from: field(p, 8, 2), to: field(p, 0x18, 4), body: field(p, 0x20, 0x10),
        type: p.add(0x14).readU32(),
        source_id: (bits & 0x400) ? p.add(0x50).readU64().toString() : '0'};
}
Interceptor.attach(wx.base.add(0x16d5180), {onEnter(args) {
    try {
        const begin = args[1].readPointer(), end = args[1].add(8).readPointer();
        const bytes = end.sub(begin).toInt32();
        if (bytes < 0 || bytes % 0x78 || bytes > 0x78 * 1024) return;
        for (let i = 0; i < Math.min(bytes / 0x78, 64); i++) {
            const event = addMsg(begin.add(i * 0x78));
            if (!event || event.type !== 49 || !event.body) continue;
            // The pinned base Hook already delivers shorter cards. Never send them twice.
            if (unescape(encodeURIComponent(event.body)).length >= 4096)
                send({event: 'long_card', ...event});
        }
    } catch (_) { send({event: 'read_error'}); }
}});
const receivingPat = new Map();
Interceptor.attach(wx.base.add(0x3565370), {
    onEnter(args) {
        this.thread = this.threadId;
        try {
            const event = addMsg(args[2]);
            if (event) receivingPat.set(this.thread, event);
            else send({event: 'read_error'});
        } catch (_) { send({event: 'read_error'}); }
    },
    onLeave() { receivingPat.delete(this.thread); }
});
Interceptor.attach(wx.base.add(0x23924a0), {
    onEnter(args) {
        // The same parser also runs while displaying old messages. Only the
        // verified incoming notification handler may create an inbound event.
        this.accept = this.returnAddress.equals(wx.base.add(0x3563f39)) && receivingPat.has(this.threadId);
        this.p = args[0]; this.thread = this.threadId;
    },
    onLeave() {
        if (!this.accept) return;
        const event = receivingPat.get(this.thread), p = this.p;
        if (event) send({event: 'pat', ...event, actor: nativeString(p),
            chat: nativeString(p.add(0x20)), target: nativeString(p.add(0x40)),
            suffix: nativeString(p.add(0x60))});
    }
});
send({event: 'ready'});
'''


def event_envelope(event: object, account: str, tick: int) -> dict | None:
    """Validate native routing before converting to the existing Hook envelope."""
    if not isinstance(event, dict) or not is_contact_id(account):
        return None
    if event.get("to") != account:
        return None
    origin = event.get("from")
    if not (is_contact_id(origin) or is_room_id(origin)) or origin == account:
        return None
    body = event.get("body")
    if not isinstance(body, str) or not 1 <= len(body.encode("utf-8")) <= 65535:
        return None
    room = origin if is_room_id(origin) else ""
    sender = origin
    message_type = event.get("type")
    if event.get("event") == "long_card":
        if message_type != 49 or len(body.encode("utf-8")) < 4096:
            return None
        if room:
            sender, sep, _ = body.partition(":\n")
            if not sep or not is_contact_id(sender):
                return None
    elif event.get("event") == "pat":
        actor, target, chat = (event.get(k) for k in ("actor", "target", "chat"))
        if not is_contact_id(actor) or not is_contact_id(target) or actor == account:
            return None
        if room:
            if chat != room:
                return None
        elif actor != origin or chat not in (origin, account):
            return None
        sender = actor
        message_type = 10002
        body = json.dumps({"kind": "pat_message", "actor_id": actor, "target_id": target,
                           "suffix": str(event.get("suffix") or "")[:256]}, ensure_ascii=False)
    else:
        return None
    try:
        source_id = int(event.get("source_id", "0"))
        if not 0 <= source_id < 2**64:
            return None
    except (ValueError, TypeError):
        return None
    record = {"fromid": origin, "toid": account, "room": room, "sender": sender,
              "msg": body, "msgtype": message_type, "msgsvrid": source_id, "time": tick,
              "event_source": "hook_receive_extension"}
    return {"sendorrecv": 2, "selfwxid": account, "msglist": [record]}
