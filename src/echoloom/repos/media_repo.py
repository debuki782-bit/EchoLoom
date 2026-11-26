"""Repository owning message_media, image_library_revisions, message_audio, and message_files."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

from ..contacts import is_group_conversation

if TYPE_CHECKING:
    from .knowledge_repo import KnowledgeRepo
    from .message_repo import MessageRepo


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _bounded_text(value: str, limit: int) -> str:
    value = value.strip()
    if not value or len(value) > limit or "\0" in value:
        raise ValueError("text is empty or exceeds its limit")
    return value


class MediaRepo:
    """Durable repository for image artifacts, curated image library, and voice audio."""

    def __init__(
        self,
        db: sqlite3.Connection,
        lock: threading.RLock,
        transaction_fn: Callable[[], AbstractContextManager[None]],
        *,
        media_root: Path,
        audio_root: Path,
        file_root: Path | None = None,
        messages: MessageRepo | None = None,
        knowledge: KnowledgeRepo | None = None,
    ) -> None:
        self.db = db
        self._lock = lock
        self._transaction = transaction_fn
        self.media_root = media_root
        self.audio_root = audio_root
        self.file_root = (
            file_root.resolve()
            if file_root is not None
            else (media_root.parent / "received-files").resolve()
        )
        self._messages = messages
        self._knowledge = knowledge

    def _message_in_scope(self, message_id: str, account_id: str) -> bool:
        if self._messages is not None:
            return self._messages._message_in_scope(message_id, account_id)
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages "
                "WHERE message_id=? AND account_id=? AND deleted_at IS NULL",
                (message_id, account_id),
            ).fetchone()
            is not None
        )

    def _conversation_for_message(self, message_id: str, account_id: str) -> str:
        if self._messages is not None:
            return self._messages._conversation_for_message(message_id, account_id)
        row = self.db.execute(
            "SELECT conversation_id FROM source_messages WHERE message_id=? AND account_id=?",
            (message_id, account_id),
        ).fetchone()
        if row is None:
            raise KeyError("source message unavailable")
        return str(row["conversation_id"])

    def archive_agent_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        artifact_path: Path,
        mime_type: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
        quick_send: bool = True,
    ) -> dict[str, str]:
        """Link a durable PNG/JPEG/GIF/WebP asset to its Agent outbound message."""
        if self._messages is None:
            raise RuntimeError("message repository is not configured on MediaRepo")
        path = artifact_path.resolve()
        if not path.is_relative_to(self.media_root) or not path.is_file():
            raise ValueError("generated image must be saved under the media root")
        data = path.read_bytes()
        if not 0 < len(data) <= 8_000_000:
            raise ValueError("generated image size out of range")
        digest = hashlib.sha256(data).hexdigest()
        image_type_ok = (
            (mime_type == "image/png" and data.startswith(b"\x89PNG\r\n\x1a\n"))
            or (mime_type == "image/jpeg" and data.startswith(b"\xff\xd8\xff"))
            or (mime_type == "image/gif" and data.startswith((b"GIF87a", b"GIF89a")))
            or (
                mime_type == "image/webp"
                and len(data) >= 12
                and data[:4] == b"RIFF"
                and data[8:12] == b"WEBP"
            )
        )
        if not image_type_ok:
            raise ValueError("unsupported image asset type or invalid header")
        if not operation_id or len(operation_id) > 256:
            raise ValueError("invalid operation ID")
        if delivery_state not in ("queued", "unknown", "not_sent"):
            raise ValueError("invalid generated-image archive state")
        raw = {
            "direction": "outbound",
            "fromid": account_id,
            "toid": conversation_id,
            "room": conversation_id if is_group_conversation(conversation_id) else "",
            "msg": "[Agent 发送图片]",
            "msgtype": 3,
            "local_operation_id": operation_id,
            "delivery_state": delivery_state,
        }
        archived = self._messages.archive_message(
            account_id=account_id,
            conversation_id=conversation_id,
            sender_id=account_id,
            raw_envelope=raw,
            source_record=raw,
            body_text=json.dumps({"kind": "image_message"}, ensure_ascii=False),
            direction="outbound",
            delivery_state=delivery_state,
        )
        image_id = (
            "img_"
            + hashlib.sha256(
                f"{account_id}\0{conversation_id}\0{archived.message_id}".encode()
            ).hexdigest()[:24]
        )
        now = _now()
        with self._transaction():
            self.db.execute(
                "INSERT OR IGNORE INTO message_media "
                "(image_id,account_id,conversation_id,message_id,original_references_json,"
                "state,artifact_path,mime_type,byte_count,sha256,source_sha256,created_at,"
                "updated_at) VALUES (?,?,?,?,?,'ready',?,?,?,?,?,?,?)",
                (
                    image_id,
                    account_id,
                    conversation_id,
                    archived.message_id,
                    "[]",
                    str(path),
                    mime_type,
                    len(data),
                    digest,
                    digest,
                    now,
                    now,
                ),
            )
            self.db.execute(
                "UPDATE message_media SET library_kind='image',library_prompt=?,quick_send=? "
                "WHERE account_id=? AND image_id=?",
                (
                    ((library_prompt if library_prompt is not None else note) or "")[:300],
                    int(bool(quick_send)),
                    account_id,
                    image_id,
                ),
            )
        if note:
            if self._knowledge is None:
                raise RuntimeError("knowledge repository is not configured on MediaRepo")
            self._knowledge.ensure_note(
                account_id=account_id,
                message_id=archived.message_id,
                content=note,
            )
        return {"message_id": archived.message_id, "image_id": image_id}

    def import_library_image(
        self,
        *,
        account_id: str,
        source_path: Path,
        description: str = "",
        display_order: int = 0,
        quick_send: bool = True,
    ) -> dict[str, object]:
        """Import a human-curated local image into the account-owned gallery."""
        if self._messages is None:
            raise RuntimeError("message repository is not configured on MediaRepo")
        source = source_path.resolve(strict=True)
        if not source.is_file() or source.suffix.lower() not in {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
        }:
            raise ValueError("只支持 PNG、JPG、GIF、WebP 图片文件。")
        if (
            not isinstance(description, str)
            or len(description) > 300
            or not isinstance(quick_send, bool)
        ):
            raise ValueError("描述最多 300 字且 quick_send 必须为布尔值。")
        if type(display_order) is not int or not 0 <= display_order <= 10000:
            raise ValueError("display_order 必须在 0–10000。")
        data = source.read_bytes()
        if not 0 < len(data) <= 8_000_000:
            raise ValueError("图片必须小于等于 8 MB。")
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }[source.suffix.lower()]
        valid = (
            (mime == "image/png" and data.startswith(b"\x89PNG\r\n\x1a\n"))
            or (mime == "image/jpeg" and data.startswith(b"\xff\xd8\xff"))
            or (mime == "image/gif" and data.startswith((b"GIF87a", b"GIF89a")))
            or (
                mime == "image/webp"
                and len(data) >= 12
                and data[:4] == b"RIFF"
                and data[8:12] == b"WEBP"
            )
        )
        if not valid:
            raise ValueError("扩展名与图片内容不匹配。")
        digest = hashlib.sha256(data).hexdigest()
        existing = self.db.execute(
            "SELECT image_id FROM message_media WHERE account_id=? AND sha256=? "
            "AND state='ready' ORDER BY created_at LIMIT 1",
            (account_id, digest),
        ).fetchone()
        if existing is not None:
            image_id = str(existing["image_id"])
            return self.manage_image_library(
                account_id=account_id,
                image_id=image_id,
                actor="manual",
                description=description or None,
                quick_send=quick_send,
                display_order=display_order,
            )
        library_conversation = "library:personal"
        marker = uuid4().hex
        raw = {"local_library_import": marker, "direction": "outbound"}
        archived = self._messages.archive_message(
            account_id=account_id,
            conversation_id=library_conversation,
            sender_id=account_id,
            raw_envelope=raw,
            source_record=raw,
            body_text=json.dumps({"kind": "image_message"}, ensure_ascii=False),
            direction="outbound",
            delivery_state="library_import",
            retention_days=3650,
        )
        suffix = source.suffix.lower()
        destination_dir = self.media_root / "manual-library"
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination = destination_dir / f"{uuid4().hex}{suffix}"
        destination.write_bytes(data)
        image_id = (
            "img_"
            + hashlib.sha256(
                f"{account_id}\0{library_conversation}\0{archived.message_id}".encode()
            ).hexdigest()[:24]
        )
        now = _now()
        with self._transaction():
            self.db.execute(
                "INSERT INTO message_media (image_id,account_id,conversation_id,message_id,"
                "original_references_json,state,artifact_path,mime_type,byte_count,sha256,"
                "source_sha256,library_kind,library_prompt,quick_send,display_order,"
                "created_at,updated_at) "
                "VALUES (?,?,?,?,'[]','ready',?,?,?,?,?,'image',?,?,?,?,?)",
                (
                    image_id,
                    account_id,
                    library_conversation,
                    archived.message_id,
                    str(destination),
                    mime,
                    len(data),
                    digest,
                    digest,
                    description.strip(),
                    int(quick_send),
                    display_order,
                    now,
                    now,
                ),
            )
            self.db.execute(
                "INSERT INTO image_library_revisions VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    uuid4().hex,
                    account_id,
                    image_id,
                    "manual",
                    "import",
                    "image",
                    description.strip(),
                    int(quick_send),
                    display_order,
                    now,
                ),
            )
        return {
            "status": "imported",
            "image_id": image_id,
            "quick_send": quick_send,
            "display_order": display_order,
        }

    def begin_image_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        original_references: Sequence[dict[str, str]],
    ) -> dict[str, object]:
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        refs_json = json.dumps(list(original_references), ensure_ascii=False, separators=(",", ":"))
        with self._transaction():
            row = self.db.execute(
                "SELECT * FROM message_media WHERE message_id=? AND account_id=? "
                "AND conversation_id=?",
                (message_id, account_id, conversation_id),
            ).fetchone()
            if row is not None:
                return dict(row)
            image_id = (
                "img_"
                + hashlib.sha256(
                    f"{account_id}\0{conversation_id}\0{message_id}".encode()
                ).hexdigest()[:24]
            )
            now = _now()
            self.db.execute(
                "INSERT INTO message_media "
                "(image_id,account_id,conversation_id,message_id,original_references_json,"
                "state,created_at,updated_at) VALUES (?,?,?,?,?,'pending',?,?)",
                (image_id, account_id, conversation_id, message_id, refs_json, now, now),
            )
            return dict(
                self.db.execute(
                    "SELECT * FROM message_media WHERE image_id=?",
                    (image_id,),
                ).fetchone()
            )

    def complete_image_ingest(
        self,
        *,
        image_id: str,
        account_id: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
        source_kind: str = "original",
    ) -> None:
        if (
            byte_count <= 0
            or len(sha256) != 64
            or len(source_sha256) != 64
            or source_kind not in ("original", "thumbnail_inferred")
        ):
            raise ValueError("invalid image metadata")
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_media SET state='ready',artifact_path=?,mime_type=?,"
                "byte_count=?,sha256=?,source_sha256=?,source_kind=?,error_code=NULL,updated_at=? "
                "WHERE image_id=? AND account_id=? AND state='pending'",
                (
                    artifact_path,
                    mime_type,
                    byte_count,
                    sha256,
                    source_sha256,
                    source_kind,
                    _now(),
                    image_id,
                    account_id,
                ),
            )
            if not cur.rowcount:
                raise KeyError("pending image ingest unavailable")

    def promote_inferred_image(
        self,
        *,
        image_id: str,
        account_id: str,
        previous_sha256: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
    ) -> bool:
        """Replace an inferred thumbnail only when a verified original is now cached."""
        if (
            byte_count <= 0
            or len(previous_sha256) != 64
            or len(sha256) != 64
            or len(source_sha256) != 64
            or mime_type
            not in {
                "image/jpeg",
                "image/png",
                "image/webp",
                "image/gif",
                "application/x-wechat-wxgf",
            }
        ):
            raise ValueError("invalid promoted image metadata")
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_media SET artifact_path=?,mime_type=?,byte_count=?,"
                "sha256=?,source_sha256=?,source_kind='original',vision_summary=NULL,"
                "vision_model_id=NULL,vision_inspected_at=NULL,updated_at=? "
                "WHERE image_id=? AND account_id=? AND state='ready' "
                "AND source_kind='thumbnail_inferred' AND sha256=?",
                (
                    artifact_path,
                    mime_type,
                    byte_count,
                    sha256,
                    source_sha256,
                    _now(),
                    image_id,
                    account_id,
                    previous_sha256,
                ),
            )
        return cur.rowcount == 1

    def fail_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> None:
        if not error_code or len(error_code) > 64:
            raise ValueError("invalid media failure code")
        with self._transaction():
            self.db.execute(
                "UPDATE message_media SET state='failed',error_code=?,updated_at=? "
                "WHERE image_id=? AND account_id=? AND state='pending'",
                (error_code, _now(), image_id, account_id),
            )

    def retry_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> bool:
        """Retry a recoverable image failure when its inbound event is replayed."""
        if error_code not in (
            "original_image_not_found_in_cache",
            "image_ingest_failed",
            "original_image_url_not_allowed",
            "image_decoder_unavailable",
        ):
            return False
        with self._transaction():
            cursor = self.db.execute(
                "UPDATE message_media SET state='pending',error_code=NULL,updated_at=? "
                "WHERE image_id=? AND account_id=? AND state='failed' AND error_code=?",
                (_now(), image_id, account_id, error_code),
            )
            return cursor.rowcount == 1

    def image_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT image_id,state,mime_type,byte_count,sha256,error_code,source_kind,"
            "vision_summary,vision_model_id,vision_inspected_at "
            "FROM message_media WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def image_metadata_for_messages(
        self, *, account_id: str, message_ids: Sequence[str]
    ) -> dict[str, dict[str, object]]:
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        with self._lock:
            rows = self.db.execute(
                "SELECT message_id,image_id,state,mime_type,byte_count,sha256,error_code,"
                "source_kind,vision_summary,vision_model_id,vision_inspected_at "
                f"FROM message_media WHERE account_id=? AND message_id IN ({placeholders})",
                (account_id, *message_ids),
            ).fetchall()
        return {str(row["message_id"]): dict(row) for row in rows}

    def save_image_description(
        self, *, account_id: str, image_id: str, description: str, model_id: str
    ) -> None:
        description = _bounded_text(description, 4000)
        model_id = _bounded_text(model_id, 100)
        with self._transaction():
            cursor = self.db.execute(
                "UPDATE message_media SET vision_summary=?,vision_model_id=?,"
                "vision_inspected_at=?,updated_at=? WHERE image_id=? AND account_id=? "
                "AND state='ready'",
                (description, model_id, _now(), _now(), image_id, account_id),
            )
            if not cursor.rowcount:
                raise KeyError("ready image unavailable")

    def get_image_by_id(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        image_id: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        query = (
            "SELECT image_id,message_id,account_id,conversation_id,"
            "original_references_json,artifact_path,mime_type,byte_count,sha256,"
            "source_sha256,state,error_code,source_kind,vision_summary,vision_model_id,"
            "vision_inspected_at "
            "FROM message_media WHERE image_id=? AND account_id=?"
        )
        if not include_unready:
            query += " AND state='ready'"
        params: tuple[object, ...] = (image_id, account_id)
        if conversation_id is not None:
            query += " AND conversation_id=?"
            params += (conversation_id,)
        row = self.db.execute(query, params).fetchone()
        if row is None:
            return None
        message = self.db.execute(
            "SELECT expires_at,deleted_at FROM source_messages WHERE message_id=? AND account_id=?",
            (row["message_id"], account_id),
        ).fetchone()
        if message is None or message["deleted_at"] is not None or message["expires_at"] <= _now():
            return None
        return dict(row)

    def list_images(
        self,
        *,
        account_id: str,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        if (
            type(limit) is not int
            or type(offset) is not int
            or not 1 <= limit <= 50
            or not 0 <= offset <= 1_000_000
        ):
            raise ValueError("image list window out of range")
        query = (
            "SELECT m.image_id,m.message_id,m.conversation_id,m.artifact_path,m.mime_type,"
            "m.byte_count,m.state,m.vision_summary,m.vision_model_id,m.vision_inspected_at,"
            "m.library_kind,m.library_prompt,m.quick_send,m.display_order,"
            "COALESCE(NULLIF(m.library_prompt,''),m.vision_summary,(SELECT a.content "
            "FROM annotations a WHERE a.message_id=m.message_id AND a.state='active' "
            "ORDER BY a.updated_at DESC LIMIT 1),'') AS description "
            "FROM message_media m JOIN source_messages s ON s.message_id=m.message_id "
            "WHERE m.account_id=? AND m.state='ready' "
            "AND s.deleted_at IS NULL AND s.expires_at>?"
        )
        params: list[object] = [account_id, _now()]
        if conversation_id is not None:
            query += " AND m.conversation_id=?"
            params.append(conversation_id)
        query += " ORDER BY s.captured_at DESC LIMIT ? OFFSET ?"
        params.extend((limit, offset))
        rows = self.db.execute(query, params)
        return [dict(row) for row in rows]

    def quick_send_images(self, *, account_id: str, limit: int = 10) -> list[dict[str, object]]:
        if type(limit) is not int or not 1 <= limit <= 30:
            raise ValueError("quick-send image limit out of range")
        rows = self.db.execute(
            "SELECT m.image_id,m.library_kind,m.library_prompt,m.display_order "
            "FROM message_media m JOIN source_messages s ON s.message_id=m.message_id "
            "WHERE m.account_id=? AND m.state='ready' AND m.quick_send=1 "
            "AND s.deleted_at IS NULL AND s.expires_at>? "
            "ORDER BY m.display_order ASC,m.created_at DESC,m.image_id LIMIT ?",
            (account_id, _now(), limit),
        )
        return [dict(row) for row in rows]

    def manage_image_library(
        self,
        *,
        account_id: str,
        image_id: str,
        actor: str = "agent",
        description: str | None = None,
        quick_send: bool | None = None,
        display_order: int | None = None,
    ) -> dict[str, object]:
        if not isinstance(image_id, str) or not image_id.startswith("img_") or len(image_id) > 64:
            return {"status": "rejected", "reason": "请使用图库返回的完整 image_id。"}
        if description is not None and (
            not isinstance(description, str) or len(description) > 300
        ):
            return {"status": "rejected", "reason": "展示描述最多 300 字。"}
        if quick_send is not None and type(quick_send) is not bool:
            return {"status": "rejected", "reason": "quick_send 必须是 true 或 false。"}
        if display_order is not None and (
            type(display_order) is not int or not 0 <= display_order <= 10000
        ):
            return {"status": "rejected", "reason": "展示顺序必须是 0–10000 的整数。"}
        with self._transaction():
            row = self.db.execute(
                "SELECT library_kind,library_prompt,quick_send,display_order "
                "FROM message_media WHERE account_id=? AND image_id=? AND state='ready'",
                (account_id, image_id),
            ).fetchone()
            if row is None:
                return {"status": "unavailable", "reason": "图库中没有这张可用图片。"}
            next_values = (
                row["library_kind"],
                row["library_prompt"] if description is None else description.strip(),
                row["quick_send"] if quick_send is None else int(quick_send),
                row["display_order"] if display_order is None else display_order,
            )
            now = _now()
            self.db.execute(
                "UPDATE message_media "
                "SET library_prompt=?,quick_send=?,display_order=?,updated_at=? "
                "WHERE account_id=? AND image_id=?",
                (next_values[1], next_values[2], next_values[3], now, account_id, image_id),
            )
            self.db.execute(
                "INSERT INTO image_library_revisions VALUES (?,?,?,?,?,?,?,?,?,?)",
                (uuid4().hex, account_id, image_id, actor, "update", *next_values, now),
            )
            return {
                "status": "updated",
                "image_id": image_id,
                "description": next_values[1],
                "quick_send": bool(next_values[2]),
                "display_order": next_values[3],
                "constraints": {
                    "description_max_chars": 300,
                    "quick_send_prompt_max_images": 10,
                    "display_order_range": "0..10000",
                },
            }

    def get_image_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT image_id,message_id,original_references_json,artifact_path,mime_type,"
            "byte_count,sha256,source_sha256,state,source_kind FROM message_media "
            "WHERE account_id=? AND conversation_id=? AND message_id=?",
            (account_id, conversation_id, message_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def get_latest_ready_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        """Return the newest unexpired image in one conversation, optionally before a turn."""
        cutoff = None
        if before_message_id:
            row = self.db.execute(
                "SELECT captured_at FROM source_messages WHERE message_id=? AND account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL",
                (before_message_id, account_id, conversation_id),
            ).fetchone()
            if row is None:
                return None
            cutoff = row["captured_at"]
        query = (
            "SELECT m.image_id,m.message_id,m.original_references_json,m.artifact_path,"
            "m.mime_type,m.byte_count,m.sha256,m.source_sha256,m.state,m.source_kind,"
            "m.vision_summary,m.vision_model_id,m.vision_inspected_at "
            "FROM message_media m JOIN source_messages s ON s.message_id=m.message_id "
            "WHERE m.account_id=? AND m.conversation_id=? "
            "AND s.deleted_at IS NULL AND s.expires_at>?"
        )
        if not include_unready:
            query += " AND m.state='ready'"
        params: list[object] = [account_id, conversation_id, _now()]
        if cutoff is not None:
            query += " AND s.captured_at<=?"
            params.append(cutoff)
        query += " ORDER BY s.captured_at DESC,m.created_at DESC LIMIT 1"
        row = self.db.execute(query, params).fetchone()
        return dict(row) if row is not None else None

    def get_image_by_reference(
        self,
        *,
        account_id: str,
        conversation_id: str,
        reference: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        rows = self.db.execute(
            "SELECT m.image_id,m.message_id,m.original_references_json,m.artifact_path,"
            "m.mime_type,m.byte_count,m.sha256,m.source_sha256,m.state "
            "FROM message_media m JOIN source_messages s ON s.message_id=m.message_id "
            "WHERE m.account_id=? AND m.conversation_id=? "
            "AND (? OR m.state='ready') "
            "AND s.deleted_at IS NULL AND s.expires_at>? "
            "ORDER BY m.created_at DESC LIMIT 100",
            (account_id, conversation_id, include_unready, _now()),
        )
        for row in rows:
            try:
                refs = json.loads(row["original_references_json"])
            except json.JSONDecodeError:
                continue
            if any(isinstance(ref, dict) and ref.get("value") == reference for ref in refs):
                return dict(row)
        return None

    def begin_audio_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        duration_ms: int | None,
    ) -> dict[str, object]:
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        with self._transaction():
            row = self.db.execute(
                "SELECT * FROM message_audio WHERE message_id=? AND account_id=? "
                "AND conversation_id=?",
                (message_id, account_id, conversation_id),
            ).fetchone()
            if row is not None:
                return dict(row)
            audio_id = (
                "aud_"
                + hashlib.sha256(
                    f"{account_id}\0{conversation_id}\0{message_id}".encode()
                ).hexdigest()[:24]
            )
            now = _now()
            self.db.execute(
                "INSERT INTO message_audio "
                "(audio_id,account_id,conversation_id,message_id,state,mime_type,"
                "duration_ms,transcript_state,created_at,updated_at) "
                "VALUES (?,?,?,?,'pending','audio/wav',?,'not_started',?,?)",
                (audio_id, account_id, conversation_id, message_id, duration_ms, now, now),
            )
            return dict(
                self.db.execute(
                    "SELECT * FROM message_audio WHERE audio_id=?",
                    (audio_id,),
                ).fetchone()
            )

    def fail_audio_ingest_for_message(
        self, *, account_id: str, message_id: str, error_code: str
    ) -> dict[str, object]:
        if not error_code or len(error_code) > 64:
            raise ValueError("invalid audio failure code")
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        record = self.db.execute(
            "SELECT audio_id,state FROM message_audio WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        if record is None:
            # Store an explicit failure record so tools can report a stable state.
            record = self.begin_audio_ingest(
                account_id=account_id,
                conversation_id=self._conversation_for_message(
                    message_id,
                    account_id,
                ),
                message_id=message_id,
                duration_ms=None,
            )
        if record["state"] == "pending":
            with self._transaction():
                self.db.execute(
                    "UPDATE message_audio SET state='failed',error_code=?,updated_at=? "
                    "WHERE audio_id=? AND account_id=? AND state='pending'",
                    (error_code, _now(), record["audio_id"], account_id),
                )
        return self.audio_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        ) or {"state": "failed", "error_code": error_code}

    def complete_audio_ingest(
        self,
        *,
        audio_id: str,
        account_id: str,
        artifact_path: str,
        byte_count: int,
        sha256: str,
        duration_ms: int,
    ) -> None:
        if byte_count <= 0 or len(sha256) != 64 or duration_ms <= 0:
            raise ValueError("invalid audio metadata")
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_audio SET state='ready',artifact_path=?,byte_count=?,"
                "sha256=?,duration_ms=?,error_code=NULL,updated_at=? "
                "WHERE audio_id=? AND account_id=? AND state='pending'",
                (artifact_path, byte_count, sha256, duration_ms, _now(), audio_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("pending audio ingest unavailable")

    def audio_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT audio_id,state,mime_type,byte_count,sha256,duration_ms,error_code,"
            "transcript,transcript_model_id,transcript_state FROM message_audio "
            "WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def audio_metadata_for_messages(
        self, *, account_id: str, message_ids: Sequence[str]
    ) -> dict[str, dict[str, object]]:
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        with self._lock:
            rows = self.db.execute(
                "SELECT message_id,audio_id,state,mime_type,byte_count,sha256,duration_ms,"
                "error_code,transcript,transcript_model_id,transcript_state "
                f"FROM message_audio WHERE account_id=? AND message_id IN ({placeholders})",
                (account_id, *message_ids),
            ).fetchall()
        return {str(row["message_id"]): dict(row) for row in rows}

    def get_audio_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT a.*,s.expires_at,s.deleted_at FROM message_audio a "
            "JOIN source_messages s ON s.message_id=a.message_id "
            "WHERE a.account_id=? AND a.conversation_id=? AND a.message_id=? "
            "AND a.state='ready'",
            (account_id, conversation_id, message_id),
        ).fetchone()
        if row is None or row["deleted_at"] is not None or row["expires_at"] <= _now():
            return None
        return dict(row)

    def get_latest_ready_audio(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
    ) -> dict[str, object] | None:
        cutoff = None
        if before_message_id:
            row = self.db.execute(
                "SELECT captured_at FROM source_messages WHERE message_id=? AND account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL",
                (before_message_id, account_id, conversation_id),
            ).fetchone()
            if row is None:
                return None
            cutoff = row["captured_at"]
        query = (
            "SELECT a.*,s.expires_at,s.deleted_at,s.captured_at FROM message_audio a "
            "JOIN source_messages s ON s.message_id=a.message_id "
            "WHERE a.account_id=? AND a.conversation_id=? AND a.state='ready' "
            "AND s.deleted_at IS NULL AND s.expires_at>?"
        )
        params: list[object] = [account_id, conversation_id, _now()]
        if cutoff is not None:
            query += " AND s.captured_at<=?"
            params.append(cutoff)
        query += " ORDER BY s.captured_at DESC,a.created_at DESC LIMIT 1"
        row = self.db.execute(query, params).fetchone()
        return dict(row) if row is not None else None

    def claim_audio_transcription(self, *, audio_id: str, account_id: str) -> str:
        with self._transaction():
            row = self.db.execute(
                "SELECT transcript,transcript_state FROM message_audio "
                "WHERE audio_id=? AND account_id=? AND state='ready'",
                (audio_id, account_id),
            ).fetchone()
            if row is None:
                return "unavailable"
            if row["transcript"]:
                return "cached"
            if row["transcript_state"] != "not_started":
                return str(row["transcript_state"])
            self.db.execute(
                "UPDATE message_audio SET transcript_state='processing',updated_at=? "
                "WHERE audio_id=? AND account_id=? AND transcript_state='not_started'",
                (_now(), audio_id, account_id),
            )
            return "claimed"

    def save_audio_transcript(
        self, *, audio_id: str, account_id: str, transcript: str, model_id: str
    ) -> None:
        transcript = _bounded_text(transcript, 12000)
        model_id = _bounded_text(model_id, 100)
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_audio SET transcript=?,transcript_model_id=?,"
                "transcript_state='ready',updated_at=? WHERE audio_id=? AND account_id=? "
                "AND state='ready' AND transcript_state='processing'",
                (transcript, model_id, _now(), audio_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("claimed audio unavailable")

    def finish_audio_transcription(self, *, audio_id: str, account_id: str, state: str) -> None:
        if state not in ("unknown", "failed"):
            raise ValueError("invalid transcription state")
        with self._transaction():
            self.db.execute(
                "UPDATE message_audio SET transcript_state=?,updated_at=? "
                "WHERE audio_id=? AND account_id=? AND transcript_state='processing'",
                (state, _now(), audio_id, account_id),
            )

    def purge_for_message(self, *, message_id: str, now: str) -> None:
        """Delete media/audio/file artifacts for one expired message inside a transaction."""
        media = self.db.execute(
            "SELECT artifact_path FROM message_media WHERE message_id=?",
            (message_id,),
        ).fetchone()
        if media and media["artifact_path"]:
            artifact = Path(media["artifact_path"]).resolve()
            if artifact.is_relative_to(self.media_root):
                artifact.unlink(missing_ok=True)
        self.db.execute(
            "UPDATE message_media SET state='deleted',updated_at=? WHERE message_id=?",
            (now, message_id),
        )
        audio = self.db.execute(
            "SELECT artifact_path FROM message_audio WHERE message_id=?",
            (message_id,),
        ).fetchone()
        if audio and audio["artifact_path"]:
            artifact = Path(audio["artifact_path"]).resolve()
            if artifact.is_relative_to(self.audio_root):
                artifact.unlink(missing_ok=True)
        self.db.execute(
            "UPDATE message_audio SET state='deleted',updated_at=? WHERE message_id=?",
            (now, message_id),
        )
        file_row = self.db.execute(
            "SELECT artifact_path FROM message_files WHERE message_id=?",
            (message_id,),
        ).fetchone()
        if file_row and file_row["artifact_path"]:
            artifact = Path(file_row["artifact_path"]).resolve()
            if artifact.is_relative_to(self.file_root):
                artifact.unlink(missing_ok=True)
        self.db.execute(
            "UPDATE message_files SET state='deleted',updated_at=? WHERE message_id=?",
            (now, message_id),
        )

    _FILE_PUBLIC_COLUMNS = (
        "file_id,account_id,conversation_id,message_id,filename,fileext,"
        "expected_bytes,expected_md5,state,mime_type,byte_count,sha256,"
        "error_code,summary,summary_model_id,summarized_at,created_at,updated_at"
    )

    def begin_file_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        original_references: Sequence[dict[str, str]],
        filename: str,
        fileext: str = "",
        expected_bytes: int | None = None,
        expected_md5: str | None = None,
    ) -> dict[str, object]:
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        clean_name = _bounded_text(filename, 512) if filename.strip() else "unnamed"
        clean_ext = fileext.strip().lstrip(".").casefold()[:32]
        refs_json = json.dumps(list(original_references), ensure_ascii=False, separators=(",", ":"))
        with self._transaction():
            row = self.db.execute(
                f"SELECT {self._FILE_PUBLIC_COLUMNS},artifact_path FROM message_files "
                "WHERE message_id=? AND account_id=? AND conversation_id=?",
                (message_id, account_id, conversation_id),
            ).fetchone()
            if row is not None:
                return dict(row)
            file_id = (
                "fil_"
                + hashlib.sha256(
                    f"{account_id}\0{conversation_id}\0{message_id}".encode()
                ).hexdigest()[:24]
            )
            now = _now()
            self.db.execute(
                "INSERT INTO message_files "
                "(file_id,account_id,conversation_id,message_id,original_references_json,"
                "filename,fileext,expected_bytes,expected_md5,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,'pending',?,?)",
                (
                    file_id,
                    account_id,
                    conversation_id,
                    message_id,
                    refs_json,
                    clean_name,
                    clean_ext,
                    expected_bytes,
                    expected_md5,
                    now,
                    now,
                ),
            )
            return dict(
                self.db.execute(
                    f"SELECT {self._FILE_PUBLIC_COLUMNS},artifact_path FROM message_files WHERE file_id=?",
                    (file_id,),
                ).fetchone()
            )

    def complete_file_ingest(
        self,
        *,
        file_id: str,
        account_id: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
    ) -> None:
        if byte_count <= 0 or len(sha256) != 64:
            raise ValueError("invalid file metadata")
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_files SET state='ready',artifact_path=?,mime_type=?,"
                "byte_count=?,sha256=?,error_code=NULL,updated_at=? "
                "WHERE file_id=? AND account_id=? AND state='pending'",
                (artifact_path, mime_type[:120], byte_count, sha256, _now(), file_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("pending file ingest unavailable")

    def fail_file_ingest_for_message(
        self,
        *,
        account_id: str,
        message_id: str,
        error_code: str,
        filename: str = "unnamed",
        fileext: str = "",
        expected_bytes: int | None = None,
        expected_md5: str | None = None,
    ) -> dict[str, object]:
        if not error_code or len(error_code) > 64:
            raise ValueError("invalid file failure code")
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        record = self.db.execute(
            "SELECT file_id,state FROM message_files WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        if record is None:
            record = self.begin_file_ingest(
                account_id=account_id,
                conversation_id=self._conversation_for_message(message_id, account_id),
                message_id=message_id,
                original_references=(),
                filename=filename,
                fileext=fileext,
                expected_bytes=expected_bytes,
                expected_md5=expected_md5,
            )
        if record["state"] == "pending":
            with self._transaction():
                self.db.execute(
                    "UPDATE message_files SET state='failed',error_code=?,updated_at=? "
                    "WHERE file_id=? AND account_id=? AND state='pending'",
                    (error_code, _now(), record["file_id"], account_id),
                )
        return self.file_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        ) or {"state": "failed", "error_code": error_code}

    def archive_agent_file(
        self,
        *,
        account_id: str,
        conversation_id: str,
        artifact_path: Path,
        filename: str,
        mime_type: str,
        operation_id: str,
        delivery_state: str,
    ) -> dict[str, str]:
        if self._messages is None:
            raise RuntimeError("message repository is not configured on MediaRepo")
        path = artifact_path.resolve()
        if not path.is_relative_to(self.file_root) or not path.is_file():
            raise ValueError("outbound file must be archived under the file root")
        size = path.stat().st_size
        if not 0 < size <= 200_000_000:
            raise ValueError("outbound file size out of range")
        if not operation_id or len(operation_id) > 256:
            raise ValueError("invalid operation ID")
        if delivery_state not in ("queued", "accepted_by_server", "unknown", "not_sent"):
            raise ValueError("invalid file archive state")
        sha256_hash = hashlib.sha256()
        md5_hash = hashlib.md5(usedforsecurity=False)
        with path.open("rb") as f:
            while chunk := f.read(1024 * 1024):
                sha256_hash.update(chunk)
                md5_hash.update(chunk)
        digest = sha256_hash.hexdigest()
        md5_hex = md5_hash.hexdigest()
        clean_name = _bounded_text(filename, 512)
        ext = Path(clean_name).suffix.lstrip(".").casefold()[:32]
        raw = {
            "direction": "outbound",
            "fromid": account_id,
            "toid": conversation_id,
            "room": conversation_id if is_group_conversation(conversation_id) else "",
            "msg": f"[Agent 发送文件] {clean_name}",
            "msgtype": 49,
            "app_type": "6",
            "local_operation_id": operation_id,
            "delivery_state": delivery_state,
        }
        archived = self._messages.archive_message(
            account_id=account_id,
            conversation_id=conversation_id,
            sender_id=account_id,
            raw_envelope=raw,
            source_record=raw,
            body_text=json.dumps(
                {
                    "kind": "file_message",
                    "app_type": "6",
                    "text": clean_name,
                    "filename": clean_name,
                    "fileext": ext,
                    "size_bytes": size,
                    "md5": md5_hex,
                },
                ensure_ascii=False,
            ),
            direction="outbound",
            delivery_state=delivery_state,
        )
        file_id = (
            "fil_"
            + hashlib.sha256(
                f"{account_id}\0{conversation_id}\0{archived.message_id}".encode()
            ).hexdigest()[:24]
        )
        now = _now()
        with self._transaction():
            self.db.execute(
                "INSERT OR IGNORE INTO message_files "
                "(file_id,account_id,conversation_id,message_id,original_references_json,"
                "filename,fileext,expected_bytes,expected_md5,state,artifact_path,mime_type,"
                "byte_count,sha256,created_at,updated_at) "
                "VALUES (?,?,?,?,'[]',?,?,?,?,'ready',?,?,?,?,?,?)",
                (
                    file_id,
                    account_id,
                    conversation_id,
                    archived.message_id,
                    clean_name,
                    ext,
                    size,
                    md5_hex,
                    str(path),
                    mime_type[:120],
                    size,
                    digest,
                    now,
                    now,
                ),
            )
        return {"message_id": archived.message_id, "file_id": file_id}

    def file_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            f"SELECT {self._FILE_PUBLIC_COLUMNS} FROM message_files WHERE account_id=? AND message_id=?",
            (account_id, message_id),
        ).fetchone()
        return dict(row) if row is not None else None

    def file_metadata_for_messages(
        self, *, account_id: str, message_ids: Sequence[str]
    ) -> dict[str, dict[str, object]]:
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        with self._lock:
            rows = self.db.execute(
                f"SELECT {self._FILE_PUBLIC_COLUMNS} "
                f"FROM message_files WHERE account_id=? AND message_id IN ({placeholders})",
                (account_id, *message_ids),
            ).fetchall()
        return {str(row["message_id"]): dict(row) for row in rows}

    def get_file_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        cols = ",".join(f"f.{col}" for col in self._FILE_PUBLIC_COLUMNS.split(","))
        row = self.db.execute(
            f"SELECT {cols},f.artifact_path,s.expires_at,s.deleted_at FROM message_files f "
            "JOIN source_messages s ON s.message_id=f.message_id "
            "WHERE f.account_id=? AND f.conversation_id=? AND f.message_id=? "
            "AND f.state='ready'",
            (account_id, conversation_id, message_id),
        ).fetchone()
        if row is None or row["deleted_at"] is not None or row["expires_at"] <= _now():
            return None
        return dict(row)

    def get_file_by_id(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        file_id: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        cols = ",".join(f"f.{col}" for col in self._FILE_PUBLIC_COLUMNS.split(","))
        query = (
            f"SELECT {cols},f.artifact_path,s.expires_at,s.deleted_at FROM message_files f "
            "JOIN source_messages s ON s.message_id=f.message_id "
            "WHERE f.account_id=? AND f.file_id=?"
        )
        params: list[object] = [account_id, file_id]
        if conversation_id is not None:
            query += " AND f.conversation_id=?"
            params.append(conversation_id)
        if not include_unready:
            query += " AND f.state='ready'"
        row = self.db.execute(query, params).fetchone()
        if row is None or row["deleted_at"] is not None or row["expires_at"] <= _now():
            return None
        return dict(row)

    def get_latest_ready_file(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        cutoff = None
        if before_message_id:
            row = self.db.execute(
                "SELECT captured_at FROM source_messages WHERE message_id=? AND account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL",
                (before_message_id, account_id, conversation_id),
            ).fetchone()
            if row is None:
                return None
            cutoff = row["captured_at"]
        cols = ",".join(f"f.{col}" for col in self._FILE_PUBLIC_COLUMNS.split(","))
        query = (
            f"SELECT {cols},f.artifact_path,s.expires_at,s.deleted_at,s.captured_at "
            "FROM message_files f "
            "JOIN source_messages s ON s.message_id=f.message_id "
            "WHERE f.account_id=? AND f.conversation_id=? "
            "AND s.deleted_at IS NULL AND s.expires_at>?"
        )
        params: list[object] = [account_id, conversation_id, _now()]
        if not include_unready:
            query += " AND f.state='ready'"
        else:
            query += " AND f.state!='deleted'"
        if cutoff is not None:
            query += " AND s.captured_at<=?"
            params.append(cutoff)
        query += " ORDER BY s.captured_at DESC,f.created_at DESC LIMIT 1"
        row = self.db.execute(query, params).fetchone()
        return dict(row) if row is not None else None

    def list_files(
        self,
        *,
        account_id: str,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        if not 1 <= limit <= 100 or offset < 0:
            raise ValueError("file list bounds out of range")
        cols = ",".join(f"f.{col}" for col in self._FILE_PUBLIC_COLUMNS.split(","))
        query = (
            f"SELECT {cols},s.captured_at FROM message_files f "
            "JOIN source_messages s ON s.message_id=f.message_id "
            "WHERE f.account_id=? AND f.state!='deleted' AND s.deleted_at IS NULL AND s.expires_at>?"
        )
        params: list[object] = [account_id, _now()]
        if conversation_id is not None:
            query += " AND f.conversation_id=?"
            params.append(conversation_id)
        query += " ORDER BY s.captured_at DESC,f.created_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = self.db.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def save_file_summary(
        self, *, account_id: str, file_id: str, summary: str, model_id: str
    ) -> None:
        summary = _bounded_text(summary, 12000)
        model_id = _bounded_text(model_id, 100)
        now = _now()
        with self._transaction():
            cur = self.db.execute(
                "UPDATE message_files SET summary=?,summary_model_id=?,summarized_at=?,updated_at=? "
                "WHERE file_id=? AND account_id=? AND state='ready'",
                (summary, model_id, now, now, file_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("ready file unavailable")
