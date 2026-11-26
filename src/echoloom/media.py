"""Automatically preserve original image data while keeping pixels out of context."""

from __future__ import annotations

import io
import json
import mimetypes
import re
import tempfile
import wave
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
from PIL import Image, ImageDraw

from .connectors.base import connector_ensure_ready, connector_fetch_received_file
from .contacts import is_group_conversation
from .media_artifacts import (
    MAX_FILE_BYTES,
    MAX_ORIGINAL_IMAGE_BYTES,  # noqa: F401 - re-exported for legacy callers
    FileIngestError,
    ImageIngestError,
    _file_hash,
    _file_md5,  # noqa: F401 - re-exported for legacy callers
    _image_format,
)
from .wechat_gateway import GatewayConfig, MessageEvent, create_gateway

if TYPE_CHECKING:
    from .memory_store import MemoryStore

MAX_MODEL_IMAGE_BYTES = 14_000_000
MAX_GIF_FRAME_PIXELS = 40_000_000




def image_for_model(path: Path, mime_type: str) -> tuple[bytes, str]:
    """Return a bounded image payload, sampling GIFs into a 4-frame contact sheet."""
    path = Path(path)
    if path.stat().st_size > MAX_MODEL_IMAGE_BYTES:
        raise ValueError("image_exceeds_model_upload_limit")
    if mime_type == 'application/x-wechat-wxgf':
        from .wxgf import wxgf_image
        payload, mime = wxgf_image(path.read_bytes(), max_pixels=MAX_GIF_FRAME_PIXELS)
        if len(payload) > MAX_MODEL_IMAGE_BYTES:
            raise ValueError('rendered_image_exceeds_model_upload_limit')
        return payload, mime
    if mime_type != "image/gif":
        if mime_type not in {"image/jpeg", "image/png", "image/webp"}:
            raise ValueError("unsupported_image_format")
        with Image.open(path) as image:
            actual_mime = {
                "JPEG": "image/jpeg", "PNG": "image/png", "WEBP": "image/webp",
            }.get(image.format or "")
            image.verify()
        if actual_mime != mime_type:
            raise ValueError("image_format_mismatch")
        return path.read_bytes(), mime_type

    with Image.open(path) as source:
        frame_count = max(1, int(getattr(source, "n_frames", 1)))
        width, height = source.size
        if width * height > MAX_GIF_FRAME_PIXELS:
            raise ValueError("gif_frame_dimensions_exceed_limit")
        count = min(4, frame_count)
        indexes = sorted({round(i * (frame_count - 1) / max(1, count - 1))
                          for i in range(count)})
        cell_width, cell_height, label_height, gap = 480, 360, 28, 8
        sheet = Image.new("RGB", (cell_width * 2 + gap * 3,
                                   (cell_height + label_height) * 2 + gap * 3), "white")
        draw = ImageDraw.Draw(sheet)
        for slot, frame_index in enumerate(indexes):
            source.seek(frame_index)
            frame = source.convert("RGB")
            if frame.width * frame.height > MAX_GIF_FRAME_PIXELS:
                raise ValueError("gif_frame_dimensions_exceed_limit")
            frame.thumbnail((cell_width, cell_height), Image.Resampling.LANCZOS)
            column, row = slot % 2, slot // 2
            x = gap + column * (cell_width + gap)
            y = gap + row * (cell_height + label_height + gap)
            sheet.paste(frame, (x, y + label_height))
            draw.text((x, y), f"Frame {frame_index + 1}/{frame_count}", fill="black")
        output = io.BytesIO()
        sheet.save(output, format="PNG", optimize=True)
        return output.getvalue(), "image/png"


class WeChatImageIngestor:
    """Download original URLs or decrypt local original `.dat` files on arrival."""

    def __init__(self, workspace: Path, *, source_root: Path | None = None,
                 transport: httpx.BaseTransport | None = None,
                 native_download_url: str | None = None, gateway=None):
        self.workspace = Path(workspace)
        self.gateway = gateway if gateway is not None else create_gateway(GatewayConfig(
            self.workspace, source_root=source_root, transport=transport,
            native_download_url=native_download_url,
        ))
        self.artifact_root = self.workspace / "data" / "received-images"

    @property
    def source_root(self):
        return self.gateway.image_source.source_root

    @source_root.setter
    def source_root(self, value):
        self.gateway.image_source.source_root = value

    @property
    def native_downloader(self):
        return getattr(getattr(self.gateway, "image_source", None), "native_downloader", None)

    @native_downloader.setter
    def native_downloader(self, value):
        self.gateway.image_source.native_downloader = value

    def ensure_native_downloader(self, account_id: str) -> bool:
        if connector_ensure_ready(self.gateway, account_id):
            return True
        downloader = self.native_downloader
        if downloader is None or not hasattr(downloader, "ensure_running"):
            return False
        return bool(downloader.ensure_running(account_id))

    def _fetch_original(
        self,
        item,
        references,
        destination,
        *,
        allow_account_scan: bool = True,
    ):
        return self.gateway.fetch_original_image(
            item, references, destination, allow_account_scan=allow_account_scan,
        )

    def _fetch_unique_thumbnail(self, item, references, destination):
        return self.gateway.fetch_thumbnail(item, references, destination)

    def _fetch_account_cached_original(self, item, references, destination):
        fetcher = getattr(self.gateway, "fetch_account_cached_original", None)
        if callable(fetcher):
            return fetcher(item, references, destination)
        source = getattr(self.gateway, "image_source", None)
        if source is not None and hasattr(source, "_fetch_account_cached_original"):
            return source._fetch_account_cached_original(item, references, destination)
        return None

    def ingest(
        self,
        *,
        item: MessageEvent,
        message_id: str,
        references: list[dict[str, str]],
        store: MemoryStore,
        allow_account_scan: bool = True,
    ) -> dict[str, object]:
        record = store.begin_image_ingest(
            account_id=item.account_id, conversation_id=item.conversation_id,
            message_id=message_id, original_references=references,
        )
        if record["state"] == "ready":
            ready = store.get_image_for_message(
                account_id=item.account_id, conversation_id=item.conversation_id,
                message_id=message_id,
            )
            if ready and self._artifact_valid(ready):
                return self._public_metadata(ready)
            store.fail_image_ingest(image_id=str(record["image_id"]),
                                    account_id=item.account_id,
                                    error_code="stored_artifact_unavailable")
            return {"state": "failed", "error_code": "stored_artifact_unavailable"}
        if record["state"] == "failed" and not store.retry_image_ingest(
            image_id=str(record["image_id"]), account_id=item.account_id,
            error_code=str(record["error_code"]),
        ):
            return {"state": "failed", "error_code": record["error_code"]}

        image_id = str(record["image_id"])
        temporary: Path | None = None
        final: Path | None = None
        try:
            self.artifact_root.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(dir=self.artifact_root, delete=False,
                                             prefix=".pending-") as temp:
                temporary = Path(temp.name)
            source_kind = "original"
            try:
                source_sha, declared_mime = self._fetch_original(
                    item, references, temporary, allow_account_scan=allow_account_scan,
                )
            except ImageIngestError as exc:
                if exc.code != "original_image_not_found_in_cache":
                    raise
                source_sha, declared_mime = self._fetch_unique_thumbnail(item, references, temporary)
                source_kind = "thumbnail_inferred"
            with temporary.open("rb") as source_file:
                header = source_file.read(16)
            mime, suffix = _image_format(header, declared_mime)
            final = self.artifact_root / f"{image_id}{suffix}"
            temporary.replace(final)
            digest, byte_count = _file_hash(final)
            store.complete_image_ingest(
                image_id=image_id, account_id=item.account_id,
                artifact_path=str(final.resolve()), mime_type=mime,
                byte_count=byte_count, sha256=digest, source_sha256=source_sha,
                source_kind=source_kind,
            )
            ready = store.get_image_for_message(
                account_id=item.account_id, conversation_id=item.conversation_id,
                message_id=message_id,
            )
            if ready is None:
                # Reference indexing must resolve the same just-saved event.
                raise ImageIngestError("saved_reference_unavailable")
            return self._public_metadata(ready)
        except ImageIngestError as exc:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if final is not None:
                final.unlink(missing_ok=True)
            store.fail_image_ingest(image_id=image_id, account_id=item.account_id,
                                    error_code=exc.code)
            return {"state": "failed", "error_code": exc.code}
        except (OSError, httpx.HTTPError, ValueError):
            if temporary is not None:
                temporary.unlink(missing_ok=True)
            if final is not None:
                final.unlink(missing_ok=True)
            store.fail_image_ingest(image_id=image_id, account_id=item.account_id,
                                    error_code="image_ingest_failed")
            return {"state": "failed", "error_code": "image_ingest_failed"}

    def prepare_for_read(
        self, image: dict[str, object], *, store: MemoryStore,
    ) -> dict[str, object]:
        """Retry delayed cache arrivals and upgrade fallbacks without replaying messages."""
        if image.get("state") == "failed":
            references = json.loads(str(image.get("original_references_json") or "[]"))
            item = MessageEvent(
                account_id=str(image["account_id"]),
                conversation_id=str(image["conversation_id"]),
                sender_id=str(image["account_id"]),
                sender_origin="native_private_fromid", text="",
                is_group=is_group_conversation(str(image["conversation_id"])),
                message_type=3, source_msg_id=None, capture_tick_ms=0,
            )
            self.ingest(
                item=item,
                message_id=str(image["message_id"]),
                references=references,
                store=store,
                allow_account_scan=True,
            )
            image = store.get_image_by_id(
                account_id=item.account_id, conversation_id=item.conversation_id,
                image_id=str(image["image_id"]), include_unready=True,
            ) or image
        if image.get("state") == "ready":
            return self.upgrade_cached_thumbnail(image, store=store)
        return image

    def upgrade_cached_thumbnail(
        self, image: dict[str, object], *, store: MemoryStore,
    ) -> dict[str, object]:
        """Recheck the verified original cache when a stored fallback is read later."""
        if image.get("source_kind") != "thumbnail_inferred":
            return image
        try:
            references = json.loads(str(image.get("original_references_json") or "[]"))
            if not isinstance(references, list):
                return image
            account_id = str(image["account_id"])
            conversation_id = str(image["conversation_id"])
            image_id = str(image["image_id"])
            artifact = Path(str(image["artifact_path"])).resolve()
            if not artifact.is_relative_to(self.artifact_root.resolve()):
                return image
            with tempfile.NamedTemporaryFile(
                dir=self.artifact_root, delete=False, prefix=".upgrade-",
            ) as temp:
                temporary = Path(temp.name)
            promoted_path: Path | None = None
            try:
                item = MessageEvent(
                    account_id=account_id,
                    conversation_id=conversation_id,
                    sender_id=account_id,
                    sender_origin="native_private_fromid",
                    text="",
                    is_group=is_group_conversation(conversation_id),
                    message_type=3,
                    source_msg_id=None,
                    capture_tick_ms=0,
                )
                source_sha, declared_mime = self._fetch_original(
                    item, references, temporary, allow_account_scan=True,
                )
                with temporary.open("rb") as source_file:
                    header = source_file.read(16)
                mime, suffix = _image_format(header, declared_mime)
                promoted_path = self.artifact_root / f"{image_id}.original{suffix}"
                temporary.replace(promoted_path)
                digest, byte_count = _file_hash(promoted_path)
                promoted = store.promote_inferred_image(
                    image_id=image_id,
                    account_id=account_id,
                    previous_sha256=str(image["sha256"]),
                    artifact_path=str(promoted_path.resolve()),
                    mime_type=mime,
                    byte_count=byte_count,
                    sha256=digest,
                    source_sha256=source_sha,
                )
                if not promoted:
                    promoted_path.unlink(missing_ok=True)
                    return image
                artifact.unlink(missing_ok=True)
                return store.get_image_by_id(
                    account_id=account_id,
                    conversation_id=conversation_id,
                    image_id=image_id,
                ) or image
            finally:
                temporary.unlink(missing_ok=True)
                if promoted_path is not None and promoted_path.exists():
                    current = store.get_image_by_id(
                        account_id=account_id,
                        conversation_id=conversation_id,
                        image_id=image_id,
                    )
                    if current is None or current.get("artifact_path") != str(promoted_path.resolve()):
                        promoted_path.unlink(missing_ok=True)
        except (ImageIngestError, OSError, httpx.HTTPError, ValueError, KeyError):
            return image







    @staticmethod
    def _artifact_valid(image: dict[str, object]) -> bool:
        path = Path(str(image["artifact_path"]))
        try:
            digest, size = _file_hash(path)
        except OSError:
            return False
        return size == image["byte_count"] and digest == image["sha256"]

    @staticmethod
    def _public_metadata(image: dict[str, object]) -> dict[str, object]:
        result = {key: image[key] for key in ("state", "mime_type", "byte_count", "sha256")}
        if image.get("source_kind") == "thumbnail_inferred":
            result["source_kind"] = "thumbnail_inferred"
        return result




class WeChatVoiceIngestor:
    """Decode Hook-captured WeChat SILK bytes to a bounded local WAV artifact."""

    MAX_VOICE_HEX_CHARS = 4_000_000
    MAX_DURATION_MS = 180_000
    SAMPLE_RATE = 24_000
    MAX_PCM_BYTES = SAMPLE_RATE * 2 * (MAX_DURATION_MS // 1000)

    def __init__(self, workspace: Path, *, gateway=None):
        self.workspace = Path(workspace)
        self.gateway = gateway if gateway is not None else create_gateway(GatewayConfig(self.workspace))
        self.artifact_root = self.workspace / "data" / "received-audio"

    def ingest(self, *, item: MessageEvent, message_id: str,
               store: MemoryStore) -> dict[str, object]:
        try:
            payload = self.gateway.voice_payload(item)
            if payload.error_code:
                return store.fail_audio_ingest_for_message(
                    account_id=item.account_id, message_id=message_id,
                    error_code=payload.error_code,
                )
            silk, duration_ms = payload.data, payload.duration_ms
            audio_fmt = getattr(payload, "format", "silk") or "silk"
            record = store.begin_audio_ingest(
                account_id=item.account_id, conversation_id=item.conversation_id,
                message_id=message_id, duration_ms=duration_ms,
            )
            if record["state"] == "ready":
                return store.audio_metadata_for_message(
                    account_id=item.account_id, message_id=message_id,
                ) or {"state": "failed", "error_code": "stored_audio_unavailable"}

            if audio_fmt == "wav":
                if len(silk) > self.MAX_PCM_BYTES + 4096:
                    raise ValueError("wav voice exceeds duration limit")
                with wave.open(io.BytesIO(silk), "rb") as parsed:
                    rate = parsed.getframerate() or self.SAMPLE_RATE
                    frames = parsed.getnframes()
                    actual_duration_ms = int(frames * 1000 // max(1, rate))
                    if actual_duration_ms <= 0 or actual_duration_ms > self.MAX_DURATION_MS:
                        raise ValueError("wav voice duration out of range")
                self.artifact_root.mkdir(parents=True, exist_ok=True)
                audio_id = str(record["audio_id"])
                temporary = self.artifact_root / ("." + audio_id + ".pending.wav")
                final = self.artifact_root / (audio_id + ".wav")
                try:
                    temporary.write_bytes(silk)
                    digest, size = _file_hash(temporary)
                    temporary.replace(final)
                    store.complete_audio_ingest(
                        audio_id=audio_id, account_id=item.account_id,
                        artifact_path=str(final.resolve()), byte_count=size, sha256=digest,
                        duration_ms=actual_duration_ms,
                    )
                finally:
                    temporary.unlink(missing_ok=True)
                return store.audio_metadata_for_message(
                    account_id=item.account_id, message_id=message_id,
                ) or {"state": "failed", "error_code": "stored_audio_unavailable"}

            import pysilk

            pcm = _BoundedBuffer(self.MAX_PCM_BYTES)
            pysilk.decode(io.BytesIO(silk), pcm, self.SAMPLE_RATE)
            pcm_bytes = pcm.getvalue()
            if not pcm_bytes:
                raise ValueError("decoded voice size out of range")
            actual_duration_ms = len(pcm_bytes) * 1000 // (self.SAMPLE_RATE * 2)
            self.artifact_root.mkdir(parents=True, exist_ok=True)
            audio_id = str(record["audio_id"])
            temporary = self.artifact_root / ("." + audio_id + ".pending.wav")
            final = self.artifact_root / (audio_id + ".wav")
            try:
                with wave.open(str(temporary), "wb") as output:
                    output.setnchannels(1)
                    output.setsampwidth(2)
                    output.setframerate(self.SAMPLE_RATE)
                    output.writeframes(pcm_bytes)
                digest, size = _file_hash(temporary)
                temporary.replace(final)
                store.complete_audio_ingest(
                    audio_id=audio_id, account_id=item.account_id,
                    artifact_path=str(final.resolve()), byte_count=size, sha256=digest,
                    duration_ms=actual_duration_ms,
                )
            finally:
                temporary.unlink(missing_ok=True)
            return store.audio_metadata_for_message(
                account_id=item.account_id, message_id=message_id,
            ) or {"state": "failed", "error_code": "stored_audio_unavailable"}
        except ImportError:
            return store.fail_audio_ingest_for_message(
                account_id=item.account_id, message_id=message_id,
                error_code="silk_decoder_unavailable",
            )
        except Exception:  # noqa: BLE001 - media failure must not block the text turn
            return store.fail_audio_ingest_for_message(
                account_id=item.account_id, message_id=message_id,
                error_code="voice_decode_failed",
            )


def _voice_duration_ms(message: object) -> int | None:
    if not isinstance(message, str) or len(message) > 65536:
        return None
    match = re.search(r"\bvoicelength\s*=\s*['\"](\d{1,7})['\"]", message)
    return int(match.group(1)) if match else None


class _BoundedBuffer(io.BytesIO):
    def __init__(self, limit: int):
        super().__init__()
        self.limit = limit

    def write(self, data: bytes) -> int:
        if self.tell() + len(data) > self.limit:
            raise ValueError("decoded voice exceeds duration limit")
        return super().write(data)










class WeChatFileIngestor:
    """Resolve and archive inbound chat files under data/received-files."""

    def __init__(self, workspace: Path, *, gateway=None):
        self.workspace = Path(workspace)
        self.gateway = gateway if gateway is not None else create_gateway(GatewayConfig(self.workspace))
        self.artifact_root = self.workspace / "data" / "received-files"

    @staticmethod
    def _safe_filename(filename: str, fileext: str) -> str:
        base = filename.replace("\\", "/").rsplit("/", 1)[-1]
        cleaned = re.sub(r'[<>:"|?*\x00-\x1f]', "_", base).strip().strip(".")
        if not cleaned or cleaned in (".", ".."):
            cleaned = "file"
        ext = fileext.strip().lstrip(".").casefold()[:32]
        if ext and not cleaned.casefold().endswith("." + ext):
            cleaned = f"{cleaned[:80]}.{ext}"
        if len(cleaned) > 120:
            stem, dot, suffix = cleaned.rpartition(".")
            cleaned = (stem[:96] + dot + suffix[:20]) if dot else cleaned[:120]
        return cleaned

    def ingest(
        self,
        *,
        item: MessageEvent,
        message_id: str,
        references: list[dict[str, str]],
        store: MemoryStore,
        metadata: dict[str, object] | None = None,
    ) -> dict[str, object]:
        values = {ref.get("field"): ref.get("value", "") for ref in references}
        meta = metadata or {}
        raw_filename = str(meta.get("filename") or values.get("title") or "").strip() or "unnamed"
        raw_ext = str(meta.get("fileext") or values.get("fileext") or "").strip().lstrip(".").casefold()
        expected_bytes_raw = meta.get("size_bytes") or values.get("totallen")
        expected_bytes = (
            int(expected_bytes_raw)
            if isinstance(expected_bytes_raw, int)
            or (isinstance(expected_bytes_raw, str) and expected_bytes_raw.isdigit())
            else None
        )
        expected_md5_raw = str(meta.get("md5") or values.get("md5") or "").strip().lower()
        expected_md5 = expected_md5_raw if re.fullmatch(r"[0-9a-f]{32}", expected_md5_raw) else None
        safe_name = self._safe_filename(raw_filename, raw_ext)
        try:
            record = store.begin_file_ingest(
                account_id=item.account_id,
                conversation_id=item.conversation_id,
                message_id=message_id,
                original_references=references,
                filename=safe_name,
                fileext=raw_ext or Path(safe_name).suffix.lstrip(".").casefold(),
                expected_bytes=expected_bytes,
                expected_md5=expected_md5,
            )
            if record["state"] == "ready":
                return store.file_metadata_for_message(
                    account_id=item.account_id, message_id=message_id,
                ) or {"state": "failed", "error_code": "stored_file_unavailable"}

            file_id = str(record["file_id"])
            self.artifact_root.mkdir(parents=True, exist_ok=True)
            temporary = self.artifact_root / f".{file_id}.pending"
            final = self.artifact_root / f"{file_id}_{safe_name}"
            if not final.resolve().is_relative_to(self.artifact_root.resolve()):
                raise FileIngestError("invalid_archived_file_path")
            temporary.unlink(missing_ok=True)
            try:
                digest, size = connector_fetch_received_file(
                    self.gateway, item, references, temporary,
                )
                if size <= 0 or size > MAX_FILE_BYTES:
                    raise FileIngestError("received_file_size_out_of_range")
                mime_type = mimetypes.guess_type(safe_name)[0] or "application/octet-stream"
                temporary.replace(final)
                store.complete_file_ingest(
                    file_id=file_id,
                    account_id=item.account_id,
                    artifact_path=str(final.resolve()),
                    mime_type=mime_type,
                    byte_count=size,
                    sha256=digest,
                )
            finally:
                temporary.unlink(missing_ok=True)
            return store.file_metadata_for_message(
                account_id=item.account_id, message_id=message_id,
            ) or {"state": "failed", "error_code": "stored_file_unavailable"}
        except FileIngestError as exc:
            return store.fail_file_ingest_for_message(
                account_id=item.account_id,
                message_id=message_id,
                error_code=exc.code,
                filename=safe_name,
                fileext=raw_ext,
                expected_bytes=expected_bytes,
                expected_md5=expected_md5,
            )
        except Exception:  # noqa: BLE001
            return store.fail_file_ingest_for_message(
                account_id=item.account_id,
                message_id=message_id,
                error_code="received_file_ingest_failed",
                filename=safe_name,
                fileext=raw_ext,
                expected_bytes=expected_bytes,
                expected_md5=expected_md5,
            )


def wechat_storage_root_from_status(status, account_id):
    """Compatibility helper delegating account storage discovery behind connector boundary."""
    from .connectors.base import resolve_storage_root
    return resolve_storage_root(status, account_id)
