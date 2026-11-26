"""Pinned WeChat cache layout, CDN retrieval and native image decoding."""
from __future__ import annotations

import hashlib
import html
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import httpx
from PIL import Image

from ..media_artifacts import MAX_ORIGINAL_IMAGE_BYTES, ImageIngestError, _file_hash, _file_md5
from ..connectors.base import MessageEvent

_MD5 = re.compile(r"^[0-9a-fA-F]{32}$")
_WECHAT_HTTP_FALLBACK_HOSTS = frozenset({"vweixinf.tc.qq.com", "wxapp.tc.qq.com"})


def wechat_storage_root_from_status(status: dict, account_id: str) -> Path | None:
    """Use the logged-in account's verified contact database to locate its storage root."""
    path = status.get("SqliteContactDbPath")
    if status.get("IsLogin") != 1 or not isinstance(path, str) or not account_id:
        return None
    candidate = Path(path)
    if (not candidate.is_absolute() or candidate.name.casefold() != "contact.db"
            or candidate.parent.name.casefold() != "contact"
            or candidate.parent.parent.name.casefold() != "db_storage"):
        return None
    account_dir = candidate.parent.parent.parent
    if not (account_dir.name.casefold() == account_id.casefold()
            or account_dir.name.casefold().startswith(account_id.casefold() + "_")):
        return None
    root = account_dir.parent
    return root if root.is_dir() else None



def _allowed_wechat_host(host: str) -> bool:
    host = host.rstrip(".").casefold()
    roots = ("qpic.cn", "qlogo.cn", "qq.com", "wechat.com", "weixin.com", "gtimg.cn")
    return any(host == root or host.endswith("." + root) for root in roots)



class HookImageSource:
    def __init__(self, workspace, *, source_root=None, transport=None,
                 native_download_url=None, base_url="http://127.0.0.1:30001"):
        self.workspace = Path(workspace)
        candidates = (Path.home() / "xwechat_files", Path.home() / "Documents" / "xwechat_files")
        self.source_root = source_root or next((p for p in candidates if p.is_dir()), candidates[0])
        self.transport = transport
        self.base_url = base_url.rstrip("/")
        self.decode_root = self.workspace / ".tmp" / "image-decode"
        self._decoded_cache_path = self.decode_root / "decoded-md5-cache.json"
        self._decoded_cache: dict[tuple[Path, int, int], str] = self._load_decoded_cache()
        self.native_downloader = None
        if native_download_url:
            from .hook_native_images import NativeImageDownloader
            self.native_downloader = NativeImageDownloader(
                workspace,
                native_download_url,
                transport=transport,
                hook_url=self.base_url,
                auto_start=(transport is None),
            )

    def _fetch_original(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]:
        identifiers = [ref["value"] for ref in references
                       if ref.get("field") in ("hdmd5", "md5", "hdfileid", "fileid")
                       and _MD5.fullmatch(ref.get("value", ""))]
        for identifier in identifiers:
            source = self._find_local_original(item, identifier)
            if source is not None:
                return self._decode_local_original(source, destination)

        if allow_account_scan:
            # WeChat may place a downloaded copy under another chat's attachment
            # directory. Reuse it only after decrypting and matching the message's
            # own content MD5; directory/time/size coincidence alone is insufficient.
            cached = self._fetch_account_cached_original(item, references, destination)
            if cached is not None:
                return cached

        if self.native_downloader is not None and self.native_downloader.fetch(
            item.account_id, references, destination,
        ):
            return _file_hash(destination)[0], ""

        # The HD URL is preferred. Mid-size and thumbnail URLs are never used
        # as substitutes for the original.
        preferred = ("cdnhdimgurl", "cdnurl", "externurl", "hdimgurl", "midimgurl")
        remote = next((ref["value"] for field in preferred for ref in references
                       if ref.get("field") == field and ref.get("value")), None)
        if remote:
            digest, mime = self._download_url(remote, destination)
            expected_md5 = next((ref["value"].casefold() for ref in references
                                 if ref.get("field") in ("hdmd5", "md5", "newmd5", "rawmd5")
                                 and _MD5.fullmatch(ref.get("value", ""))), None)
            if expected_md5 and _file_md5(destination) != expected_md5:
                raise ImageIngestError("original_image_md5_mismatch")
            return digest, mime
        has_md5_reference = bool(identifiers) or any(
            ref.get("field") in ("newmd5", "rawmd5")
            and _MD5.fullmatch(ref.get("value", ""))
            for ref in references
        )
        if has_md5_reference:
            raise ImageIngestError("original_image_not_found_in_cache")
        raise ImageIngestError("original_image_url_or_id_missing")


    def _fetch_account_cached_original(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, str] | None:
        expected_md5s = {
            ref["value"].casefold() for ref in references
            if ref.get("field") in ("md5", "hdmd5", "newmd5", "rawmd5")
            and _MD5.fullmatch(ref.get("value", ""))
        }
        expected_lengths = {
            int(ref["value"]) for ref in references
            if ref.get("field") in ("length", "hdlength")
            and ref.get("value", "").isdigit()
            and 1 <= int(ref["value"]) <= MAX_ORIGINAL_IMAGE_BYTES
        }
        if not expected_md5s or not self.source_root.is_dir():
            return None

        account_dirs = [
            path for path in self.source_root.iterdir()
            if path.is_dir() and (
                path.name.casefold() == item.account_id.casefold()
                or path.name.casefold().startswith(item.account_id.casefold() + "_")
            )
        ]
        candidates: list[Path] = []
        for account_dir in account_dirs:
            attach = account_dir / "msg" / "attach"
            if not attach.is_dir():
                continue
            for source in attach.glob("**/Img/*.dat"):
                if source.name.casefold().endswith("_t.dat"):
                    continue
                try:
                    resolved = source.resolve()
                    size = resolved.stat().st_size
                except OSError:
                    continue
                if not resolved.is_relative_to(self.source_root.resolve()):
                    raise ImageIngestError("source_path_out_of_scope")
                if size > MAX_ORIGINAL_IMAGE_BYTES + 4096:
                    continue
                candidates.append(resolved)

        # Declared lengths can describe the upload before WXGF recompression.
        # They prioritize probes; only decoded MD5 decides message identity.
        candidates.sort(key=lambda p: (
            not any(length <= p.stat().st_size <= length + 4096 for length in expected_lengths),
            not p.name.casefold().endswith('_h.dat'),
            -p.stat().st_mtime,
        ))

        probe = destination.with_name(destination.name + ".account-probe")
        cache_dirty = False
        try:
            probes = 0
            for source in candidates:
                stat = source.stat()
                key = (source, stat.st_size, stat.st_mtime_ns)
                known_md5 = self._decoded_cache.get(key)
                if known_md5 is not None and known_md5 not in expected_md5s:
                    continue
                if probes >= 64:
                    break
                probes += 1
                try:
                    source_sha, mime = self._decode_local_original(source, probe)
                    decoded_md5 = _file_md5(probe)
                    self._decoded_cache[key] = decoded_md5
                    cache_dirty = True
                    if decoded_md5 not in expected_md5s:
                        continue
                    with probe.open("rb") as decoded, destination.open("wb") as output:
                        while chunk := decoded.read(1024 * 1024):
                            output.write(chunk)
                    return source_sha, mime
                except httpx.TransportError:
                    # A disconnected Hook cannot decode the remaining candidates.
                    raise ImageIngestError('image_decoder_unavailable') from None
                except (ImageIngestError, OSError, ValueError):
                    continue
                finally:
                    probe.unlink(missing_ok=True)
        finally:
            if cache_dirty:
                self._save_decoded_cache()
            probe.unlink(missing_ok=True)
        return None

    def _load_decoded_cache(self) -> dict[tuple[Path, int, int], str]:
        if not self._decoded_cache_path.is_file():
            return {}
        try:
            raw = json.loads(self._decoded_cache_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        if not isinstance(raw, list):
            return {}
        loaded: dict[tuple[Path, int, int], str] = {}
        for entry in raw[-2048:]:
            if (
                isinstance(entry, list)
                and len(entry) == 4
                and isinstance(entry[0], str)
                and isinstance(entry[1], int)
                and isinstance(entry[2], int)
                and isinstance(entry[3], str)
            ):
                loaded[(Path(entry[0]), entry[1], entry[2])] = entry[3]
        return loaded

    def _save_decoded_cache(self) -> None:
        if len(self._decoded_cache) > 2048:
            trimmed = dict(list(self._decoded_cache.items())[-2048:])
            self._decoded_cache.clear()
            self._decoded_cache.update(trimmed)
        entries = [
            [str(path), size, mtime_ns, digest]
            for (path, size, mtime_ns), digest in self._decoded_cache.items()
        ]
        temp_path = self._decoded_cache_path.with_name(f".cache-{os.getpid()}.tmp")
        try:
            self.decode_root.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(json.dumps(entries, ensure_ascii=False), encoding="utf-8")
            temp_path.replace(self._decoded_cache_path)
        except OSError:
            temp_path.unlink(missing_ok=True)


    def _fetch_unique_thumbnail(self, item: MessageEvent, references: list[dict[str, str]],
                                destination: Path) -> tuple[str, str]:
        """Conservatively infer a thumbnail, never treating it as a verified original."""
        values = {ref.get("field"): ref.get("value", "") for ref in references}
        try:
            expected = int(values.get("cdnthumblength", ""))
            width, height = int(values.get("cdnthumbwidth", "")), int(values.get("cdnthumbheight", ""))
            timing = item.event_time_context or {}
            if timing.get("timezone") != "Asia/Shanghai":
                raise ValueError("unsupported timezone")
            minute = datetime.fromisoformat(str(timing["local_minute"]))
            start = minute.replace(tzinfo=timezone(timedelta(hours=8))).timestamp()
            if not 1 <= expected <= 2_000_000 or width <= 0 or height <= 0:
                raise ValueError("invalid thumbnail metadata")
        except (KeyError, ValueError, TypeError):
            raise ImageIngestError("original_image_not_found_in_cache") from None
        if not self.source_root.is_dir():
            raise ImageIngestError("original_image_not_found_in_cache")
        group_hash = hashlib.md5(item.conversation_id.encode(), usedforsecurity=False).hexdigest()
        candidates = []
        for account in self.source_root.iterdir():
            if not account.is_dir() or not (account.name == item.account_id
                    or account.name.startswith(item.account_id + "_")):
                continue
            base = account / "msg" / "attach" / group_hash
            if not base.is_dir():
                continue
            for source in base.glob("**/Img/*_t.dat"):
                stat = source.stat()
                if start - 5 <= stat.st_mtime < start + 65 and expected <= stat.st_size <= expected + 4096:
                    resolved = source.resolve()
                    if not resolved.is_relative_to(self.source_root.resolve()):
                        raise ImageIngestError("source_path_out_of_scope")
                    candidates.append(resolved)
                    if len(candidates) > 12:
                        raise ImageIngestError("thumbnail_match_ambiguous")
        matches: list[tuple[Path, str, str]] = []
        for source in candidates:
            probe = destination.with_name(destination.name + ".probe")
            try:
                digest, mime = self._decode_local_original(source, probe)
                if probe.stat().st_size != expected:
                    continue
                with Image.open(probe) as image:
                    actual_width, actual_height = image.size
                    if actual_width < 1 or actual_height < 1:
                        continue
                    if abs(actual_width / actual_height - width / height) > 0.05:
                        continue
                    image.verify()
                matches.append((source, digest, mime))
            except (ImageIngestError, OSError, ValueError):
                continue
            finally:
                probe.unlink(missing_ok=True)
        if len(matches) != 1:
            raise ImageIngestError("thumbnail_match_ambiguous" if matches else "original_image_not_found_in_cache")
        source, _, _ = matches[0]
        return self._decode_local_original(source, destination)


    def _find_local_original(self, item: MessageEvent, identifier: str) -> Path | None:
        if not self.source_root.is_dir():
            return None
        group_hash = hashlib.md5(item.conversation_id.encode(),
                                 usedforsecurity=False).hexdigest()
        account_dirs = [path for path in self.source_root.iterdir()
                        if path.is_dir() and (path.name == item.account_id
                        or path.name.startswith(item.account_id + "_"))]
        for filename in (f"{identifier}_h.dat", f"{identifier}.dat"):
            candidates: list[Path] = []
            for account_dir in account_dirs:
                attach = account_dir / "msg" / "attach" / group_hash
                if attach.is_dir():
                    candidates.extend(attach.glob(f"**/Img/{filename}"))
            if candidates:
                source = max(candidates, key=lambda path: path.stat().st_mtime).resolve()
                if not source.is_relative_to(self.source_root.resolve()):
                    raise ImageIngestError("source_path_out_of_scope")
                if source.stat().st_size > MAX_ORIGINAL_IMAGE_BYTES:
                    raise ImageIngestError("source_image_too_large")
                return source
        return None


    def _decode_local_original(self, source: Path, destination: Path) -> tuple[str, str]:
        self.decode_root.mkdir(parents=True, exist_ok=True)
        decoded = self.decode_root / f"{destination.name}.decoded"
        decoded.unlink(missing_ok=True)
        try:
            with httpx.Client(timeout=30, trust_env=False, follow_redirects=False,
                              transport=self.transport) as hook:
                response = hook.post(f"{self.base_url}/Decode_Pic", json={
                    "src_path": str(source), "dst_path": str(decoded),
                    "mode": 1, "wide": True,
                })
            if response.status_code != 200:
                raise ImageIngestError("hook_decode_http_error")
            result = response.json()
            if not isinstance(result, dict) or result.get("ret") != 0 or not decoded.is_file():
                raise ImageIngestError("hook_decode_failed")
            if decoded.stat().st_size > MAX_ORIGINAL_IMAGE_BYTES:
                raise ImageIngestError("decoded_image_too_large")
            source_sha, _ = _file_hash(source)
            with decoded.open("rb") as source_stream, destination.open("wb") as target:
                while chunk := source_stream.read(1024 * 1024):
                    target.write(chunk)
            return source_sha, ""
        finally:
            decoded.unlink(missing_ok=True)


    def _download_url(self, original_url: str, destination: Path) -> tuple[str, str]:
        current = html.unescape(original_url)
        original_http_url: str | None = None
        for _ in range(4):
            parsed = urlsplit(current)
            if (not parsed.hostname or parsed.username or parsed.password
                    or not _allowed_wechat_host(parsed.hostname)):
                raise ImageIngestError("original_image_url_not_allowed")
            try:
                port = parsed.port
            except ValueError as exc:
                raise ImageIngestError("original_image_url_not_allowed") from exc
            if parsed.scheme == "http":
                if port not in (None, 80):
                    raise ImageIngestError("original_image_url_not_allowed")
                if current != original_http_url:
                    if parsed.hostname.casefold() not in _WECHAT_HTTP_FALLBACK_HOSTS:
                        raise ImageIngestError("original_image_url_not_allowed")
                    original_http_url = current
                    current = parsed._replace(scheme="https", netloc=parsed.hostname).geturl()
                    continue
            elif parsed.scheme != "https" or port not in (None, 443):
                raise ImageIngestError("original_image_url_not_allowed")
            try:
                with httpx.Client(timeout=20, trust_env=False, follow_redirects=False,
                                  transport=self.transport) as client, client.stream(
                                      "GET", current,
                                  ) as response:
                    if response.status_code in (301, 302, 303, 307, 308):
                        location = response.headers.get("location")
                        if not location:
                            raise ImageIngestError("image_redirect_missing_location")
                        current = urljoin(current, location)
                        continue
                    if response.status_code != 200:
                        raise ImageIngestError("image_url_fetch_failed")
                    declared_mime = response.headers.get("content-type", "").split(";", 1)[0]
                    digest = hashlib.sha256()
                    size = 0
                    with destination.open("wb") as output:
                        for chunk in response.iter_bytes():
                            size += len(chunk)
                            if size > MAX_ORIGINAL_IMAGE_BYTES:
                                raise ImageIngestError("original_image_too_large")
                            digest.update(chunk)
                            output.write(chunk)
                    if size == 0:
                        raise ImageIngestError("original_image_empty")
                    return digest.hexdigest(), declared_mime
            except httpx.ConnectError as exc:
                if (original_http_url is not None and current != original_http_url
                        and urlsplit(current).hostname == urlsplit(original_http_url).hostname
                        and "CERTIFICATE_VERIFY_FAILED" in str(exc)):
                    current = original_http_url
                    continue
                raise
        raise ImageIngestError("image_redirect_limit")

