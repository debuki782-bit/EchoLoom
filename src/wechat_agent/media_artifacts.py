"""Media artifact validation shared by archives and WeChat adapters."""
import hashlib
from pathlib import Path

MAX_ORIGINAL_IMAGE_BYTES = 200_000_000
MAX_FILE_BYTES = MAX_ORIGINAL_IMAGE_BYTES


class ImageIngestError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.code = code


class FileIngestError(ImageIngestError):
    """Raised when an inbound or archived file transfer cannot be verified or staged."""



def _image_format(header: bytes, declared_mime: str = "") -> tuple[str, str]:
    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg", ".jpg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png", ".png"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif", ".gif"
    if len(header) >= 12 and header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "image/webp", ".webp"
    if header.startswith(b"wxgf"):
        return "application/x-wechat-wxgf", ".wxgf"
    raise ImageIngestError("unsupported_original_image_format")



def _file_hash(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size



def _file_md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as file:
        while chunk := file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()

