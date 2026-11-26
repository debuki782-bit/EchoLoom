"""Compatibility exports for the pinned native-image transport."""
from .adapters.hook_native_images import (
    MAX_BYTES,
    MD5,
    NativeImageDownloader,
    download_spec,
    file_download_spec,
    file_upload_spec,
)

__all__ = [
    "MAX_BYTES",
    "MD5",
    "NativeImageDownloader",
    "download_spec",
    "file_download_spec",
    "file_upload_spec",
]
