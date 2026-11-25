"""Create a tiny deterministic PNG for an explicitly authorized send test."""

import hashlib
import struct
import zlib
from pathlib import Path


def chunk(kind: bytes, data: bytes) -> bytes:
    payload = kind + data
    return struct.pack(">I", len(data)) + payload + struct.pack(">I", zlib.crc32(payload))


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    destination = root / ".tmp" / "codex-image-probe-20261029.png"
    destination.parent.mkdir(parents=True, exist_ok=True)
    width = height = 128
    scanlines = bytearray()
    for y in range(height):
        scanlines.append(0)
        for x in range(width):
            scanlines.extend((0, 120, 255) if (x // 16 + y // 16) % 2 else (255, 235, 0))
    image = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(bytes(scanlines), level=9))
        + chunk(b"IEND", b"")
    )
    destination.write_bytes(image)
    print({
        "path": str(destination), "bytes": len(image),
        "sha256": hashlib.sha256(image).hexdigest(),
    })


if __name__ == "__main__":
    main()
