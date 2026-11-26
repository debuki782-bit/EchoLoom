"""Backend-independent bounds for the local inbound HTTP envelope."""
from fastapi import HTTPException, Request

MAX_BODY_BYTES = 131072


async def bounded_body(request: Request) -> bytes:
    chunks = bytearray()
    async for chunk in request.stream():
        if len(chunks) + len(chunk) > MAX_BODY_BYTES:
            raise HTTPException(413, "callback too large")
        chunks.extend(chunk)
    return bytes(chunks)
