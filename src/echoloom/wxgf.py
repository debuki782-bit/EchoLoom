"""Render static WXGF's HEVC payload at its native pixel dimensions.

The source container is retained in the archive. No WeChat encryption keys are
handled here: decryption remains the existing Hook Decode_Pic operation.
"""
import io

from PIL import Image


def wxgf_image(data: bytes, *, max_pixels: int = 40_000_000) -> tuple[bytes, str]:
    if len(data) < 15 or data[:4] != b'wxgf':
        raise ValueError('invalid_wxgf_header')
    partitions = []
    offset = max(4, data[4])
    while offset < len(data) - 6:
        index = data.find(b'\x00\x00\x01', offset)
        if index < 0:
            break
        # Both Annex-B 3-byte and 4-byte start codes occur in WXGF.
        header = index + 3
        offset = index + 3
        for start in (index - 1, index):
            if start < 4 or (start != index and data[start] != 0):
                continue
            size = int.from_bytes(data[start - 4:start], 'big')
            if ((data[header] >> 1) & 63) == 32 and 0 < size <= len(data) - start:
                partitions.append(data[start:start + size])
                offset = start + size
                break
    if not partitions:
        raise ValueError('wxgf_hevc_payload_missing')
    payload = max(partitions, key=len)
    if len(partitions) > 1 and len(payload) < len(data) * .6:
        raise ValueError('wxgf_animation_requires_multitrack_decoder')
    try:
        import av
        with av.open(io.BytesIO(payload), format='hevc', mode='r') as container:
            frame = next(container.decode(video=0))
            if frame.width * frame.height > max_pixels:
                raise ValueError('wxgf_dimensions_exceed_limit')
            frame = frame.reformat(format='rgb24')
            image = Image.frombytes('RGB', (frame.width, frame.height), bytes(frame.planes[0]),
                                    'raw', 'RGB', frame.planes[0].line_size, 1)
            output = io.BytesIO()
            image.save(output, format='PNG')
            return output.getvalue(), 'image/png'
    except ImportError as exc:
        raise ValueError('wxgf_decoder_not_installed') from exc
    except (StopIteration, RuntimeError) as exc:
        raise ValueError('wxgf_frame_decode_failed') from exc
    except av.error.FFmpegError as exc:
        raise ValueError('wxgf_frame_decode_failed') from exc
