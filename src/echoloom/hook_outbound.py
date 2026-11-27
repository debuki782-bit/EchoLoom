"""Compatibility aliases; concrete I/O now lives in adapters."""

from .adapters.hook_text import HookTextSender
from .wechat_gateway import SendResult as HookSendResult

__all__ = ["HookSendResult", "HookTextSender"]
