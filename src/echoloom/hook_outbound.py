"""Compatibility aliases; concrete I/O now lives in adapters."""
from .adapters.hook_text import HookTextSender as HookTextSender
from .wechat_gateway import SendResult as HookSendResult

__all__ = ["HookTextSender", "HookSendResult"]
