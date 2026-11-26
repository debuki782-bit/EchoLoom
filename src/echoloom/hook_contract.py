"""Compatibility imports for the original Hook observation boundary."""
from .adapters.hook_events import parse_callback as parse_callback
from .wechat_gateway import MessageEvent as HookObservation
from .wechat_gateway import SenderOrigin as SenderOrigin

__all__ = ["HookObservation", "SenderOrigin", "parse_callback"]
