"""Isolated WeChat PC desktop Hook connector implementing the ChannelConnector contract."""

from ..adapters.hook import HookGateway

WeChatHookConnector = HookGateway

__all__ = ["WeChatHookConnector"]
