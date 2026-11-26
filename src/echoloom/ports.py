"""Ports that keep the application independent of Hook and model SDKs."""

from typing import Protocol


class Clock(Protocol):
    def now(self) -> str: ...
