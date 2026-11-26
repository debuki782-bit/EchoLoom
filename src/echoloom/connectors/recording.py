"""Offline recording connector for replay, simulation, and adapter testing."""

from ..adapters.recording import RecordingGateway

RecordingConnector = RecordingGateway

__all__ = ["RecordingConnector", "RecordingGateway"]
