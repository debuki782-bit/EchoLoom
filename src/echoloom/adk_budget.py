"""Legacy import compatibility; production uses session_runtime and runtime_scope."""
from .frameworks import adk_budget as _legacy


def __getattr__(name):
    return getattr(_legacy, name)
