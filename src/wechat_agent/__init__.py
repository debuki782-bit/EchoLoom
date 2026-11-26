"""Backward-compatibility alias package forwarding ``wechat_agent.*`` to ``echoloom.*``."""

from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
import types

_CANONICAL_PKG = "echoloom"
_LEGACY_PKG = "wechat_agent"


class _ModuleAliasProxy(types.ModuleType):
    """Module proxy forwarding attribute access to its canonical ``echoloom`` module."""

    def __init__(self, alias_name: str, target_name: str) -> None:
        super().__init__(alias_name)
        object.__setattr__(self, "_target_name", target_name)

    def _target(self) -> types.ModuleType:
        return importlib.import_module(object.__getattribute__(self, "_target_name"))

    def __getattr__(self, name: str):
        return getattr(self._target(), name)

    def __setattr__(self, name: str, value) -> None:
        setattr(self._target(), name, value)

    def __delattr__(self, name: str) -> None:
        delattr(self._target(), name)

    def __dir__(self) -> list[str]:
        return sorted(set(dir(self._target())))


class _LegacyAliasLoader(importlib.abc.Loader):
    def __init__(self, target_name: str) -> None:
        self._target_name = target_name

    def create_module(self, spec: importlib.machinery.ModuleSpec):
        importlib.import_module(self._target_name)
        return _ModuleAliasProxy(spec.name, self._target_name)

    def exec_module(self, module) -> None:
        return None


class _LegacyAliasFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname: str, path=None, target=None):
        if not fullname.startswith(_LEGACY_PKG + "."):
            return None
        target_name = f"{_CANONICAL_PKG}.{fullname[len(_LEGACY_PKG) + 1:]}"
        try:
            target_spec = importlib.util.find_spec(target_name)
        except (ImportError, ValueError):
            return None
        if target_spec is None:
            return None
        is_pkg = target_spec.submodule_search_locations is not None
        spec = importlib.machinery.ModuleSpec(
            fullname,
            _LegacyAliasLoader(target_name),
            is_package=is_pkg,
        )
        if is_pkg:
            spec.submodule_search_locations = list(
                target_spec.submodule_search_locations or []
            )
        return spec


if not any(isinstance(finder, _LegacyAliasFinder) for finder in sys.meta_path):
    sys.meta_path.insert(0, _LegacyAliasFinder())


def __getattr__(name: str):
    canonical = importlib.import_module(_CANONICAL_PKG)
    if hasattr(canonical, name):
        return getattr(canonical, name)
    sub_target = f"{_CANONICAL_PKG}.{name}"
    importlib.import_module(sub_target)
    proxy = _ModuleAliasProxy(f"{_LEGACY_PKG}.{name}", sub_target)
    sys.modules[f"{_LEGACY_PKG}.{name}"] = proxy
    globals()[name] = proxy
    return proxy


def __dir__() -> list[str]:
    canonical = importlib.import_module(_CANONICAL_PKG)
    return sorted(set(globals()) | set(dir(canonical)))
