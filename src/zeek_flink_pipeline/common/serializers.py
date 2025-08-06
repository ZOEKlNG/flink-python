"""Simple JSON helpers."""
from __future__ import annotations

try:  # pragma: no cover - optional dependency
    import orjson as json
except Exception:  # pragma: no cover
    import json  # type: ignore


def dumps(data) -> bytes:
    return json.dumps(data).encode("utf-8")


def loads(data: bytes):
    return json.loads(data)
