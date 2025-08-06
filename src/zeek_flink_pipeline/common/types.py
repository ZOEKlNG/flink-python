from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class UnifiedLog:
    timestamp: int
    uid: str
    user_id: str
    app_id: str = ""
    bytes_length: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PacketSequence:
    user_id: str
    app_id: str
    timestamp: int
    packets: List[UnifiedLog] = field(default_factory=list)


@dataclass
class SampleFile:
    user_id: str
    user_name: str
    app_id: str
    sequences: List[PacketSequence] = field(default_factory=list)
