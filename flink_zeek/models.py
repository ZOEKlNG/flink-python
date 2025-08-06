from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class UnifiedLog:
    """Representation of a normalized log event coming from different Zeek topics."""

    timestamp: int
    uid: str
    source_ip: str
    destination_ip: str
    bytes_length: int
    user_id: str
    app_id: str
    raw: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PacketSequence:
    """A chunk of ordered packets belonging to the same session."""

    user_id: str
    app_id: str
    timestamp: int
    packets: List[UnifiedLog]


@dataclass
class SampleFile:
    """Final sample file ready for downstream detection systems."""

    user_id: str
    user_name: str
    app_id: str
    generation_timestamp: int
    packet_sequences: List[PacketSequence]
    metadata: Dict[str, Any]
