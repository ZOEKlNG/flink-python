"""Utilities for normalising Zeek logs."""
from __future__ import annotations

from typing import Any, Dict

from ..common.types import UnifiedLog
from ..common.utils import parse_tlv_user_id


def to_unified_log(record: Dict[str, Any]) -> UnifiedLog:
    """Convert different Zeek log formats to UnifiedLog."""
    user_id = parse_tlv_user_id(record.get("user_id_tlv", "")) or ""
    bytes_length = 0
    if "payload_bytes" in record:
        bytes_length = int(record["payload_bytes"])
    elif "record_length" in record:
        bytes_length = int(record["record_length"])
    elif "content_length_pair" in record:
        pair = record["content_length_pair"] or []
        bytes_length = int(sum(pair))
    app_id = record.get("host") or record.get("sni") or ""
    return UnifiedLog(
        timestamp=int(record["timestamp"]),
        uid=record["uid"],
        user_id=user_id,
        app_id=app_id,
        bytes_length=bytes_length,
        metadata=record,
    )
