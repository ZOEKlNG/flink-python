"""Common utility helpers."""
from __future__ import annotations

from typing import Optional

from .exceptions import TLVParseError


def parse_tlv_user_id(tlv: str) -> Optional[str]:
    """Extract user_id from a TLV encoded hex string.

    The TLV format is defined as:
    - 1 byte type (ignored)
    - 1 byte length
    - N bytes UTF-8 encoded value
    """
    if not tlv:
        return None
    try:
        data = bytes.fromhex(tlv)
    except ValueError as exc:  # non-hex input
        raise TLVParseError(str(exc)) from exc
    if len(data) < 2:
        raise TLVParseError("TLV too short")
    length = data[1]
    if len(data) < 2 + length:
        raise TLVParseError("Invalid length in TLV")
    value = data[2 : 2 + length]
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise TLVParseError("Value is not valid UTF-8") from exc
