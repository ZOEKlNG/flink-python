from __future__ import annotations

from typing import Optional


def parse_user_id_tlv(tlv: str) -> Optional[str]:
    """Parse a TLV encoded hex string and return the embedded user_id.

    The TLV format is expected to be: [type][length][value]. All parts are
    encoded as hexadecimal characters. Only the ``value`` part is returned and
    decoded as UTF-8 string. If parsing fails, ``None`` is returned.
    """

    try:
        data = bytes.fromhex(tlv)
    except ValueError:
        return None

    if len(data) < 2:
        return None

    length = data[1]
    value = data[2 : 2 + length]
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError:
        return None
