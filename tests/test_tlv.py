from flink_zeek.tlv import parse_user_id_tlv


def test_parse_user_id_tlv():
    tlv = "010A5A68616E6773616E"
    assert parse_user_id_tlv(tlv) == "Zhangsan"


def test_parse_user_id_tlv_invalid():
    assert parse_user_id_tlv("ZZZZ") is None
