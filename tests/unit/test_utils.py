import pytest

from zeek_flink_pipeline.common import exceptions, utils


def test_parse_tlv_user_id():
    tlv = "01085A68616E6773616E"
    assert utils.parse_tlv_user_id(tlv) == "Zhangsan"


def test_invalid_hex():
    with pytest.raises(exceptions.TLVParseError):
        utils.parse_tlv_user_id("zz")
