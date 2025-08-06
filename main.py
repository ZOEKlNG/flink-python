from __future__ import annotations

import dataclasses
import json
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema

from flink_zeek.models import UnifiedLog
from flink_zeek.tlv import parse_user_id_tlv
from flink_zeek.session_processor import SessionProcessor
from flink_zeek.app_processor import ApplicationProcessor


def deserialize_log(raw: str) -> UnifiedLog:
    data = json.loads(raw)
    user_id = parse_user_id_tlv(data.get("user_id_tlv", "")) or ""
    if "content_length_pair" in data:
        bytes_length = sum(data.get("content_length_pair", []))
        app_id = data.get("host", "")
    elif "payload_bytes" in data:
        bytes_length = data.get("payload_bytes", 0)
        app_id = data.get("host", "")
    else:
        bytes_length = data.get("record_length", 0)
        app_id = data.get("sni", "")

    return UnifiedLog(
        timestamp=data.get("timestamp", 0),
        uid=data.get("uid", ""),
        source_ip=data.get("source_ip", ""),
        destination_ip=data.get("destination_ip", ""),
        bytes_length=bytes_length,
        user_id=user_id,
        app_id=app_id,
        raw=data,
    )


def main() -> None:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {"bootstrap.servers": "kafka:9092", "group.id": "flink-zeek"}

    topics = [
        "zeek_http_log",
        "zeek_tcp_log",
        "zeek_udp_log",
        "zeek_quic_log",
        "zeek_https_log",
        "zeek_session_timeout",
    ]

    consumer = FlinkKafkaConsumer(topics, SimpleStringSchema(), kafka_props)
    stream = env.add_source(consumer).map(deserialize_log)

    session_proc = SessionProcessor()
    sequences = stream.key_by(lambda log: log.user_id).flat_map(lambda log: session_proc.process(log))

    app_proc = ApplicationProcessor()
    samples = sequences.key_by(lambda seq: seq.user_id).flat_map(lambda seq: app_proc.process(seq))

    producer = FlinkKafkaProducer(
        topic="detection_sample_files",
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_props,
    )

    samples.map(lambda s: json.dumps(dataclasses.asdict(s))).add_sink(producer)

    env.execute("flink-zeek-pipeline")


if __name__ == "__main__":
    main()
