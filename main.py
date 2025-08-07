from __future__ import annotations

import dataclasses
import json
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment, OutputTag
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.functions import ProcessFunction
from pyflink.common.serialization import SimpleStringSchema

from flink_zeek.models import UnifiedLog, PacketSequence
from flink_zeek.tlv import parse_user_id_tlv
from flink_zeek.session_processor import SessionProcessor
from flink_zeek.app_processor import ApplicationProcessor


WEIXIN_TAG = OutputTag("weixin")
QQ_TAG = OutputTag("qq")


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

    protocol = data.get("protocol") or data.get("proto") or data.get("log_type", "")

    return UnifiedLog(
        timestamp=data.get("timestamp", 0),
        uid=data.get("uid", ""),
        source_ip=data.get("source_ip", ""),
        destination_ip=data.get("destination_ip", ""),
        bytes_length=bytes_length,
        user_id=user_id,
        app_id=app_id,
        protocol=protocol,
        raw=data,
    )


class AppSplitter(ProcessFunction):
    """Route logs to side outputs based on ``app_id``."""

    def process_element(self, value: UnifiedLog, ctx: ProcessFunction.Context, out):
        if value.app_id == "weixin":
            ctx.output(WEIXIN_TAG, value)
        elif value.app_id == "qq":
            ctx.output(QQ_TAG, value)


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

    # Split stream by application id using side outputs
    split_stream = stream.process(AppSplitter())
    weixin_stream = split_stream.get_side_output(WEIXIN_TAG)
    qq_stream = split_stream.get_side_output(QQ_TAG)

    def build_app_pipeline(app_stream):
        session_proc = SessionProcessor()
        app_proc = ApplicationProcessor()

        def handle_log(log: UnifiedLog) -> Iterable[PacketSequence]:
            if log.protocol in {"tcp", "udp", "https", "quic"}:
                return session_proc.process(log)
            if log.protocol == "http":
                # Example placeholder for domain embedding
                log.raw["embedded_domain"] = log.raw.get("host", "")
                return [
                    PacketSequence(
                        user_id=log.user_id,
                        app_id=log.app_id,
                        protocol=log.protocol,
                        timestamp=log.timestamp,
                        packets=[log],
                    )
                ]
            return []

        sequences = app_stream.key_by(lambda l: l.user_id).flat_map(handle_log)
        return sequences.key_by(lambda s: s.user_id).flat_map(lambda s: app_proc.process(s))

    weixin_samples = build_app_pipeline(weixin_stream)
    qq_samples = build_app_pipeline(qq_stream)

    all_samples = weixin_samples.union(qq_samples)

    producer = FlinkKafkaProducer(
        topic="detection_sample_files",
        serialization_schema=SimpleStringSchema(),
        producer_config=kafka_props,
    )

    all_samples.map(lambda s: json.dumps(dataclasses.asdict(s))).add_sink(producer)

    env.execute("flink-zeek-pipeline")


if __name__ == "__main__":
    main()
