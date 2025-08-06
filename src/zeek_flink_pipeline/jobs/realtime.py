"""Entry point for the real-time streaming job."""
from __future__ import annotations

from pathlib import Path

import yaml
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import (KafkaOffsetsInitializer,
                                                 KafkaSource)

from ..app_agg.operator import AppAggregator
from ..common.serializers import dumps, loads
from ..preprocess.parser import to_unified_log
from ..session_agg.operator import SessionAggregator
from ..sinks.kafka_sink import build_kafka_sink


def load_config() -> dict:
    path = Path(__file__).resolve().parents[1] / "config" / "default.yaml"
    with open(path) as fh:
        return yaml.safe_load(fh)


def main():
    cfg = load_config()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(cfg["kafka"]["bootstrap_servers"])
        .set_topics(cfg["kafka"]["source_topics"])
        .set_group_id("zeek-flink")
        .set_value_only_deserializer(SimpleStringSchema())
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .build()
    )

    ds = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka_source",
    )
    unified = ds.map(lambda s: to_unified_log(loads(s)))
    sequences = unified.key_by(lambda u: u.user_id).process(SessionAggregator())
    samples = sequences.key_by(lambda p: p.user_id).process(AppAggregator())

    sink = build_kafka_sink(
        cfg["kafka"]["sink_topic"], cfg["kafka"]["bootstrap_servers"]
    )
    samples.map(lambda s: dumps(s.__dict__).decode("utf-8")).sink_to(sink)

    env.execute("zeek_realtime")


if __name__ == "__main__":  # pragma: no cover - manual execution only
    main()
