"""Kafka sink factory."""
from __future__ import annotations

from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaRecordSerializationSchema, KafkaSink)


def build_kafka_sink(topic: str, servers: str) -> KafkaSink:
    serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    return (
        KafkaSink.builder()
        .set_bootstrap_servers(servers)
        .set_record_serializer(serializer)
        .build()
    )
