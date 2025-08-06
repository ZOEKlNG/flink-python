# Zeek Flink Pipeline

This project implements a simplified real-time processing pipeline that consumes Zeek logs from Kafka, aggregates them with Apache Flink, and produces detection samples.  It follows the architecture described in `架构.md` and is organised as a Python package for easier deployment.

## Features

* Ingest Zeek logs from multiple Kafka topics.
* Normalise records and extract `user_id` from TLV encoded field.
* Aggregate packets by session and then by application.
* Emit aggregated samples to a Kafka sink.

## Development

```bash
pip install -e .
pre-commit run --files <changed files>
pytest
```

## Running

The main streaming job can be submitted to a Flink cluster using:

```bash
python -m zeek_flink_pipeline.jobs.realtime
```

This uses PyFlink's DataStream API and expects Kafka and Flink to be available.
