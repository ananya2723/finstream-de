# FinStream Architecture

FinStream is a compact, production-inspired medallion pipeline for transaction
analytics. It is designed to show streaming ingestion, layered data modeling,
data-quality checks, orchestration, and dashboard consumption.

## System Flow

```text
transaction_producer.py
        |
        v
Kafka topic: raw_transactions
        |
        v
bronze_consumer.py -> data/bronze.db
        |
        v
bronze_to_silver.py -> data/silver.db
        |
        v
silver_to_gold.py -> data/gold.db
        |
        v
dashboard.py
```

## Layers

Bronze stores immutable raw events exactly as they arrived from Kafka. This
keeps a replayable source of truth for downstream transforms.

Silver standardizes records, removes duplicate transaction IDs, filters invalid
amounts and timestamps, normalizes status and null values, and computes rolling
category-level anomaly scores.

Gold models the cleaned data as dimensions, facts, and aggregates:

- `dim_account`
- `dim_merchant`
- `dim_date`
- `fact_transactions`
- `agg_daily_category`
- `agg_hourly_volume`
- `data_quality_runs`

## Operational Features

- Docker Compose starts Kafka, Zookeeper, producer, consumer, pipeline runner,
  Kafka UI, and dashboard.
- Health checks prevent Kafka-dependent services from starting before the broker
  is ready.
- `pipeline_runner.py` can run transforms once or continuously.
- Data-quality metrics are persisted and surfaced in the dashboard.
- GitHub Actions runs syntax checks and pytest on every push and pull request.

## Portfolio Talking Points

- Event-driven ingestion using Kafka.
- Medallion architecture with raw, cleaned, and analytics-ready layers.
- Incremental batch processing based on ingestion timestamps.
- Dimensional modeling for dashboard-friendly analytics.
- Automated data-quality reporting.
- Containerized local environment with health checks.
- CI-backed tests for transformation correctness.
