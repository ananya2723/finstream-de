# FinStream Architecture

FinStream is a compact, production-inspired medallion pipeline for transaction
and market-data analytics. It is designed to show streaming ingestion, layered
data modeling, data-quality checks, orchestration, and dashboard consumption.

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

Optional real-data path:

```text
market_data_producer.py
        |
        v
Kafka topic: raw_market_ticks
        |
        v
market_bronze_consumer.py -> data/bronze.db
        |
        v
bronze_to_silver.run_market() -> data/silver.db
        |
        v
silver_to_gold.run_market() -> data/gold.db
        |
        v
dashboard.py
```

## Layers

Bronze stores immutable raw events exactly as they arrived from Kafka. This
keeps a replayable source of truth for downstream transforms.

Silver standardizes records, removes duplicate transaction IDs, filters invalid
amounts and timestamps, normalizes status and null values, and computes rolling
category-level anomaly scores. For market ticks, Silver validates symbol, price,
volume, and event time, then computes price-change anomaly scores.

Gold models the cleaned data as dimensions, facts, and aggregates:

- `dim_account`
- `dim_merchant`
- `dim_date`
- `fact_transactions`
- `agg_daily_category`
- `agg_hourly_volume`
- `data_quality_runs`
- `dim_symbol`
- `fact_market_ticks`
- `agg_market_minute_bars`
- `latest_market_prices`
- `market_quality_runs`

## Operational Features

- Docker Compose starts Kafka, Zookeeper, producer, consumer, pipeline runner,
  Kafka UI, and dashboard.
- Health checks prevent Kafka-dependent services from starting before the broker
  is ready.
- `pipeline_runner.py` can run transforms once or continuously.
- Data-quality metrics are persisted and surfaced in the dashboard.
- Real market-data services are optional and enabled with Docker Compose's
  `real-data` profile plus `FINNHUB_API_KEY`.
- GitHub Actions runs syntax checks and pytest on every push and pull request.

## Portfolio Talking Points

- Event-driven ingestion using Kafka.
- Medallion architecture with raw, cleaned, and analytics-ready layers.
- Dual-source design: synthetic payment transactions and real market ticks.
- Incremental batch processing based on ingestion timestamps.
- Dimensional modeling for dashboard-friendly analytics.
- Automated data-quality reporting.
- Containerized local environment with health checks.
- CI-backed tests for transformation correctness.
