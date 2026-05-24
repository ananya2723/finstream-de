FinStream DE
============

FinStream DE is a production-inspired real-time data engineering project for
financial transaction analytics. It simulates payment events, streams them
through Kafka, lands raw events in a Bronze layer, validates and anomaly-scores
records in Silver, models analytics-ready Gold tables, and serves a Streamlit
dashboard. It also supports an optional real market-data mode using Finnhub
WebSocket ticks for crypto/equity symbols.

This repo is designed as a portfolio project for data engineering roles. It
demonstrates event ingestion, medallion architecture, dimensional modeling,
data-quality checks, container orchestration, automated transform runs, and CI.

What It Shows
-------------

- Kafka producer and consumer for streaming-style ingestion.
- Dual-source architecture: synthetic transaction events and real market ticks.
- Bronze, Silver, and Gold data layers using SQLite for local portability.
- Incremental transforms based on ingestion and load timestamps.
- Data-quality metrics for duplicates, invalid amounts, invalid timestamps, and
  valid-row counts.
- Category-level rolling z-score anomaly detection.
- Real market tick cleaning, minute bars, latest prices, and price-move anomaly
  detection.
- Star schema with facts, dimensions, and aggregate tables.
- Streamlit dashboard reading only from the Gold layer.
- Docker Compose health checks and a continuous pipeline runner.
- Pytest coverage and GitHub Actions CI.

Architecture
------------

```text
Synthetic transaction producer
        |
        v
Kafka topic: raw_transactions
        |
        v
Bronze: raw immutable events
        |
        v
Silver: cleaned + validated + anomaly scored
        |
        v
Gold: facts + dimensions + aggregates
        |
        v
Streamlit dashboard
```

Optional real-data path:

```text
Finnhub WebSocket
        |
        v
Kafka topic: raw_market_ticks
        |
        v
Bronze: raw market ticks
        |
        v
Silver: clean market ticks + price anomaly scores
        |
        v
Gold: latest prices + minute bars + market facts
        |
        v
Streamlit dashboard
```

More detail: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

Core Files
----------

- `transaction_producer.py` publishes simulated transactions to Kafka.
- `bronze_consumer.py` consumes Kafka events and appends raw records to
  `data/bronze.db`.
- `market_data_producer.py` streams real Finnhub trades to Kafka when
  `FINNHUB_API_KEY` is provided.
- `market_bronze_consumer.py` consumes real market ticks into Bronze.
- `bronze_to_silver.py` deduplicates, validates, cleans, quality-checks, and
  anomaly-scores transaction and market records into `data/silver.db`.
- `silver_to_gold.py` builds dimensions, facts, aggregate tables, and quality
  metadata for both data products in `data/gold.db`.
- `pipeline_runner.py` runs the transforms once or continuously.
- `dashboard.py` reads the Gold layer and renders analytics.
- `finstream_dag.py` provides an optional Airflow DAG for the transform steps.

Quickstart
----------

Start the full stack:

```bash
docker compose up --build
```

Open:

- Dashboard: http://localhost:8501
- Kafka UI: http://localhost:8080

The `pipeline` service refreshes Silver and Gold automatically every 60 seconds.
To trigger a manual refresh:

```bash
docker compose run --rm dashboard python pipeline_runner.py --once
```

Real Market Data Mode
---------------------

The default stack runs synthetic transactions only, so anyone can clone and run
the project without secrets. To enable real crypto/equity market ticks, create a
free Finnhub API key and start the optional `real-data` profile:

```bash
cp .env.example .env
# edit .env and set FINNHUB_API_KEY
docker compose --profile real-data up -d --build
```

Watch the real-data services:

```bash
docker compose logs -f market-producer
docker compose logs -f market-consumer
docker compose logs -f pipeline
```

The dashboard will show market analytics after ticks are ingested and the
pipeline refreshes Gold. You can force a refresh with:

```bash
make transform
```

Useful Commands
---------------

```bash
make build       # build Docker images
make up          # start services in the background
make logs        # follow logs
make market-up   # start optional Finnhub real-data services
make transform   # run Bronze -> Silver -> Gold once
make test        # run pytest locally
make down        # stop services
```

Local Development
-----------------

Install dependencies:

```bash
python3 -m venv finstream-env
source finstream-env/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

The Docker image uses `requirements-docker.txt`, which intentionally excludes
Airflow. The producer, consumer, transforms, and dashboard do not import Airflow;
`apache-airflow` is only needed if you want to run `finstream_dag.py` in an
Airflow environment:

```bash
pip install -r requirements-airflow.txt
```

Run tests:

```bash
pytest -q
```

Configuration
-------------

Runtime settings are environment-driven through `finstream_config.py`.

| Variable | Default | Purpose |
| --- | --- | --- |
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `KAFKA_TOPIC` | `raw_transactions` | Source topic |
| `MARKET_KAFKA_TOPIC` | `raw_market_ticks` | Real market-data topic |
| `KAFKA_GROUP_ID` | `bronze-ingestion-group` | Consumer group |
| `MARKET_KAFKA_GROUP_ID` | `market-bronze-ingestion-group` | Market consumer group |
| `BRONZE_DB` | `data/bronze.db` | Bronze SQLite path |
| `SILVER_DB` | `data/silver.db` | Silver SQLite path |
| `GOLD_DB` | `data/gold.db` | Gold SQLite path |
| `TRANSFORM_INTERVAL_SECONDS` | `120` | Continuous transform interval |
| `FINNHUB_API_KEY` | empty | Enables real Finnhub WebSocket ingestion |
| `MARKET_SYMBOLS` | `BINANCE:BTCUSDT,BINANCE:ETHUSDT,AAPL,MSFT,TSLA` | Real market symbols |

Interview Talking Points
------------------------

- Why Bronze is append-only and keeps raw payloads.
- Why synthetic transaction data and real market data are modeled as separate
  data products.
- How Silver applies data-quality gates and idempotent inserts.
- Why Gold uses dimensional modeling and precomputed aggregates.
- How Kafka decouples event production from ingestion.
- How health checks improve container startup reliability.
- How CI and tests prove the transform contract.

Manual Local Run
----------------

Start Kafka services:

```bash
docker compose up zookeeper kafka kafka-ui
```

Then run these in separate terminals:

```bash
python transaction_producer.py
python bronze_consumer.py
FINNHUB_API_KEY=... python market_data_producer.py
python market_bronze_consumer.py
python pipeline_runner.py
streamlit run dashboard.py
```
## Dashboard Preview

### Real-Time Market Analytics Dashboard

(<img width="1646" height="814" alt="pipeline" src="https://github.com/user-attachments/assets/de4d9d20-dd7e-4842-b5ca-0ddd30a6da73" />
### Gold Layer Market Table
<img width="1692" height="412" alt="gold_layer_table" src="https://github.com/user-attachments/assets/80477ea4-b687-413d-aa05-3431b384e304" />
### Live Kafka + Spark Pipeline

<img width="1698" height="966" alt="dashboard" src="https://github.com/user-attachments/assets/637f26da-8e6d-4041-9b4d-eccd8583927a" />
screenshots/dashboard.png)

