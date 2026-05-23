FinStream DE
============

FinStream DE is a small real-time data engineering project for financial
transactions. It simulates payment events, ingests them through Kafka, stores raw
events in a Bronze SQLite layer, transforms them into a cleaned Silver layer, and
loads a Gold star schema for Streamlit analytics.

Architecture
------------

- `transaction_producer.py` publishes simulated transactions to Kafka topic
  `raw_transactions`.
- `bronze_consumer.py` consumes Kafka events and appends raw records to
  `data/bronze.db`.
- `bronze_to_silver.py` deduplicates, validates, cleans, and anomaly-scores
  records into `data/silver.db`.
- `silver_to_gold.py` builds dimensions, facts, and aggregate tables in
  `data/gold.db`.
- `dashboard.py` reads only the Gold layer and renders Streamlit analytics.
- `finstream_dag.py` can run the Bronze-to-Silver and Silver-to-Gold transforms
  from Airflow.

Run With Docker
---------------

Start Kafka, producer, consumer, Kafka UI, and the dashboard:

```bash
docker compose up --build
```

Open:

- Dashboard: http://localhost:8501
- Kafka UI: http://localhost:8080

In another terminal, run the transformation steps whenever Bronze has data:

```bash
python bronze_to_silver.py
python silver_to_gold.py
```

Local Run
---------

Install dependencies:

```bash
python3 -m venv finstream-env
source finstream-env/bin/activate
pip install -r requirements.txt
```

The Docker image uses `requirements-docker.txt`, which intentionally excludes
Airflow. The producer, consumer, transforms, and dashboard do not import Airflow;
`apache-airflow` is only needed if you want to run `finstream_dag.py` in an
Airflow environment.

Start Kafka with Docker:

```bash
docker compose up zookeeper kafka kafka-ui
```

Then run these in separate terminals:

```bash
python transaction_producer.py
python bronze_consumer.py
python bronze_to_silver.py
python silver_to_gold.py
streamlit run dashboard.py
```

Configuration
-------------

`transaction_producer.py` and `bronze_consumer.py` read `KAFKA_BROKER` from the
environment. If it is not set, they default to `localhost:9092`.

Docker Compose sets `KAFKA_BROKER=kafka:29092` for the app containers.
