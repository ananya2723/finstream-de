"""
Kafka consumer for real market ticks.

Reads raw_market_ticks and writes append-only records to Bronze.
"""

import json
import os
import sqlite3
import time
from datetime import datetime

from finstream_config import settings
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = settings.kafka_broker
TOPIC = settings.market_kafka_topic
GROUP_ID = settings.market_kafka_group_id
BRONZE_DB = settings.bronze_db


def init_bronze_market():
    os.makedirs(os.path.dirname(BRONZE_DB), exist_ok=True)
    conn = sqlite3.connect(BRONZE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_market_ticks (
            ingest_id    INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at  TEXT NOT NULL,
            tick_id      TEXT,
            event_time   TEXT,
            source       TEXT,
            symbol       TEXT,
            price        REAL,
            volume       REAL,
            raw_payload  TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("[MARKET BRONZE] Table initialized.")


def write_bronze_market(tick: dict):
    conn = sqlite3.connect(BRONZE_DB)
    conn.execute("""
        INSERT INTO raw_market_ticks
        (ingested_at, tick_id, event_time, source, symbol, price, volume, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        tick.get("tick_id"),
        tick.get("event_time"),
        tick.get("source", "unknown"),
        tick.get("symbol"),
        tick.get("price"),
        tick.get("volume"),
        json.dumps(tick)
    ))
    conn.commit()
    conn.close()


def create_consumer():
    while True:
        try:
            return KafkaConsumer(
                TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset="earliest",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=True
            )
        except NoBrokersAvailable:
            print(f"[MARKET BRONZE] Kafka not ready at {KAFKA_BROKER}; retrying in 5s...")
            time.sleep(5)


def main():
    init_bronze_market()
    consumer = create_consumer()
    print(f"[MARKET BRONZE] Consuming from '{TOPIC}'...")
    for msg in consumer:
        tick = msg.value
        write_bronze_market(tick)
        print(f"[MARKET BRONZE] Ingested: {tick.get('symbol')} | offset={msg.offset}")


if __name__ == "__main__":
    main()
