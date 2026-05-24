"""
Kafka Consumer — Bronze Layer Ingestion
Reads raw_transactions topic, writes append-only to bronze.db (raw, immutable).
No transformations — exact replica of what Kafka delivers.
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
TOPIC = settings.kafka_topic
GROUP_ID = settings.kafka_group_id
BRONZE_DB = settings.bronze_db


def init_bronze():
    os.makedirs(os.path.dirname(BRONZE_DB), exist_ok=True)
    conn = sqlite3.connect(BRONZE_DB)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS raw_transactions (
            ingest_id       INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at     TEXT NOT NULL,
            transaction_id  TEXT,
            event_time      TEXT,
            account_id      TEXT,
            merchant        TEXT,
            category        TEXT,
            amount          REAL,
            currency        TEXT,
            payment_mode    TEXT,
            status          TEXT,
            city            TEXT,
            raw_payload     TEXT        -- full JSON blob, source of truth
        )
    """)
    conn.commit()
    conn.close()
    print("[BRONZE] Table initialized.")


def write_bronze(txn: dict):
    conn = sqlite3.connect(BRONZE_DB)
    c = conn.cursor()
    c.execute("""
        INSERT INTO raw_transactions
        (ingested_at, transaction_id, event_time, account_id, merchant,
         category, amount, currency, payment_mode, status, city, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        txn.get("transaction_id"),
        txn.get("event_time"),
        txn.get("account_id"),
        txn.get("merchant"),
        txn.get("category"),
        txn.get("amount"),
        txn.get("currency", "INR"),
        txn.get("payment_mode"),
        txn.get("status"),
        txn.get("city"),
        json.dumps(txn)
    ))
    conn.commit()
    conn.close()


def main():
    init_bronze()
    consumer = create_consumer()
    print(f"[BRONZE] Consuming from '{TOPIC}'...")
    for msg in consumer:
        txn = msg.value
        write_bronze(txn)
        print(f"[BRONZE] Ingested: {txn.get('transaction_id')} | offset={msg.offset}")


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
            print(f"[BRONZE] Kafka not ready at {KAFKA_BROKER}; retrying in 5s...")
            time.sleep(5)


if __name__ == "__main__":
    main()
