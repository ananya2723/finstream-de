import json
import sqlite3
from datetime import datetime, timedelta

import bronze_to_silver
import silver_to_gold


def insert_bronze_rows(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE raw_transactions (
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
            raw_payload     TEXT
        )
    """)

    base = datetime(2026, 1, 1, 10, 0, 0)
    rows = []
    for idx in range(8):
        txn = {
            "transaction_id": f"TXN_TEST_{idx}",
            "event_time": (base + timedelta(minutes=idx)).isoformat(),
            "account_id": "ACC0001",
            "merchant": "Amazon",
            "category": "E-Commerce",
            "amount": 100 + idx,
            "currency": "INR",
            "payment_mode": "UPI",
            "status": "SUCCESS",
            "city": "Bengaluru",
        }
        rows.append((
            datetime.utcnow().isoformat(),
            txn["transaction_id"],
            txn["event_time"],
            txn["account_id"],
            txn["merchant"],
            txn["category"],
            txn["amount"],
            txn["currency"],
            txn["payment_mode"],
            txn["status"],
            txn["city"],
            json.dumps(txn),
        ))

    duplicate = list(rows[0])
    duplicate[0] = datetime.utcnow().isoformat()
    invalid_amount = list(rows[1])
    invalid_amount[1] = "TXN_BAD_AMOUNT"
    invalid_amount[6] = -10

    conn.executemany("""
        INSERT INTO raw_transactions
        (ingested_at, transaction_id, event_time, account_id, merchant,
         category, amount, currency, payment_mode, status, city, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows + [tuple(duplicate), tuple(invalid_amount)])
    conn.commit()
    conn.close()


def test_bronze_to_silver_to_gold(tmp_path, monkeypatch):
    bronze_db = tmp_path / "bronze.db"
    silver_db = tmp_path / "silver.db"
    gold_db = tmp_path / "gold.db"
    insert_bronze_rows(bronze_db)

    monkeypatch.setattr(bronze_to_silver, "BRONZE_DB", str(bronze_db))
    monkeypatch.setattr(bronze_to_silver, "SILVER_DB", str(silver_db))
    monkeypatch.setattr(silver_to_gold, "SILVER_DB", str(silver_db))
    monkeypatch.setattr(silver_to_gold, "GOLD_DB", str(gold_db))

    assert bronze_to_silver.run() == 8
    assert silver_to_gold.run() == 8

    silver_conn = sqlite3.connect(silver_db)
    quality = silver_conn.execute("""
        SELECT input_rows, duplicate_rows, invalid_amount_rows, valid_rows
        FROM data_quality_runs
        ORDER BY run_id DESC LIMIT 1
    """).fetchone()
    silver_conn.close()

    assert quality == (10, 1, 1, 8)

    gold_conn = sqlite3.connect(gold_db)
    fact_count = gold_conn.execute("SELECT COUNT(*) FROM fact_transactions").fetchone()[0]
    dim_merchant_count = gold_conn.execute("SELECT COUNT(*) FROM dim_merchant").fetchone()[0]
    dq_count = gold_conn.execute("SELECT COUNT(*) FROM data_quality_runs").fetchone()[0]
    gold_conn.close()

    assert fact_count == 8
    assert dim_merchant_count == 1
    assert dq_count == 1
