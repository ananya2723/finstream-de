import json
import sqlite3
from datetime import datetime, timedelta

import bronze_to_silver
import silver_to_gold


def insert_market_rows(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE raw_market_ticks (
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

    base = datetime(2026, 1, 1, 10, 0, 0)
    rows = []
    for idx in range(8):
        tick = {
            "tick_id": f"BINANCE:BTCUSDT_{idx}",
            "event_time": (base + timedelta(seconds=idx)).isoformat(),
            "source": "finnhub",
            "symbol": "BINANCE:BTCUSDT",
            "price": 50000 + idx,
            "volume": 0.5 + idx,
        }
        rows.append((
            datetime.utcnow().isoformat(),
            tick["tick_id"],
            tick["event_time"],
            tick["source"],
            tick["symbol"],
            tick["price"],
            tick["volume"],
            json.dumps(tick),
        ))

    duplicate = list(rows[0])
    invalid_price = list(rows[1])
    invalid_price[1] = "BINANCE:BTCUSDT_BAD_PRICE"
    invalid_price[5] = -1

    conn.executemany("""
        INSERT INTO raw_market_ticks
        (ingested_at, tick_id, event_time, source, symbol, price, volume, raw_payload)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows + [tuple(duplicate), tuple(invalid_price)])
    conn.commit()
    conn.close()


def test_market_bronze_to_silver_to_gold(tmp_path, monkeypatch):
    bronze_db = tmp_path / "bronze.db"
    silver_db = tmp_path / "silver.db"
    gold_db = tmp_path / "gold.db"
    insert_market_rows(bronze_db)

    monkeypatch.setattr(bronze_to_silver, "BRONZE_DB", str(bronze_db))
    monkeypatch.setattr(bronze_to_silver, "SILVER_DB", str(silver_db))
    monkeypatch.setattr(silver_to_gold, "SILVER_DB", str(silver_db))
    monkeypatch.setattr(silver_to_gold, "GOLD_DB", str(gold_db))

    assert bronze_to_silver.run_market() == 8
    assert silver_to_gold.run_market() == 8

    silver_conn = sqlite3.connect(silver_db)
    quality = silver_conn.execute("""
        SELECT input_rows, duplicate_rows, invalid_price_rows, valid_rows
        FROM market_quality_runs
        ORDER BY run_id DESC LIMIT 1
    """).fetchone()
    silver_conn.close()

    assert quality == (10, 1, 1, 8)

    gold_conn = sqlite3.connect(gold_db)
    fact_count = gold_conn.execute("SELECT COUNT(*) FROM fact_market_ticks").fetchone()[0]
    bar_count = gold_conn.execute("SELECT COUNT(*) FROM agg_market_minute_bars").fetchone()[0]
    latest_count = gold_conn.execute("SELECT COUNT(*) FROM latest_market_prices").fetchone()[0]
    symbol_count = gold_conn.execute("SELECT COUNT(*) FROM dim_symbol").fetchone()[0]
    gold_conn.close()

    assert fact_count == 8
    assert bar_count == 1
    assert latest_count == 1
    assert symbol_count == 1
