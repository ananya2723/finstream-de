"""
Bronze → Silver Transform
- Deduplicates on transaction_id
- Casts and validates types
- Normalises nulls / bad values
- Computes rolling z-score anomaly flag per category
- Writes to silver.db
Run by Airflow DAG: bronze_to_silver
"""

import sqlite3
import os
import pandas as pd
import numpy as np
from datetime import datetime

from data_quality import build_quality_report, init_quality_table, record_quality_report
from finstream_config import settings

BRONZE_DB = settings.bronze_db
SILVER_DB = settings.silver_db


def init_silver(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_transactions (
            transaction_id  TEXT PRIMARY KEY,
            event_time      TEXT,
            ingested_at     TEXT,
            account_id      TEXT,
            merchant        TEXT,
            category        TEXT,
            amount          REAL,
            currency        TEXT,
            payment_mode    TEXT,
            status          TEXT,
            city            TEXT,
            is_anomaly      INTEGER DEFAULT 0,
            anomaly_score   REAL DEFAULT 0.0,
            transformed_at  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clean_market_ticks (
            tick_id         TEXT PRIMARY KEY,
            event_time      TEXT,
            ingested_at     TEXT,
            source          TEXT,
            symbol          TEXT,
            price           REAL,
            volume          REAL,
            price_change_pct REAL DEFAULT 0.0,
            is_price_anomaly INTEGER DEFAULT 0,
            anomaly_score   REAL DEFAULT 0.0,
            transformed_at  TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_quality_runs (
            run_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at              TEXT NOT NULL,
            input_rows              INTEGER NOT NULL,
            duplicate_rows          INTEGER NOT NULL,
            invalid_price_rows      INTEGER NOT NULL,
            invalid_event_time_rows INTEGER NOT NULL,
            valid_rows              INTEGER NOT NULL,
            anomaly_rows            INTEGER NOT NULL
        )
    """)
    init_quality_table(conn)
    conn.commit()


def get_last_ingested_at(silver_conn):
    """Only process new Bronze rows since last Silver run."""
    row = silver_conn.execute(
        "SELECT MAX(ingested_at) FROM clean_transactions"
    ).fetchone()
    return row[0] if row[0] else "1970-01-01"


def compute_anomaly_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Rolling z-score per category on amount."""
    df = df.sort_values("event_time").copy()
    df["anomaly_score"] = 0.0

    for category, group in df.groupby("category"):
        amounts = group["amount"].values
        scores = []
        window = []
        for amt in amounts:
            if len(window) >= 5:
                mean = np.mean(window[-30:])
                std = np.std(window[-30:]) or 1.0
                scores.append(abs((amt - mean) / std))
            else:
                scores.append(0.0)
            window.append(amt)
        df.loc[group.index, "anomaly_score"] = scores

    df["is_anomaly"] = (df["anomaly_score"] > 3.0).astype(int)
    return df


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return row is not None


def get_last_market_ingested_at(silver_conn):
    row = silver_conn.execute(
        "SELECT MAX(ingested_at) FROM clean_market_ticks"
    ).fetchone()
    return row[0] if row[0] else "1970-01-01"


def compute_market_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["symbol", "event_time"]).copy()
    df["price_change_pct"] = 0.0
    df["anomaly_score"] = 0.0

    for symbol, group in df.groupby("symbol"):
        prices = group["price"].astype(float)
        pct_change = prices.pct_change().fillna(0.0) * 100
        rolling_std = pct_change.rolling(window=30, min_periods=5).std().replace(0, np.nan)
        score = (pct_change.abs() / rolling_std).replace([np.inf, -np.inf], 0).fillna(0.0)
        df.loc[group.index, "price_change_pct"] = pct_change
        df.loc[group.index, "anomaly_score"] = score

    df["is_price_anomaly"] = (df["anomaly_score"] > 3.0).astype(int)
    return df


def run_market():
    os.makedirs(os.path.dirname(SILVER_DB), exist_ok=True)
    bronze_conn = sqlite3.connect(BRONZE_DB)
    silver_conn = sqlite3.connect(SILVER_DB)
    init_silver(silver_conn)

    if not table_exists(bronze_conn, "raw_market_ticks"):
        print("[MARKET SILVER] No Bronze market table yet.")
        bronze_conn.close()
        silver_conn.close()
        return 0

    last_run = get_last_market_ingested_at(silver_conn)
    print(f"[MARKET SILVER] Processing market ticks ingested after: {last_run}")

    raw_df = pd.read_sql(
        "SELECT * FROM raw_market_ticks WHERE ingested_at > ?",
        bronze_conn, params=(last_run,)
    )

    if raw_df.empty:
        print("[MARKET SILVER] No new market ticks to process.")
        bronze_conn.close()
        silver_conn.close()
        return 0

    df = raw_df.copy()
    duplicate_rows = int(df.duplicated(subset="tick_id").sum())
    df = df.drop_duplicates(subset="tick_id")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    invalid_price_rows = int((df["price"].isna() | (df["price"] <= 0)).sum())
    df = df[df["price"] > 0].copy()
    parsed_event_time = pd.to_datetime(df["event_time"], errors="coerce")
    invalid_event_time_rows = int(parsed_event_time.isna().sum())
    df["event_time"] = parsed_event_time.dt.strftime("%Y-%m-%dT%H:%M:%S")
    df = df[df["event_time"].notna()].copy()
    df["source"] = df["source"].fillna("unknown")
    df["symbol"] = df["symbol"].str.upper()
    df = df[df["symbol"].notna()].copy()
    df["transformed_at"] = datetime.utcnow().isoformat()

    df = compute_market_scores(df)

    cols = ["tick_id", "event_time", "ingested_at", "source", "symbol",
            "price", "volume", "price_change_pct", "is_price_anomaly",
            "anomaly_score", "transformed_at"]

    written = 0
    for _, row in df[cols].iterrows():
        try:
            cursor = silver_conn.execute("""
                INSERT OR IGNORE INTO clean_market_ticks VALUES
                (?,?,?,?,?,?,?,?,?,?,?)
            """, tuple(row))
            written += cursor.rowcount
        except Exception as e:
            print(f"[MARKET SILVER] Skip {row['tick_id']}: {e}")

    report = {
        "input_rows": int(len(raw_df)),
        "duplicate_rows": duplicate_rows,
        "invalid_price_rows": invalid_price_rows,
        "invalid_event_time_rows": invalid_event_time_rows,
        "valid_rows": int(len(df)),
        "anomaly_rows": int(df["is_price_anomaly"].sum()) if not df.empty else 0,
    }
    silver_conn.execute("""
        INSERT INTO market_quality_runs
        (checked_at, input_rows, duplicate_rows, invalid_price_rows,
         invalid_event_time_rows, valid_rows, anomaly_rows)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        report["input_rows"],
        report["duplicate_rows"],
        report["invalid_price_rows"],
        report["invalid_event_time_rows"],
        report["valid_rows"],
        report["anomaly_rows"],
    ))

    silver_conn.commit()
    bronze_conn.close()
    silver_conn.close()
    print(f"[MARKET SILVER] ✅ Wrote {written} market ticks to Silver.")
    print(f"[MARKET SILVER] Data quality: {report}")
    return written


def run():
    os.makedirs(os.path.dirname(SILVER_DB), exist_ok=True)
    bronze_conn = sqlite3.connect(BRONZE_DB)
    silver_conn = sqlite3.connect(SILVER_DB)
    init_silver(silver_conn)

    last_run = get_last_ingested_at(silver_conn)
    print(f"[SILVER] Processing Bronze rows ingested after: {last_run}")

    raw_df = pd.read_sql(
        "SELECT * FROM raw_transactions WHERE ingested_at > ?",
        bronze_conn, params=(last_run,)
    )

    if raw_df.empty:
        print("[SILVER] No new rows to process.")
        return 0

    print(f"[SILVER] {len(raw_df)} new rows from Bronze.")

    # --- Clean & Validate ---
    df = raw_df.copy()
    df = df.drop_duplicates(subset="transaction_id")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0].copy()
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
    df = df[df["event_time"].notna()].copy()
    df["status"] = df["status"].str.upper().fillna("UNKNOWN")
    df["currency"] = df["currency"].fillna("INR")
    df["city"] = df["city"].fillna("Unknown")
    df["transformed_at"] = datetime.utcnow().isoformat()

    # --- Anomaly Detection ---
    df = compute_anomaly_scores(df)

    # --- Write Silver ---
    cols = ["transaction_id", "event_time", "ingested_at", "account_id",
            "merchant", "category", "amount", "currency", "payment_mode",
            "status", "city", "is_anomaly", "anomaly_score", "transformed_at"]

    written = 0
    for _, row in df[cols].iterrows():
        try:
            cursor = silver_conn.execute("""
                INSERT OR IGNORE INTO clean_transactions VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, tuple(row))
            written += cursor.rowcount
        except Exception as e:
            print(f"[SILVER] Skip {row['transaction_id']}: {e}")

    report = build_quality_report(raw_df, df)
    record_quality_report(silver_conn, report)

    silver_conn.commit()
    bronze_conn.close()
    silver_conn.close()
    print(f"[SILVER] ✅ Wrote {written} rows to Silver.")
    print(f"[SILVER] Data quality: {report}")
    return written


if __name__ == "__main__":
    run()
