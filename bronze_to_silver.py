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

BRONZE_DB = "data/bronze.db"
SILVER_DB = "data/silver.db"


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


def run():
    os.makedirs(os.path.dirname(SILVER_DB), exist_ok=True)
    bronze_conn = sqlite3.connect(BRONZE_DB)
    silver_conn = sqlite3.connect(SILVER_DB)
    init_silver(silver_conn)

    last_run = get_last_ingested_at(silver_conn)
    print(f"[SILVER] Processing Bronze rows ingested after: {last_run}")

    df = pd.read_sql(
        "SELECT * FROM raw_transactions WHERE ingested_at > ?",
        bronze_conn, params=(last_run,)
    )

    if df.empty:
        print("[SILVER] No new rows to process.")
        return 0

    print(f"[SILVER] {len(df)} new rows from Bronze.")

    # --- Clean & Validate ---
    df = df.drop_duplicates(subset="transaction_id")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"] > 0].copy()
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
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
            silver_conn.execute("""
                INSERT OR IGNORE INTO clean_transactions VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, tuple(row))
            written += 1
        except Exception as e:
            print(f"[SILVER] Skip {row['transaction_id']}: {e}")

    silver_conn.commit()
    bronze_conn.close()
    silver_conn.close()
    print(f"[SILVER] ✅ Wrote {written} rows to Silver.")
    return written


if __name__ == "__main__":
    run()
