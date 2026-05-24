"""
Silver → Gold Transform — Star Schema Modelling
Creates a proper dimensional warehouse:

  dim_account      (account_id, city, first_seen, last_seen)
  dim_merchant     (merchant_id, merchant_name, category)
  dim_date         (date_id, full_date, year, month, day, weekday, is_weekend)
  fact_transactions (surrogate_key, transaction_id, date_id, account_id,
                     merchant_id, amount, status, payment_mode,
                     is_anomaly, anomaly_score, event_hour)

Run by Airflow DAG: silver_to_gold
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime, date
import hashlib

from finstream_config import settings

SILVER_DB = settings.silver_db
GOLD_DB   = settings.gold_db


# ─────────────────────────── Schema ───────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_account (
    account_id   TEXT PRIMARY KEY,
    city         TEXT,
    first_seen   TEXT,
    last_seen    TEXT
);

CREATE TABLE IF NOT EXISTS dim_merchant (
    merchant_id   TEXT PRIMARY KEY,
    merchant_name TEXT,
    category      TEXT
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id    TEXT PRIMARY KEY,   -- YYYY-MM-DD
    full_date  TEXT,
    year       INTEGER,
    month      INTEGER,
    day        INTEGER,
    weekday    TEXT,
    is_weekend INTEGER
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    surrogate_key  TEXT PRIMARY KEY,
    transaction_id TEXT UNIQUE,
    date_id        TEXT,
    account_id     TEXT,
    merchant_id    TEXT,
    amount         REAL,
    currency       TEXT,
    payment_mode   TEXT,
    status         TEXT,
    city           TEXT,
    event_hour     INTEGER,
    is_anomaly     INTEGER,
    anomaly_score  REAL,
    loaded_at      TEXT,
    FOREIGN KEY (date_id)    REFERENCES dim_date(date_id),
    FOREIGN KEY (account_id) REFERENCES dim_account(account_id),
    FOREIGN KEY (merchant_id) REFERENCES dim_merchant(merchant_id)
);

CREATE TABLE IF NOT EXISTS agg_daily_category (
    agg_date       TEXT,
    category       TEXT,
    total_amount   REAL,
    txn_count      INTEGER,
    anomaly_count  INTEGER,
    avg_amount     REAL,
    PRIMARY KEY (agg_date, category)
);

CREATE TABLE IF NOT EXISTS agg_hourly_volume (
    agg_date    TEXT,
    hour        INTEGER,
    txn_count   INTEGER,
    total_amount REAL,
    PRIMARY KEY (agg_date, hour)
);

CREATE TABLE IF NOT EXISTS data_quality_runs (
    run_id                  INTEGER PRIMARY KEY,
    checked_at              TEXT NOT NULL,
    source_layer            TEXT NOT NULL,
    target_layer            TEXT NOT NULL,
    input_rows              INTEGER NOT NULL,
    duplicate_rows          INTEGER NOT NULL,
    invalid_amount_rows     INTEGER NOT NULL,
    invalid_event_time_rows INTEGER NOT NULL,
    valid_rows              INTEGER NOT NULL,
    anomaly_rows            INTEGER NOT NULL
);
"""


def surrogate_key(transaction_id: str) -> str:
    return hashlib.md5(transaction_id.encode()).hexdigest()[:16]


def merchant_id(merchant_name: str) -> str:
    return "MRC_" + merchant_name.upper().replace(" ", "_")


def get_last_loaded(gold_conn) -> str:
    row = gold_conn.execute("SELECT MAX(loaded_at) FROM fact_transactions").fetchone()
    return row[0] if row[0] else "1970-01-01"


def upsert_dim_account(gold_conn, df: pd.DataFrame):
    accounts = df.groupby("account_id").agg(
        city=("city", "first"),
        first_seen=("event_time", "min"),
        last_seen=("event_time", "max")
    ).reset_index()
    for _, row in accounts.iterrows():
        gold_conn.execute("""
            INSERT INTO dim_account VALUES (?,?,?,?)
            ON CONFLICT(account_id) DO UPDATE SET
                last_seen = excluded.last_seen,
                city = excluded.city
        """, (row.account_id, row.city, row.first_seen, row.last_seen))


def upsert_dim_merchant(gold_conn, df: pd.DataFrame):
    merchants = df[["merchant", "category"]].drop_duplicates()
    for _, row in merchants.iterrows():
        mid = merchant_id(row.merchant)
        gold_conn.execute("""
            INSERT OR IGNORE INTO dim_merchant VALUES (?,?,?)
        """, (mid, row.merchant, row.category))


def upsert_dim_date(gold_conn, dates):
    for d in dates:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d").date()
            gold_conn.execute("""
                INSERT OR IGNORE INTO dim_date VALUES (?,?,?,?,?,?,?)
            """, (
                d, str(dt),
                dt.year, dt.month, dt.day,
                dt.strftime("%A"),
                1 if dt.weekday() >= 5 else 0
            ))
        except:
            pass


def load_fact(gold_conn, df: pd.DataFrame):
    loaded_at = datetime.utcnow().isoformat()
    written = 0
    for _, row in df.iterrows():
        try:
            event_dt = datetime.fromisoformat(row["event_time"])
            date_id = event_dt.strftime("%Y-%m-%d")
            mid = merchant_id(row["merchant"])
            sk = surrogate_key(row["transaction_id"])
            cursor = gold_conn.execute("""
                INSERT OR IGNORE INTO fact_transactions VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sk, row["transaction_id"], date_id,
                row["account_id"], mid,
                row["amount"], row["currency"], row["payment_mode"],
                row["status"], row["city"], event_dt.hour,
                int(row["is_anomaly"]), float(row["anomaly_score"]),
                loaded_at
            ))
            written += cursor.rowcount
        except Exception as e:
            print(f"[GOLD] Skip {row.get('transaction_id')}: {e}")
    return written


def refresh_aggregates(gold_conn):
    """Rebuild daily and hourly aggregate tables from fact."""
    gold_conn.execute("DELETE FROM agg_daily_category")
    gold_conn.execute("""
        INSERT INTO agg_daily_category
        SELECT
            date_id,
            m.category,
            SUM(f.amount),
            COUNT(*),
            SUM(f.is_anomaly),
            AVG(f.amount)
        FROM fact_transactions f
        JOIN dim_merchant m ON f.merchant_id = m.merchant_id
        GROUP BY date_id, m.category
    """)

    gold_conn.execute("DELETE FROM agg_hourly_volume")
    gold_conn.execute("""
        INSERT INTO agg_hourly_volume
        SELECT date_id, event_hour, COUNT(*), SUM(amount)
        FROM fact_transactions
        GROUP BY date_id, event_hour
    """)
    print("[GOLD] Aggregates refreshed.")


def sync_quality_reports(silver_conn, gold_conn):
    quality_df = pd.read_sql(
        "SELECT * FROM data_quality_runs ORDER BY run_id DESC LIMIT 20",
        silver_conn
    )
    if quality_df.empty:
        return

    for _, row in quality_df.iterrows():
        gold_conn.execute("""
            INSERT OR REPLACE INTO data_quality_runs VALUES
            (?,?,?,?,?,?,?,?,?,?)
        """, (
            int(row["run_id"]),
            row["checked_at"],
            row["source_layer"],
            row["target_layer"],
            int(row["input_rows"]),
            int(row["duplicate_rows"]),
            int(row["invalid_amount_rows"]),
            int(row["invalid_event_time_rows"]),
            int(row["valid_rows"]),
            int(row["anomaly_rows"]),
        ))


def run():
    os.makedirs(os.path.dirname(GOLD_DB), exist_ok=True)
    silver_conn = sqlite3.connect(SILVER_DB)
    gold_conn   = sqlite3.connect(GOLD_DB)

    # Init schema
    for stmt in SCHEMA.strip().split(";"):
        if stmt.strip():
            gold_conn.execute(stmt)
    gold_conn.commit()

    last_loaded = get_last_loaded(gold_conn)
    print(f"[GOLD] Loading Silver rows with transformed_at > {last_loaded}")

    df = pd.read_sql(
        "SELECT * FROM clean_transactions WHERE transformed_at > ?",
        silver_conn, params=(last_loaded,)
    )

    if df.empty:
        print("[GOLD] No new Silver rows.")
        return 0

    print(f"[GOLD] {len(df)} rows to load.")

    upsert_dim_account(gold_conn, df)
    upsert_dim_merchant(gold_conn, df)

    dates = set()
    for et in df["event_time"].dropna():
        try:
            dates.add(datetime.fromisoformat(et).strftime("%Y-%m-%d"))
        except:
            pass
    upsert_dim_date(gold_conn, dates)

    written = load_fact(gold_conn, df)
    refresh_aggregates(gold_conn)
    sync_quality_reports(silver_conn, gold_conn)

    gold_conn.commit()
    silver_conn.close()
    gold_conn.close()
    print(f"[GOLD] ✅ Loaded {written} rows into fact_transactions.")
    return written


if __name__ == "__main__":
    run()
