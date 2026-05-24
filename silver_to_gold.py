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

CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol       TEXT PRIMARY KEY,
    asset_type   TEXT,
    first_seen   TEXT,
    last_seen    TEXT
);

CREATE TABLE IF NOT EXISTS fact_market_ticks (
    tick_key         TEXT PRIMARY KEY,
    tick_id          TEXT UNIQUE,
    symbol           TEXT,
    event_time       TEXT,
    event_minute     TEXT,
    price            REAL,
    volume           REAL,
    price_change_pct REAL,
    is_price_anomaly INTEGER,
    anomaly_score    REAL,
    source           TEXT,
    loaded_at        TEXT,
    FOREIGN KEY (symbol) REFERENCES dim_symbol(symbol)
);

CREATE TABLE IF NOT EXISTS agg_market_minute_bars (
    symbol        TEXT,
    event_minute  TEXT,
    open_price    REAL,
    high_price    REAL,
    low_price     REAL,
    close_price   REAL,
    total_volume  REAL,
    tick_count     INTEGER,
    anomaly_count  INTEGER,
    PRIMARY KEY (symbol, event_minute)
);

CREATE TABLE IF NOT EXISTS latest_market_prices (
    symbol           TEXT PRIMARY KEY,
    event_time       TEXT,
    price            REAL,
    volume           REAL,
    price_change_pct REAL,
    is_price_anomaly INTEGER,
    anomaly_score    REAL,
    source           TEXT
);

CREATE TABLE IF NOT EXISTS market_quality_runs (
    run_id                  INTEGER PRIMARY KEY,
    checked_at              TEXT NOT NULL,
    input_rows              INTEGER NOT NULL,
    duplicate_rows          INTEGER NOT NULL,
    invalid_price_rows      INTEGER NOT NULL,
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


def get_last_market_loaded(gold_conn) -> str:
    row = gold_conn.execute("SELECT MAX(loaded_at) FROM fact_market_ticks").fetchone()
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


def asset_type(symbol: str) -> str:
    return "crypto" if ":" in symbol else "equity"


def upsert_dim_symbol(gold_conn, df: pd.DataFrame):
    symbols = df.groupby("symbol").agg(
        first_seen=("event_time", "min"),
        last_seen=("event_time", "max")
    ).reset_index()
    for _, row in symbols.iterrows():
        gold_conn.execute("""
            INSERT INTO dim_symbol VALUES (?,?,?,?)
            ON CONFLICT(symbol) DO UPDATE SET
                last_seen = excluded.last_seen,
                asset_type = excluded.asset_type
        """, (row.symbol, asset_type(row.symbol), row.first_seen, row.last_seen))


def load_market_fact(gold_conn, df: pd.DataFrame):
    loaded_at = datetime.utcnow().isoformat()
    written = 0
    for _, row in df.iterrows():
        try:
            event_dt = datetime.fromisoformat(row["event_time"])
            event_minute = event_dt.strftime("%Y-%m-%dT%H:%M:00")
            tick_key = hashlib.md5(row["tick_id"].encode()).hexdigest()[:16]
            cursor = gold_conn.execute("""
                INSERT OR IGNORE INTO fact_market_ticks VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                tick_key,
                row["tick_id"],
                row["symbol"],
                row["event_time"],
                event_minute,
                float(row["price"]),
                float(row["volume"]),
                float(row["price_change_pct"]),
                int(row["is_price_anomaly"]),
                float(row["anomaly_score"]),
                row["source"],
                loaded_at,
            ))
            written += cursor.rowcount
        except Exception as e:
            print(f"[MARKET GOLD] Skip {row.get('tick_id')}: {e}")
    return written


def refresh_market_aggregates(gold_conn):
    market_df = pd.read_sql(
        "SELECT * FROM fact_market_ticks ORDER BY symbol, event_time",
        gold_conn
    )
    if market_df.empty:
        return

    gold_conn.execute("DELETE FROM agg_market_minute_bars")
    grouped = market_df.groupby(["symbol", "event_minute"])
    for (symbol, event_minute), group in grouped:
        ordered = group.sort_values("event_time")
        gold_conn.execute("""
            INSERT OR REPLACE INTO agg_market_minute_bars VALUES
            (?,?,?,?,?,?,?,?,?)
        """, (
            symbol,
            event_minute,
            float(ordered["price"].iloc[0]),
            float(ordered["price"].max()),
            float(ordered["price"].min()),
            float(ordered["price"].iloc[-1]),
            float(ordered["volume"].sum()),
            int(len(ordered)),
            int(ordered["is_price_anomaly"].sum()),
        ))

    gold_conn.execute("DELETE FROM latest_market_prices")
    latest = market_df.sort_values("event_time").groupby("symbol").tail(1)
    for _, row in latest.iterrows():
        gold_conn.execute("""
            INSERT OR REPLACE INTO latest_market_prices VALUES
            (?,?,?,?,?,?,?,?)
        """, (
            row["symbol"],
            row["event_time"],
            float(row["price"]),
            float(row["volume"]),
            float(row["price_change_pct"]),
            int(row["is_price_anomaly"]),
            float(row["anomaly_score"]),
            row["source"],
        ))
    print("[MARKET GOLD] Market aggregates refreshed.")


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


def sync_market_quality_reports(silver_conn, gold_conn):
    if not table_exists(silver_conn, "market_quality_runs"):
        return
    quality_df = pd.read_sql(
        "SELECT * FROM market_quality_runs ORDER BY run_id DESC LIMIT 20",
        silver_conn
    )
    if quality_df.empty:
        return

    for _, row in quality_df.iterrows():
        gold_conn.execute("""
            INSERT OR REPLACE INTO market_quality_runs VALUES
            (?,?,?,?,?,?,?,?)
        """, (
            int(row["run_id"]),
            row["checked_at"],
            int(row["input_rows"]),
            int(row["duplicate_rows"]),
            int(row["invalid_price_rows"]),
            int(row["invalid_event_time_rows"]),
            int(row["valid_rows"]),
            int(row["anomaly_rows"]),
        ))


def table_exists(conn, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    ).fetchone()
    return row is not None


def run_market():
    os.makedirs(os.path.dirname(GOLD_DB), exist_ok=True)
    silver_conn = sqlite3.connect(SILVER_DB)
    gold_conn = sqlite3.connect(GOLD_DB)

    for stmt in SCHEMA.strip().split(";"):
        if stmt.strip():
            gold_conn.execute(stmt)
    gold_conn.commit()

    if not table_exists(silver_conn, "clean_market_ticks"):
        print("[MARKET GOLD] No Silver market table yet.")
        silver_conn.close()
        gold_conn.close()
        return 0

    last_loaded = get_last_market_loaded(gold_conn)
    print(f"[MARKET GOLD] Loading market ticks with transformed_at > {last_loaded}")

    df = pd.read_sql(
        "SELECT * FROM clean_market_ticks WHERE transformed_at > ?",
        silver_conn, params=(last_loaded,)
    )

    if df.empty:
        print("[MARKET GOLD] No new Silver market ticks.")
        sync_market_quality_reports(silver_conn, gold_conn)
        gold_conn.commit()
        silver_conn.close()
        gold_conn.close()
        return 0

    upsert_dim_symbol(gold_conn, df)
    written = load_market_fact(gold_conn, df)
    refresh_market_aggregates(gold_conn)
    sync_market_quality_reports(silver_conn, gold_conn)

    gold_conn.commit()
    silver_conn.close()
    gold_conn.close()
    print(f"[MARKET GOLD] ✅ Loaded {written} rows into fact_market_ticks.")
    return written


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
        sync_quality_reports(silver_conn, gold_conn)
        gold_conn.commit()
        silver_conn.close()
        gold_conn.close()
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
