"""Data-quality reporting helpers for the Bronze to Silver transform."""

from __future__ import annotations

import sqlite3
from datetime import datetime

import pandas as pd


def init_quality_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_runs (
            run_id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            checked_at             TEXT NOT NULL,
            source_layer           TEXT NOT NULL,
            target_layer           TEXT NOT NULL,
            input_rows             INTEGER NOT NULL,
            duplicate_rows         INTEGER NOT NULL,
            invalid_amount_rows    INTEGER NOT NULL,
            invalid_event_time_rows INTEGER NOT NULL,
            valid_rows             INTEGER NOT NULL,
            anomaly_rows           INTEGER NOT NULL
        )
    """)
    conn.commit()


def build_quality_report(raw_df: pd.DataFrame, clean_df: pd.DataFrame) -> dict:
    if raw_df.empty:
        return {
            "input_rows": 0,
            "duplicate_rows": 0,
            "invalid_amount_rows": 0,
            "invalid_event_time_rows": 0,
            "valid_rows": 0,
            "anomaly_rows": 0,
        }

    amounts = pd.to_numeric(raw_df["amount"], errors="coerce")
    event_times = pd.to_datetime(raw_df["event_time"], errors="coerce")

    return {
        "input_rows": int(len(raw_df)),
        "duplicate_rows": int(raw_df.duplicated(subset="transaction_id").sum()),
        "invalid_amount_rows": int((amounts.isna() | (amounts <= 0)).sum()),
        "invalid_event_time_rows": int(event_times.isna().sum()),
        "valid_rows": int(len(clean_df)),
        "anomaly_rows": int(clean_df["is_anomaly"].sum()) if "is_anomaly" in clean_df else 0,
    }


def record_quality_report(conn: sqlite3.Connection, report: dict) -> None:
    conn.execute("""
        INSERT INTO data_quality_runs
        (checked_at, source_layer, target_layer, input_rows, duplicate_rows,
         invalid_amount_rows, invalid_event_time_rows, valid_rows, anomaly_rows)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        "bronze",
        "silver",
        report["input_rows"],
        report["duplicate_rows"],
        report["invalid_amount_rows"],
        report["invalid_event_time_rows"],
        report["valid_rows"],
        report["anomaly_rows"],
    ))
    conn.commit()
