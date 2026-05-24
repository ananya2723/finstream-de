"""Shared runtime configuration for FinStream services."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_broker: str = os.getenv("KAFKA_BROKER", "localhost:9092")
    kafka_topic: str = os.getenv("KAFKA_TOPIC", "raw_transactions")
    market_kafka_topic: str = os.getenv("MARKET_KAFKA_TOPIC", "raw_market_ticks")
    kafka_group_id: str = os.getenv("KAFKA_GROUP_ID", "bronze-ingestion-group")
    market_kafka_group_id: str = os.getenv("MARKET_KAFKA_GROUP_ID", "market-bronze-ingestion-group")
    bronze_db: str = os.getenv("BRONZE_DB", "data/bronze.db")
    silver_db: str = os.getenv("SILVER_DB", "data/silver.db")
    gold_db: str = os.getenv("GOLD_DB", "data/gold.db")
    transform_interval_seconds: int = int(os.getenv("TRANSFORM_INTERVAL_SECONDS", "120"))
    finnhub_api_key: str = os.getenv("FINNHUB_API_KEY", "")
    market_symbols: str = os.getenv("MARKET_SYMBOLS", "BINANCE:BTCUSDT,BINANCE:ETHUSDT,AAPL,MSFT,TSLA")


settings = Settings()
