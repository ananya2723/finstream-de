"""
Real-time market data producer.

Streams trades from Finnhub WebSocket and publishes normalized ticks to Kafka.
Requires FINNHUB_API_KEY. Example symbols:
  BINANCE:BTCUSDT,BINANCE:ETHUSDT,AAPL,MSFT,TSLA
"""

import json
import time
from datetime import datetime

from finstream_config import settings
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import websocket

KAFKA_BROKER = settings.kafka_broker
TOPIC = settings.market_kafka_topic
SYMBOLS = [symbol.strip() for symbol in settings.market_symbols.split(",") if symbol.strip()]


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
        except NoBrokersAvailable:
            print(f"[MARKET PRODUCER] Kafka not ready at {KAFKA_BROKER}; retrying in 5s...")
            time.sleep(5)


def normalize_trade(trade: dict) -> dict:
    event_ts = datetime.utcfromtimestamp(trade["t"] / 1000).isoformat()
    symbol = trade["s"]
    return {
        "tick_id": f"{symbol}_{trade['t']}_{trade.get('p')}_{trade.get('v')}",
        "event_time": event_ts,
        "source": "finnhub",
        "symbol": symbol,
        "price": float(trade["p"]),
        "volume": float(trade.get("v", 0.0)),
        "conditions": trade.get("c", []),
        "raw_payload": trade,
    }


def main():
    if not settings.finnhub_api_key:
        raise SystemExit(
            "[MARKET PRODUCER] FINNHUB_API_KEY is required for real market data. "
            "Create a free Finnhub key and run with FINNHUB_API_KEY=..."
        )

    producer = create_producer()
    ws_url = f"wss://ws.finnhub.io?token={settings.finnhub_api_key}"

    def on_open(ws):
        for symbol in SYMBOLS:
            ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
            print(f"[MARKET PRODUCER] Subscribed: {symbol}")

    def on_message(ws, message):
        payload = json.loads(message)
        if payload.get("type") != "trade":
            return

        for trade in payload.get("data", []):
            tick = normalize_trade(trade)
            producer.send(TOPIC, key=tick["symbol"], value=tick)
            producer.flush()
            print(
                f"[MARKET PRODUCER] → {tick['symbol']} "
                f"price={tick['price']} volume={tick['volume']}"
            )

    def on_error(ws, error):
        print(f"[MARKET PRODUCER] WebSocket error: {error}")

    def on_close(ws, status_code, message):
        print(f"[MARKET PRODUCER] WebSocket closed: {status_code} {message}")

    while True:
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)
        print("[MARKET PRODUCER] Reconnecting in 5s...")
        time.sleep(5)


if __name__ == "__main__":
    main()
