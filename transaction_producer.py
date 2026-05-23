"""
Kafka Producer — simulates live financial transactions.
Publishes to topic: raw_transactions
"""

import json
import os
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "raw_transactions"

MERCHANTS = [
    "Amazon", "Flipkart", "Swiggy", "Zomato", "Netflix",
    "Uber", "Ola", "BigBasket", "Myntra", "PhonePe",
    "PayTM", "HDFC ATM", "SBI ATM", "Airtel", "Jio"
]

CATEGORIES = {
    "Amazon": "E-Commerce", "Flipkart": "E-Commerce", "Myntra": "E-Commerce",
    "Swiggy": "Food & Dining", "Zomato": "Food & Dining",
    "Netflix": "Entertainment",
    "Uber": "Transport", "Ola": "Transport",
    "BigBasket": "Groceries",
    "PhonePe": "UPI Transfer", "PayTM": "UPI Transfer",
    "HDFC ATM": "ATM Withdrawal", "SBI ATM": "ATM Withdrawal",
    "Airtel": "Utilities", "Jio": "Utilities"
}

ACCOUNT_IDS = [f"ACC{str(i).zfill(4)}" for i in range(1, 21)]
PAYMENT_MODES = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Wallet"]

def generate_transaction():
    merchant = random.choice(MERCHANTS)
    amount = round(random.uniform(50, 15000), 2)
    # ~8% anomaly injection
    if random.random() < 0.08:
        amount = round(random.uniform(50000, 200000), 2)
    return {
        "transaction_id": f"TXN{int(time.time()*1000)}_{random.randint(100,999)}",
        "event_time": datetime.utcnow().isoformat(),
        "account_id": random.choice(ACCOUNT_IDS),
        "merchant": merchant,
        "category": CATEGORIES[merchant],
        "amount": amount,
        "currency": "INR",
        "payment_mode": random.choice(PAYMENT_MODES),
        "status": "SUCCESS" if random.random() > 0.03 else "FAILED",
        "city": random.choice(["Bengaluru", "Mumbai", "Delhi", "Hyderabad", "Chennai", "Pune"]),
    }

def main():
    producer = create_producer()
    print(f"[PRODUCER] Connected to Kafka. Publishing to '{TOPIC}'...")
    while True:
        txn = generate_transaction()
        producer.send(TOPIC, key=txn["account_id"], value=txn)
        print(f"[PRODUCER] → {txn['transaction_id']} | {txn['merchant']} | ₹{txn['amount']}")
        time.sleep(random.uniform(0.5, 1.5))


def create_producer():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
        except NoBrokersAvailable:
            print(f"[PRODUCER] Kafka not ready at {KAFKA_BROKER}; retrying in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    main()
