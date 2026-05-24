from datetime import datetime

from transaction_producer import CATEGORIES, generate_transaction


def test_generate_transaction_contract():
    txn = generate_transaction()

    required_fields = {
        "transaction_id",
        "event_time",
        "account_id",
        "merchant",
        "category",
        "amount",
        "currency",
        "payment_mode",
        "status",
        "city",
    }

    assert required_fields.issubset(txn)
    assert txn["transaction_id"].startswith("TXN")
    assert datetime.fromisoformat(txn["event_time"])
    assert txn["category"] == CATEGORIES[txn["merchant"]]
    assert txn["amount"] > 0
    assert txn["currency"] == "INR"
    assert txn["status"] in {"SUCCESS", "FAILED"}
