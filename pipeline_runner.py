"""Run FinStream batch transforms once or continuously."""

import argparse
import time

import bronze_to_silver
from finstream_config import settings
import silver_to_gold


def run_once() -> tuple[int, int]:
    silver_rows = bronze_to_silver.run()
    market_silver_rows = bronze_to_silver.run_market()
    gold_rows = silver_to_gold.run()
    market_gold_rows = silver_to_gold.run_market()
    return silver_rows + market_silver_rows, gold_rows + market_gold_rows


def run_forever(interval_seconds: int) -> None:
    while True:
        silver_rows, gold_rows = run_once()
        print(
            "[PIPELINE] completed run "
            f"silver_rows={silver_rows} gold_rows={gold_rows} "
            f"next_run_in={interval_seconds}s"
        )
        time.sleep(interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FinStream transforms.")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run Bronze to Silver and Silver to Gold once, then exit.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=settings.transform_interval_seconds,
        help="Seconds between continuous transform runs.",
    )
    args = parser.parse_args()

    if args.once:
        run_once()
    else:
        run_forever(args.interval_seconds)


if __name__ == "__main__":
    main()
