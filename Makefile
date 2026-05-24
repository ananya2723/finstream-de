.PHONY: build up down logs transform test ci clean

build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

transform:
	docker compose run --rm dashboard python pipeline_runner.py --once

test:
	python -m pytest -q

ci:
	python -m py_compile transaction_producer.py bronze_consumer.py bronze_to_silver.py silver_to_gold.py dashboard.py finstream_dag.py pipeline_runner.py
	python -m pytest -q

clean:
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
