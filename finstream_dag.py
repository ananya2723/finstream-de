"""
Airflow DAG: finstream_pipeline
Runs every 2 minutes:
  1. bronze_to_silver  — clean + anomaly score new Bronze rows
  2. silver_to_gold    — load into star schema + refresh aggregates

Place this file in your $AIRFLOW_HOME/dags/ folder.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys, os

# Make transforms importable
sys.path.insert(0, os.path.dirname(__file__))
import bronze_to_silver
import silver_to_gold

default_args = {
    "owner": "ananya",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "email_on_failure": False,
}

with DAG(
    dag_id="finstream_pipeline",
    description="Real-time financial analytics — Bronze→Silver→Gold medallion pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/2 * * * *",   # every 2 minutes
    catchup=False,
    tags=["data-engineering", "finstream", "medallion"],
) as dag:

    task_bronze_to_silver = PythonOperator(
        task_id="bronze_to_silver",
        python_callable=bronze_to_silver.run,
        doc_md="""
        **Bronze → Silver**
        - Deduplicates on transaction_id
        - Casts and validates types
        - Computes rolling z-score anomaly score per category
        - Writes cleaned rows to silver.db
        """
    )

    task_silver_to_gold = PythonOperator(
        task_id="silver_to_gold",
        python_callable=silver_to_gold.run,
        doc_md="""
        **Silver → Gold**
        - Upserts dim_account, dim_merchant, dim_date
        - Loads fact_transactions (star schema)
        - Refreshes agg_daily_category and agg_hourly_volume
        """
    )

    # Pipeline dependency
    task_bronze_to_silver >> task_silver_to_gold