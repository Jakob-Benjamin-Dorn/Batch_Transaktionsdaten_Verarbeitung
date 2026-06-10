from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.sdk import dag, task


DATA_DIR = "/opt/airflow/data"
CONTRACT_PATH = (
    "/opt/airflow/knowledge_base/data_contract.json"
)


@dag(
    dag_id="batch_transaction_pipeline",
    description=(
        "Daily transaction pipeline from Bronze "
        "to validated AI report."
    ),
    schedule="@daily",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "jakob",
    },
    tags=[
        "portfolio",
        "spark",
        "data-quality",
        "ai-summary",
    ],
)
def batch_transaction_pipeline():

    @task.bash(
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def generate_bronze(logical_date: str) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/generator/generate_transactions.py \
        --logical-date "{logical_date}" \
        --output-dir "{DATA_DIR}"
        """

    @task.bash(
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
    )
    def transform_to_silver() -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/spark/transform_transactions.py \
          --logical-date "$AIRFLOW_CTX_LOGICAL_DATE" \
          --data-dir "{DATA_DIR}"
        """