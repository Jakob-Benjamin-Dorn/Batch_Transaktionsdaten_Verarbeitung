from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import dag, task


DAG_ID = "batch_transaction_pipeline"

DATA_DIR = "/opt/airflow/data"

CONTRACT_PATH = (
    "/opt/airflow/knowledge_base/"
    "data_contract.json"
)


@dag(
    dag_id=DAG_ID,
    description=(
        "Daily batch pipeline from synthetic Bronze data "
        "to validated Gold metrics, a PostgreSQL serving layer "
        "and an AI-ready payload."
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
    tags=[
        "portfolio",
        "spark",
        "data-quality",
        "batch",
    ],
)
def batch_transaction_pipeline():

    @task.bash(
        task_id="generate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=10),
    )
    def generate_bronze(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/generator/generate_transactions.py \
          --logical-date "{logical_date}" \
          --output-dir "{DATA_DIR}"
        """

    @task.bash(
        task_id="transform_to_silver",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=20),
    )
    def transform_to_silver(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/spark/transform_transactions.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}"
        """

    @task.bash(
        task_id="evaluate_quality_gate",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )
    def evaluate_quality_gate(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/spark/evaluate_quality_gate.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}" \
          --contract-path "{CONTRACT_PATH}"
        """

    @task.bash(
        task_id="build_gold",
        retries=1,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=15),
    )
    def build_gold(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/spark/build_gold.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}"
        """

    @task.bash(
        task_id="load_to_postgres",
        retries=1,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=5),
    )
    def load_to_postgres(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/analytics/load_to_postgres.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}"
        """

    @task.bash(
        task_id="prepare_ai_payload",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )
    def prepare_ai_payload(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/ai_summary/prepare_payload.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}" \
          --contract-path "{CONTRACT_PATH}"
        """

    logical_date = "{{ ds }}"

    bronze_task = generate_bronze(
        logical_date=logical_date,
    )

    silver_task = transform_to_silver(
        logical_date=logical_date,
    )

    quality_gate_task = evaluate_quality_gate(
        logical_date=logical_date,
    )

    gold_task = build_gold(
        logical_date=logical_date,
    )

    load_task = load_to_postgres(
        logical_date=logical_date,
    )

    payload_task = prepare_ai_payload(
        logical_date=logical_date,
    )

    trigger_ai_report = TriggerDagRunOperator(
        task_id="trigger_ai_reporting_pipeline",
        trigger_dag_id="ai_reporting_pipeline",
        trigger_run_id=(
            "ai_report__{{ run_id }}"
        ),
        conf={
            "logical_date": "{{ ds }}",
            "source_dag_id": "{{ dag.dag_id }}",
            "source_run_id": "{{ run_id }}",
        },
        wait_for_completion=False,
        reset_dag_run=False,
        retries=0,
    )

    (
        bronze_task
        >> silver_task
        >> quality_gate_task
        >> gold_task
        >> load_task
        >> payload_task
        >> trigger_ai_report
    )


dag = batch_transaction_pipeline()