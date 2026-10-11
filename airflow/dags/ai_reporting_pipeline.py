from __future__ import annotations

from datetime import timedelta

from airflow.sdk import dag, task

DAG_ID = "ai_reporting_pipeline"

DATA_DIR = "/opt/airflow/data"


@dag(
    dag_id=DAG_ID,
    description=(
        "Optional AI reporting pipeline based on an "
        "aggregated and privacy-safe payload."
    ),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "portfolio",
        "ai-summary",
        "deepseek",
        "reporting",
    ],
)
def ai_reporting_pipeline():

    @task.bash(
        task_id="create_ai_summary",
        retries=3,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=10),
    )
    def create_ai_summary(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/ai_summary/create_ai_summary.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}"
        """

    @task.bash(
        task_id="render_markdown",
        retries=0,
        execution_timeout=timedelta(minutes=5),
    )
    def render_markdown(
        logical_date: str,
    ) -> str:
        return f"""
        set -euo pipefail

        python /opt/airflow/src/ai_summary/render_summary.py \
          --logical-date "{logical_date}" \
          --data-dir "{DATA_DIR}"
        """

    # Der Core-DAG übergibt das Datum über dag_run.conf.
    # Bei einem manuellen Test ohne conf verwenden wir {{ ds }}.
    logical_date = (
        "{{ dag_run.conf.get('logical_date', ds) }}"
    )

    create_summary_task = create_ai_summary(
        logical_date=logical_date,
    )

    render_markdown_task = render_markdown(
        logical_date=logical_date,
    )

    create_summary_task >> render_markdown_task


dag = ai_reporting_pipeline()
