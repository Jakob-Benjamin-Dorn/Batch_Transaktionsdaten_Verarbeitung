from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

DAG_DIR = Path("/opt/airflow/dags")


def load_dag_module(filename: str):
    path = DAG_DIR / filename

    spec = importlib.util.spec_from_file_location(
        path.stem,
        path,
    )

    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load DAG module: {path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


class TestDagStructure(unittest.TestCase):
    def test_core_dag_structure(self):
        module = load_dag_module("batch_transaction_pipeline.py")

        dag = module.dag

        self.assertEqual(
            dag.dag_id,
            "batch_transaction_pipeline",
        )

        expected_tasks = {
            "generate_bronze",
            "transform_to_silver",
            "evaluate_quality_gate",
            "build_gold",
            "load_to_postgres",
            "prepare_ai_payload",
            "trigger_ai_reporting_pipeline",
        }

        self.assertEqual(
            set(dag.task_ids),
            expected_tasks,
        )

        self.assertFalse(dag.catchup)
        self.assertEqual(dag.max_active_runs, 1)

        self.assertEqual(
            dag.get_task("evaluate_quality_gate").retries,
            0,
        )

        self.assertEqual(
            dag.get_task("load_to_postgres").retries,
            1,
        )

        self.assertEqual(
            dag.get_task("prepare_ai_payload").retries,
            0,
        )

    def test_ai_dag_structure(self):
        module = load_dag_module("ai_reporting_pipeline.py")

        dag = module.dag

        self.assertEqual(
            dag.dag_id,
            "ai_reporting_pipeline",
        )

        self.assertEqual(
            set(dag.task_ids),
            {
                "create_ai_summary",
                "render_markdown",
            },
        )

        self.assertIsNone(dag.schedule)
        self.assertFalse(dag.catchup)

        self.assertEqual(
            dag.get_task("create_ai_summary").retries,
            3,
        )

        self.assertEqual(
            dag.get_task("render_markdown").retries,
            0,
        )

    def test_core_dependencies(self):
        module = load_dag_module("batch_transaction_pipeline.py")

        dag = module.dag

        expected_chain = [
            (
                "generate_bronze",
                "transform_to_silver",
            ),
            (
                "transform_to_silver",
                "evaluate_quality_gate",
            ),
            (
                "evaluate_quality_gate",
                "build_gold",
            ),
            (
                "build_gold",
                "load_to_postgres",
            ),
            (
                "load_to_postgres",
                "prepare_ai_payload",
            ),
            (
                "prepare_ai_payload",
                "trigger_ai_reporting_pipeline",
            ),
        ]

        for upstream_id, downstream_id in expected_chain:
            upstream = dag.get_task(upstream_id)

            self.assertIn(
                downstream_id,
                upstream.downstream_task_ids,
            )

    def test_ai_dependencies(self):
        module = load_dag_module("ai_reporting_pipeline.py")

        dag = module.dag

        create_summary = dag.get_task("create_ai_summary")

        self.assertIn(
            "render_markdown",
            create_summary.downstream_task_ids,
        )


if __name__ == "__main__":
    unittest.main()
