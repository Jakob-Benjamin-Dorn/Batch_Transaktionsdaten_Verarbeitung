#!/usr/bin/env bash

set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$project_root"

mkdir -p \
  airflow/dags \
  src/generator \
  src/spark \
  src/ai_summary/templates \
  src/common \
  knowledge_base \
  tests/unit \
  tests/integration \
  examples

echo "Project directories initialized."
echo "Docker-managed volumes will be created by Docker Compose."
