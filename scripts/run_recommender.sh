#!/usr/bin/env bash
set -euo pipefail

DAG_ID="ao3_build_recommendations"

MODE="${1:-recommend}"
TOP_N="${2:-20}"

case "$MODE" in
  update)
    PATH_PARAM="update and recommend"
    ;;
  recommend)
    PATH_PARAM="recommend"
    ;;
  *)
    echo "Usage: bash $0 [update|recommend] [top_n]"
    echo "  update     rebuilds parquet files, then recommends"
    echo "  recommend  uses existing parquet files only"
    exit 1
    ;;
esac

docker compose exec airflow-scheduler airflow dags unpause ao3_build_recommendations
docker compose exec airflow-scheduler airflow dags trigger "$DAG_ID" --conf "{\"path\": \"${PATH_PARAM}\", \"top_n\": ${TOP_N}}"