#!/usr/bin/env bash
set -euo pipefail

DAG_ID="ao3_build_recommendations"

MODE="${1:-recommend}"
TOP_N="${2:-20}"

case "$MODE" in
  update-and-recommend)
    PATH_PARAM="update and recommend"
    ;;
  recommend)
    PATH_PARAM="recommend"
    ;;
  *)
    echo "Usage: bash $0 [update-and-recommend|recommend] [top_n]"
    echo "  update-and-recommend  rebuilds recommendations"
    echo "  recommend  uses existing data from the previous run to build recommendations"
    exit 1
    ;;
esac

docker compose exec airflow-scheduler airflow dags trigger "$DAG_ID" --conf "{\"path\": \"${PATH_PARAM}\", \"top_n\": ${TOP_N}}"