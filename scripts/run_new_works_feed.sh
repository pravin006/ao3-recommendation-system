#!/usr/bin/env bash
set -euo pipefail

DAG_ID="ao3_feed_to_postgres"

docker compose exec airflow-scheduler airflow dags trigger "$DAG_ID" 