#!/usr/bin/env bash
set -euo pipefail

RSS_FEED_DAG_ID="ao3_feed_to_postgres"
ENRICH_DAG_ID="ao3_collect_and_enrich"
RECOMMENDATIONS_DAG_ID="ao3_build_recommendations"

docker compose exec airflow-scheduler airflow dags unpause "$RSS_FEED_DAG_ID"
docker compose exec airflow-scheduler airflow dags unpause "$ENRICH_DAG_ID"
docker compose exec airflow-scheduler airflow dags unpause "$RECOMMENDATIONS_DAG_ID"