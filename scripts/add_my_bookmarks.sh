#!/usr/bin/env bash
set -euo pipefail

DAG_ID="ao3_collect_and_enrich"

MODE="manual"


if [ "$#" -lt 1 ]; then
    echo "Usage: bash $0 link1 link2 link3 ..."
    exit 1
fi

first=true
    
links="["

for link in "$@"; do
    if [ "$first" = true ]; then
        links+="\"$link\""
        first=false
    else
        links+=",\"$link\""
    fi
done

links+="]"

docker compose exec airflow-scheduler airflow dags trigger "$DAG_ID" --conf "{\"source_mode\": \"${MODE}\", \"manual_work_links\": ${links}}"