build image and start
docker compose build --no-cache

docker compose up airflow-init

docker compose up -d



Check db
docker compose exec ao3-postgres psql -U ao3_user -d ao3_db

\dt

SELECT * FROM all_works ORDER BY published DESC LIMIT 10;