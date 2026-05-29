# Archive of Our Own Recommender

A local recommendation pipeline for Archive of Our Own (AO3) works. The project collects AO3 works from tag RSS feeds, streaming it using Kafka, scrapes each work for tags and users' bookmark, stores them in PostgreSQL, and runs recommender jobs through Apache Airflow.

## Recommender background

This project was first prototyped as a Goodreads recommender. The notebook version loaded Goodreads book metadata and interaction data, filtered books by rating count and average rating, cleaned titles, created TF-IDF vectors for title search, built a sparse user-item matrix, and found similar users/items with cosine nearest neighbors and used and holdout testing for the evaluation.

The original prototype included:

- **Content lookup:** TF-IDF search over cleaned book titles.
- **Collaborative filtering:** sparse user-item matrices built from ratings.
- **User-based recommendations:** find similar users, collect works/books they liked, then rank unseen items.
- **Item-based recommendations:** find items similar to the user’s liked items.
- **Evaluation:** hold out part of a user’s known interactions and measure Precision and Recall.

The item-based collaborative filtering performed the best: Precision@100 was 0.1300 and Recall@100 was 0.4183. This means that, on average, 13 of the top 100 recommendations matched the user’s held-out books, and the model recovered about 41.83% of the user’s hidden liked/rated books.

For AO3, the same general recommendation idea is adapted away from Goodreads-specific fields like `average_rating`, and `ratings_count` towards users bookmarks, supplemented with tags like fandoms, relationships and characters.

## What this project does

- Uses Airflow DAGs and helper scripts to run ingestion and recommendation workflows.
- Ingests AO3 tag RSS feeds through Kafka, consuming newly posted works.
- Adds your own AO3 bookmarked works as the preference seed for recommendations.
- Gathers bookmarks made by public users on new works after 7 days and the user's bookmarked works for the collaborative filter.
- Builds recommendations based on collaborative and tag based scores.

## Requirements

- Docker Desktop or Docker Engine with Docker Compose
- Git
- Bash-compatible shell
- At least 4 GB RAM available to Docker; more is better for Airflow

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/pravin006/ao3-recommendation-system
cd ao3-recommendation-system
```

### 2. Create a local `.env` file

Create `.env` in the project root. An example is given in .env.example

```env
FERNET_KEY=replace_with_generated_key
AIRFLOW_UID=50000
AIRFLOW__API_AUTH__JWT_SECRET=your_jwt_secret_here
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```

To create and insert a `FERNET_KEY` and `AIRFLOW__API_AUTH__JWT_SECRET` into your `.env` you can use this command:

```bash
docker run --rm apache/airflow:3.2.1 python -c "import secrets; from cryptography.fernet import Fernet; print(f'FERNET_KEY={Fernet.generate_key().decode()}\nAIRFLOW__API_AUTH__JWT_SECRET={secrets.token_hex(32)}')" >> .env
```

`AIRFLOW_UID` can be set as `50000` or, on Linux, you may prefer to set it to your user ID:

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Build and start the containers

```bash
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

Airflow should be available at:

```text
http://localhost:8080
```

Default local credentials, unless changed in `.env`:

```text
username: airflow
password: airflow
```

### 4. Create the Kafka topic

Kafka topic auto-creation is disabled, so create the AO3 topic manually:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ao3.new_works --partitions 1 --replication-factor 1
```

Check that the topic exists:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

### 5. Start the DAGs

```bash
bash scripts/start_dags.sh
```

## Usage

### Add AO3 RSS feed links

AO3 tag feeds can be found from tag pages. Feed URLs usually look like this:

```text
https://archiveofourown.org/tags/<tag-id-or-tag-name>/feed.atom
```

Add one or more feed links:

```bash
bash scripts/add_rss_links.sh "https://archiveofourown.org/tags/2007008/feed.atom" \
  "https://archiveofourown.org/tags/124953688/feed.atom"
```

### Add your own bookmarked works

Use AO3 work links or bookmark links as your personal preference seed:

```bash
bash scripts/add_my_bookmarks.sh "https://archiveofourown.org/works/WORK_ID" \
  "https://archiveofourown.org/works/ANOTHER_WORK_ID"
```

### Retrieve new works from feeds

This process runs automatically every 6 hours. You can manually run it if you want by:

```bash
bash scripts/run_new_works_feed.sh
```

### Run recommendations

Updates to the recommendations are run automatically every Sunday at 00:00.You can run the full update-and-recommend flow manually by:

```bash
bash scripts/run_recommender.sh update-and-recommend
```

You can view the latest recommendations built in the Airflow UI:

```bash
bash scripts/run_recommender.sh recommend
```

Or triggering it directly:

### Database access

Open a `psql` shell in the AO3 application database:

```bash
docker compose exec ao3-postgres psql -U ao3_user -d ao3_db
```

Run SQL commands inside `psql` such as:

```sql
\dt

SELECT * FROM all_works ORDER BY published DESC LIMIT 10;

SELECT * FROM public_bookmarks LIMIT 10;

SELECT * FROM my_rss_links;
```

