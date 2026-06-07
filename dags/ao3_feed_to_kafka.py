# import re
# import feedparser
import pendulum

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


# FEED_URL = "https://archiveofourown.org/tags/2007008/feed.atom"


def extract_work_id(url: str) -> str | None:
    """
    Returns a work ID string from URLs like:
    https://archiveofourown.org/works/33023536
    """
    import re
    match = re.search(r"/works/(\d+)", url)

    if match:
        return match.group(1)

    return None

def wait_for_kafka(bootstrap_servers: str, topic: str) -> None:
    import time
    from confluent_kafka.admin import AdminClient

    while True:
        try:
            admin = AdminClient(
                {
                    "bootstrap.servers": bootstrap_servers,
                    "socket.timeout.ms": 3000,
                }
            )

            metadata = admin.list_topics(timeout=5)

            if topic in metadata.topics:
                print(f"Kafka is ready. Found topic: {topic}")
                return

            print(f"Kafka is reachable, but topic {topic} does not exist yet.")

        except Exception as exc:
            print(f"Kafka not ready yet: {exc}")

        time.sleep(3)

def delivery_report(err, msg):
    if err is not None:
        print(f"Kafka delivery failed: {err}")
    else:
        print(
            f"Kafka message delivered to "
            f"{msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )

@dag(
    dag_id="ao3_feed_to_postgres",
    schedule="0 */6 * * *",
    # schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 5, 14, tz="Asia/Singapore"),
    catchup=False,
    tags=["ao3", "rss", "postgres"],
)
def ao3_feed_pipeline():

    @task
    def get_rss_links() -> list[str]:
        hook = PostgresHook(postgres_conn_id="ao3_postgres")

        feed_rows = hook.get_records(
            """
            SELECT link
            FROM my_rss_links
            ORDER BY link;
            """
        )

        feed_urls = [row[0] for row in feed_rows]
        return feed_urls

    @task
    def scrape_and_insert(feed_urls: list[str]):
        import feedparser
        import os
        import json
        from confluent_kafka import Producer

        if not feed_urls:
            print("No RSS feed URLs found. Exiting.")
            return

        # feed = feedparser.parse(FEED_URL)

        # hook = PostgresHook(postgres_conn_id="ao3_postgres")

        sent = 0
        skipped = 0

        # sql = """
        # INSERT INTO all_works
        #     (id, published, link, status, title, author)
        # VALUES
        #     (%s, %s, %s, %s, %s, %s)
        # ON CONFLICT (id) DO NOTHING;
        # """

        # conn = hook.get_conn()
        # cursor = conn.cursor()

        bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "kafka:9092",
        )

        topic = os.getenv(
            "KAFKA_AO3_NEW_WORKS_TOPIC",
            "ao3.new_works",
        )

        wait_for_kafka(bootstrap_servers, topic)

        producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "client.id": "ao3-feed-producer",
            }
        )



        # try:
        for feed_url in feed_urls:
            print(f"Reading RSS feed: {feed_url}")
            feed = feedparser.parse(feed_url)

            for entry in feed.entries:
                link = entry.get("link")
                work_id = extract_work_id(link)

                if not work_id:
                    print(f"Could not extract work ID for entry: {link}")
                    skipped += 1
                    continue
                
                event = {
                    "work_id": work_id,
                    "published": entry.get("published"),
                    "link": link,
                    "status": "New",
                    "title": entry.get("title"),
                    "author": entry.get("author"),
                    "source_feed_url": feed_url,
                }

                producer.produce(
                    topic=topic,
                    key=work_id,
                    value=json.dumps(event).encode("utf-8"),
                    callback=delivery_report,
                )

                producer.poll(0)
                sent += 1
                print(f"Sent work {work_id} to Kafka.")
        
        producer.flush()
        print(f"Sent: {sent}")
        print(f"Skipped: {skipped}")

                # published = entry.get("published")
                # title = entry.get("title")
                # author = entry.get("author")
                # status = "New"

                    # cursor.execute(
                    #     sql,
                    #     (
                    #         work_id,
                    #         published,
                    #         link,
                    #         status,
                    #         title,
                    #         author,
                    #     ),
                    # )

                    # if cursor.rowcount == 0:
                    #     print(f"Work {work_id} already exists. Skipping.")
                    #     skipped += 1
                    # else:
                    #     print(f"Inserted work {work_id}.")
                    #     inserted += 1

        #         conn.commit()

        # except Exception:
        #     conn.rollback()
        #     raise

        # finally:
        #     cursor.close()
        #     conn.close()

        # print(f"Sent: {sent}")
        # print(f"Skipped: {skipped}")

    scrape_and_insert(get_rss_links())


ao3_feed_pipeline()