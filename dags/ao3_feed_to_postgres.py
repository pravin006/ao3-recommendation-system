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

        if not feed_urls:
            print("No RSS feed URLs found. Exiting.")
            return

        # feed = feedparser.parse(FEED_URL)

        hook = PostgresHook(postgres_conn_id="ao3_postgres")

        inserted = 0
        skipped = 0

        sql = """
        INSERT INTO all_works
            (id, published, link, status, title, author)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """

        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
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

                    published = entry.get("published")
                    title = entry.get("title")
                    author = entry.get("author")
                    status = "New"

                    cursor.execute(
                        sql,
                        (
                            work_id,
                            published,
                            link,
                            status,
                            title,
                            author,
                        ),
                    )

                    if cursor.rowcount == 0:
                        print(f"Work {work_id} already exists. Skipping.")
                        skipped += 1
                    else:
                        print(f"Inserted work {work_id}.")
                        inserted += 1

                conn.commit()

        except Exception:
            conn.rollback()
            raise

        finally:
            cursor.close()
            conn.close()

        print(f"Inserted: {inserted}")
        print(f"Skipped: {skipped}")

    scrape_and_insert(get_rss_links())


ao3_feed_pipeline()