from __future__ import annotations

# import json
# import math
# import re
# import time
from typing import Any

import pendulum
# import requests
# from bs4 import BeautifulSoup

from airflow.sdk import dag, task, Param, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


BASE_URL = "https://archiveofourown.org"

HEADERS = {
    "User-Agent": "AO3 recommender student project - small-scale metadata parser"
}

POSTGRES_CONN_ID = "ao3_postgres"


# ---------------------------------------------------------------------
# HTTP / parsing helpers
# ---------------------------------------------------------------------

def get_html(
    url: str,
    delay: float = 2.0,
    retries: int = 3,
    backoff: float = 2.0,
) -> str:
    """
    Fetch a page slowly and retry temporary failures.
    """
    import requests
    import time

    last_error = None

    for attempt in range(1, retries + 1):
        time.sleep(delay)

        try:
            response = requests.get(
                url,
                headers=HEADERS,
                timeout=(10, 20),
            )

            if response.status_code == 200:
                print(f"Fetch successful for {url}.")
                return response.text

            last_error = Exception(
                f"Failed to fetch {url}. Status code: {response.status_code}"
            )

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            last_error = exc

        if attempt < retries:
            wait = backoff * attempt
            print(f"Fetch failed for {url}. Retrying in {wait:.1f}s...")
            time.sleep(wait)

    raise Exception(f"Failed to fetch {url} after {retries} attempts: {last_error}")


def extract_work_id(url: str) -> str | None:
    import re
    match = re.search(r"/works/(\d+)", url)
    return match.group(1) if match else None


def clean_number(value: str | None) -> int | str | None:
    if not value:
        return None

    cleaned = value.replace(",", "").strip()

    if cleaned.isdigit():
        return int(cleaned)

    return cleaned


def extract_tag_texts(soup: BeautifulSoup, dd_class: str) -> list[str]:
    section = soup.select_one(f"dd.{dd_class}")

    if not section:
        return []

    return [tag.get_text(strip=True) for tag in section.select("a.tag")]


def extract_stats_text(soup: BeautifulSoup) -> dict[str, Any]:
    stats: dict[str, Any] = {}
    stats_dl = soup.select_one("dl.stats")

    if not stats_dl:
        return stats

    current_key = None

    for element in stats_dl.find_all(["dt", "dd"], recursive=False):
        if element.name == "dt":
            current_key = (
                element.get_text(strip=True)
                .replace(":", "")
                .lower()
            )
        elif element.name == "dd" and current_key:
            stats[current_key] = clean_number(element.get_text(strip=True))
            current_key = None

    return stats


def parse_work_page(html: str, url: str) -> dict[str, Any]:
    import re
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    title_element = soup.select_one("h2.title")
    author_element = soup.select_one("h3.byline")
    language_element = soup.select_one("dd.language")
    summary_element = soup.select_one("div.summary blockquote")

    rating = extract_tag_texts(soup, "rating")
    categories = extract_tag_texts(soup, "category")
    fandoms = extract_tag_texts(soup, "fandom")
    relationships = extract_tag_texts(soup, "relationship")
    characters = extract_tag_texts(soup, "character")
    freeform_tags = extract_tag_texts(soup, "freeform")

    stats = extract_stats_text(soup)
    work_id = extract_work_id(url)

    # Add characters inferred from relationship text,
    # preserving the behavior you tested in the notebook.
    for relationship in relationships:
        split_relationship = re.split(r"[/|&]", relationship)

        for character in split_relationship:
            character = character.strip()

            if character and character not in characters:
                characters.append(character)

    return {
        "work_id": work_id,
        "url": url,
        "title": title_element.get_text(" ", strip=True) if title_element else None,
        "author": author_element.get_text(" ", strip=True) if author_element else None,
        "rating": rating,
        "categories": categories,
        "fandoms": fandoms,
        "relationships": relationships,
        "characters": characters,
        "freeform_tags": freeform_tags,
        "language": language_element.get_text(strip=True) if language_element else None,
        "summary": summary_element.get_text(" ", strip=True) if summary_element else None,
        "published": stats.get("published"),
        "words": stats.get("words"),
        "chapters": stats.get("chapters"),
        "comments": stats.get("comments"),
        "kudos": stats.get("kudos"),
        "bookmarks_count": stats.get("bookmarks"),
        "hits": stats.get("hits"),
    }


# ---------------------------------------------------------------------
# Bookmark helpers
# ---------------------------------------------------------------------

def get_total_bookmark_pages(bookmarks_count: int | str | None, per_page: int = 20) -> int:
    import math
    if not bookmarks_count:
        return 0

    try:
        bookmarks_count_int = int(bookmarks_count)
    except (TypeError, ValueError):
        return 0

    return math.ceil(bookmarks_count_int / per_page)


def get_bookmarks_page_url(work_url: str, page: int) -> str:
    work_id = extract_work_id(work_url)

    if not work_id:
        raise ValueError(f"Could not extract work ID from {work_url}")

    return f"{BASE_URL}/works/{work_id}/bookmarks?page={page}"


def parse_bookmark_users(html: str) -> list[str]:
    import re
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    users: set[str] = set()

    for link in soup.select('a[href*="/pseuds/"]'):
        href = link.get("href", "")
        match = re.match(r"^/users/([^/]+)/pseuds/([^/?#]+)", href)

        if match:
            users.add(match.group(2))

    return sorted(users)


def collect_all_bookmark_users(
    work_url: str,
    bookmarks_count: int | str | None,
    delay: float = 3.0,
) -> list[str]:
    total_pages = get_total_bookmark_pages(bookmarks_count)
    all_users: set[str] = set()

    for page in range(1, total_pages + 1):
        bookmarks_url = get_bookmarks_page_url(work_url, page)
        print(f"Fetching bookmarks page {page}/{total_pages}: {bookmarks_url}")

        try:
            bookmarks_html = get_html(bookmarks_url, delay=delay)
            page_users = parse_bookmark_users(bookmarks_html)
            all_users.update(page_users)
        except Exception as exc:
            print(f"Could not fetch bookmark page {page} for {work_url}: {exc}")

    return sorted(all_users)


def collect_work_data_and_bookmark_usernames_from_a_link(
    work_url: str,
) -> tuple[dict[str, Any] | None, list[str]]:
    """
    Returns:
    - work_data
    - bookmark_users
    - skip_reason
    """
    print(f"Fetching work: {work_url}")

    try:
        work_html = get_html(work_url)
    except Exception as exc:
        print(f"Error fetching work {work_url}: {exc}. Skipping this work.")
        return None, []

    work_data = parse_work_page(work_html, work_url)

    ratings = work_data.get("rating", [])

    if "Explicit" in ratings:
        print(f"Skipping explicit work: {work_url}")
        return work_data, []

    bookmarks_count = work_data.get("bookmarks_count", 0)

    try:
        bookmark_users = collect_all_bookmark_users(
            work_url=work_url,
            bookmarks_count=bookmarks_count,
            delay=3.0,
        )

        if not bookmark_users:
            author = work_data.get("author")
            bookmark_users = [author] if author else []

    except Exception as exc:
        print(f"Could not fetch bookmarks for {work_url}: {exc}")
        author = work_data.get("author")
        bookmark_users = [author] if author else []

    return work_data, bookmark_users


# ---------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------

def add_work_to_db(
    cursor,
    work_id: str,
    published: str | None,
    link: str,
    status: str,
    title: str | None,
    author: str | None,
) -> None:
    cursor.execute(
        """
        INSERT INTO all_works
            (id, published, link, status, title, author)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        """,
        (work_id, published, link, status, title, author),
    )
    if cursor.rowcount == 0:
        print(f"Work {work_id} already exists in the database. Skipping.")
    else:
        print(f"Successfully added work {work_id} to the database.")


def update_my_interactions_table(cursor, work_id: str) -> None:
    cursor.execute(
        """
        INSERT INTO my_bookmarks (work_id)
        VALUES (%s)
        ON CONFLICT (work_id) DO NOTHING;
        """,
        (work_id,),
    )


def update_public_interactions_table(cursor, user_id: str, work_id: str) -> None:
    cursor.execute(
        """
        INSERT INTO public_bookmarks (user_id, work_id)
        VALUES (%s, %s)
        ON CONFLICT (user_id, work_id) DO NOTHING;
        """,
        (user_id, work_id),
    )


def update_all_works_table_tags(
    cursor,
    work_id: str,
    rating: str | None,
    categories: str | None,
    fandoms: str | None,
    relationships: str | None,
    characters: str | None,
    freeform_tags: str | None,
    summary: str | None,
) -> None:
    cursor.execute(
        """
        UPDATE all_works
        SET
            rating = %s,
            categories = %s,
            fandoms = %s,
            relationships = %s,
            characters = %s,
            freeform_tags = %s,
            summary = %s
        WHERE id = %s;
        """,
        (
            rating,
            categories,
            fandoms,
            relationships,
            characters,
            freeform_tags,
            summary,
            work_id,
        ),
    )
    print(f"Work {work_id} updated with tags.")


def update_all_works_table_status(cursor, work_id: str, new_status: str) -> None:
    cursor.execute(
        """
        UPDATE all_works
        SET status = %s
        WHERE id = %s;
        """,
        (new_status, work_id),
    )
    print(f"Work {work_id} status updated to {new_status}.")

def collect_and_update_with_new_works(
    links_list: list[str],
    update_my_interactions: bool,
) -> dict[str, int]:
    import json
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    processed = 0
    scraped = 0
    fetch_failed = 0

    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        for link in links_list:
            processed += 1

            work_data, bookmark_users = (
                collect_work_data_and_bookmark_usernames_from_a_link(link)
            )

            work_id = extract_work_id(link)


            if work_data is None:
                print(f"No work data returned for {link}. Skipping.")
                continue

            work_id = work_data.get("work_id")

            if not work_id:
                print(f"Could not determine work ID for {link}. Skipping.")
                continue

            try:
                if update_my_interactions:
                    add_work_to_db(
                        cursor=cursor,
                        work_id=work_id,
                        published=work_data.get("published"),
                        link=work_data.get("url"),
                        status="New",
                        title=work_data.get("title"),
                        author=work_data.get("author"),
                    )

                    update_my_interactions_table(cursor, work_id)

                for username in bookmark_users:
                    if username:
                        update_public_interactions_table(
                            cursor,
                            username,
                            work_id,
                        )

                update_all_works_table_tags(
                    cursor=cursor,
                    work_id=work_id,
                    rating=json.dumps(work_data.get("rating", [])),
                    categories=json.dumps(work_data.get("categories", [])),
                    fandoms=json.dumps(work_data.get("fandoms", [])),
                    relationships=json.dumps(work_data.get("relationships", [])),
                    characters=json.dumps(work_data.get("characters", [])),
                    freeform_tags=json.dumps(work_data.get("freeform_tags", [])),
                    summary=work_data.get("summary"),
                )

                update_all_works_table_status(
                    cursor,
                    work_id,
                    "Scraped",
                )

                conn.commit()
                scraped += 1

                print(f"Work {work_id} scraped and updated.")

            except Exception as exc:
                conn.rollback()
                print(f"Error adding/updating work {work_id}: {exc}")

    finally:
        cursor.close()
        conn.close()

    return {
        "processed": processed,
        "scraped": scraped,
        "fetch_failed": fetch_failed,
    }


# ---------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------

@dag(
    dag_id="ao3_collect_and_enrich",
    # schedule="0 */6 * * *",
    schedule="*/7 * * * *",
    start_date=pendulum.datetime(2026, 5, 14, tz="Asia/Singapore"),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["ao3", "postgres", "scraping"],
    params={
        "source_mode": Param(
            "database",
            type="string",
            enum=["database", "manual"],
            title="Where should links come from?",
            description="Use 'database' for scheduled New works, or 'manual' to paste links below.",
        ),
        "manual_work_links": Param(
            [],
            type="array",
            title="Manual AO3 work links",
            description="Paste one AO3 work URL per line. Used only when source_mode = manual.",
        ),
        "database_batch_limit": Param(
            50,
            type="integer",
            minimum=1,
            maximum=500,
            title="Maximum database links per scheduled run",
        ),
    },
)
def ao3_collect_and_enrich():

    @task
    def resolve_links() -> dict[str, Any]:
        context = get_current_context()
        params = context["params"]

        source_mode = params["source_mode"]
        manual_work_links = params["manual_work_links"] or []
        database_batch_limit = params["database_batch_limit"]

        if source_mode == "manual":
            cleaned_links = [
                link.strip()
                for link in manual_work_links
                if isinstance(link, str) and link.strip()
            ]

            print(f"Manual mode selected. Received {len(cleaned_links)} links.")

            return {
                "links": cleaned_links,
                "update_my_interactions": True,
                "source_mode": "manual",
            }

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            """
            SELECT link
            FROM all_works
            WHERE status = 'New'
            ORDER BY published ASC NULLS LAST
            LIMIT %s;
            """,
            parameters=(database_batch_limit,),
        )

        database_links = [row[0] for row in rows]

        print(f"Database mode selected. Found {len(database_links)} New works.")

        return {
            "links": database_links,
            "update_my_interactions": False,
            "source_mode": "database",
        }

    @task
    def scrape_and_update(payload: dict[str, Any]) -> dict[str, int]:
        links = payload["links"]
        update_my_interactions = payload["update_my_interactions"]

        if not links:
            print("No links to process.")
            return {
                "processed": 0,
                "scraped": 0,
                "fetch_failed": 0,
            }

        return collect_and_update_with_new_works(
            links_list=links,
            update_my_interactions=update_my_interactions,
        )

    scrape_and_update(resolve_links())


ao3_collect_and_enrich()