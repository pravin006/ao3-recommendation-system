import json
import os
import signal
# import sys
import time

import psycopg2
from confluent_kafka import Consumer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_AO3_NEW_WORKS_TOPIC",
    "ao3.new_works",
)

POSTGRES_DSN = os.getenv(
    "AO3_POSTGRES_DSN",
    "postgresql://ao3_user:ao3_password@ao3-postgres:5432/ao3_db",
)

def wait_for_kafka():
    from confluent_kafka.admin import AdminClient

    while True:
        try:
            admin = AdminClient(
                {
                    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                    "socket.timeout.ms": 3000,
                }
            )

            metadata = admin.list_topics(timeout=5)

            if KAFKA_TOPIC in metadata.topics:
                print(f"Kafka is ready. Found topic: {KAFKA_TOPIC}")
                return

            print(f"Kafka is reachable, but topic {KAFKA_TOPIC} does not exist yet.")

        except Exception as exc:
            print(f"Kafka not ready yet: {exc}")

        time.sleep(3)

running = True


def handle_shutdown(signum, frame):
    global running
    print("Shutdown requested.")
    running = False


signal.signal(signal.SIGTERM, handle_shutdown)
signal.signal(signal.SIGINT, handle_shutdown)


def get_postgres_connection():
    while True:
        try:
            conn = psycopg2.connect(POSTGRES_DSN)
            conn.autocommit = False
            print("Connected to Postgres.")
            return conn
        except Exception as exc:
            print(f"Postgres not ready yet: {exc}")
            time.sleep(3)


def insert_work(conn, event: dict) -> bool:
    sql = """
    INSERT INTO all_works
        (id, published, link, status, title, author)
    VALUES
        (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    """

    work_id = event["work_id"]

    with conn.cursor() as cursor:
        cursor.execute(
            sql,
            (
                work_id,
                event.get("published"),
                event.get("link"),
                event.get("status", "New"),
                event.get("title"),
                event.get("author"),
            ),
        )

        inserted = cursor.rowcount == 1

    conn.commit()

    if inserted:
        print(f"Inserted work {work_id}.")
    else:
        print(f"Work {work_id} already exists. Skipping.")

    return inserted


def main():
    wait_for_kafka()

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ao3-postgres-writer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    conn = get_postgres_connection()

    consumer.subscribe([KAFKA_TOPIC])
    print(f"Listening to Kafka topic: {KAFKA_TOPIC}")

    try:
        while running:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))

                if "work_id" not in event:
                    print(f"Bad message, missing work_id: {event}")
                    consumer.commit(message=msg, asynchronous=False)
                    continue

                insert_work(conn, event)

                # Commit Kafka offset only after Postgres insert/skip succeeds.
                consumer.commit(message=msg, asynchronous=False)

            except psycopg2.Error as exc:
                conn.rollback()
                print(f"Postgres error. Message will be retried: {exc}")

                try:
                    conn.close()
                except Exception:
                    pass

                conn = get_postgres_connection()

            except Exception as exc:
                print(f"Bad message or processing error: {exc}")
                print("Committing offset to avoid being stuck on this message.")
                consumer.commit(message=msg, asynchronous=False)

    finally:
        print("Closing consumer and Postgres connection.")
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()