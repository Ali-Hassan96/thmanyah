import psycopg2.extras
psycopg2.extras.register_uuid()
import psycopg2
import time
import uuid
import random
from datetime import datetime, timezone

DB_CONFIG = {
    "host": "localhost",      # use "postgres" if running inside Docker
    "port": 5436,
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword"
}

CONTENT_TYPES = ["podcast", "newsletter", "video"]
EVENT_TYPES = ["play", "pause", "finish", "click"]
DEVICES = ["ios", "android", "web-chrome", "web-safari"]

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def seed_content(cur):
    content_id = uuid.uuid4()
    slug = f"auto-content-{content_id.hex[:8]}"

    cur.execute(
        """
        INSERT INTO content (
            id, slug, title, content_type, length_seconds, publish_ts
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (slug) DO NOTHING
        """,
        (
            content_id,
            slug,
            f"Auto Generated Content {slug[-4:]}",
            random.choice(CONTENT_TYPES),
            random.randint(300, 3600),
            datetime.now(timezone.utc)
        )
    )

    return content_id

def seed_engagement(cur, content_id):
    cur.execute(
        """
        INSERT INTO engagement_events (
            content_id,
            user_id,
            event_type,
            event_ts,
            duration_ms,
            device,
            raw_payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            content_id,
            uuid.uuid4(),
            random.choice(EVENT_TYPES),
            datetime.now(timezone.utc),
            random.choice([None, random.randint(1000, 1_000_000)]),
            random.choice(DEVICES),
            '{"source":"python-seeder"}'
        )
    )

def run():
    print("🚀 Python seeder started (every 30 seconds)")
    while True:
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    content_id = seed_content(cur)
                    seed_engagement(cur, content_id)
                conn.commit()

            print(f"✅ Seeded content + engagement at {datetime.utcnow().isoformat()}")

        except Exception as e:
            print("❌ Seeding error:", e)
        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    content_id = seed_content(cur)
                    seed_engagement(cur, content_id)
                conn.commit()

            print(f"✅ Seeded content + engagement at {datetime.utcnow().isoformat()}")

        except Exception as e:
            print("❌ Seeding error:", e)

        try:
            with get_connection() as conn:
                with conn.cursor() as cur:
                    content_id = seed_content(cur)
                    seed_engagement(cur, content_id)
                conn.commit()

            print(f"✅ Seeded content + engagement at {datetime.utcnow().isoformat()}")

        except Exception as e:
            print("❌ Seeding error:", e)
        time.sleep(10) 

if __name__ == "__main__":
    run()
