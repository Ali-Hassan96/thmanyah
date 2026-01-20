import psycopg2
import psycopg2.extras
import redis
import json
import time
from datetime import datetime, timezone

# ---------------- CONFIG ----------------
PG_CONFIG = {
    "host": "localhost",      # "postgres" if inside Docker
    "port": 5436,
    "dbname": "postgres",
    "user": "postgres",
    "password": "mysecretpassword"
}

REDIS_CONFIG = {
    "host": "localhost",      # "redis" if inside Docker
    "port": 6379,
    "decode_responses": True
}

SYNC_INTERVAL = 5     # seconds
CACHE_TTL = 60       # 10 minutes

# ---------------------------------------
psycopg2.extras.register_uuid()

r = redis.Redis(**REDIS_CONFIG)

def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)

last_sync_content = datetime.min.replace(tzinfo=timezone.utc)
last_sync_engagement = datetime.min.replace(tzinfo=timezone.utc)

def sync_content():
    global last_sync_content
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM content
                WHERE updated_at > %s
            """, (last_sync_content,))
            rows = cur.fetchall()
            if rows:
                pipe = r.pipeline()
                for row in rows:
                    key = f"content:{row['id']}"
                    pipe.set(key, json.dumps(row, default=str), ex=CACHE_TTL)
                    if row["updated_at"] > last_sync_content:
                        last_sync_content = row["updated_at"]
                pipe.execute()
                print(f"🔄 Synced {len(rows)} content rows at {datetime.utcnow().isoformat()}")

def sync_engagement():
    global last_sync_engagement
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM engagement_events
                WHERE updated_at > %s
            """, (last_sync_engagement,))
            rows = cur.fetchall()
            if rows:
                pipe = r.pipeline()
                for row in rows:
                    key = f"engagement:{row['id']}"
                    pipe.set(key, json.dumps(row, default=str), ex=CACHE_TTL)
                    if row["updated_at"] > last_sync_engagement:
                        last_sync_engagement = row["updated_at"]
                pipe.execute()
                print(f"🔄 Synced {len(rows)} engagement rows at {datetime.utcnow().isoformat()}")

def run():
    print("🚀 Real-time Postgres → Redis sync started")
    while True:
        try:
            sync_content()
            sync_engagement()
            total_keys = r.dbsize()
            content_keys = len(r.keys("content:*"))
            engagement_keys = len(r.keys("engagement:*"))

            print("Total keys:", total_keys)
            print("Content keys:", content_keys)
            print("Engagement keys:", engagement_keys)
        except Exception as e:
            print("❌ Sync error:", e)
        time.sleep(SYNC_INTERVAL)

if __name__ == "__main__":
    run()
