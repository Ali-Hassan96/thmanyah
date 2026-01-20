#!/usr/bin/env python3
"""
Stream engagement_events from PostgreSQL, enrich with content data,
calculate engagement metrics, and write to Redis for low-latency analytics.

Features:
- Streaming ingestion from engagement_events (not batch)
- Fast enrichment and aggregation (joining with content table)
- Low-latency analytics (<5s), suitable for dashboards or alerting
"""

import os
import sys
import time
import json
import psycopg2
import redis
from datetime import datetime
from decimal import Decimal

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Streaming configuration
STREAMING_INTERVAL = int(os.getenv("STREAMING_INTERVAL", "5"))  # seconds

def get_postgres_connection():
    """Get PostgreSQL connection"""
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )

def get_redis_connection():
    """Get Redis connection"""
    return redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        db=REDIS_DB,
        decode_responses=True
    )

def get_latest_event_id(redis_client):
    """Get the latest processed event ID from Redis"""
    try:
        latest_id = redis_client.get("streaming:latest_event_id")
        return int(latest_id) if latest_id else 0
    except:
        return 0

def stream_new_events(pg_conn, redis_client, start_id):
    """Stream new events, enrich with content data, and write to Redis"""
    try:
        with pg_conn.cursor() as cur:
            # Query new events with content enrichment
            query = """
                SELECT 
                    e.id,
                    e.content_id,
                    e.user_id,
                    e.event_type,
                    e.event_ts,
                    e.duration_ms,
                    e.device,
                    c.content_type,
                    c.length_seconds
                FROM engagement_events e
                LEFT JOIN content c ON e.content_id = c.id
                WHERE e.id > %s
                ORDER BY e.id ASC
                LIMIT 1000
            """
            
            cur.execute(query, (start_id,))
            rows = cur.fetchall()
            
            if not rows:
                return start_id, 0
            
            event_count = 0
            latest_id = start_id
            
            for row in rows:
                event_id, content_id, user_id, event_type, event_ts, duration_ms, device, content_type, length_seconds = row
                
                # Calculate engagement_seconds (convert ms to seconds)
                engagement_seconds = None
                if duration_ms is not None:
                    engagement_seconds = round(duration_ms / 1000.0, 2)
                
                # Calculate engagement_pct = engagement_seconds / length_seconds
                # Round to 2 decimal places
                # Set to NULL if either length_seconds or duration_ms is missing
                engagement_pct = None
                if length_seconds is not None and engagement_seconds is not None and length_seconds > 0:
                    engagement_pct = round((engagement_seconds / length_seconds) * 100, 2)
                
                # Build enriched event data
                event_data = {
                    "id": event_id,
                    "content_id": str(content_id) if content_id else None,
                    "user_id": str(user_id) if user_id else None,
                    "event_type": event_type,
                    "event_ts": event_ts.isoformat() if event_ts else None,
                    "duration_ms": duration_ms,
                    "device": device,
                    "content_type": content_type,
                    "length_seconds": length_seconds,
                    "engagement_seconds": engagement_seconds,
                    "engagement_pct": engagement_pct
                }
                
                # Store individual event
                redis_client.setex(
                    f"event:{event_id}",
                    3600,  # 1 hour TTL
                    json.dumps(event_data)
                )
                
                # Store by content_id for fast lookup
                if content_id:
                    redis_client.lpush(f"content:{content_id}:events", json.dumps(event_data))
                    redis_client.ltrim(f"content:{content_id}:events", 0, 99)  # Keep last 100 events
                    redis_client.expire(f"content:{content_id}:events", 3600)
                
                # Store by event_type for analytics
                redis_client.lpush(f"events:{event_type}", json.dumps(event_data))
                redis_client.ltrim(f"events:{event_type}", 0, 999)  # Keep last 1000 events
                redis_client.expire(f"events:{event_type}", 3600)
                
                # Update latest event ID
                if event_id > latest_id:
                    latest_id = event_id
                
                event_count += 1
            
            # Store latest event ID for resumption
            redis_client.set("streaming:latest_event_id", latest_id)
            
            # Update stats
            redis_client.incrby("streaming:total_events", event_count)
            redis_client.setex("streaming:last_update", 60, json.dumps({
                "timestamp": datetime.now().isoformat(),
                "event_count": event_count,
                "latest_id": latest_id
            }))
            
            return latest_id, event_count
            
    except Exception as e:
        print(f"❌ Error streaming events: {e}")
        import traceback
        traceback.print_exc()
        return start_id, 0

def wait_for_services():
    """Wait for PostgreSQL and Redis to be ready"""
    import socket
    
    # Wait for PostgreSQL
    print("⏳ Waiting for PostgreSQL...")
    for i in range(30):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((POSTGRES_HOST, POSTGRES_PORT))
            sock.close()
            if result == 0:
                print("✅ PostgreSQL is ready")
                break
        except:
            pass
        time.sleep(2)
    else:
        print("❌ PostgreSQL not ready")
        return False
    
    # Wait for Redis
    print("⏳ Waiting for Redis...")
    for i in range(30):
        try:
            r = get_redis_connection()
            r.ping()
            print("✅ Redis is ready")
            break
        except:
            pass
        time.sleep(2)
    else:
        print("❌ Redis not ready")
        return False
    
    return True

def main():
    print("=" * 80)
    print("🚀 Engagement Events Streaming to Redis")
    print("=" * 80)
    print(f"Streaming Interval: {STREAMING_INTERVAL} seconds")
    print(f"PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
    print("=" * 80)
    
    if not wait_for_services():
        print("❌ Services not ready, exiting")
        sys.exit(1)
    
    print("\n✅ All services ready!")
    time.sleep(2)
    
    pg_conn = get_postgres_connection()
    redis_client = get_redis_connection()
    
    # Get starting point
    start_id = get_latest_event_id(redis_client)
    print(f"📌 Resuming from event ID: {start_id}")
    print(f"\n🔄 Starting streaming (checking every {STREAMING_INTERVAL} seconds)...")
    print("=" * 80)
    
    try:
        while True:
            start_id, event_count = stream_new_events(pg_conn, redis_client, start_id)
            
            if event_count > 0:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{timestamp}] ✅ Processed {event_count} events (latest ID: {start_id})")
            else:
                print(f"⏳ No new events, waiting {STREAMING_INTERVAL} seconds...")
            
            time.sleep(STREAMING_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Streaming stopped by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        pg_conn.close()

if __name__ == "__main__":
    main()
