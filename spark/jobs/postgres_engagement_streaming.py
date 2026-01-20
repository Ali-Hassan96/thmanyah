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
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round as spark_round, lit, 
    col("duration_ms") / 1000.0, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Streaming configuration
STREAMING_INTERVAL = int(os.getenv("STREAMING_INTERVAL", "5"))  # seconds
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create Spark session
spark = SparkSession.builder \
    .appName("PostgreSQLEngagementStreaming") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 80)
print("📊 PostgreSQL Engagement Events Streaming to Redis")
print("=" * 80)
print(f"Streaming Interval: {STREAMING_INTERVAL} seconds")
print(f"PostgreSQL: {POSTGRES_JDBC_URL}")
print(f"Redis: {REDIS_HOST}:{REDIS_PORT}")
print("=" * 80)

def get_latest_event_id():
    """Get the latest event ID from Redis to resume streaming"""
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        latest_id = r.get("streaming:latest_event_id")
        return int(latest_id) if latest_id else 0
    except:
        return 0

def write_to_redis(batch_df, epoch_id):
    """Write enriched events to Redis"""
    try:
        import redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        
        # Collect batch data
        rows = batch_df.collect()
        
        if not rows:
            print(f"⏭️  Epoch {epoch_id}: No new events")
            return
        
        latest_id = 0
        event_count = 0
        
        for row in rows:
            event_id = row.id
            content_id = str(row.content_id) if row.content_id else None
            
            # Build event data
            event_data = {
                "id": event_id,
                "content_id": content_id,
                "user_id": str(row.user_id) if row.user_id else None,
                "event_type": row.event_type,
                "event_ts": row.event_ts.isoformat() if row.event_ts else None,
                "duration_ms": row.duration_ms,
                "device": row.device,
                "content_type": row.content_type,
                "length_seconds": row.length_seconds,
                "engagement_seconds": float(row.engagement_seconds) if row.engagement_seconds else None,
                "engagement_pct": float(row.engagement_pct) if row.engagement_pct else None
            }
            
            # Store individual event
            r.setex(
                f"event:{event_id}",
                3600,  # 1 hour TTL
                json.dumps(event_data)
            )
            
            # Store by content_id for fast lookup
            if content_id:
                r.lpush(f"content:{content_id}:events", json.dumps(event_data))
                r.ltrim(f"content:{content_id}:events", 0, 99)  # Keep last 100 events
                r.expire(f"content:{content_id}:events", 3600)
            
            # Store by event_type for analytics
            r.lpush(f"events:{row.event_type}", json.dumps(event_data))
            r.ltrim(f"events:{row.event_type}", 0, 999)  # Keep last 1000 events
            r.expire(f"events:{row.event_type}", 3600)
            
            # Update latest event ID
            if event_id > latest_id:
                latest_id = event_id
            
            event_count += 1
        
        # Store latest event ID for resumption
        r.set("streaming:latest_event_id", latest_id)
        
        # Update stats
        r.incrby("streaming:total_events", event_count)
        r.setex("streaming:last_update", 60, json.dumps({
            "epoch_id": epoch_id,
            "event_count": event_count,
            "latest_id": latest_id
        }))
        
        print(f"✅ Epoch {epoch_id}: Cached {event_count} events to Redis (latest ID: {latest_id})")
        
    except Exception as e:
        print(f"❌ Error writing to Redis in epoch {epoch_id}: {e}")
        import traceback
        traceback.print_exc()

# Read content table for enrichment (cached lookup)
print("\n📥 Loading content table for enrichment...")
df_content = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "content") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load() \
    .select("id", "content_type", "length_seconds") \
    .cache()

content_count = df_content.count()
print(f"✅ Loaded {content_count} content items for enrichment")

# Get starting point
start_id = get_latest_event_id()
print(f"📌 Resuming from event ID: {start_id}")

# Define engagement_events schema
engagement_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("content_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_ts", TimestampType(), True),
    StructField("duration_ms", IntegerType(), True),
    StructField("device", StringType(), True),
    StructField("raw_payload", StringType(), True),
    StructField("updated_at", TimestampType(), True)
])

# Create streaming query
print(f"\n🔄 Starting streaming query (checking every {STREAMING_INTERVAL} seconds)...")

# Read engagement_events in micro-batches
def read_new_events():
    """Read new events since last check"""
    query = f"""
        (SELECT id, content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload, updated_at
         FROM engagement_events 
         WHERE id > {start_id}
         ORDER BY id ASC
         LIMIT 1000) AS new_events
    """
    
    df_events = spark.read \
        .format("jdbc") \
        .option("url", POSTGRES_JDBC_URL) \
        .option("dbtable", query) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    return df_events

# Main streaming loop
print("\n🚀 Starting streaming ingestion...")
print("=" * 80)

try:
    while True:
        # Read new events
        df_events = read_new_events()
        event_count = df_events.count()
        
        if event_count > 0:
            print(f"\n📥 Read {event_count} new events")
            
            # Join with content table for enrichment
            df_enriched = df_events.join(
                df_content,
                df_events.content_id == df_content.id,
                "left"
            ).select(
                df_events["*"],
                df_content.content_type,
                df_content.length_seconds
            )
            
            # Calculate engagement_seconds (convert ms to seconds)
            df_enriched = df_enriched.withColumn(
                "engagement_seconds",
                when(col("duration_ms").isNotNull(), col("duration_ms") / 1000.0)
                .otherwise(None)
            )
            
            # Calculate engagement_pct = engagement_seconds / length_seconds
            # Round to 2 decimal places
            # Set to NULL if either length_seconds or duration_ms is missing
            df_enriched = df_enriched.withColumn(
                "engagement_pct",
                when(
                    col("length_seconds").isNotNull() & 
                    col("engagement_seconds").isNotNull() & 
                    (col("length_seconds") > 0),
                    spark_round(col("engagement_seconds") / col("length_seconds") * 100, 2)
                ).otherwise(None)
            )
            
            # Select final columns
            df_final = df_enriched.select(
                "id",
                "content_id",
                "user_id",
                "event_type",
                "event_ts",
                "duration_ms",
                "device",
                "content_type",
                "length_seconds",
                "engagement_seconds",
                "engagement_pct"
            )
            
            # Write to Redis
            write_to_redis(df_final, int(time.time()))
            
            # Update start_id for next iteration
            max_id = df_final.agg({"id": "max"}).collect()[0][0]
            if max_id:
                start_id = max_id
        else:
            print(f"⏳ No new events, waiting {STREAMING_INTERVAL} seconds...")
        
        time.sleep(STREAMING_INTERVAL)
        
except KeyboardInterrupt:
    print("\n\n🛑 Streaming stopped by user")
except Exception as e:
    print(f"\n❌ Streaming error: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
