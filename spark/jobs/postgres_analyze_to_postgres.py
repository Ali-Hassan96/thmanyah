"""
Spark script to read from PostgreSQL, analyze/aggregate data, and write results back to PostgreSQL

This script:
1. Reads content and engagement_events from PostgreSQL
2. Performs analysis and aggregations
3. Writes analyzed results back to PostgreSQL in new tables

Run with:
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1 \
  /opt/spark/scripts/postgres_analyze_to_postgres.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, max as spark_max, 
    min as spark_min, when, date_format, to_date, round as spark_round
)

# PostgreSQL configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")

POSTGRES_JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Create Spark session
spark = SparkSession.builder \
    .appName("PostgreSQLAnalyzeToPostgreSQL") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
    .getOrCreate()

print("=" * 80)
print("PostgreSQL → Spark Analysis → PostgreSQL")
print("=" * 80)

# Read content table from PostgreSQL
print("\n📥 Reading content table from PostgreSQL...")
df_content = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "content") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

content_count = df_content.count()
print(f"✅ Loaded {content_count} content records")

# Read engagement_events table from PostgreSQL
print("\n📥 Reading engagement_events table from PostgreSQL...")
df_engagement = spark.read \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "engagement_events") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .load()

engagement_count = df_engagement.count()
print(f"✅ Loaded {engagement_count} engagement events")

print("\n" + "=" * 80)
print("🔍 Analyzing and aggregating data...")
print("=" * 80)

# Join content with engagement events
df_joined = df_content.join(
    df_engagement,
    df_content.id == df_engagement.content_id,
    "left"
)

# Calculate engagement_seconds and engagement_pct for enrichment
df_joined = df_joined.withColumn(
    "engagement_seconds",
    when(col("duration_ms").isNotNull(), col("duration_ms") / 1000.0).otherwise(None)
).withColumn(
    "engagement_pct",
    when(
        col("length_seconds").isNotNull() & 
        col("engagement_seconds").isNotNull() & 
        (col("length_seconds") > 0),
        spark_round(col("engagement_seconds") / col("length_seconds") * 100, 2)
    ).otherwise(None)
)

# Analysis 1: Overall content engagement metrics
print("\n📊 Creating content engagement metrics...")
df_content_metrics = df_joined.groupBy(
    df_content.id.alias("content_id"),
    df_content.slug.alias("content_slug"),
    df_content.title.alias("content_title"),
    df_content.content_type.alias("content_type"),
    df_content.length_seconds.alias("content_length_seconds"),
    df_content.publish_ts.alias("content_publish_ts")
).agg(
    count(df_engagement.id).alias("total_events"),
    count(when(df_engagement.event_type == "play", 1)).alias("play_count"),
    count(when(df_engagement.event_type == "pause", 1)).alias("pause_count"),
    count(when(df_engagement.event_type == "finish", 1)).alias("finish_count"),
    count(when(df_engagement.event_type == "click", 1)).alias("click_count"),
    spark_sum(df_engagement.duration_ms).alias("total_duration_ms"),
    avg(df_engagement.duration_ms).alias("avg_duration_ms"),
    spark_max(df_engagement.event_ts).alias("last_event_ts"),
    spark_min(df_engagement.event_ts).alias("first_event_ts"),
    count(df_engagement.user_id).alias("unique_users")
).withColumn(
    "completion_rate",
    when(col("play_count") > 0, col("finish_count") / col("play_count")).otherwise(0.0)
).withColumn(
    "engagement_score",
    (col("total_events") * 0.3 + col("unique_users") * 0.4 + col("completion_rate") * 100 * 0.3)
)

print("Content metrics preview:")
df_content_metrics.show(5, truncate=False)

# Analysis 2: Daily engagement metrics
# Group by content, event_type, and date to get daily metrics per event type
# Includes length_seconds from content table and event_type from engagement_events
print("\n📊 Creating daily engagement metrics...")
df_daily_metrics = df_joined.groupBy(
    df_content.id.alias("content_id"),
    df_content.slug.alias("content_slug"),
    df_content.title.alias("content_title"),
    df_content.content_type.alias("content_type"),
    df_content.length_seconds.alias("length_seconds"),  # From content table
    df_engagement.event_type.alias("event_type"),  # From engagement_events
    to_date(df_engagement.event_ts).alias("event_date")
).agg(
    count(df_engagement.id).alias("events_count"),
    spark_sum(df_engagement.duration_ms).alias("total_duration_ms"),
    avg(df_engagement.duration_ms).alias("avg_duration_ms"),
    count(df_engagement.user_id).alias("unique_users"),
    avg(col("engagement_seconds")).alias("avg_engagement_seconds"),
    avg(col("engagement_pct")).alias("avg_engagement_pct")
).filter(
    col("event_date").isNotNull()
)

print("Daily metrics preview:")
df_daily_metrics.show(5, truncate=False)

# Analysis 3: Content type performance
print("\n📊 Creating content type performance analysis...")
df_content_type_analysis = df_joined.groupBy(
    df_content.content_type.alias("content_type")
).agg(
    count(df_content.id).alias("total_content_items"),
    count(df_engagement.id).alias("total_events"),
    count(when(df_engagement.event_type == "play", 1)).alias("total_plays"),
    count(when(df_engagement.event_type == "finish", 1)).alias("total_finishes"),
    avg(df_engagement.duration_ms).alias("avg_duration_ms"),
    count(df_engagement.user_id).alias("total_unique_users")
).withColumn(
    "avg_events_per_content",
    col("total_events") / col("total_content_items")
).withColumn(
    "completion_rate",
    when(col("total_plays") > 0, col("total_finishes") / col("total_plays")).otherwise(0.0)
)

print("Content type analysis preview:")
df_content_type_analysis.show(truncate=False)

# Analysis 4: Device performance
print("\n📊 Creating device performance analysis...")
df_device_analysis = df_engagement.groupBy(
    col("device")
).agg(
    count("*").alias("total_events"),
    count(when(col("event_type") == "play", 1)).alias("play_count"),
    count(when(col("event_type") == "finish", 1)).alias("finish_count"),
    avg(col("duration_ms")).alias("avg_duration_ms"),
    count(col("user_id")).alias("unique_users")
).withColumn(
    "completion_rate",
    when(col("play_count") > 0, col("finish_count") / col("play_count")).otherwise(0.0)
).orderBy(col("total_events").desc())

print("Device analysis preview:")
df_device_analysis.show(truncate=False)

print("\n" + "=" * 80)
print("💾 Writing analyzed data back to PostgreSQL...")
print("=" * 80)

# Write content engagement metrics to PostgreSQL
print("\n💾 Writing content_engagement_metrics...")
df_content_metrics.write \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "content_engagement_metrics") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Written content_engagement_metrics table")

# Write daily metrics to PostgreSQL
print("\n💾 Writing content_engagement_daily...")
df_daily_metrics.write \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "content_engagement_daily") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Written content_engagement_daily table")

# Write content type analysis to PostgreSQL
print("\n💾 Writing content_type_analysis...")
df_content_type_analysis.write \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "content_type_analysis") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Written content_type_analysis table")

# Write device analysis to PostgreSQL
print("\n💾 Writing device_analysis...")
df_device_analysis.write \
    .format("jdbc") \
    .option("url", POSTGRES_JDBC_URL) \
    .option("dbtable", "device_analysis") \
    .option("user", POSTGRES_USER) \
    .option("password", POSTGRES_PASSWORD) \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("✅ Written device_analysis table")

print("\n" + "=" * 80)
print("✅ Analysis Complete!")
print("=" * 80)
print("\n📊 Analysis tables created in PostgreSQL:")
print("  1. content_engagement_metrics - Overall metrics per content")
print("  2. content_engagement_daily - Daily metrics per content")
print("  3. content_type_analysis - Performance by content type")
print("  4. device_analysis - Performance by device")
print("\nYou can query these tables directly in PostgreSQL!")

spark.stop()
