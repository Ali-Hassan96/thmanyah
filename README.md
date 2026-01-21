# Ahmed Help - Data Pipeline & Analytics Platform

A complete data pipeline system with PostgreSQL, Redis, Spark, and automated services for data seeding, streaming, analysis, and caching.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose installed
- Ports available: 5436, 6379, 8080, 8081

### Start Everything

```bash
docker compose up -d
```

This starts all services:
- PostgreSQL database
- Redis cache
- Spark cluster (master + worker)
- Automated services (seeding, streaming, analysis, caching)

### Stop Everything

```bash
docker compose down
```

### View Logs

```bash
docker compose logs -f
```

## 📊 Services & Access Points

### PostgreSQL Database
- **Host:** `localhost`
- **Port:** `5436`
- **Database:** `postgres`
- **Username:** `postgres`
- **Password:** `mysecretpassword`

**Connection String:**
```
postgresql://postgres:mysecretpassword@localhost:5436/postgres
```

**Tables:**
- `content` - Content items
- `engagement_events` - User engagement events
- `content_engagement_metrics` - Spark analysis results
- `content_engagement_daily` - Daily metrics by event type
- `content_type_analysis` - Performance by content type
- `device_analysis` - Performance by device

### Redis Cache
- **Host:** `localhost`
- **Port:** `6379`
- **No password required**

**Redis Keys:**
- `event:{id}` - Enriched events with engagement_pct
- `content:{id}:events` - Events by content (last 100)
- `events:{type}` - Events by event type (last 1000)
- `content:list` - All content items
- `stats:*` - Engagement statistics
- `analysis:*` - Analysis results

### RedisInsight (Redis GUI)
- **URL:** http://localhost:8081
- **To Connect:**
  1. Click "Redis Databases"
  2. Click "Add Redis Database"
  3. Enter:
     - Database alias: `Local Redis`
     - Host: `redis`
     - Port: `6379`
     - Username: (leave empty)
     - Password: (leave empty)

### Spark Web UI
- **URL:** http://localhost:8080
- View Spark jobs, executors, and applications

## 🔄 Automated Services

### 1. Auto-Seed Service
- **What it does:** Continuously creates new content and engagement events
- **Interval:** Every 30 seconds
- **Container:** `auto_seed`

### 2. Auto-Stream Engagement Service
- **What it does:** Streams new engagement events, enriches with content data, calculates engagement metrics, writes to Redis
- **Interval:** Every 5 seconds
- **Container:** `auto_stream_engagement`
- **Enrichment:**
  - Joins with content table to get `content_type` and `length_seconds`
  - Calculates `engagement_seconds` (duration_ms / 1000)
  - Calculates `engagement_pct` (engagement_seconds / length_seconds × 100, rounded to 2 decimals)

### 3. Auto-Cache Redis Service
- **What it does:** Caches PostgreSQL data to Redis for fast access
- **Interval:** Every 30 seconds
- **Container:** `auto_cache_redis`

### 4. Auto-Analyze PostgreSQL Service
- **What it does:** Runs Spark analysis jobs to create aggregated metrics
- **Interval:** Every 60 seconds
- **Container:** `auto_analyze_postgres`
- **Creates tables:**
  - `content_engagement_metrics`
  - `content_engagement_daily` (with length_seconds and event_type)
  - `content_type_analysis`
  - `device_analysis`

## 📝 Example Queries

### PostgreSQL

```sql
-- View all content
SELECT * FROM content LIMIT 10;

-- View recent engagement events
SELECT * FROM engagement_events ORDER BY event_ts DESC LIMIT 10;

-- View daily metrics by event type
SELECT * FROM content_engagement_daily 
WHERE event_date = CURRENT_DATE 
ORDER BY events_count DESC;

-- View content engagement metrics
SELECT * FROM content_engagement_metrics 
ORDER BY total_events DESC;
```

### Redis (via redis-cli)

```bash
# Connect to Redis
docker exec -it redis_db redis-cli

# View all keys
KEYS *

# Get a specific event
GET event:76

# Get events for a content
LRANGE content:{content_id}:events 0 10

# Get total events count
GET streaming:total_events
```

## 🛠️ Manual Operations

### Run Spark Analysis Manually

```bash
docker exec -it spark_master spark-submit \
  --master spark://spark-master:7077 \
  --packages org.postgresql:postgresql:42.7.1 \
  /opt/spark/scripts/postgres_analyze_to_postgres.py
```

### Check Service Status

```bash
docker compose ps
```

### View Service Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f auto-stream-engagement
docker compose logs -f auto-analyze-postgres
```

## 📁 Project Structure

```
.
├── compose.yaml              # Docker Compose configuration
├── database/
│   ├── migrations/           # Database schema migrations
│   └── seeds/                # Initial seed data
├── services/                 # Python services
│   ├── auto_seed.py          # Data seeding service
│   ├── auto_cache_to_redis.py # PostgreSQL to Redis caching
│   └── auto_stream_engagement_to_redis.py # Streaming service
├── spark/
│   ├── jobs/                 # Spark job scripts
│   │   ├── postgres_analyze_to_postgres.py
│   │   └── ...
│   └── services/             # Spark service scripts
│       └── auto_analyze_postgres.py
└── README.md                 # This file
```

## 🔧 Configuration

### Environment Variables

Edit `compose.yaml` to change:
- Database passwords
- Service intervals (SEED_INTERVAL, STREAMING_INTERVAL, ANALYSIS_INTERVAL, CACHE_INTERVAL)
- Port mappings
- Memory/CPU limits

### Reset Database

```bash
# Stop and remove volumes
docker compose down -v

# Start fresh (will run migrations)
docker compose up -d
```

## 📈 Data Flow

```
PostgreSQL (Source)
    ↓
Auto-Seed Service → Creates content & events
    ↓
Auto-Stream Service → Enriches & streams to Redis (<5s latency)
    ↓
Redis (Cache/Streaming Store)
    ↓
RedisInsight (View data)
    ↓
Auto-Analyze Service → Spark analysis → PostgreSQL (Analysis tables)
    ↓
Auto-Cache Service → Caches analysis results to Redis
```

## 🐛 Troubleshooting

### Services not starting
```bash
docker compose logs [service-name]
```

### Redis connection issues
- Make sure you use `redis` as host (not `127.0.0.1`) when connecting from RedisInsight
- Check Redis is running: `docker compose ps redis_db`

### PostgreSQL connection issues
- Use port `5436` (not 5432) from your local machine
- Use port `5432` from inside Docker containers

### No data in Redis
- Wait a few seconds for services to start
- Check streaming service logs: `docker compose logs auto-stream-engagement`
- Verify data exists in PostgreSQL first

## 📚 More Information

- Spark jobs documentation: `spark/docs/README.md`
- Streaming guide: `spark/docs/STREAMING_GUIDE.md`
