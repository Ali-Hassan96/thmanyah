#!/usr/bin/env python3
"""
Automated Engagement Events Streaming Service
Streams engagement_events from PostgreSQL, enriches with content data,
and writes to Redis for low-latency analytics
"""

import os
import sys
import time
import subprocess
from datetime import datetime

# Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "mysecretpassword")
STREAMING_INTERVAL = int(os.getenv("STREAMING_INTERVAL", "5"))  # seconds
SPARK_MASTER = "spark://spark-master:7077"
MAX_RETRIES = 30
RETRY_DELAY = 10  # seconds

def wait_for_service(host, port, service_name, max_retries=MAX_RETRIES):
    """Wait for a service to be ready"""
    import socket
    for i in range(max_retries):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            if result == 0:
                print(f"✅ {service_name} is ready")
                return True
        except Exception as e:
            pass
        if i < max_retries - 1:
            print(f"⏳ Waiting for {service_name}... ({i+1}/{max_retries})")
            time.sleep(RETRY_DELAY)
    print(f"❌ {service_name} did not become ready in time")
    return False

def check_spark_master():
    """Check if Spark Master is ready"""
    return wait_for_service("spark-master", 7077, "Spark Master")

def check_postgres():
    """Check if PostgreSQL is ready"""
    return wait_for_service("postgres", 5432, "PostgreSQL")

def check_redis():
    """Check if Redis is ready"""
    return wait_for_service("redis", 6379, "Redis")

def run_streaming_job():
    """Run the Spark streaming job"""
    print("=" * 80)
    print("🔄 Running Engagement Events Streaming Job")
    print("=" * 80)
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--packages", "org.postgresql:postgresql:42.7.1",
        "/opt/spark/scripts/postgres_engagement_streaming.py"
    ]
    
    env = os.environ.copy()
    env.update({
        "POSTGRES_HOST": POSTGRES_HOST,
        "POSTGRES_PORT": str(POSTGRES_PORT),
        "POSTGRES_DB": POSTGRES_DB,
        "POSTGRES_USER": POSTGRES_USER,
        "POSTGRES_PASSWORD": POSTGRES_PASSWORD,
        "REDIS_HOST": os.getenv("REDIS_HOST", "redis"),
        "REDIS_PORT": os.getenv("REDIS_PORT", "6379"),
        "STREAMING_INTERVAL": str(STREAMING_INTERVAL)
    })
    
    try:
        result = subprocess.run(
            cmd,
            env=env,
            timeout=None  # Run indefinitely
        )
        
        if result.returncode != 0:
            print(f"❌ Streaming job exited with code {result.returncode}")
            return False
        return True
            
    except subprocess.TimeoutExpired:
        print("⏱️  Streaming job timed out")
        return False
    except Exception as e:
        print(f"❌ Error running streaming job: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    print("=" * 80)
    print("🚀 Automated Engagement Events Streaming Service")
    print("=" * 80)
    print(f"Streaming Interval: {STREAMING_INTERVAL} seconds")
    print("=" * 80)
    
    # Wait for dependencies
    print("\n📋 Checking dependencies...")
    if not check_postgres():
        print("❌ PostgreSQL not ready, exiting")
        sys.exit(1)
    
    if not check_redis():
        print("❌ Redis not ready, exiting")
        sys.exit(1)
    
    if not check_spark_master():
        print("❌ Spark Master not ready, exiting")
        sys.exit(1)
    
    print("\n✅ All dependencies ready!")
    time.sleep(5)  # Give services a moment to fully initialize
    
    print("\n🚀 Starting streaming job...")
    print("=" * 80)
    
    # Run streaming job (will run indefinitely)
    run_streaming_job()

if __name__ == "__main__":
    main()
