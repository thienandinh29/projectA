import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file from project root
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Broker & Cache
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_TTL_SECONDS = int(os.getenv("REDIS_TTL_SECONDS", 604800))      # 7 days for exact match
REDIS_LSH_TTL_SECONDS = int(os.getenv("REDIS_LSH_TTL_SECONDS", 86400))  # 24 hours for LSH near-duplicate window
REDIS_SEMANTIC_TTL_SECONDS = int(os.getenv("REDIS_SEMANTIC_TTL_SECONDS", 7200))  # 120 minutes for semantic embedding window

# Topics
TOPIC_RSS = os.getenv("TOPIC_RSS", "financial.news.rss")
TOPIC_GDELT = os.getenv("TOPIC_GDELT", "financial.news.gdelt")
TOPIC_SEC = os.getenv("TOPIC_SEC", "financial.filings.sec")

# SEC Settings
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "FinancialPipeline research@example.com")
SEC_MAX_REQUESTS_PER_SECOND = float(os.getenv("SEC_MAX_REQUESTS_PER_SECOND", 8.0))

# Intervals
RSS_POLL_INTERVAL_SECONDS = int(os.getenv("RSS_POLL_INTERVAL_SECONDS", 180))
GDELT_POLL_INTERVAL_SECONDS = int(os.getenv("GDELT_POLL_INTERVAL_SECONDS", 900))
SEC_POLL_INTERVAL_SECONDS = int(os.getenv("SEC_POLL_INTERVAL_SECONDS", 300))
