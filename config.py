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

# Tier 3 semantic dedup cosine threshold. Calibrated empirically by
# scripts/calibrate_tier3.py (v2: 63-pair labeled corpus, 7 bands, gated sweep
# using the production entity extractor and antonym polarity guard).
# Measured on all-MiniLM-L6-v2 (fastembed ONNX):
#   - v1 (hand-picked 0.65, no gates): gated precision 0.43 — 8 distinct-event
#     false merges, including same-company different-event pairs.
#   - v1 fix (0.88, no entity extractor): precision 1.000 but recall 0.05 —
#     Tier 3 was near-inert because hard negatives (0.44-0.74) overlapped
#     same-event rewrites (0.25-0.92).
#   - v2 (0.76, production extractor + antonym guard): precision 1.000,
#     RECALL 0.150, F1 0.261, 0 false merges, gate false-reject 0%.
#     Highest surviving negative 0.7425; extractor coverage corpus 58% /
#     live 37% (oracle-perfect tags would only add 0.03 threshold headroom).
# Precision-first policy: a wrong merge hides a real event from canonical PiT
# queries; a missed duplicate is only redundancy. Full curve in
# scripts/calibration_results.json.
SEMANTIC_COSINE_THRESHOLD = float(os.getenv("SEMANTIC_COSINE_THRESHOLD", 0.76))

# Tier 3 scan cap: max window entries fetched per event (newest-first).
# Bounds worst-case latency — the full-window scan measured 366ms p50 at a
# 5,000-entry window (benchmark.py --tier3). Trade-off: a duplicate whose
# canonical is older than the newest N entries in the 120-min window is not
# found by Tier 3 (documented limitation, parity-tested in
# tests/test_tier3_hard_negatives.py). 0 = unlimited (brute force).
SEMANTIC_SCAN_LIMIT = int(os.getenv("SEMANTIC_SCAN_LIMIT", 1000))

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
