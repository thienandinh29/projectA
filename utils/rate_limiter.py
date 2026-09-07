import time
import threading
import logging
from typing import Callable, Any
import requests

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token Bucket Rate Limiter to comply with external API constraints
    (specifically SEC EDGAR's strict <= 10 req/s limit).
    """
    def __init__(self, max_per_second: float = 8.0):
        self.capacity = max_per_second
        self.fill_rate = max_per_second
        self.tokens = max_per_second
        self.last_update = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        """Blocks until a token is available."""
        with self.lock:
            while True:
                now = time.monotonic()
                elapsed = now - self.last_update
                self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                # Calculate sleep duration until next token is available
                needed = 1.0 - self.tokens
                sleep_time = needed / self.fill_rate
                time.sleep(max(0.01, sleep_time))


class ResilientHttpClient:
    """
    HTTP client wrapped with rate limiting and exponential backoff on HTTP 429 / 503.
    """
    def __init__(self, user_agent: str, max_requests_per_sec: float = 8.0, timeout: float = 10.0):
        self.user_agent = user_agent
        self.timeout = timeout
        self.rate_limiter = RateLimiter(max_per_second=max_requests_per_sec)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov"
        })

    def get(self, url: str, headers: dict = None, max_retries: int = 4) -> requests.Response:
        """
        Executes a rate-limited GET request with exponential backoff on rate-limit violations.
        """
        req_headers = dict(self.session.headers)
        if headers:
            req_headers.update(headers)

        backoff = 1.0
        for attempt in range(1, max_retries + 1):
            self.rate_limiter.acquire()
            try:
                resp = self.session.get(url, headers=req_headers, timeout=self.timeout)
                if resp.status_code == 429:
                    wait_time = float(resp.headers.get("Retry-After", backoff))
                    logger.warning(
                        f"Rate limited by SEC EDGAR (HTTP 429). Attempt {attempt}/{max_retries}. Sleeping {wait_time:.2f}s..."
                    )
                    time.sleep(wait_time)
                    backoff *= 2.0
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == max_retries:
                    logger.error(f"HTTP request failed after {max_retries} attempts: {url} - {e}")
                    raise
                logger.warning(f"Request error: {e}. Retrying in {backoff:.2f}s...")
                time.sleep(backoff)
                backoff *= 2.0
        raise RuntimeError(f"Exceeded max retries for {url}")
