"""
Financial entity extraction for the Tier 3 ticker/entity gate.

The original extractor (workers.rss_worker.extract_tickers) matched only
$TICKER and bare ALL-CAPS tokens — measured coverage on the calibration
corpus was 13%, which made the entity gate a no-op on 87% of headlines.
This module replaces it with an alias dictionary covering the companies and
institutions that dominate macro/news headlines, plus the original regex as
a fallback for unlisted tickers.

Scope decisions:
  - Companies map to tickers; macro institutions (Federal Reserve, ECB, OPEC,
    ...) map to canonical codes so cross-entity pairs like "ECB raises rates"
    vs "Fed raises rates" are gate-blocked too.
  - Countries and persons are deliberately NOT entities: mapping them
    false-blocks legitimate rewrites ("Washington ..." vs "... for China").
  - Ambiguous aliases that collide with common finance/general vocabulary
    are excluded after live measurement: "bp" (= basis points, NOT BP oil),
    "chase" (verb). Kept-but-watchlist: "visa", "shell", "meta" — in
    finance-domain feeds they overwhelmingly denote the companies.
  - Gate semantics for edge cases (defined behavior, see semantic_dedup):
      * no entities on either side  -> gate bypassed (macro news merges on
        cosine + polarity alone)
      * entities on one side only   -> gate bypassed
      * entities on both sides      -> merge allowed iff the sets OVERLAP
        (any common entity); blocked iff disjoint. Multi-ticker headlines
        ("Goldman upgrades Ford") therefore merge with any story mentioning
        Goldman OR Ford.
  - Codes returned for institutions (FED, ECB) are also stored in
    CommonEvent.tickers_mentioned; they are entity codes, not tradable
    tickers — acceptable for this schema's filtering use.

The dictionary is a heuristic ceiling-raiser (coverage measured by
scripts/calibrate_tier3.py: 58% of calibration-corpus titles, 37% of live
lakehouse titles — up from 29% for the bare regex; most unmatched headlines
are fund names / market commentary with no single primary entity, for which
the gate is a documented no-op. Oracle-perfect tags would only add ~0.03 of
threshold headroom). Swapping to an NER model later only requires changing
this module's extract_financial_entities().
"""

import re
from typing import List, Set

# alias (lowercase, as it appears in text) -> canonical entity code
ENTITY_ALIASES = {
    # ── Mega-cap tech / consumer ─────────────────────────────────────────
    "apple": "AAPL", "microsoft": "MSFT", "tesla": "TSLA", "amazon": "AMZN",
    "alphabet": "GOOGL", "google": "GOOGL", "youtube": "GOOGL", "meta": "META",
    "facebook": "META", "instagram": "META", "nvidia": "NVDA", "netflix": "NFLX",
    "disney": "DIS", "hulu": "DIS", "ford": "F", "general motors": "GM",
    "rivian": "RIVN", "lucid": "LCID", "toyota": "TM",
    # ── Banks / brokers / asset managers ─────────────────────────────────
    "jpmorgan": "JPM", "j.p. morgan": "JPM", "jp morgan": "JPM",
    "goldman sachs": "GS", "goldman": "GS",
    "morgan stanley": "MS", "bank of america": "BAC",
    "wells fargo": "WFC", "citigroup": "C", "citi": "C", "barclays": "BCS",
    "hsbc": "HSBC", "blackrock": "BLK", "citadel": "CIT", "robinhood": "HOOD",
    "coinbase": "COIN", "paypal": "PYPL", "visa": "V", "mastercard": "MA",
    # NOTE: "chase" (verb) and "bp" (basis points) deliberately absent —
    # measured false-positive collisions on live/finance text.
    # ── Pharma / healthcare ──────────────────────────────────────────────
    "pfizer": "PFE", "novartis": "NVS", "moderna": "MRNA", "merck": "MRK",
    "johnson & johnson": "JNJ", "abbvie": "ABBV", "eli lilly": "LLY",
    "amgen": "AMGN", "gilead": "GILD", "regeneron": "REGN",
    # ── Energy / industrials ─────────────────────────────────────────────
    "exxon": "XOM", "exxonmobil": "XOM", "chevron": "CVX", "shell": "SHEL",
    "lockheed martin": "LMT", "lockheed": "LMT",
    "boeing": "BA", "general electric": "GE",
    # ── Retail / consumer ────────────────────────────────────────────────
    "walmart": "WMT", "mcdonald": "MCD", "starbucks": "SBUX", "nike": "NKE",
    "coca-cola": "KO", "coca cola": "KO", "pepsi": "PEP",
    "procter & gamble": "PG", "procter and gamble": "PG", "uber": "UBER",
    "airbnb": "ABNB",
    # ── Software / semis / telecom ───────────────────────────────────────
    "salesforce": "CRM", "oracle": "ORCL", "intel": "INTC", "amd": "AMD",
    "qualcomm": "QCOM", "micron": "MU", "tsmc": "TSM", "samsung": "SSNLF",
    "ibm": "IBM", "cisco": "CSCO", "adobe": "ADBE", "palantir": "PLTR",
    "snowflake": "SNOW", "shopify": "SHOP", "verizon": "VZ", "comcast": "CMCSA",
    "t-mobile": "TMUS", "delta air lines": "DAL", "delta airlines": "DAL",
    "united airlines": "UAL", "american airlines": "AAL",
    "southwest airlines": "LUV", "carnival": "CCL", "royal caribbean": "RCL",
    # ── Macro institutions (entity gate for cross-institution pairs) ─────
    "federal reserve": "FED", "the fed": "FED", "fed": "FED", "fomc": "FED",
    "federal open market committee": "FED",
    "european central bank": "ECB", "ecb": "ECB",
    "bank of england": "BOE", "bank of japan": "BOJ", "boj": "BOJ",
    "people's bank of china": "PBOC",
    "international monetary fund": "IMF", "imf": "IMF",
    "opec": "OPEC", "organization of the petroleum exporting countries": "OPEC",
    "u.s. treasury": "UST", "us treasury": "UST", "treasury department": "UST",
}

# Longest aliases first so "bank of england" wins over any shorter overlap.
_ALIAS_PATTERN = re.compile(
    "|".join(
        r"(?<![a-z])" + re.escape(alias) + r"(?![a-z])"
        for alias in sorted(ENTITY_ALIASES, key=len, reverse=True)
    )
)

# Fallback for unlisted tickers: $TICKER or bare ALL-CAPS token (kept from
# the original rss_worker extractor). Excludes common non-ticker ALL-CAPS
# tokens observed in live feeds (CEO, ETF, IPO, APY, CD, PDF, ...).
_TICKER_REGEX = re.compile(r"(?:\$([A-Z]{1,5})\b|\b([A-Z]{2,5})\b)")
_COMMON_EXCLUDE_WORDS = {
    "THE", "AND", "FOR", "NEW", "GDP", "FED", "CPI", "USA", "USD", "EUR",
    "RATE", "BANK", "NEWS", "BILL", "POST", "TECH", "AI", "ECB", "OPEC",
    "IMF", "FOMC", "BOE", "BOJ",
    "CEO", "CFO", "COO", "CTO", "IPO", "ETF", "APY", "CD", "SPAC", "PDF",
    "SPX", "LBO", "CDO", "ABS", "ARM", "UK", "EU", "NYSE",
    "NASDAQ", "SEC", "FTC", "DOJ", "FBI", "CIA", "DOW",
    # Measured false-positive fires on live/corpus headlines:
    "US", "PMI", "FDA",
}


def extract_financial_entities(text: str) -> List[str]:
    """
    Extracts canonical entity codes (tickers + institution codes) from text.
    Dictionary phrase match first, then $TICKER / ALL-CAPS fallback.
    """
    if not text:
        return []
    found: Set[str] = set()

    lowered = text.lower()
    for m in _ALIAS_PATTERN.finditer(lowered):
        found.add(ENTITY_ALIASES[m.group(0)])

    for m in _TICKER_REGEX.findall(text):
        t = m[0] if m[0] else m[1]
        if t and t not in _COMMON_EXCLUDE_WORDS and len(t) <= 5:
            found.add(t.upper())

    return sorted(found)
