"""
Shared financial polarity lexicon — single source of truth for both
Tier 2 (MinHash LSH polarity anchors) and Tier 3 (semantic merge polarity guard).

Design notes
------------
- Class-based, not keyword-equality: two headlines conflict only when one is
  positive-only and the other negative-only. Plain set inequality (the previous
  Tier 3 rule) wrongly blocked true duplicates like "Tesla beats Q3 estimates"
  vs "Tesla beats Q3 estimates, shares rise" because the sets differ.
- Inflection normalization resolves -s/-es/-ies/-ed/-ing forms, so "missed",
  "tumbled", "surged", "hiking" map to their lexicon base forms.
- A small negation window ("not", "fails to", "without", ...) within 3 tokens
  before a polarity word flips its class ("did not beat" -> negative).
  This is a heuristic, not a sentiment model; the calibration script
  (scripts/calibrate_tier3.py) measures its failure rate.
"""

import re
from typing import List, Optional, Set, Tuple

POSITIVE = frozenset({
    "beat", "surge", "soar", "spike", "jump", "rally", "rebound",
    "climb", "gain", "rise", "raise", "hike", "boost", "upgrade",
    "exceed", "outperform", "bullish", "record", "strong", "top",
})

NEGATIVE = frozenset({
    "miss", "plunge", "plummet", "crash", "sink", "fall", "drop",
    "decline", "tumble", "slump", "slide", "loss", "cut", "lower",
    "downgrade", "underperform", "bearish", "warn", "weak", "slash",
})

# All base forms; kept for callers that just need "is this a polarity word".
POLARITY_KEYWORDS = POSITIVE | NEGATIVE

# Tokens that flip the class of a following polarity word within the window.
NEGATORS = frozenset({
    "not", "no", "never", "without", "except", "excluding",
    "fail", "fails", "failing",
    # tokenizer splits contractions ("didn't" -> "didn t")
    "didn", "wasn", "isn", "aren", "weren", "don", "doesn",
    "couldn", "wouldn", "shouldn", "won",
})

NEGATION_WINDOW = 3  # tokens to look back from a polarity word

# Irregular past-tense forms that suffix stripping cannot resolve.
IRREGULAR_FORMS = {
    "rose": "rise", "fell": "fall", "sank": "sink", "slid": "slide",
    "shrank": "shrink", "sprang": "spring",
}

# Word-level antonym map (symmetric). A conflict requires an actual
# opposite-direction WORD PAIR across the two profiles — sharing a negative
# word while using different positive verbs ("surges ... cuts" vs
# "rallies ... cuts") is a same-direction paraphrase, not a conflict.
ANTONYMS = {
    # earnings/results dimension
    "beat": {"miss", "underperform"}, "miss": {"beat", "top", "exceed", "outperform"},
    "top": {"miss"}, "exceed": {"miss", "underperform"},
    "outperform": {"miss", "underperform"}, "underperform": {"beat", "top", "exceed", "outperform"},
    # rates/actions dimension
    "hike": {"cut", "slash"}, "raise": {"cut", "slash"}, "boost": {"cut", "slash"},
    "cut": {"hike", "raise", "boost"}, "slash": {"hike", "raise", "boost"},
    # price-action dimension
    "surge": {"plunge", "plummet", "crash", "slump", "sink"},
    "soar": {"plunge", "plummet", "crash", "slump", "sink"},
    "spike": {"plunge", "plummet", "crash", "slump", "sink"},
    "jump": {"plunge", "plummet", "sink", "slump", "tumble"},
    "rally": {"plunge", "plummet", "crash", "sink", "slump"},
    "rebound": {"plunge", "plummet", "crash", "slump"},
    "climb": {"tumble", "plunge", "plummet", "crash", "slump", "sink", "fall"},
    "rise": {"fall", "drop", "decline", "sink", "slump", "plunge", "plummet", "tumble"},
    "gain": {"loss", "drop", "fall", "decline"},
    "plunge": {"surge", "soar", "spike", "jump", "rally", "rebound", "climb", "rise"},
    "plummet": {"surge", "soar", "spike", "jump", "rally", "rebound", "climb", "rise"},
    "crash": {"surge", "soar", "spike", "rally", "rebound", "climb", "rise"},
    "slump": {"surge", "soar", "spike", "jump", "rally", "rebound", "climb", "rise"},
    "sink": {"surge", "soar", "spike", "jump", "rally", "climb", "rise"},
    "tumble": {"jump", "climb", "rise", "rally", "rebound", "gain"},
    "fall": {"rise", "jump", "surge", "soar", "spike", "rally", "climb", "gain", "rebound"},
    "drop": {"rise", "jump", "surge", "rally", "climb", "gain", "rebound"},
    "decline": {"rise", "surge", "jump", "rally", "rebound", "gain", "climb"},
    "loss": {"gain"},
    # ratings/sentiment dimension
    "upgrade": {"downgrade"}, "downgrade": {"upgrade"},
    "bullish": {"bearish"}, "bearish": {"bullish"},
    "strong": {"weak"}, "weak": {"strong"},
    "record": frozenset(),  # no antonym
}


def _antonyms_of(words: Set[str]) -> Set[str]:
    out: Set[str] = set()
    for w in words:
        out |= ANTONYMS.get(w, frozenset())
    return out

_TOKEN_SPLIT = re.compile(r"[^\w\s]")


def tokenize(text: str) -> List[str]:
    """Lowercases and strips punctuation, returning word tokens."""
    return _TOKEN_SPLIT.sub(" ", text.lower()).split()


def base_form(token: str) -> Optional[str]:
    """Resolves an inflected token to its polarity lexicon base form, if any."""
    if token in POLARITY_KEYWORDS:
        return token
    irregular = IRREGULAR_FORMS.get(token)
    if irregular and irregular in POLARITY_KEYWORDS:
        return irregular
    candidates: List[str] = []
    if token.endswith("ies") and len(token) > 4:
        candidates.append(token[:-3] + "y")          # rallies -> rally
    if token.endswith("es") and len(token) > 3:
        candidates.append(token[:-2])                # misses -> miss
    if token.endswith("s") and len(token) > 2:
        candidates.append(token[:-1])                # beats -> beat
    if token.endswith("ed") and len(token) > 3:
        candidates.append(token[:-2])                # missed -> miss
        candidates.append(token[:-1])                # raised -> raise
        stem = token[:-2]
        if len(stem) > 2 and stem[-1] == stem[-2]:
            candidates.append(stem[:-1])             # topped -> top, stopped -> stop
    if token.endswith("ing") and len(token) > 5:
        stem = token[:-3]
        candidates.append(stem)                      # cutting -> cutt
        candidates.append(stem + "e")                # hiking -> hike
        if len(stem) > 2 and stem[-1] == stem[-2]:
            candidates.append(stem[:-1])             # cutting -> cut
    for cand in candidates:
        if cand in POLARITY_KEYWORDS:
            return cand
    return None


def polarity_profile_from_tokens(tokens: List[str]) -> Tuple[Set[str], Set[str]]:
    """
    Computes (positive_hits, negative_hits) base-form sets for a token list,
    applying the negation window.
    """
    pos: Set[str] = set()
    neg: Set[str] = set()
    for i, tok in enumerate(tokens):
        base = base_form(tok)
        if base is None:
            continue
        is_negative = base in NEGATIVE
        window = tokens[max(0, i - NEGATION_WINDOW):i]
        if any(w in NEGATORS for w in window):
            is_negative = not is_negative
        (neg if is_negative else pos).add(base)
    return pos, neg


def polarity_profile(text: str) -> Tuple[Set[str], Set[str]]:
    """Computes (positive_hits, negative_hits) base-form sets for a headline."""
    return polarity_profile_from_tokens(tokenize(text))


def has_conflict(profile_a: Tuple[Set[str], Set[str]],
                 profile_b: Tuple[Set[str], Set[str]]) -> bool:
    """
    True when the profiles contain an actual opposite-direction word pair:

      1. Same word, opposite class — covers negation ("beat" vs "did not beat").
      2. A positive word on one side whose ANTONYM appears as negative on the
         other (beat vs miss, hike vs cut, upgrade vs downgrade, strong vs weak).

    Deliberately NOT conflicts:
      - Plain set differences: "beats Q3 estimates" vs "beats Q3 estimates,
        shares rise" (pos {beat} vs {beat, rise}) — same direction, true dup.
      - Shared negative + different positive verbs: "surges ... cuts" vs
        "rallies ... cuts" — same-direction paraphrase (mixed vs mixed).
      - Any pair where one side has no polarity signal at all.
    """
    pos_a, neg_a = profile_a
    pos_b, neg_b = profile_b
    same_word_opposite = bool((pos_a & neg_b) or (pos_b & neg_a))
    antonym_clash = bool(
        (pos_a & _antonyms_of(neg_b)) or (pos_b & _antonyms_of(neg_a))
    )
    return same_word_opposite or antonym_clash


def serialize_profile(profile: Tuple[Set[str], Set[str]]) -> str:
    """Serializes a profile for Redis storage: '+beat,+rally,-miss'."""
    pos, neg = profile
    parts = ["+" + w for w in sorted(pos)] + ["-" + w for w in sorted(neg)]
    return ",".join(parts)


def deserialize_profile(raw: str) -> Tuple[Set[str], Set[str]]:
    """Inverse of serialize_profile. Empty/garbage input yields empty sets."""
    pos: Set[str] = set()
    neg: Set[str] = set()
    for part in (raw or "").split(","):
        part = part.strip()
        if part.startswith("+"):
            pos.add(part[1:])
        elif part.startswith("-"):
            neg.add(part[1:])
    return pos, neg
