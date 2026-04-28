"""
Cross-source deduplication.

Catches the same property listed on multiple channels (LoopNet + Crexi, etc.)
via address normalization + fuzzy matching, merges the records (preferring the
more complete one and the lower price), and tags the merged deal with
`sources=[...]` so downstream scoring can give a multi-source confidence boost.
"""

from __future__ import annotations

import dataclasses
import logging
import re
from difflib import SequenceMatcher
from typing import Optional

from scraper import Deal

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Address normalization
# ---------------------------------------------------------------------------

# Order matters here: longer/more-specific tokens first, with word-boundary
# regexes so "St" doesn't eat "Street".
_ABBREVIATIONS = [
    (r"\bSTREET\b",     "ST"),
    (r"\bAVENUE\b",     "AVE"),
    (r"\bBOULEVARD\b",  "BLVD"),
    (r"\bDRIVE\b",      "DR"),
    (r"\bCOURT\b",      "CT"),
    (r"\bPLACE\b",      "PL"),
    (r"\bROAD\b",       "RD"),
    (r"\bLANE\b",       "LN"),
    (r"\bTERRACE\b",    "TER"),
    (r"\bPARKWAY\b",    "PKWY"),
    (r"\bHIGHWAY\b",    "HWY"),
    (r"\bNORTH\b",      "N"),
    (r"\bSOUTH\b",      "S"),
    (r"\bEAST\b",       "E"),
    (r"\bWEST\b",       "W"),
    (r"\bNORTHEAST\b",  "NE"),
    (r"\bNORTHWEST\b",  "NW"),
    (r"\bSOUTHEAST\b",  "SE"),
    (r"\bSOUTHWEST\b",  "SW"),
]

# Unit/apt/suite designators — strip everything from these onward.
_UNIT_RE = re.compile(
    r"\b(?:APT|APARTMENT|UNIT|SUITE|STE|#|NO\.?|BLDG|BUILDING|FL|FLOOR|RM|ROOM)"
    r"\b\.?\s*[A-Z0-9-]*",
    re.IGNORECASE,
)

# Range hyphens in street numbers: "3920-34 S Calumet" → "3920 S Calumet"
_RANGE_RE = re.compile(r"^(\d+)\s*-\s*\d+\b")


def normalize_address(addr: str) -> str:
    """Canonicalize an address for comparison.

    "3920-34 S Calumet Ave" and "3920 South Calumet Avenue" both → "3920 S CALUMET AVE".
    """
    if not addr:
        return ""

    s = addr.upper()
    # Strip unit/apt/suite tail before doing anything else
    s = _UNIT_RE.sub(" ", s)
    # Collapse range numbers ("3920-34" → "3920")
    s = _RANGE_RE.sub(r"\1", s)
    # Drop punctuation
    s = re.sub(r"[.,#]", " ", s)
    # Apply directional + street-type abbreviations
    for pattern, replacement in _ABBREVIATIONS:
        s = re.sub(pattern, replacement, s)
    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()
    return s


def address_similarity(a: str, b: str) -> float:
    """Fuzzy similarity ratio in [0.0, 1.0] between two normalized addresses."""
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

# Fields that count toward "completeness" when picking the better record,
# and that get filled in on the surviving record from secondaries.
_COMPLETENESS_FIELDS = (
    "address", "city", "zip_code", "neighborhood",
    "asset_class", "units", "sqft", "year_built",
    "price", "price_per_unit", "price_per_sqft", "cap_rate", "noi",
    "gross_rent", "market_rent_est",
    "days_on_market", "listing_date",
    "broker", "broker_phone", "broker_email",
    "latitude", "longitude",
    # off-market signals — important to carry forward when an off-market
    # source matches an on-market listing
    "tax_delinquency_amount", "violation_count",
    "foreclosure_date", "foreclosure_case",
)


def _completeness(d: Deal) -> int:
    return sum(
        1 for name in _COMPLETENESS_FIELDS
        if getattr(d, name, None) not in (None, "", 0)
    )


def _merge(primary: Deal, secondary: Deal) -> Deal:
    """Merge `secondary` into `primary` field-by-field.

    `primary` is the more complete record. Empty fields on `primary` get filled
    in from `secondary`. Price falls to the lower of the two (a price drop is
    more interesting than the higher number).
    """
    for name in _COMPLETENESS_FIELDS:
        p_val = getattr(primary, name, None)
        s_val = getattr(secondary, name, None)
        if (p_val in (None, "", 0)) and s_val not in (None, "", 0):
            setattr(primary, name, s_val)

    # Always keep the lowest non-null price
    if primary.price is not None and secondary.price is not None:
        primary.price = min(primary.price, secondary.price)
        if primary.units and primary.units > 0:
            primary.price_per_unit = primary.price / primary.units
        if primary.sqft and primary.sqft > 0:
            primary.price_per_sqft = primary.price / primary.sqft

    # Track all sources this deal appeared on
    combined = set(primary.sources or [primary.source]) | set(secondary.sources or [secondary.source])
    primary.sources = sorted(s for s in combined if s)

    return primary


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def deduplicate(
    deals: list[Deal],
    similarity_threshold: float = 0.85,
) -> tuple[list[Deal], dict]:
    """Cross-source dedup.

    Returns (unique_deals, stats) where stats is::

        {"raw": int, "unique": int, "merged": int, "cross_source_merges": int}

    A "cross_source_merge" is one where the merged sources span > 1 distinct
    listing site (e.g. crexi + loopnet). Same-site dupes still count toward
    `merged` but not `cross_source_merges`.
    """
    unique: list[Deal] = []
    seen_urls: dict[str, int] = {}  # url → index in unique
    merged_count = 0
    cross_source_merges = 0

    for incoming in deals:
        # Initialize the sources list if the scraper didn't (most haven't been touched).
        if not incoming.sources:
            incoming.sources = [incoming.source] if incoming.source else []

        match_idx: Optional[int] = None

        # 1. Exact URL match (cheap, definitive)
        url_key = (incoming.url or "").strip().lower().rstrip("/")
        if url_key and url_key in seen_urls:
            match_idx = seen_urls[url_key]

        # 2. Fuzzy address match across already-accepted unique deals
        if match_idx is None:
            norm_in = normalize_address(incoming.address)
            if norm_in:
                best_score = 0.0
                for i, existing in enumerate(unique):
                    norm_ex = normalize_address(existing.address)
                    if not norm_ex:
                        continue
                    score = address_similarity(norm_in, norm_ex)
                    if score > best_score and score >= similarity_threshold:
                        best_score = score
                        match_idx = i

        if match_idx is not None:
            existing = unique[match_idx]
            # Pick the more complete record as primary
            if _completeness(incoming) > _completeness(existing):
                primary, secondary = incoming, existing
            else:
                primary, secondary = existing, incoming

            merged = _merge(primary, secondary)
            unique[match_idx] = merged

            # Re-index URL pointer if the surviving record's URL is new
            new_url = (merged.url or "").strip().lower().rstrip("/")
            if new_url:
                seen_urls[new_url] = match_idx

            merged_count += 1
            if len(merged.sources) > 1:
                cross_source_merges += 1
                logger.info(
                    "DEDUP: merged %s from %s",
                    merged.address or merged.url or "<no addr>",
                    " + ".join(merged.sources),
                )
            continue

        # No match — accept as new unique
        unique.append(incoming)
        if url_key:
            seen_urls[url_key] = len(unique) - 1

    stats = {
        "raw": len(deals),
        "unique": len(unique),
        "merged": merged_count,
        "cross_source_merges": cross_source_merges,
    }
    logger.info(
        "Dedup: %d raw → %d unique (%d merged, %d cross-source)",
        stats["raw"], stats["unique"], stats["merged"], stats["cross_source_merges"],
    )
    return unique, stats
