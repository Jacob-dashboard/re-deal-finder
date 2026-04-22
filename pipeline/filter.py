"""
Deal filter — applies config criteria and deduplicates across sources.

Criteria applied (all from config.py):
  - Price <= MAX_PRICE
  - Cap rate >= MIN_CAP_RATE  (skipped if cap rate unknown)
  - Units >= MIN_UNITS        (skipped if units unknown)
  - Asset class in ASSET_CLASSES
  - Neighborhood in TARGET_NEIGHBORHOODS (not in EXCLUDE_NEIGHBORHOODS)
  - Not a duplicate (same address/URL across sources)
"""

import logging
import re
from typing import Optional

from scraper import Deal
import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Normalize helpers
# ---------------------------------------------------------------------------

def _normalize_address(addr: str) -> str:
    """Strip noise for dedup comparison."""
    addr = (addr or "").upper()
    addr = re.sub(r"\s+", " ", addr)
    addr = re.sub(r"[.,#]", "", addr)
    addr = addr.strip()
    return addr


def _neighborhood_tier(neighborhood: str) -> str:
    """Return 'target', 'excluded', or 'unknown'."""
    if not neighborhood:
        return "unknown"
    nbhd_lower = neighborhood.lower()
    for n in config.TARGET_NEIGHBORHOODS:
        if n.lower() in nbhd_lower or nbhd_lower in n.lower():
            return "target"
    for n in config.EXCLUDE_NEIGHBORHOODS:
        if n.lower() in nbhd_lower or nbhd_lower in n.lower():
            return "excluded"
    return "unknown"


def _asset_class_ok(deal: Deal) -> bool:
    """Return True if deal asset class matches config."""
    if not deal.asset_class or deal.asset_class.lower() in ("unknown", ""):
        return True  # unknown = allow (can't filter what we don't know)
    ac = deal.asset_class.lower()
    return any(allowed in ac or ac in allowed for allowed in config.ASSET_CLASSES)


def _neighborhood_ok(deal: Deal) -> bool:
    """Return True if neighborhood is not in the excluded list."""
    tier = _neighborhood_tier(deal.neighborhood)
    if tier == "excluded":
        return False
    # Also check zip code against excluded neighborhoods (belt-and-suspenders)
    zip_excluded = {
        "60621": "Englewood",
        "60636": "West Englewood",
        "60628": "Roseland",
        "60619": "Chatham",
        "60617": "South Shore",
        "60644": "Austin",
        "60624": "West Garfield Park",
        "60624": "East Garfield Park",
        "60623": None,   # Little Village — override (it's a TARGET), not excluded
    }
    if deal.zip_code:
        excluded_nbhd = zip_excluded.get(deal.zip_code)
        if excluded_nbhd is not None:
            return False
    return True


# ---------------------------------------------------------------------------
# Primary filter
# ---------------------------------------------------------------------------

def passes_criteria(deal: Deal) -> tuple[bool, list[str]]:
    """
    Returns (passed, [reasons_if_rejected]).
    reasons list is empty when passed=True.
    """
    reasons = []

    # Price
    if deal.price is not None and deal.price > config.MAX_PRICE:
        reasons.append(f"price ${deal.price:,.0f} > max ${config.MAX_PRICE:,.0f}")

    # Cap rate (only fail if we have a value AND it's below minimum)
    if deal.cap_rate is not None and deal.cap_rate < config.MIN_CAP_RATE:
        reasons.append(f"cap rate {deal.cap_rate:.1f}% < min {config.MIN_CAP_RATE}%")

    # Units (only fail if we have a value AND it's below minimum)
    if deal.units is not None and deal.units < config.MIN_UNITS:
        reasons.append(f"units {deal.units} < min {config.MIN_UNITS}")

    # Asset class
    if not _asset_class_ok(deal):
        reasons.append(f"asset class '{deal.asset_class}' not in {config.ASSET_CLASSES}")

    # Neighborhood exclusion
    if not _neighborhood_ok(deal):
        reasons.append(f"neighborhood '{deal.neighborhood}' is excluded")

    return (len(reasons) == 0, reasons)


def filter_deals(deals: list[Deal], verbose: bool = False) -> list[Deal]:
    """
    Apply all criteria filters. Returns list of passing deals.
    Logs reason for each rejection when verbose=True.
    """
    passed = []
    rejected = 0
    for deal in deals:
        ok, reasons = passes_criteria(deal)
        if ok:
            passed.append(deal)
        else:
            rejected += 1
            if verbose:
                logger.debug(
                    "FILTERED OUT: %s [%s] — %s",
                    deal.address or deal.url,
                    deal.source,
                    "; ".join(reasons),
                )

    logger.info(
        "Filter: %d in → %d passed, %d rejected",
        len(deals), len(passed), rejected,
    )
    return passed


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def deduplicate(deals: list[Deal]) -> list[Deal]:
    """
    Remove duplicate deals across sources.
    Dedup key: normalized address (primary) or URL (secondary).
    When duplicates exist, prefer the one with more data (more fields filled).
    """
    seen_addresses: dict[str, Deal] = {}
    seen_urls: dict[str, Deal] = {}
    unique: list[Deal] = []
    dupes = 0

    def _completeness(d: Deal) -> int:
        """Count non-None, non-empty scalar fields."""
        import dataclasses
        return sum(
            1 for f in dataclasses.fields(d)
            if getattr(d, f.name) not in (None, "", [], {})
            and f.name not in ("raw", "score_breakdown")
        )

    for deal in deals:
        addr_key = _normalize_address(deal.address)
        url_key  = (deal.url or "").strip().lower().rstrip("/")

        # Check if we've seen this URL before
        if url_key and url_key in seen_urls:
            existing = seen_urls[url_key]
            if _completeness(deal) > _completeness(existing):
                seen_urls[url_key] = deal
                # Swap in unique list
                idx = unique.index(existing)
                unique[idx] = deal
            dupes += 1
            continue

        # Check address match (only for non-empty addresses)
        if addr_key and addr_key in seen_addresses:
            existing = seen_addresses[addr_key]
            if _completeness(deal) > _completeness(existing):
                seen_addresses[addr_key] = deal
                idx = unique.index(existing)
                unique[idx] = deal
            dupes += 1
            continue

        # New unique deal
        if addr_key:
            seen_addresses[addr_key] = deal
        if url_key:
            seen_urls[url_key] = deal
        unique.append(deal)

    logger.info("Dedup: %d in → %d unique, %d duplicates removed", len(deals), len(unique), dupes)
    return unique


# ---------------------------------------------------------------------------
# Combined entry point
# ---------------------------------------------------------------------------

def apply(deals: list[Deal], verbose: bool = False) -> list[Deal]:
    """Filter then deduplicate. Returns clean list."""
    filtered = filter_deals(deals, verbose=verbose)
    return deduplicate(filtered)
