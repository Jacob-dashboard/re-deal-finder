"""
Chicago Data Portal scraper — Building Violations + Permits.

Sources (Socrata open data — free, no auth required for basic use):
  - Building Violations:  https://data.cityofchicago.org/resource/22u3-xenr.json
  - Building Permits:     https://data.cityofchicago.org/resource/ydr8-5enu.json

Strategy for deal sourcing:
  1. Pull OPEN violations from the last 2 years in target ZIP codes
  2. Count violations per property address
  3. Properties with 3+ open violations = distressed / value-add opportunity
  4. Cross-reference with permits to flag properties with recent rehab activity
     (renovating neighbor = proof of concept for the block)

This is the MOST RELIABLE scraper in this pipeline — public API, no auth,
stable endpoints, clean JSON data.
"""

import logging
import time
from collections import defaultdict
from datetime import date, timedelta
from typing import Optional
import re

import requests

from scraper import Deal
import config

logger = logging.getLogger(__name__)

VIOLATIONS_URL = f"{config.CHICAGO_DATA_PORTAL_BASE}/{config.CHICAGO_VIOLATIONS_DATASET}"
PERMITS_URL    = f"{config.CHICAGO_DATA_PORTAL_BASE}/ydr8-5enu.json"

# Minimum violations to flag a property as distressed
MIN_VIOLATIONS = 3

# Look back this many days for violations
VIOLATION_LOOKBACK_DAYS = 730  # ~2 years


def _build_headers() -> dict:
    headers = {
        "Accept": "application/json",
        "User-Agent": "re-deal-finder/1.0 (Chicago multifamily research)",
    }
    if config.CHICAGO_APP_TOKEN:
        headers["X-App-Token"] = config.CHICAGO_APP_TOKEN
    return headers


def _socrata_get(url: str, params: dict) -> Optional[list[dict]]:
    """GET a Socrata endpoint with retry and rate limiting."""
    for attempt in range(config.MAX_RETRIES):
        try:
            time.sleep(0.5)  # Chicago portal is generous but be polite
            resp = requests.get(
                url,
                headers=_build_headers(),
                params=params,
                timeout=config.REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                logger.warning("Chicago Data Portal: rate limited, waiting 10s")
                time.sleep(10)
            else:
                logger.warning("Chicago Data Portal: HTTP %s for %s", resp.status_code, url)
                return None
        except Exception as e:
            logger.error("Chicago Data Portal: error on attempt %d: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    return None


def _zip_to_neighborhood(zipcode: str) -> Optional[str]:
    """Map ZIP code to neighborhood."""
    ZIP_MAP = {
        "60618": "Avondale",
        "60623": "Little Village",
        "60608": "Pilsen",
        "60647": "Logan Square",
        "60651": "Humboldt Park",
        "60641": "Belmont Cragin",
        "60632": "Brighton Park",
        "60629": "Gage Park",
        "60639": "Hermosa",
        "60622": "West Town",
        "60612": "West Town",
    }
    return ZIP_MAP.get(zipcode)


def _address_neighborhood(address: str, zip_code: str = "") -> str:
    """Best-effort neighborhood from address or ZIP."""
    if zip_code:
        nbhd = _zip_to_neighborhood(zip_code.strip()[:5])
        if nbhd:
            return nbhd
    return ""


def _normalize_address(addr: str) -> str:
    """Normalize address for deduplication."""
    return re.sub(r"\s+", " ", (addr or "").upper().strip())


def fetch_violations(zip_codes: list[str] = None, limit_per_zip: int = 1000) -> list[dict]:
    """
    Fetch open building violations from Chicago Data Portal.
    Returns raw violation records.

    NOTE: The violations dataset has no zip_code field. We use a lat/lon bounding
    box that covers all target neighborhoods, then filter in Python by street name
    heuristics. Neighborhoods lie in roughly:
      lat: 41.83 – 41.96  lon: -87.64 – -87.74
    """
    # Bounding box covering all target neighborhoods
    # (Little Village, Pilsen, Logan Square, Avondale, Humboldt Park, West Town, etc.)
    LAT_MIN, LAT_MAX = 41.83, 41.96
    LON_MIN, LON_MAX = -87.74, -87.64

    cutoff_date = (date.today() - timedelta(days=VIOLATION_LOOKBACK_DAYS)).isoformat()
    all_violations = []

    params = {
        "$where": (
            f"violation_status='OPEN' "
            f"AND violation_date >= '{cutoff_date}' "
            f"AND latitude >= '{LAT_MIN}' AND latitude <= '{LAT_MAX}' "
            f"AND longitude >= '{LON_MIN}' AND longitude <= '{LON_MAX}'"
        ),
        "$limit": limit_per_zip,
        "$order": "violation_date DESC",
    }
    if config.CHICAGO_APP_TOKEN:
        params["$$app_token"] = config.CHICAGO_APP_TOKEN

    logger.info("Chicago Data: fetching open violations (bounding box, cutoff %s)", cutoff_date)
    rows = _socrata_get(VIOLATIONS_URL, params)
    if rows:
        logger.info("Chicago Data: %d violations returned", len(rows))
        all_violations.extend(rows)
    else:
        logger.warning("Chicago Data: no violations returned")

    return all_violations


def _lat_lon_to_neighborhood(lat: Optional[float], lon: Optional[float]) -> Optional[str]:
    """
    Rough lat/lon → neighborhood mapping using bounding boxes.
    Covers target neighborhoods only.
    """
    if lat is None or lon is None:
        return None
    # Bounding boxes: (lat_min, lat_max, lon_min, lon_max, name)
    BOXES = [
        (41.934, 41.952, -87.720, -87.693, "Avondale"),
        (41.843, 41.858, -87.720, -87.695, "Little Village"),
        (41.851, 41.865, -87.668, -87.649, "Pilsen"),
        (41.915, 41.935, -87.715, -87.688, "Logan Square"),
        (41.897, 41.918, -87.732, -87.710, "Humboldt Park"),
        (41.934, 41.956, -87.756, -87.728, "Belmont Cragin"),
        (41.823, 41.838, -87.712, -87.680, "Brighton Park"),
        (41.823, 41.838, -87.680, -87.650, "Gage Park"),
        (41.823, 41.840, -87.730, -87.712, "Archer Heights"),
        (41.918, 41.940, -87.756, -87.728, "Hermosa"),
        (41.882, 41.905, -87.690, -87.657, "West Town"),
    ]
    for lat_min, lat_max, lon_min, lon_max, name in BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            return name
    return None


def aggregate_by_property(violations: list[dict]) -> dict[str, dict]:
    """
    Group violations by property address.
    Returns: {normalized_address: {count, address, neighborhood, violations:[...]}}
    """
    props: dict[str, dict] = {}

    for v in violations:
        raw_addr = v.get("address", "")
        key = _normalize_address(raw_addr)
        if not key:
            continue

        # Parse lat/lon
        lat = lon = None
        try:
            lat = float(v.get("latitude", ""))
            lon = float(v.get("longitude", ""))
        except (TypeError, ValueError):
            pass

        neighborhood = _lat_lon_to_neighborhood(lat, lon)

        if key not in props:
            props[key] = {
                "address": raw_addr,
                "neighborhood": neighborhood or "",
                "lat": lat,
                "lon": lon,
                "violation_count": 0,
                "violations": [],
                "latest_date": "",
                "violation_types": set(),
            }
        props[key]["violation_count"] += 1
        props[key]["violations"].append(v)
        vdate = v.get("violation_date", "")
        if vdate > props[key]["latest_date"]:
            props[key]["latest_date"] = vdate
        vtype = v.get("violation_description", "")
        if vtype:
            props[key]["violation_types"].add(vtype[:60])

    # Convert sets to lists for JSON serialization
    for p in props.values():
        p["violation_types"] = list(p["violation_types"])

    return props


def _prop_to_deal(addr_data: dict) -> Deal:
    """Convert aggregated property violation data to a Deal."""
    address      = addr_data["address"]
    neighborhood = addr_data.get("neighborhood", "")

    return Deal(
        source="chicago_violations",
        channel="off_market",
        off_market=True,
        address=address,
        city="Chicago",
        state="IL",
        neighborhood=neighborhood,
        latitude=addr_data.get("lat"),
        longitude=addr_data.get("lon"),
        asset_class="unknown",   # violations don't tell us unit count
        violation_count=addr_data["violation_count"],
        listing_date=addr_data.get("latest_date", ""),
        raw={
            "violation_types": addr_data.get("violation_types", []),
            "violation_count": addr_data["violation_count"],
        },
    )


def scrape(dry_run: bool = False, limit: int = 100) -> list[Deal]:
    """
    Main entry point. Returns Deal objects for distressed properties
    with multiple open code violations.
    dry_run=True returns synthetic stub data.
    """
    if dry_run:
        logger.info("Chicago Violations: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Chicago Violations: fetching open violations for %d ZIP codes", len(config.TARGET_ZIP_CODES))
    violations = fetch_violations()

    if not violations:
        logger.warning("Chicago Violations: no violations data retrieved")
        return []

    logger.info("Chicago Violations: %d total violation records, aggregating by property", len(violations))
    by_property = aggregate_by_property(violations)

    # Filter: only properties with MIN_VIOLATIONS or more open violations
    distressed = {
        addr: data for addr, data in by_property.items()
        if data["violation_count"] >= MIN_VIOLATIONS
    }

    logger.info(
        "Chicago Violations: %d/%d properties have %d+ open violations",
        len(distressed), len(by_property), MIN_VIOLATIONS,
    )

    # Convert to Deals, sorted by violation count desc
    deals = []
    for addr_data in sorted(distressed.values(), key=lambda x: x["violation_count"], reverse=True):
        deals.append(_prop_to_deal(addr_data))
        if len(deals) >= limit:
            break

    logger.info("Chicago Violations: returning %d distressed property deals", len(deals))
    return deals


def scrape_live_sample(neighborhood: str = "Little Village", limit: int = 10) -> list[Deal]:
    """
    Convenience function: fetch a live sample for a target neighborhood.
    Uses lat/lon bounding box (dataset has no zip_code field).
    Used for end-to-end pipeline validation.
    """
    # Bounding boxes per neighborhood
    SAMPLE_BOXES = {
        "Little Village": (41.843, 41.858, -87.720, -87.695),
        "Logan Square":   (41.915, 41.935, -87.715, -87.688),
        "Avondale":       (41.934, 41.952, -87.720, -87.693),
        "West Town":      (41.882, 41.905, -87.690, -87.657),
        "Humboldt Park":  (41.897, 41.918, -87.732, -87.710),
    }
    box = SAMPLE_BOXES.get(neighborhood, SAMPLE_BOXES["Little Village"])
    lat_min, lat_max, lon_min, lon_max = box

    logger.info("Chicago Violations (live sample): %s, limit %d", neighborhood, limit)

    params = {
        "$where": (
            f"violation_status='OPEN' "
            f"AND latitude >= '{lat_min}' AND latitude <= '{lat_max}' "
            f"AND longitude >= '{lon_min}' AND longitude <= '{lon_max}'"
        ),
        "$limit": 500,
        "$order": "violation_date DESC",
    }
    rows = _socrata_get(VIOLATIONS_URL, params)
    if not rows:
        return []

    logger.info("Chicago Violations (live sample): %d raw rows", len(rows))
    by_property = aggregate_by_property(rows)
    distressed = {a: d for a, d in by_property.items() if d["violation_count"] >= MIN_VIOLATIONS}
    logger.info("Chicago Violations (live sample): %d distressed properties", len(distressed))

    deals = []
    for addr_data in sorted(distressed.values(), key=lambda x: x["violation_count"], reverse=True)[:limit]:
        deals.append(_prop_to_deal(addr_data))

    return deals


def _stub_deals() -> list[Deal]:
    """Synthetic deals for dry-run / testing."""
    return [
        Deal(
            source="chicago_violations",
            channel="off_market",
            off_market=True,
            address="2205 S Millard Ave",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="unknown",
            violation_count=7,
            raw={"violation_types": ["HEAT", "RODENT", "EGRESS"], "stub": True},
        ),
        Deal(
            source="chicago_violations",
            channel="off_market",
            off_market=True,
            address="3312 W Armitage Ave",
            city="Chicago",
            state="IL",
            zip_code="60647",
            neighborhood="Logan Square",
            asset_class="unknown",
            violation_count=5,
            raw={"violation_types": ["PLUMBING", "ELECTRICAL"], "stub": True},
        ),
        Deal(
            source="chicago_violations",
            channel="off_market",
            off_market=True,
            address="4017 W Fullerton Ave",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Hermosa",
            asset_class="unknown",
            violation_count=4,
            raw={"violation_types": ["HEAT", "ROOF"], "stub": True},
        ),
        Deal(
            source="chicago_violations",
            channel="off_market",
            off_market=True,
            address="3605 W 26th St",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="unknown",
            violation_count=9,
            raw={"violation_types": ["HEAT", "RODENT", "EGRESS", "ELECTRICAL", "PLUMBING"], "stub": True},
        ),
        Deal(
            source="chicago_violations",
            channel="off_market",
            off_market=True,
            address="4521 W Armitage Ave",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Belmont Cragin",
            asset_class="unknown",
            violation_count=3,
            raw={"violation_types": ["EGRESS", "HEAT"], "stub": True},
        ),
    ]
