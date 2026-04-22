"""
Cook County off-market scraper — tax delinquency + recorder of deeds.

Sources:
  1. Cook County Treasurer — annual delinquent tax sale list (CSV/JSON)
     https://www.cookcountytreasurer.com/  (published each spring before sale)
  2. Cook County Assessor open data (DataCatalog)
     https://datacatalog.cookcountyil.gov/

Workflow:
  - Pull properties with open/unpaid tax balances
  - Filter for multifamily in target neighborhoods (via PIN lookup in assessor data)
  - Cross-ref with property address to neighborhood mapping
  - Output as Deal objects (off_market=True)

NOTE: The Treasurer's delinquency CSV is published seasonally (typically March-April
before the annual tax sale). This scraper caches the last-known URL pattern and
falls back gracefully when the file is unavailable.
"""

import csv
import io
import logging
import re
import time
from typing import Optional

import requests

from scraper import Deal
import config

logger = logging.getLogger(__name__)

# Cook County DataCatalog (Socrata) — Assessor parcel data
ASSESSOR_API = "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json"  # Residential & Commercial characteristics

# Cook County Treasurer SCAVENGER / Annual tax sale — changes URL annually
# Pattern: https://www.cookcountytreasurer.com/pdfs/YYYY_scavenger_sale.csv
# We'll try the current year and fall back to prior year
import datetime
_CURRENT_YEAR = datetime.date.today().year
TREASURER_CSV_URLS = [
    f"https://www.cookcountytreasurer.com/pdfs/{_CURRENT_YEAR}_annual_tax_sale.csv",
    f"https://www.cookcountytreasurer.com/pdfs/{_CURRENT_YEAR - 1}_annual_tax_sale.csv",
    # Scavenger sale (every 2 years)
    f"https://www.cookcountytreasurer.com/pdfs/{_CURRENT_YEAR}_scavenger_sale.csv",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,text/csv,*/*;q=0.8",
}

# Multifamily property class codes (Cook County Assessor classification)
MF_CLASS_CODES = {
    "211": "two-flat",
    "212": "three-flat",
    "213": "small apartment 4 units",
    "214": "small apartment 5-6 units",
    "215": "small apartment 7+ units",
    "218": "rooming house",
    "234": "commercial / residential mixed-use",
    "295": "residential cooperative",
    "299": "residential miscellaneous",
    "313": "apartment building 7-24 units",
    "314": "apartment building 25-99 units",
    "315": "apartment building 100+ units",
    "318": "residential hotel",
    "390": "mixed-use commercial/residential",
    "391": "mixed-use commercial/residential",
    "399": "commercial miscellaneous w/ residential",
}


def _fetch_csv(url: str) -> Optional[list[dict]]:
    """Attempt to fetch a CSV file, return rows as list of dicts."""
    try:
        time.sleep(1.0)
        resp = requests.get(url, headers=HEADERS, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 200 and resp.content:
            content = resp.text
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        logger.debug("Cook County CSV: HTTP %s for %s", resp.status_code, url)
        return None
    except Exception as e:
        logger.debug("Cook County CSV: error fetching %s: %s", url, e)
        return None


def _fetch_assessor_data(pin: str) -> Optional[dict]:
    """
    Look up a PIN in the Cook County Assessor data catalog.
    Returns property characteristics including classification and address.
    """
    try:
        params = {"pin": pin, "$limit": 1}
        resp = requests.get(
            ASSESSOR_API,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data[0] if data else None
    except Exception:
        pass
    return None


def _normalize_pin(raw: str) -> str:
    """Normalize PIN to 14-digit format with dashes: XX-XX-XXX-XXX-XXXX."""
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 14:
        return f"{digits[:2]}-{digits[2:4]}-{digits[4:7]}-{digits[7:10]}-{digits[10:]}"
    return digits


def _address_in_target_neighborhood(address: str) -> Optional[str]:
    """Heuristic: check if address string contains a target neighborhood keyword."""
    upper = address.upper()
    for nbhd in config.TARGET_NEIGHBORHOODS:
        if nbhd.upper() in upper:
            return nbhd
    # ZIP code fallback
    for zipcode in config.TARGET_ZIP_CODES:
        if zipcode in address:
            return f"ZIP {zipcode}"
    return None


def _zip_to_neighborhood(zipcode: str) -> Optional[str]:
    """Map ZIP code to neighborhood name."""
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


def _csv_row_to_deal(row: dict) -> Optional[Deal]:
    """
    Convert a treasurer CSV row to a Deal.
    Column names vary by year — handle common variants.
    """
    try:
        # Normalize keys
        row = {k.strip().lower().replace(" ", "_"): (v or "").strip() for k, v in row.items()}

        pin = _normalize_pin(
            row.get("pin", "") or row.get("property_index_number", "") or row.get("parcel", "")
        )
        if not pin:
            return None

        address = (
            row.get("address", "")
            or row.get("property_address", "")
            or row.get("site_address", "")
        )
        city    = row.get("city", "Chicago")
        zipcode = row.get("zip", "") or row.get("zip_code", "")
        neighborhood = _zip_to_neighborhood(zipcode) or _address_in_target_neighborhood(address)

        # Skip properties NOT in target neighborhoods
        if not neighborhood:
            return None

        # Parse tax delinquency amount
        raw_amount = row.get("total_amount_due", "") or row.get("amount_due", "") or row.get("taxes_due", "")
        amount = None
        try:
            amount = float(re.sub(r"[^\d.]", "", raw_amount))
        except (ValueError, TypeError):
            pass

        # Property class — filter for multifamily codes
        prop_class = row.get("class", "") or row.get("property_class", "") or row.get("classification", "")
        if prop_class and prop_class not in MF_CLASS_CODES:
            # Skip non-multifamily if we have class info
            # If no class info, include it (we can't know)
            if prop_class:
                return None

        asset_class = MF_CLASS_CODES.get(prop_class, "multifamily")

        deal = Deal(
            source="cook_county_tax",
            channel="off_market",
            off_market=True,
            external_id=pin,
            address=address,
            city=city,
            state="IL",
            zip_code=zipcode,
            neighborhood=neighborhood,
            asset_class=asset_class,
            tax_delinquency_amount=amount,
            raw={k: v for k, v in row.items() if v},
        )
        return deal
    except Exception as e:
        logger.debug("Cook County: error parsing row: %s", e)
        return None


def scrape(dry_run: bool = False, limit: int = 100) -> list[Deal]:
    """
    Main entry point. Returns Deal objects for tax-delinquent multifamily.
    dry_run=True returns synthetic stub data.
    """
    if dry_run:
        logger.info("Cook County Tax: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Cook County Tax: attempting to fetch delinquency CSV")
    rows = None
    for url in TREASURER_CSV_URLS:
        logger.info("Cook County Tax: trying %s", url)
        rows = _fetch_csv(url)
        if rows:
            logger.info("Cook County Tax: fetched %d rows from %s", len(rows), url)
            break

    if not rows:
        logger.warning(
            "Cook County Tax: no CSV available from known URLs. "
            "The annual sale list may not be published yet, or URLs have changed. "
            "Returning empty results."
        )
        return []

    deals: list[Deal] = []
    for row in rows:
        if len(deals) >= limit:
            break
        deal = _csv_row_to_deal(row)
        if deal:
            deals.append(deal)

    logger.info("Cook County Tax: %d qualified deals found", len(deals))
    return deals


def _stub_deals() -> list[Deal]:
    """Synthetic deals for dry-run / testing."""
    return [
        Deal(
            source="cook_county_tax",
            channel="off_market",
            off_market=True,
            external_id="17-28-318-015-0000",
            address="3527 W 26th St",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="small apartment 7+ units",
            units=8,
            tax_delinquency_amount=18_450.00,
            raw={"stub": True, "class": "215"},
        ),
        Deal(
            source="cook_county_tax",
            channel="off_market",
            off_market=True,
            external_id="13-25-207-008-0000",
            address="2821 N Pulaski Rd",
            city="Chicago",
            state="IL",
            zip_code="60641",
            neighborhood="Belmont Cragin",
            asset_class="small apartment 4 units",
            units=None,
            tax_delinquency_amount=9_200.00,
            raw={"stub": True, "class": "213"},
        ),
        Deal(
            source="cook_county_tax",
            channel="off_market",
            off_market=True,
            external_id="16-24-115-022-0000",
            address="4318 S Kedzie Ave",
            city="Chicago",
            state="IL",
            zip_code="60632",
            neighborhood="Brighton Park",
            asset_class="mixed-use commercial/residential",
            units=6,
            tax_delinquency_amount=34_100.00,
            raw={"stub": True, "class": "390"},
        ),
    ]
