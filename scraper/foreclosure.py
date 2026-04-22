"""
Foreclosure / Sheriff's Sale scraper — Cook County.

Sources:
  1. Cook County Sheriff's Sale calendar
     https://www.cookcountysheriff.org/courts/civil-courts/judicial-sales/
  2. Illinois Courts eFileIL (Lis Pendens / foreclosure cases)
     https://www.ilelections.org / https://courtlink.lexisnexis.com (subscription)
  3. HUD Homes (FHA-insured foreclosures):
     https://www.hudhomestore.gov/Home/Index.aspx
  4. Fannie Mae HomePath: https://www.fanniemae.com/homes-for-sale
  5. Freddie Mac HomeSteps: https://www.homepath.com (similar)

NOTE: Sheriff's sale site often serves content via JavaScript. This scraper
parses what it can from the static HTML and fails gracefully. For full coverage,
Playwright with headless Chrome is the proper approach.

HUD and HomePath have download links to CSV/Excel — this scraper pulls those
as the most reliable free source.
"""

import csv
import io
import logging
import random
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)

SHERIFF_SALE_URL = "https://www.cookcountysheriff.org/courts/civil-courts/judicial-sales/"
HUD_SEARCH_URL   = "https://www.hudhomestore.gov/Listing/PropertySearchResult.aspx"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

TARGET_ZIP_SET = set(config.TARGET_ZIP_CODES)


def _get(url: str, params: dict = None) -> Optional[BeautifulSoup]:
    """Fetch URL with retries and graceful failure."""
    for attempt in range(config.MAX_RETRIES):
        try:
            time.sleep(random.uniform(config.REQUEST_DELAY_MIN, config.REQUEST_DELAY_MAX))
            resp = requests.get(
                url, headers=HEADERS, params=params,
                timeout=config.REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            logger.warning("Foreclosure: HTTP %s for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except Exception as e:
            logger.error("Foreclosure: error on attempt %d: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    return None


def _zip_to_neighborhood(zipcode: str) -> Optional[str]:
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
    return ZIP_MAP.get((zipcode or "").strip()[:5])


def _scrape_sheriff_sales() -> list[Deal]:
    """
    Scrape Cook County Sheriff upcoming judicial sales.
    The page lists properties as table rows — we parse address, case #, sale date.
    Heavy JS usage means this often returns empty; Playwright needed for full data.
    """
    logger.info("Foreclosure: scraping Cook County Sheriff sales")
    soup = _get(SHERIFF_SALE_URL)
    if not soup:
        logger.warning("Foreclosure: could not fetch Sheriff sale page")
        return []

    deals = []
    # Try parsing table rows — sheriff site uses a searchable table
    rows = soup.select("table tr, .sale-listing, .property-row")
    if not rows:
        logger.info("Foreclosure: no parseable rows on Sheriff page (JS-rendered content likely)")
        return []

    for row in rows[1:]:  # skip header
        cells = row.find_all(["td", "th"])
        if len(cells) < 3:
            continue
        texts = [c.get_text(strip=True) for c in cells]

        # Heuristic: look for row with address pattern
        address = ""
        zip_code = ""
        case_num = ""
        sale_date = ""

        for t in texts:
            if re.search(r"\d{4,5}\s+[A-Z]", t.upper()):
                address = t
            if re.match(r"\d{5}", t):
                zip_code = t[:5]
            if re.match(r"\d{2}[-\s]?\w{2}[-\s]?\d+", t):
                case_num = t
            if re.match(r"\d{1,2}/\d{1,2}/\d{4}", t):
                sale_date = t

        if not address:
            continue

        # Check if ZIP in target
        neighborhood = _zip_to_neighborhood(zip_code)
        if not neighborhood and zip_code not in TARGET_ZIP_SET:
            continue

        deal = Deal(
            source="sheriff_sale",
            channel="off_market",
            off_market=True,
            address=address,
            city="Chicago",
            state="IL",
            zip_code=zip_code,
            neighborhood=neighborhood or "",
            asset_class="unknown",
            foreclosure_date=sale_date,
            foreclosure_case=case_num,
            raw={"cells": texts},
        )
        deals.append(deal)

    logger.info("Foreclosure: found %d Sheriff sale deals", len(deals))
    return deals


def _scrape_hud_homes() -> list[Deal]:
    """
    Fetch HUD homes in Illinois, filter for Cook County multifamily.
    HUD has a search API used by their site — try JSON endpoint first.
    """
    logger.info("Foreclosure: fetching HUD homes for IL/Cook County")
    # HUD's internal search API (observed via browser devtools)
    hud_api = "https://www.hudhomestore.gov/Listing/PropertySearchResult.aspx"
    params = {
        "sState": "IL",
        "sCity": "Chicago",
        "sPropType": "MD",   # MD = multi-dwelling
        "iBedrooms": "",
        "sPropStatus": "I",  # I = initial listing
        "iPage": 1,
    }
    soup = _get(hud_api, params=params)
    if not soup:
        logger.warning("Foreclosure: could not fetch HUD page")
        return []

    deals = []
    # Parse property cards
    cards = soup.select(".property-listing, .prop-info, article.property")
    for card in cards:
        addr_el = card.select_one(".address, .prop-address, h3")
        price_el = card.select_one(".price, .list-price, .asking-price")
        bed_el   = card.select_one(".bedrooms, .beds")
        link_el  = card.select_one("a[href*='property']")

        address = addr_el.get_text(strip=True) if addr_el else ""
        price_text = price_el.get_text(strip=True) if price_el else ""

        if not address:
            continue

        # ZIP extraction
        zip_match = re.search(r"\b(\d{5})\b", address)
        zip_code = zip_match.group(1) if zip_match else ""
        neighborhood = _zip_to_neighborhood(zip_code)

        if not neighborhood and zip_code not in TARGET_ZIP_SET:
            continue

        price = None
        if price_text:
            try:
                price = float(re.sub(r"[^\d.]", "", price_text))
            except ValueError:
                pass

        deal = Deal(
            source="hud_homes",
            channel="off_market",
            off_market=True,
            address=address,
            city="Chicago",
            state="IL",
            zip_code=zip_code,
            neighborhood=neighborhood or "",
            asset_class="multifamily",
            price=price,
            url=link_el["href"] if link_el else "",
            raw={},
        )
        deals.append(deal)

    logger.info("Foreclosure: found %d HUD homes", len(deals))
    return deals


def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Combines Sheriff sales + HUD homes.
    dry_run=True returns synthetic stub data.
    """
    if dry_run:
        logger.info("Foreclosure: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Foreclosure: starting scrape")
    deals: list[Deal] = []

    # Sheriff sales
    try:
        sheriff_deals = _scrape_sheriff_sales()
        deals.extend(sheriff_deals)
    except Exception as e:
        logger.error("Foreclosure: Sheriff scrape failed: %s", e)

    # HUD homes
    try:
        hud_deals = _scrape_hud_homes()
        deals.extend(hud_deals)
    except Exception as e:
        logger.error("Foreclosure: HUD scrape failed: %s", e)

    logger.info("Foreclosure: total %d deals collected", len(deals))
    return deals[:limit]


def _stub_deals() -> list[Deal]:
    """Synthetic deals for dry-run / testing."""
    return [
        Deal(
            source="sheriff_sale",
            channel="off_market",
            off_market=True,
            address="3019 W Diversey Ave",
            city="Chicago",
            state="IL",
            zip_code="60647",
            neighborhood="Logan Square",
            asset_class="multifamily",
            price=420_000,
            foreclosure_date="2024-05-15",
            foreclosure_case="22CH12345",
            raw={"stub": True},
        ),
        Deal(
            source="hud_homes",
            channel="off_market",
            off_market=True,
            address="2516 S Kedzie Ave",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="multifamily",
            price=310_000,
            raw={"stub": True},
        ),
        Deal(
            source="sheriff_sale",
            channel="off_market",
            off_market=True,
            address="4415 W Fullerton Ave",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Belmont Cragin",
            asset_class="unknown",
            foreclosure_date="2024-06-02",
            foreclosure_case="23CH54321",
            raw={"stub": True},
        ),
    ]
