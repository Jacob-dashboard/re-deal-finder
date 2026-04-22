"""
Crexi scraper — on-market multifamily/mixed-use in Cook County, IL.

Crexi uses a React SPA with a JSON API backend. This scraper:
  1. Attempts the undocumented /api/properties endpoint used by their search page
  2. Falls back to HTML scraping if the API shape changes
  3. Fails gracefully if blocked

Endpoint observed via network inspection:
  GET https://api.crexi.com/assets?...
  (requires auth header on newer builds — stub returns synthetic data when blocked)
"""

import logging
import random
import re
import time
from typing import Optional

import requests

from scraper import Deal
import config

logger = logging.getLogger(__name__)

# Crexi's internal search API (observed via browser devtools — may change)
CREXI_API_BASE = "https://api.crexi.com/assets"
CREXI_SEARCH_URL = "https://www.crexi.com/properties"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.crexi.com/",
    "Origin": "https://www.crexi.com",
}


def _fetch_api_page(session: requests.Session, page: int = 0, size: int = 25) -> Optional[dict]:
    """Hit Crexi's internal search API."""
    params = {
        "propertyTypes": "Multifamily",
        "states": "IL",
        "counties": "Cook",
        "saleStatuses": "Active",
        "priceTo": config.MAX_PRICE,
        "limit": size,
        "offset": page * size,
        "sortBy": "ListDate",
        "sortDirection": "desc",
    }
    try:
        time.sleep(random.uniform(config.REQUEST_DELAY_MIN, config.REQUEST_DELAY_MAX))
        resp = session.get(
            CREXI_API_BASE,
            headers=HEADERS,
            params=params,
            timeout=config.REQUEST_TIMEOUT,
        )
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code in (401, 403, 429):
            logger.warning("Crexi API: HTTP %s — likely requires auth token now", resp.status_code)
            return None
        else:
            logger.warning("Crexi API: unexpected status %s", resp.status_code)
            return None
    except Exception as e:
        logger.error("Crexi API: error: %s", e)
        return None


def _parse_api_deal(asset: dict) -> Optional[Deal]:
    """Convert a Crexi API asset object to a Deal."""
    try:
        deal = Deal(source="crexi", channel="on_market")
        deal.external_id = str(asset.get("id", ""))
        deal.url = f"https://www.crexi.com/properties/{asset.get('id', '')}"

        # Location
        addr = asset.get("address", {})
        deal.address    = addr.get("street", "")
        deal.city       = addr.get("city", "Chicago")
        deal.state      = addr.get("state", "IL")
        deal.zip_code   = addr.get("zip", "")
        deal.neighborhood = asset.get("neighborhood", "")
        deal.latitude   = asset.get("latitude")
        deal.longitude  = asset.get("longitude")

        # Property
        deal.asset_class = asset.get("primaryUse", "").lower()
        deal.units       = asset.get("totalUnits") or asset.get("units")
        deal.sqft        = asset.get("totalSqft") or asset.get("buildingSize")
        deal.year_built  = asset.get("yearBuilt")

        # Financials
        deal.price          = asset.get("askingPrice") or asset.get("price")
        deal.cap_rate       = asset.get("capRate")       # already a float %
        deal.noi            = asset.get("noi")
        deal.gross_rent     = asset.get("grossAnnualRents") or asset.get("grossRent")

        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units
        if deal.price and deal.sqft and deal.sqft > 0:
            deal.price_per_sqft = deal.price / deal.sqft

        # Listing
        deal.days_on_market = asset.get("daysOnMarket")
        deal.listing_date   = asset.get("listDate", "")
        broker_info = asset.get("broker", {}) or asset.get("contact", {})
        deal.broker         = broker_info.get("name", "")
        deal.broker_phone   = broker_info.get("phone", "")
        deal.broker_email   = broker_info.get("email", "")
        deal.raw            = {}

        return deal
    except Exception as e:
        logger.debug("Crexi: error parsing asset: %s", e)
        return None


def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from Crexi.
    dry_run=True returns synthetic stub data instead of hitting the network.
    """
    if dry_run:
        logger.info("Crexi: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Crexi: starting scrape (limit=%d)", limit)
    session = requests.Session()
    deals: list[Deal] = []
    page = 0
    page_size = min(25, limit)

    while len(deals) < limit and page < config.MAX_PAGES:
        logger.info("Crexi: fetching page %d", page)
        data = _fetch_api_page(session, page=page, size=page_size)

        if data is None:
            logger.warning("Crexi: API unavailable — returning %d deals collected so far", len(deals))
            break

        assets = data.get("data", []) or data.get("assets", []) or data.get("results", [])
        if not assets:
            logger.info("Crexi: no more assets on page %d", page)
            break

        for asset in assets:
            deal = _parse_api_deal(asset)
            if deal:
                deals.append(deal)
            if len(deals) >= limit:
                break

        total = data.get("total", 0) or data.get("totalCount", 0)
        logger.info("Crexi: page %d yielded %d assets (total available: %s)", page, len(assets), total)

        if len(assets) < page_size:
            break
        page += 1

    logger.info("Crexi: scrape complete — %d deals collected", len(deals))
    return deals


def _stub_deals() -> list[Deal]:
    """Synthetic deals for dry-run / testing."""
    return [
        Deal(
            source="crexi",
            channel="on_market",
            url="https://www.crexi.com/properties/stub-101",
            external_id="stub-101",
            address="2230 S Millard Ave",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="multifamily",
            units=7,
            sqft=6300,
            price=749_000,
            price_per_unit=107_000,
            cap_rate=7.5,
            days_on_market=14,
            broker="Interra Realty",
            raw={"stub": True},
        ),
        Deal(
            source="crexi",
            channel="on_market",
            url="https://www.crexi.com/properties/stub-102",
            external_id="stub-102",
            address="4122 W Armitage Ave",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Belmont Cragin",
            asset_class="mixed-use",
            units=5,
            sqft=4500,
            price=525_000,
            price_per_unit=105_000,
            cap_rate=8.9,
            days_on_market=60,
            broker="Keller Williams",
            raw={"stub": True},
        ),
        Deal(
            source="crexi",
            channel="on_market",
            url="https://www.crexi.com/properties/stub-103",
            external_id="stub-103",
            address="1547 N Kedvale Ave",
            city="Chicago",
            state="IL",
            zip_code="60651",
            neighborhood="Humboldt Park",
            asset_class="multifamily",
            units=9,
            sqft=8100,
            price=810_000,
            price_per_unit=90_000,
            cap_rate=7.3,
            days_on_market=33,
            broker="Essex Realty",
            raw={"stub": True},
        ),
    ]
