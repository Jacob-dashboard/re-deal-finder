"""
Crexi scraper — on-market multifamily/mixed-use in Cook County, IL.

Crexi uses a React SPA. This scraper uses Playwright (headless Chromium) to
render the page and parse the resulting HTML. Falls back to the undocumented
JSON API as a secondary attempt.
"""

import logging
import random
import re
import time
from typing import Optional

from bs4 import BeautifulSoup
import requests

from scraper import Deal
import config

logger = logging.getLogger(__name__)

# Crexi search URL with Cook County MF filters
CREXI_SEARCH_URL = (
    "https://www.crexi.com/properties"
    "?propertyType=Multifamily&states=IL"
)

# Fallback: Crexi's internal search API (observed via browser devtools — may change)
CREXI_API_BASE = "https://api.crexi.com/assets"

VIEWPORT = {"width": 1280, "height": 900}
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

API_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.crexi.com/",
    "Origin": "https://www.crexi.com",
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> Optional[float]:
    text = re.sub(r"[^\d.]", "", text or "")
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _parse_int(text: str) -> Optional[int]:
    m = re.search(r"[\d,]+", text or "")
    if m:
        try:
            return int(m.group().replace(",", ""))
        except ValueError:
            pass
    return None


def _parse_cap_rate(text: str) -> Optional[float]:
    m = re.search(r"([\d.]+)\s*%", text or "")
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _parse_dom(text: str) -> Optional[int]:
    """Extract days-on-market from strings like '14 days', '2 months'."""
    text = text.lower().strip()
    m_days = re.search(r"(\d+)\s*day", text)
    if m_days:
        return int(m_days.group(1))
    m_months = re.search(r"(\d+)\s*month", text)
    if m_months:
        return int(m_months.group(1)) * 30
    return None


def _parse_card(card) -> Optional[Deal]:
    """Parse a single Crexi listing card into a Deal."""
    try:
        deal = Deal(source="crexi", channel="on_market")

        # URL — look for property links
        link = card.select_one(
            "a[href*='/properties/'], a[href*='/listing/'], a.property-link"
        )
        if link and link.get("href"):
            href = link["href"]
            deal.url = href if href.startswith("http") else f"https://www.crexi.com{href}"
            # Extract external_id from URL
            m = re.search(r"/properties/(\d+)", href)
            if m:
                deal.external_id = m.group(1)

        # Address
        for sel in [
            "[data-qa='property-address']",
            "[class*='address']",
            "[class*='Address']",
            "address",
        ]:
            el = card.select_one(sel)
            if el:
                deal.address = el.get_text(strip=True)
                break

        # Price
        for sel in [
            "[data-qa='asking-price']",
            "[class*='price']",
            "[class*='Price']",
        ]:
            el = card.select_one(sel)
            if el:
                deal.price = _parse_price(el.get_text(strip=True))
                if deal.price:
                    break

        # Units
        for el in card.select("[class*='detail'], [class*='stat'], [class*='info']"):
            text = el.get_text(strip=True)
            tl = text.lower()
            if "unit" in tl and not deal.units:
                deal.units = _parse_int(text)
            elif "cap" in tl and not deal.cap_rate:
                deal.cap_rate = _parse_cap_rate(text)
            elif any(x in tl for x in ["sq ft", "sqft", "sf"]) and not deal.sqft:
                deal.sqft = _parse_int(text)
            elif any(x in tl for x in ["day", "month"]) and "on market" in tl and not deal.days_on_market:
                deal.days_on_market = _parse_dom(text)

        # Asset class
        for sel in [
            "[data-qa='property-type']",
            "[class*='propertyType']",
            "[class*='property-type']",
        ]:
            el = card.select_one(sel)
            if el:
                deal.asset_class = el.get_text(strip=True).lower()
                break

        # Price per unit
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        deal.city = "Chicago"
        deal.state = "IL"
        return deal
    except Exception as e:
        logger.debug("Crexi: error parsing card: %s", e)
        return None


# ---------------------------------------------------------------------------
# Playwright scrape
# ---------------------------------------------------------------------------

def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Scrape Crexi using headless Chromium via Playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("Crexi: playwright not installed — run: pip install playwright && playwright install chromium")
        return []

    deals: list[Deal] = []
    max_pages = min(3, config.MAX_PAGES)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport=VIEWPORT,
                user_agent=USER_AGENT,
                locale="en-US",
            )
            page = ctx.new_page()
            page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())

            logger.info("Crexi: navigating to %s", CREXI_SEARCH_URL)
            try:
                page.goto(CREXI_SEARCH_URL, wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                logger.warning("Crexi: page load timed out")
                browser.close()
                return []

            # Check for blocks
            title = page.title().lower()
            if any(kw in title for kw in ["captcha", "access denied", "just a moment", "blocked"]):
                logger.warning("Crexi: CAPTCHA/block detected — returning empty")
                browser.close()
                return []

            for page_num in range(1, max_pages + 1):
                if len(deals) >= limit:
                    break

                logger.info("Crexi: scraping page %d", page_num)

                # Wait for property cards
                card_selector = (
                    "[data-qa='property-card'], "
                    "[class*='PropertyCard'], "
                    "[class*='property-card'], "
                    "[class*='listing-card'], "
                    "article[class*='card']"
                )
                try:
                    page.wait_for_selector(card_selector, timeout=20_000)
                except PWTimeout:
                    logger.warning("Crexi: no cards appeared on page %d — blocked or no results", page_num)
                    break

                time.sleep(random.uniform(3, 5))

                html = page.content()
                soup = BeautifulSoup(html, "lxml")

                cards = (
                    soup.select("[data-qa='property-card']")
                    or soup.select("[class*='PropertyCard']")
                    or soup.select("[class*='property-card']")
                    or soup.select("article[class*='card']")
                )

                if not cards:
                    logger.info("Crexi: no cards found on page %d", page_num)
                    break

                for card in cards:
                    deal = _parse_card(card)
                    if deal and deal.url:
                        deals.append(deal)
                    if len(deals) >= limit:
                        break

                logger.info("Crexi: page %d → %d cards (total: %d)", page_num, len(cards), len(deals))

                if page_num >= max_pages or len(deals) >= limit:
                    break

                # Next page
                next_btn = page.query_selector(
                    "button[aria-label*='next'], "
                    "a[aria-label*='next'], "
                    "[class*='nextPage']:not([class*='disabled']), "
                    "[class*='next-page']:not([class*='disabled'])"
                )
                if not next_btn:
                    logger.info("Crexi: no next-page button — end of results at page %d", page_num)
                    break

                try:
                    next_btn.click()
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    time.sleep(random.uniform(3, 5))
                except PWTimeout:
                    logger.warning("Crexi: timeout on next page — stopping")
                    break

            browser.close()

    except Exception as e:
        logger.warning("Crexi: Playwright error — %s — returning %d deals collected", e, len(deals))

    logger.info("Crexi: Playwright scrape complete — %d deals", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Fallback: JSON API
# ---------------------------------------------------------------------------

def _fetch_api_page(session: requests.Session, page: int = 0, size: int = 25) -> Optional[dict]:
    """Attempt Crexi's internal search API (may require auth on newer builds)."""
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
        resp = session.get(CREXI_API_BASE, headers=API_HEADERS, params=params, timeout=config.REQUEST_TIMEOUT)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code in (401, 403, 429):
            logger.warning("Crexi API: HTTP %s — likely requires auth token", resp.status_code)
            return None
        else:
            logger.warning("Crexi API: unexpected status %s", resp.status_code)
            return None
    except Exception as e:
        logger.error("Crexi API: error: %s", e)
        return None


def _parse_api_deal(asset: dict) -> Optional[Deal]:
    try:
        deal = Deal(source="crexi", channel="on_market")
        deal.external_id = str(asset.get("id", ""))
        deal.url = f"https://www.crexi.com/properties/{asset.get('id', '')}"
        addr = asset.get("address", {})
        deal.address    = addr.get("street", "")
        deal.city       = addr.get("city", "Chicago")
        deal.state      = addr.get("state", "IL")
        deal.zip_code   = addr.get("zip", "")
        deal.neighborhood = asset.get("neighborhood", "")
        deal.latitude   = asset.get("latitude")
        deal.longitude  = asset.get("longitude")
        deal.asset_class = asset.get("primaryUse", "").lower()
        deal.units       = asset.get("totalUnits") or asset.get("units")
        deal.sqft        = asset.get("totalSqft") or asset.get("buildingSize")
        deal.year_built  = asset.get("yearBuilt")
        deal.price       = asset.get("askingPrice") or asset.get("price")
        deal.cap_rate    = asset.get("capRate")
        deal.noi         = asset.get("noi")
        deal.gross_rent  = asset.get("grossAnnualRents") or asset.get("grossRent")
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units
        if deal.price and deal.sqft and deal.sqft > 0:
            deal.price_per_sqft = deal.price / deal.sqft
        deal.days_on_market = asset.get("daysOnMarket")
        deal.listing_date   = asset.get("listDate", "")
        broker_info = asset.get("broker", {}) or asset.get("contact", {})
        deal.broker         = broker_info.get("name", "")
        deal.broker_phone   = broker_info.get("phone", "")
        deal.broker_email   = broker_info.get("email", "")
        deal.raw = {}
        return deal
    except Exception as e:
        logger.debug("Crexi: error parsing API asset: %s", e)
        return None


def _scrape_via_api(limit: int) -> list[Deal]:
    """Try the undocumented JSON API as fallback."""
    session = requests.Session()
    deals: list[Deal] = []
    page = 0
    page_size = min(25, limit)

    while len(deals) < limit and page < config.MAX_PAGES:
        data = _fetch_api_page(session, page=page, size=page_size)
        if data is None:
            break
        assets = data.get("data", []) or data.get("assets", []) or data.get("results", [])
        if not assets:
            break
        for asset in assets:
            deal = _parse_api_deal(asset)
            if deal:
                deals.append(deal)
            if len(deals) >= limit:
                break
        if len(assets) < page_size:
            break
        page += 1

    return deals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from Crexi.
    Tries Playwright first, then falls back to JSON API.
    dry_run=True returns synthetic stub data.
    """
    if dry_run:
        logger.info("Crexi: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Crexi: starting Playwright scrape (limit=%d)", limit)
    deals = _scrape_with_playwright(limit)

    if not deals:
        logger.info("Crexi: Playwright returned nothing — trying JSON API fallback")
        deals = _scrape_via_api(limit)

    if not deals:
        logger.warning("Crexi: both Playwright and API returned empty — site likely blocked")

    logger.info("Crexi: scrape complete — %d deals", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Stub data
# ---------------------------------------------------------------------------

def _stub_deals() -> list[Deal]:
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
