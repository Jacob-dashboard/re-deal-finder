"""
Realtor.com scraper — on-market multifamily listings in Chicago.

Uses Playwright to render the React SPA search results page.
Realtor.com uses stable data-testid attributes that make parsing reliable.
"""

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)

BASE_URL = "https://www.realtor.com"

# Multi-family homes in Chicago under $1.2M
SEARCH_URL = (
    "https://www.realtor.com/realestateandhomes-search/Chicago_IL"
    "/type-multi-family-home/price-na-1200000"
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> Optional[float]:
    # Handle "850K" → 850000, "$1.1M" → 1100000
    text = (text or "").strip().replace(",", "")
    m_k = re.search(r"\$([\d.]+)[Kk]", text)
    if m_k:
        try:
            return float(m_k.group(1)) * 1000
        except ValueError:
            pass
    m_m = re.search(r"\$([\d.]+)[Mm]", text)
    if m_m:
        try:
            return float(m_m.group(1)) * 1_000_000
        except ValueError:
            pass
    digits = re.sub(r"[^\d.]", "", text)
    try:
        return float(digits) if digits else None
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


def _parse_card(card) -> Optional[Deal]:
    """Parse a realtor.com property card into a Deal."""
    try:
        deal = Deal(source="realtor", channel="on_market", asset_class="multifamily")
        deal.city  = "Chicago"
        deal.state = "IL"

        # URL
        link = card.select_one(
            "a[data-testid='property-anchor'], "
            "a[href*='/realestateandhomes-detail/'], "
            "a[href*='realtor.com']"
        )
        if link and link.get("href"):
            href = link["href"]
            deal.url = href if href.startswith("http") else urljoin(BASE_URL, href)

        # Address line
        addr_el = card.select_one(
            "[data-testid='card-address-1'], "
            "[data-testid='prop-card-address'], "
            "[class*='address'], address"
        )
        city_el = card.select_one(
            "[data-testid='card-address-2'], "
            "[class*='city']"
        )

        street = addr_el.get_text(strip=True) if addr_el else ""
        city_text = city_el.get_text(strip=True) if city_el else ""

        if street:
            deal.address = street

        # Extract ZIP from city text
        if city_text:
            zm = re.search(r"\b(6\d{4})\b", city_text)
            if zm:
                deal.zip_code = zm.group(1)
                deal.neighborhood = _zip_to_neighborhood(deal.zip_code) or ""

        # Price
        price_el = card.select_one(
            "[data-testid='card-price'], "
            "[class*='price'], .Price"
        )
        if price_el:
            deal.price = _parse_price(price_el.get_text(strip=True))

        # Beds / baths / sqft
        for stat_el in card.select(
            "[data-testid*='bed'], [data-testid*='bath'], [data-testid*='sqft'], "
            "[class*='bed'], [class*='bath'], [class*='sqft'], "
            "li[data-testid]"
        ):
            text = stat_el.get_text(strip=True).lower()
            if "bed" in text:
                n = _parse_int(text)
                if n and n >= 4 and deal.units is None:
                    # Infer units: 2 beds per unit
                    deal.units = max(n // 2, 2)
            elif "sqft" in text or "sq ft" in text:
                if deal.sqft is None:
                    deal.sqft = _parse_int(text)

        # Also look for unit count in listing text
        for el in card.select("[class*='detail'], [class*='tag'], [class*='meta'], li"):
            text = el.get_text(strip=True).lower()
            m = re.search(r"(\d+)\s*unit", text)
            if m:
                try:
                    deal.units = int(m.group(1))
                except ValueError:
                    pass

        # Days on market
        for el in card.select("[data-testid*='dom'], [class*='days']"):
            text = el.get_text(strip=True).lower()
            m = re.search(r"(\d+)\s*day", text)
            if m:
                deal.days_on_market = int(m.group(1))

        # Price per unit
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        return deal
    except Exception as e:
        logger.debug("Realtor: error parsing card: %s", e)
        return None


# ---------------------------------------------------------------------------
# Playwright scrape
# ---------------------------------------------------------------------------

def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Render realtor.com search results with Playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("Realtor: playwright not installed")
        return []

    deals: list[Deal] = []
    max_pages = min(3, config.MAX_PAGES)

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1280, "height": 900},
                user_agent=USER_AGENT,
                locale="en-US",
            )
            page = ctx.new_page()
            page.route(
                "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}",
                lambda r: r.abort(),
            )

            logger.info("Realtor: navigating to %s", SEARCH_URL)
            try:
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=35_000)
            except PWTimeout:
                logger.warning("Realtor: page load timed out")
                browser.close()
                return []

            title = page.title().lower()
            if any(kw in title for kw in ["captcha", "access denied", "just a moment", "blocked"]):
                logger.warning("Realtor: blocked — title: %s", title)
                browser.close()
                return []

            # Wait for property cards
            card_selectors = (
                "[data-testid='property-card'], "
                "[data-testid='rdc-property-card'], "
                "[class*='PropertyCard'], "
                "[class*='property-card']"
            )
            try:
                page.wait_for_selector(card_selectors, timeout=25_000)
            except PWTimeout:
                logger.warning("Realtor: no property cards appeared — title: %s", page.title())
                snip = page.content()[:500]
                logger.debug("Realtor HTML snippet: %s", snip)
                browser.close()
                return []

            for page_num in range(1, max_pages + 1):
                if len(deals) >= limit:
                    break

                time.sleep(random.uniform(2, 4))
                html = page.content()
                soup = BeautifulSoup(html, "lxml")

                cards = (
                    soup.select("[data-testid='property-card']")
                    or soup.select("[data-testid='rdc-property-card']")
                    or soup.select("[class*='PropertyCard']")
                    or soup.select("article[class*='card']")
                )

                if not cards:
                    logger.info("Realtor: no cards found on page %d", page_num)
                    break

                logger.info("Realtor: page %d → %d cards", page_num, len(cards))

                for card in cards:
                    deal = _parse_card(card)
                    if deal and (deal.url or deal.address):
                        deals.append(deal)
                    if len(deals) >= limit:
                        break

                if page_num >= max_pages or len(deals) >= limit:
                    break

                # Try next page button
                next_btn = page.query_selector(
                    "a[aria-label='Go to next page'], "
                    "button[aria-label='Go to next page'], "
                    "[data-testid='pagination-next']"
                )
                if not next_btn:
                    logger.info("Realtor: no next-page button at page %d", page_num)
                    break

                try:
                    next_btn.click()
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    time.sleep(random.uniform(2, 3))
                except PWTimeout:
                    logger.warning("Realtor: timeout on next page")
                    break

            browser.close()

    except Exception as e:
        logger.warning("Realtor: Playwright error — %s — %d deals so far", e, len(deals))

    logger.info("Realtor: %d deals collected", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from Realtor.com.
    """
    if dry_run:
        logger.info("Realtor: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Realtor: starting Playwright scrape (limit=%d)", limit)
    deals = _scrape_with_playwright(limit)

    if not deals:
        logger.warning("Realtor: no deals retrieved — site may have blocked headless browser")

    return deals


# ---------------------------------------------------------------------------
# Stub data
# ---------------------------------------------------------------------------

def _stub_deals() -> list[Deal]:
    return [
        Deal(
            source="realtor",
            channel="on_market",
            url="https://www.realtor.com/realestateandhomes-detail/stub-1",
            address="3210 W 26th St",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="multifamily",
            units=6,
            sqft=5400,
            price=599_000,
            price_per_unit=99_833,
            days_on_market=12,
            raw={"stub": True},
        ),
        Deal(
            source="realtor",
            channel="on_market",
            url="https://www.realtor.com/realestateandhomes-detail/stub-2",
            address="2718 N Kedzie Ave",
            city="Chicago",
            state="IL",
            zip_code="60647",
            neighborhood="Logan Square",
            asset_class="multifamily",
            units=8,
            sqft=7200,
            price=949_000,
            price_per_unit=118_625,
            days_on_market=3,
            raw={"stub": True},
        ),
    ]
