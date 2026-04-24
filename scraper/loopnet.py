"""
LoopNet scraper — on-market multifamily/mixed-use in Cook County, IL.

Uses Playwright (headless Chromium) to bypass JS rendering and anti-bot
measures. Falls back gracefully with a warning if blocked.
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

BASE_URL = "https://www.loopnet.com"
SEARCH_URL = (
    "https://www.loopnet.com/search/multifamily-properties/"
    "cook-county-il/for-sale/"
)

# Realistic viewport + UA to avoid bot detection
VIEWPORT = {"width": 1280, "height": 800}
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Parsers (shared between Playwright and stub paths)
# ---------------------------------------------------------------------------

def _parse_price(text: str) -> Optional[float]:
    text = re.sub(r"[^\d.]", "", text or "")
    try:
        return float(text)
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


def _parse_listing_card(card, base_url: str = BASE_URL) -> Optional[Deal]:
    """Parse a single LoopNet listing card into a Deal."""
    try:
        deal = Deal(source="loopnet", channel="on_market")

        # URL
        link = card.select_one("a.js-listingCard-link, a[href*='/listing/']")
        if link and link.get("href"):
            deal.url = urljoin(base_url, link["href"])

        # Address
        addr_el = card.select_one(
            "[data-testid='address'], .listingCard-propertyAddress, "
            ".listing-address, [class*='address']"
        )
        if addr_el:
            deal.address = addr_el.get_text(separator=", ", strip=True)

        # Price
        price_el = card.select_one(
            "[data-testid='price'], .listingCard-priceValue, .price, "
            "[class*='price']"
        )
        if price_el:
            deal.price = _parse_price(price_el.get_text(strip=True))

        # Units / cap rate / sqft from detail elements
        for el in card.select(".listingCard-detail, .listing-detail, [class*='detail']"):
            text = el.get_text(strip=True)
            if "unit" in text.lower():
                deal.units = _parse_int(text)
            elif "cap rate" in text.lower():
                deal.cap_rate = _parse_cap_rate(text)
            elif any(x in text.lower() for x in ["sq ft", "sqft", "sf"]):
                deal.sqft = _parse_int(text)

        # Asset class
        type_el = card.select_one(
            ".listingCard-propertyType, [class*='property-type'], [class*='propertyType']"
        )
        if type_el:
            deal.asset_class = type_el.get_text(strip=True).lower()

        # Broker
        broker_el = card.select_one(".listingCard-brokerName, .broker-name, [class*='broker']")
        if broker_el:
            deal.broker = broker_el.get_text(strip=True)

        # Listing date / DOM
        date_el = card.select_one("[data-testid='listing-date'], .listingCard-date, [class*='date']")
        if date_el:
            deal.listing_date = date_el.get_text(strip=True)

        # Price per unit
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        return deal
    except Exception as e:
        logger.debug("LoopNet: error parsing card: %s", e)
        return None


# ---------------------------------------------------------------------------
# Playwright fetch
# ---------------------------------------------------------------------------

def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Scrape LoopNet using headless Chromium via Playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("LoopNet: playwright not installed — run: pip install playwright && playwright install chromium")
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

            # Block images/fonts to speed up
            page.route("**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}", lambda r: r.abort())

            logger.info("LoopNet: navigating to search page")
            try:
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=30_000)
            except PWTimeout:
                logger.warning("LoopNet: page load timed out — site may be blocking headless browsers")
                browser.close()
                return []

            # Detect CAPTCHA / challenge
            title = page.title().lower()
            if any(kw in title for kw in ["captcha", "challenge", "access denied", "just a moment"]):
                logger.warning("LoopNet: CAPTCHA/challenge detected on initial load — returning empty")
                browser.close()
                return []

            for page_num in range(1, max_pages + 1):
                if len(deals) >= limit:
                    break

                logger.info("LoopNet: scraping page %d", page_num)

                # Wait for listing cards (any of the known selectors)
                try:
                    page.wait_for_selector(
                        "article.listingCard, [data-testid='listing-card'], "
                        ".listing-card, li.placard",
                        timeout=15_000,
                    )
                except PWTimeout:
                    logger.warning("LoopNet: no cards appeared on page %d — blocked or end of results", page_num)
                    break

                # Small human-like delay after render
                time.sleep(random.uniform(3, 5))

                html = page.content()
                soup = BeautifulSoup(html, "lxml")

                cards = (
                    soup.select("article.listingCard")
                    or soup.select("[data-testid='listing-card']")
                    or soup.select(".listing-card")
                    or soup.select("li.placard")
                )

                if not cards:
                    logger.info("LoopNet: no cards found on page %d", page_num)
                    break

                for card in cards:
                    deal = _parse_listing_card(card)
                    if deal and deal.url:
                        deals.append(deal)
                    if len(deals) >= limit:
                        break

                logger.info("LoopNet: page %d → %d cards (total: %d)", page_num, len(cards), len(deals))

                if page_num >= max_pages or len(deals) >= limit:
                    break

                # Try to go to next page
                next_btn = page.query_selector(
                    "a[aria-label='Next page'], .pagination-next:not(.disabled), "
                    "[class*='nextPage']:not([class*='disabled'])"
                )
                if not next_btn:
                    logger.info("LoopNet: no next-page button — end of results at page %d", page_num)
                    break

                try:
                    next_btn.click()
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    time.sleep(random.uniform(3, 5))
                except PWTimeout:
                    logger.warning("LoopNet: timeout waiting for next page — stopping")
                    break

            browser.close()

    except Exception as e:
        logger.warning("LoopNet: Playwright error — %s — returning %d deals collected", e, len(deals))

    logger.info("LoopNet: Playwright scrape complete — %d deals", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from LoopNet.
    dry_run=True returns synthetic stub data instead of hitting the network.
    """
    if dry_run:
        logger.info("LoopNet: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("LoopNet: starting Playwright scrape (limit=%d)", limit)
    deals = _scrape_with_playwright(limit)

    if not deals:
        logger.warning("LoopNet: no deals retrieved — site likely blocked headless browser")

    return deals


# ---------------------------------------------------------------------------
# Stub data
# ---------------------------------------------------------------------------

def _stub_deals() -> list[Deal]:
    """Synthetic deals for dry-run / testing."""
    return [
        Deal(
            source="loopnet",
            channel="on_market",
            url="https://www.loopnet.com/listing/stub-1",
            address="2345 N Pulaski Rd",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Hermosa",
            asset_class="multifamily",
            units=8,
            sqft=7200,
            price=680_000,
            price_per_unit=85_000,
            cap_rate=7.8,
            days_on_market=22,
            broker="Marcus & Millichap",
            raw={"stub": True},
        ),
        Deal(
            source="loopnet",
            channel="on_market",
            url="https://www.loopnet.com/listing/stub-2",
            address="3801 W 26th St",
            city="Chicago",
            state="IL",
            zip_code="60623",
            neighborhood="Little Village",
            asset_class="mixed-use",
            units=6,
            sqft=5400,
            price=595_000,
            price_per_unit=99_167,
            cap_rate=8.2,
            days_on_market=45,
            broker="SVN Chicago",
            raw={"stub": True},
        ),
        Deal(
            source="loopnet",
            channel="on_market",
            url="https://www.loopnet.com/listing/stub-3",
            address="1912 N Kedzie Ave",
            city="Chicago",
            state="IL",
            zip_code="60647",
            neighborhood="Logan Square",
            asset_class="multifamily",
            units=12,
            sqft=10800,
            price=1_100_000,
            price_per_unit=91_667,
            cap_rate=7.1,
            days_on_market=8,
            broker="Kiser Group",
            raw={"stub": True},
        ),
    ]
