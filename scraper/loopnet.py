"""
LoopNet scraper — on-market multifamily/mixed-use in Cook County, IL.

LoopNet aggressively blocks headless scrapers. This implementation:
  1. Rotates user agents
  2. Adds browser-like headers
  3. Rate-limits between requests
  4. Fails gracefully and logs when blocked (403/429/CAPTCHA)

If blocked consistently, next step is Playwright with stealth plugin.
"""

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)

BASE_URL = "https://www.loopnet.com"
SEARCH_URL = (
    "https://www.loopnet.com/search/multifamily-properties/"
    "cook-county-il/for-sale/"
)

HEADERS_POOL = [
    {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.loopnet.com/",
        "Upgrade-Insecure-Requests": "1",
    },
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
    },
]


def _get(url: str, session: requests.Session, params: dict = None) -> Optional[BeautifulSoup]:
    """GET with retry, rate-limiting, and graceful failure."""
    for attempt in range(config.MAX_RETRIES):
        try:
            time.sleep(random.uniform(config.REQUEST_DELAY_MIN, config.REQUEST_DELAY_MAX))
            headers = random.choice(HEADERS_POOL)
            resp = session.get(
                url,
                headers=headers,
                params=params,
                timeout=config.REQUEST_TIMEOUT,
                allow_redirects=True,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "lxml")
                # Detect CAPTCHA / Cloudflare challenge pages
                title = soup.title.string if soup.title else ""
                if any(kw in title.lower() for kw in ["captcha", "challenge", "access denied", "just a moment"]):
                    logger.warning("LoopNet: CAPTCHA/challenge detected. Consider Playwright.")
                    return None
                return soup
            elif resp.status_code in (403, 429):
                wait = (2 ** attempt) * 5
                logger.warning(
                    "LoopNet: HTTP %s (attempt %d/%d), waiting %ds",
                    resp.status_code, attempt + 1, config.MAX_RETRIES, wait,
                )
                time.sleep(wait)
            else:
                logger.warning("LoopNet: unexpected status %s for %s", resp.status_code, url)
                return None
        except requests.RequestException as e:
            logger.error("LoopNet: request error on attempt %d: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    logger.error("LoopNet: all retries exhausted for %s", url)
    return None


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
        addr_el = card.select_one("[data-testid='address'], .listingCard-propertyAddress, .listing-address")
        if addr_el:
            deal.address = addr_el.get_text(separator=", ", strip=True)

        # Price
        price_el = card.select_one("[data-testid='price'], .listingCard-priceValue, .price")
        if price_el:
            deal.price = _parse_price(price_el.get_text(strip=True))

        # Units
        for el in card.select(".listingCard-detail, .listing-detail, [class*='detail']"):
            text = el.get_text(strip=True)
            if "unit" in text.lower():
                deal.units = _parse_int(text)
            elif "cap rate" in text.lower():
                deal.cap_rate = _parse_cap_rate(text)
            elif any(x in text.lower() for x in ["sq ft", "sqft", "sf"]):
                deal.sqft = _parse_int(text)

        # Asset class
        type_el = card.select_one(".listingCard-propertyType, [class*='property-type']")
        if type_el:
            deal.asset_class = type_el.get_text(strip=True).lower()

        # Broker
        broker_el = card.select_one(".listingCard-brokerName, .broker-name")
        if broker_el:
            deal.broker = broker_el.get_text(strip=True)

        # Listing date / DOM
        date_el = card.select_one("[data-testid='listing-date'], .listingCard-date")
        if date_el:
            deal.listing_date = date_el.get_text(strip=True)

        # Price per unit
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        return deal
    except Exception as e:
        logger.debug("LoopNet: error parsing card: %s", e)
        return None


def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from LoopNet.
    dry_run=True returns synthetic stub data instead of hitting the network.
    """
    if dry_run:
        logger.info("LoopNet: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("LoopNet: starting scrape (max_pages=%d, limit=%d)", config.MAX_PAGES, limit)
    session = requests.Session()
    deals: list[Deal] = []

    for page in range(1, config.MAX_PAGES + 1):
        if len(deals) >= limit:
            break

        params = {"page": page} if page > 1 else {}
        logger.info("LoopNet: fetching page %d", page)
        soup = _get(SEARCH_URL, session, params)

        if soup is None:
            logger.warning("LoopNet: failed to fetch page %d, stopping pagination", page)
            break

        # Try multiple card selectors across LoopNet's changing DOM
        cards = (
            soup.select("article.listingCard")
            or soup.select("[data-testid='listing-card']")
            or soup.select(".listing-card")
            or soup.select("li.placard")
        )

        if not cards:
            logger.info("LoopNet: no cards found on page %d — likely end of results or blocked", page)
            break

        for card in cards:
            deal = _parse_listing_card(card)
            if deal and deal.url:
                deals.append(deal)
            if len(deals) >= limit:
                break

        logger.info("LoopNet: page %d yielded %d cards (total so far: %d)", page, len(cards), len(deals))

        # Check for next page
        next_btn = soup.select_one("a[aria-label='Next page'], .pagination-next:not(.disabled)")
        if not next_btn:
            logger.info("LoopNet: reached last page at page %d", page)
            break

    logger.info("LoopNet: scrape complete — %d deals collected", len(deals))
    return deals


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
