"""
FSBO scraper — Craigslist Chicago + FSBO.com commercial/investment properties.

Sources:
  1. Craigslist Chicago — "commercial / business" category:
     https://chicago.craigslist.org/search/Chicago/clt  (commercial real estate)
  2. Craigslist Chicago — "real estate for sale":
     https://chicago.craigslist.org/search/rea  (keyword filter: apartment, multifamily)
  3. FSBO.com — Chicago multifamily listings
  4. Zillow "for sale by owner" (Chicago) — scrape-resistant but worth trying

Strategy:
  - Search for keywords: "apartment building", "multifamily", "multi-family", "mixed use",
    "6 flat", "8 flat", "investment property" + target neighborhood names
  - Extract price, unit count, neighborhood from text where possible
  - Rate limit: Craigslist is lenient; respect with 1-2s delays
"""

import logging
import random
import re
import time
from typing import Optional
from urllib.parse import urlencode, quote_plus

import requests
from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)

CL_BASE = "https://chicago.craigslist.org"
CL_COMMERCIAL_URL = f"{CL_BASE}/search/clt"
CL_REALESTATE_URL = f"{CL_BASE}/search/rea"

FSBO_URL = "https://www.fsbo.com/search-results"

SEARCH_KEYWORDS = [
    "apartment building",
    "multifamily",
    "multi family",
    "multi-family",
    "mixed use",
    "6 flat",
    "6-flat",
    "8 flat",
    "8-flat",
    "investment property",
    "income property",
    "12 unit",
    "6 unit",
    "8 unit",
]

NEIGHBORHOOD_KEYWORDS = [nbhd.lower() for nbhd in config.TARGET_NEIGHBORHOODS]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def _get(url: str, params: dict = None) -> Optional[BeautifulSoup]:
    for attempt in range(config.MAX_RETRIES):
        try:
            time.sleep(random.uniform(1.0, 2.5))
            resp = requests.get(url, headers=HEADERS, params=params, timeout=config.REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return BeautifulSoup(resp.text, "lxml")
            logger.warning("FSBO: HTTP %s for %s (attempt %d)", resp.status_code, url, attempt + 1)
        except Exception as e:
            logger.error("FSBO: error on attempt %d: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
    return None


def _parse_price(text: str) -> Optional[float]:
    m = re.search(r"\$\s*([\d,]+(?:\.\d+)?)", text or "")
    if m:
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


def _parse_units(text: str) -> Optional[int]:
    """Try to extract unit count from listing text."""
    patterns = [
        r"(\d+)\s*(?:unit|flat|apt|apartment|family|plex)s?",
        r"(\d+)[-\s]?(?:unit|flat|family)",
        r"(\d+)\s*unit",
    ]
    for pat in patterns:
        m = re.search(pat, (text or "").lower())
        if m:
            try:
                n = int(m.group(1))
                if 4 <= n <= 200:  # sanity check
                    return n
            except ValueError:
                pass
    return None


def _detect_neighborhood(text: str) -> Optional[str]:
    """Check if any target neighborhood name appears in text."""
    lower_text = text.lower()
    for nbhd in NEIGHBORHOOD_KEYWORDS:
        if nbhd in lower_text:
            # Return canonical name
            for canonical in config.TARGET_NEIGHBORHOODS:
                if canonical.lower() == nbhd:
                    return canonical
    # ZIP code fallback
    for zipcode in config.TARGET_ZIP_CODES:
        if zipcode in text:
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
            if zipcode in ZIP_MAP:
                return ZIP_MAP[zipcode]
    return None


def _is_relevant(title: str, body: str = "") -> bool:
    """Check if listing text contains investment property keywords."""
    combined = (title + " " + body).lower()
    return any(kw in combined for kw in SEARCH_KEYWORDS)


def _scrape_craigslist(url: str, category: str, limit: int = 30) -> list[Deal]:
    """Scrape a Craigslist search URL for investment property listings."""
    logger.info("FSBO/CL: scraping %s", url)
    deals = []

    for page_offset in range(0, min(limit * 2, 240), 120):  # CL paginates by 120
        if len(deals) >= limit:
            break

        params = {
            "query": "apartment building OR multifamily OR multi-family OR mixed use OR investment property",
            "srchType": "A",
            "hasPic": 0,
            "s": page_offset,
        }
        soup = _get(url, params=params)
        if not soup:
            logger.warning("FSBO/CL: failed to fetch %s offset=%d", url, page_offset)
            break

        # Craigslist listing rows
        items = (
            soup.select("li.cl-static-search-result")
            or soup.select(".result-row")
            or soup.select("li.result-row")
        )
        if not items:
            logger.info("FSBO/CL: no items found at offset %d", page_offset)
            break

        for item in items:
            if len(deals) >= limit:
                break

            title_el = item.select_one(".title, a.titlestring, .cl-app-anchor")
            price_el = item.select_one(".price, .result-price")
            hood_el  = item.select_one(".result-hood, .label, .location")
            link_el  = item.select_one("a.titlestring, a.cl-app-anchor, a[href*='/d/']")

            title    = title_el.get_text(strip=True) if title_el else ""
            price_text = price_el.get_text(strip=True) if price_el else ""
            hood_text  = hood_el.get_text(strip=True) if hood_el else ""
            link_url   = ""
            if link_el and link_el.get("href"):
                href = link_el["href"]
                link_url = href if href.startswith("http") else CL_BASE + href

            if not title or not _is_relevant(title):
                continue

            neighborhood = _detect_neighborhood(title + " " + hood_text)
            if not neighborhood:
                continue

            price = _parse_price(price_text)
            units = _parse_units(title)

            deal = Deal(
                source="craigslist",
                channel="off_market",
                off_market=True,
                address=hood_text or title[:80],
                city="Chicago",
                state="IL",
                neighborhood=neighborhood,
                asset_class="multifamily",
                units=units,
                price=price,
                url=link_url,
                raw={"title": title, "category": category},
            )
            deals.append(deal)

        # CL pagination check
        next_btn = soup.select_one("a.button.next, .cl-next-page")
        if not next_btn:
            break

    logger.info("FSBO/CL: %s yielded %d deals", category, len(deals))
    return deals


def _scrape_fsbo_dot_com(limit: int = 20) -> list[Deal]:
    """
    Scrape FSBO.com for Chicago multifamily listings.
    FSBO.com has simpler HTML structure — more parse-friendly.
    """
    logger.info("FSBO.com: scraping Chicago investment properties")
    params = {
        "state": "illinois",
        "city": "chicago",
        "beds": "",
        "baths": "",
        "type": "multi-family",
    }
    soup = _get(FSBO_URL, params=params)
    if not soup:
        logger.warning("FSBO.com: failed to fetch")
        return []

    deals = []
    listings = soup.select(".listing-card, .property-card, .listing, article")

    for listing in listings[:limit]:
        addr_el  = listing.select_one(".address, .street, h2, h3")
        price_el = listing.select_one(".price, .asking-price")
        bed_el   = listing.select_one(".beds, .bedrooms")
        link_el  = listing.select_one("a[href*='property'], a[href*='listing']")

        address    = addr_el.get_text(strip=True) if addr_el else ""
        price_text = price_el.get_text(strip=True) if price_el else ""
        link_url   = link_el["href"] if link_el else ""

        if not address:
            continue

        neighborhood = _detect_neighborhood(address)
        if not neighborhood:
            continue

        price = _parse_price(price_text)
        units = _parse_units(address + " " + (bed_el.get_text() if bed_el else ""))

        deal = Deal(
            source="fsbo_dot_com",
            channel="off_market",
            off_market=True,
            address=address,
            city="Chicago",
            state="IL",
            neighborhood=neighborhood,
            asset_class="multifamily",
            units=units,
            price=price,
            url=link_url if link_url.startswith("http") else f"https://www.fsbo.com{link_url}",
            raw={},
        )
        deals.append(deal)

    logger.info("FSBO.com: %d deals", len(deals))
    return deals


def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Combines Craigslist + FSBO.com.
    dry_run=True returns synthetic stub data.
    """
    if dry_run:
        logger.info("FSBO: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("FSBO: starting scrape")
    deals: list[Deal] = []

    try:
        cl_re = _scrape_craigslist(CL_REALESTATE_URL, "real_estate", limit=limit // 2)
        deals.extend(cl_re)
    except Exception as e:
        logger.error("FSBO: Craigslist real estate scrape failed: %s", e)

    try:
        cl_comm = _scrape_craigslist(CL_COMMERCIAL_URL, "commercial", limit=limit // 2)
        deals.extend(cl_comm)
    except Exception as e:
        logger.error("FSBO: Craigslist commercial scrape failed: %s", e)

    try:
        fsbo = _scrape_fsbo_dot_com(limit=limit // 4)
        deals.extend(fsbo)
    except Exception as e:
        logger.error("FSBO: FSBO.com scrape failed: %s", e)

    logger.info("FSBO: total %d deals collected", len(deals))
    return deals[:limit]


def _stub_deals() -> list[Deal]:
    return [
        Deal(
            source="craigslist",
            channel="off_market",
            off_market=True,
            address="Pilsen area — 6 flat, great condition",
            city="Chicago",
            state="IL",
            neighborhood="Pilsen",
            asset_class="multifamily",
            units=6,
            price=549_000,
            url="https://chicago.craigslist.org/stub-1",
            raw={"title": "6 flat Pilsen — owner selling direct", "stub": True},
        ),
        Deal(
            source="craigslist",
            channel="off_market",
            off_market=True,
            address="Logan Square — 8 unit apartment building",
            city="Chicago",
            state="IL",
            neighborhood="Logan Square",
            asset_class="multifamily",
            units=8,
            price=875_000,
            url="https://chicago.craigslist.org/stub-2",
            raw={"title": "8 unit Logan Square investment property", "stub": True},
        ),
        Deal(
            source="fsbo_dot_com",
            channel="off_market",
            off_market=True,
            address="4721 W 26th St, Chicago IL",
            city="Chicago",
            state="IL",
            neighborhood="Little Village",
            asset_class="mixed-use",
            units=5,
            price=490_000,
            url="https://www.fsbo.com/stub-3",
            raw={"stub": True},
        ),
    ]
