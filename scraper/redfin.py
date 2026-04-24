"""
Redfin scraper — on-market multifamily listings in Chicago.

Strategy:
  1. Redfin stingray internal JSON API (fast, no browser, clean data)
  2. Playwright fallback if API is blocked/returns nothing
"""

import json
import logging
import random
import re
import time
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scraper import Deal
import config

logger = logging.getLogger(__name__)

BASE_URL     = "https://www.redfin.com"
STINGRAY_API = "https://www.redfin.com/stingray/api/gis"

# Chicago = region_id 29470, region_type 6 (city)
REGION_ID   = 29470
REGION_TYPE = 6

# Playwright fallback search URL
SEARCH_URL = (
    "https://www.redfin.com/city/29470/IL/Chicago"
    "/filter/property-type=multifamily,max-price=1200000"
)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent":      USER_AGENT,
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://www.redfin.com/",
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def _parse_num(val) -> Optional[float]:
    if isinstance(val, (int, float)):
        return float(val)
    text = re.sub(r"[^\d.]", "", str(val or ""))
    try:
        return float(text) if text else None
    except ValueError:
        return None


def _extract_units_from_text(text: str) -> Optional[int]:
    """Try to parse unit count from listing title / remarks."""
    patterns = [
        r"(\d+)\s*[-\s]?(?:unit|flat|apt|apartment|family|plex)s?",
        r"(\d+)[-\s]?(?:flat|unit|family)",
        r"(?:two|2)[-\s]?flat",
        r"(?:three|3)[-\s]?flat",
        r"(?:six|6)[-\s]?flat",
        r"(\d+)[-]?(?:unit)",
    ]
    two_flat  = re.search(r"\btwo\s*-?\s*flat\b", text.lower())
    three_flat= re.search(r"\bthree\s*-?\s*flat\b", text.lower())
    if two_flat:   return 2
    if three_flat: return 3
    for pat in patterns:
        m = re.search(pat, text.lower())
        if m:
            try:
                n = int(m.group(1)) if m.lastindex else None
                if n and 2 <= n <= 100:
                    return n
            except (ValueError, IndexError):
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


def _val(obj) -> Optional[float]:
    """Extract value from Redfin's {value, level} dict or plain scalar."""
    if isinstance(obj, dict):
        v = obj.get("value")
        return float(v) if v is not None else None
    if isinstance(obj, (int, float)):
        return float(obj)
    return None


def _home_to_deal(home: dict) -> Optional[Deal]:
    """Convert a Redfin API home object to a Deal.

    Redfin v8 API returns flat objects where many fields are {value, level} dicts.
    Direct string fields: city, state, zip, url.
    Dict fields: streetLine, price, sqFt, dom, yearBuilt.
    """
    try:
        deal = Deal(source="redfin", channel="on_market")

        # URL
        url_path = home.get("url", "")
        if url_path:
            deal.url = urljoin(BASE_URL, url_path)

        # Address — streetLine is {value, level}
        street_raw = home.get("streetLine", {})
        deal.address  = (street_raw.get("value", "") if isinstance(street_raw, dict)
                         else str(street_raw or ""))
        deal.city     = home.get("city", "Chicago")
        deal.state    = home.get("state", "IL")
        deal.zip_code = home.get("zip", "") or home.get("postalCode", "")
        deal.neighborhood = _zip_to_neighborhood(str(deal.zip_code)) or ""

        # Price
        price_raw = home.get("price", {})
        deal.price = _val(price_raw)

        # sqFt
        sqft_raw = home.get("sqFt", {})
        sqft_v = _val(sqft_raw)
        deal.sqft = int(sqft_v) if sqft_v else None

        # Days on market — field is 'dom' in v8
        dom_raw = home.get("dom", {})
        dom_v = _val(dom_raw)
        deal.days_on_market = int(dom_v) if dom_v is not None else None

        # Year built
        yb_raw = home.get("yearBuilt", {})
        yb_v = _val(yb_raw)
        deal.year_built = int(yb_v) if yb_v else None

        # Beds (direct int)
        beds_raw = home.get("beds")

        # Unit count: try listing remarks first
        remarks = home.get("listingRemarks", "") or ""
        full_text = f"{deal.address} {remarks}"
        deal.units = _extract_units_from_text(full_text)

        # Fallback: infer units from beds (Chicago multi-family: ~2 beds/unit)
        if deal.units is None and beds_raw:
            try:
                beds = int(beds_raw)
                if beds >= 4:
                    deal.units = max(beds // 2, 2)
            except (ValueError, TypeError):
                pass

        deal.asset_class = "multifamily"

        # Price per unit / sqft
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units
        if deal.price and deal.sqft and deal.sqft > 0:
            deal.price_per_sqft = deal.price / deal.sqft

        return deal
    except Exception as e:
        logger.debug("Redfin: error parsing home: %s", e)
        return None


# ---------------------------------------------------------------------------
# API scrape
# ---------------------------------------------------------------------------

def _scrape_api(limit: int) -> list[Deal]:
    """Hit Redfin's stingray API for Chicago multifamily listings."""
    deals: list[Deal] = []
    session = requests.Session()
    max_pages = min(3, config.MAX_PAGES)

    for page_num in range(1, max_pages + 1):
        if len(deals) >= limit:
            break

        params = {
            "al":          1,
            "market":      "chicago",
            "num_homes":   min(50, limit - len(deals) + 10),
            "ord":         "redfin-recommended-asc",
            "page_number": page_num,
            "status":      9,          # for-sale
            "uipt":        4,          # multi-family
            "v":           8,
            "region_id":   REGION_ID,
            "region_type": REGION_TYPE,
            "max_price":   config.MAX_PRICE,
        }

        try:
            time.sleep(random.uniform(1.5, 3.0))
            resp = session.get(
                STINGRAY_API, headers=HEADERS, params=params,
                timeout=config.REQUEST_TIMEOUT,
            )
        except Exception as e:
            logger.warning("Redfin API: request error: %s", e)
            break

        if resp.status_code != 200:
            logger.warning("Redfin API: HTTP %s on page %d", resp.status_code, page_num)
            break

        # Redfin prepends `{}&&` to prevent JSON hijacking — strip it
        text = resp.text
        if text.startswith("{}&&"):
            text = text[4:]
        elif text.startswith("{}"):
            text = text[2:]

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            logger.warning("Redfin API: JSON parse error: %s | preview: %s", e, text[:200])
            break

        payload = data.get("payload", {})
        homes   = payload.get("homes", [])

        if not homes:
            logger.info("Redfin API: no homes on page %d — stopping", page_num)
            break

        logger.info("Redfin API: page %d → %d homes", page_num, len(homes))

        for home in homes:
            deal = _home_to_deal(home)
            if deal and deal.address:
                deals.append(deal)
            if len(deals) >= limit:
                break

    logger.info("Redfin API: %d deals collected", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Playwright fallback
# ---------------------------------------------------------------------------

def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Render the Redfin search results page with Playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("Redfin: playwright not installed")
        return []

    deals: list[Deal] = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=USER_AGENT,
                locale="en-US",
            )
            page = ctx.new_page()
            page.route(
                "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}",
                lambda r: r.abort(),
            )

            logger.info("Redfin PW: navigating to %s", SEARCH_URL)
            try:
                page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=35_000)
            except PWTimeout:
                logger.warning("Redfin PW: page load timed out")
                browser.close()
                return []

            title = page.title().lower()
            if any(kw in title for kw in ["captcha", "access denied", "just a moment", "blocked"]):
                logger.warning("Redfin PW: blocked — title: %s", title)
                browser.close()
                return []

            # Wait for listing cards
            card_selectors = (
                "[class*='HomeCardContainer'], "
                "[data-rf-test-id='abp-homecard'], "
                ".HomeCard, [class*='homeCard']"
            )
            try:
                page.wait_for_selector(card_selectors, timeout=25_000)
            except PWTimeout:
                logger.warning("Redfin PW: no listing cards appeared — checking page title: %s", page.title())
                # Save HTML snippet for debugging
                snip = page.content()[:500]
                logger.debug("Redfin PW HTML snippet: %s", snip)
                browser.close()
                return []

            time.sleep(random.uniform(2, 3))
            html  = page.content()
            soup  = BeautifulSoup(html, "lxml")
            browser.close()

        cards = (
            soup.select("[class*='HomeCardContainer']")
            or soup.select("[data-rf-test-id='abp-homecard']")
            or soup.select(".HomeCard")
        )
        logger.info("Redfin PW: found %d cards", len(cards))

        for card in cards[:limit]:
            deal = Deal(source="redfin", channel="on_market", asset_class="multifamily")
            deal.city  = "Chicago"
            deal.state = "IL"

            # URL
            link = card.select_one("a[href*='/home/']")
            if link and link.get("href"):
                href = link["href"]
                deal.url = href if href.startswith("http") else urljoin(BASE_URL, href)

            # Address — Redfin uses class containing "Address"
            addr_el = card.select_one("[class*='Address'], [class*='address']")
            if addr_el:
                full_addr = addr_el.get_text(strip=True)
                # "4709 W Washington Blvd, Chicago, IL 60644" → split out street
                m_comma = re.match(r"^([^,]+),", full_addr)
                deal.address = m_comma.group(1).strip() if m_comma else full_addr

            # Price — class contains "Price--value"
            price_el = card.select_one("[class*='Price--value'], [class*='Homecard__Price']")
            if price_el:
                deal.price = _parse_num(price_el.get_text(strip=True))

            # ZIP from URL path: /IL/Chicago/ADDR-60618/home/...
            if deal.url:
                zm = re.search(r"-(\d{5})/", deal.url)
                if zm:
                    deal.zip_code = zm.group(1)
                    deal.neighborhood = _zip_to_neighborhood(deal.zip_code) or ""

            # Also try ZIP from address text
            if not deal.zip_code and deal.address:
                zm = re.search(r"\b(6\d{4})\b", deal.address)
                if zm:
                    deal.zip_code = zm.group(1)
                    deal.neighborhood = _zip_to_neighborhood(deal.zip_code) or ""

            # Stats: beds for unit inference
            stats_el = card.select_one("[class*='Stats']")
            if stats_el:
                stats_txt = stats_el.get_text(strip=True)
                bm = re.search(r"(\d+)\s*bed", stats_txt)
                if bm:
                    try:
                        beds = int(bm.group(1))
                        if beds >= 4:
                            deal.units = max(beds // 2, 2)
                    except ValueError:
                        pass
                sqm = re.search(r"([\d,]+)\s*sq", stats_txt)
                if sqm:
                    try:
                        deal.sqft = int(sqm.group(1).replace(",", ""))
                    except ValueError:
                        pass

            # Try unit count from description text
            desc_el = card.select_one("[class*='Homecard__Body'], [class*='description']")
            if desc_el:
                deal.units = deal.units or _extract_units_from_text(desc_el.get_text())

            # Price per unit
            if deal.price and deal.units and deal.units > 0:
                deal.price_per_unit = deal.price / deal.units

            if deal.url or deal.address:
                deals.append(deal)

    except Exception as e:
        logger.warning("Redfin PW: error — %s", e)

    logger.info("Redfin PW: %d deals collected", len(deals))
    return deals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from Redfin.
    Runs stingray API + Playwright scrape and merges results.
    API is fast and returns price/address. Playwright catches actual
    multifamily buildings (two-flats, etc.) that the API may miss.
    """
    if dry_run:
        logger.info("Redfin: dry-run mode — returning stub deals")
        return _stub_deals()

    logger.info("Redfin: starting scrape (limit=%d)", limit)

    # Run API first (fast, good data quality)
    api_deals = _scrape_api(limit)

    # Run Playwright to catch multifamily buildings the API misses
    pw_deals = _scrape_with_playwright(limit // 2 + 10)

    # Merge: prefer API deals (have better data), add Playwright-only ones
    seen_urls: set = {d.url for d in api_deals if d.url}
    seen_addrs: set = {
        (d.address or "").upper().strip()
        for d in api_deals if d.address
    }
    extra = []
    for d in pw_deals:
        url_key  = d.url or ""
        addr_key = (d.address or "").upper().strip()
        if url_key and url_key in seen_urls:
            continue
        if addr_key and addr_key in seen_addrs:
            continue
        extra.append(d)
        if url_key:
            seen_urls.add(url_key)
        if addr_key:
            seen_addrs.add(addr_key)

    deals = api_deals + extra
    logger.info("Redfin: %d API + %d Playwright-only = %d total", len(api_deals), len(extra), len(deals))

    if not deals:
        logger.warning("Redfin: no deals retrieved from any method")

    return deals[:limit]


# ---------------------------------------------------------------------------
# Stub data
# ---------------------------------------------------------------------------

def _stub_deals() -> list[Deal]:
    return [
        Deal(
            source="redfin",
            channel="on_market",
            url="https://www.redfin.com/IL/Chicago/2345-N-Pulaski-Rd/home/12345",
            address="2345 N Pulaski Rd",
            city="Chicago",
            state="IL",
            zip_code="60639",
            neighborhood="Hermosa",
            asset_class="multifamily",
            units=6,
            sqft=5400,
            price=650_000,
            price_per_unit=108_333,
            days_on_market=18,
            raw={"stub": True},
        ),
        Deal(
            source="redfin",
            channel="on_market",
            url="https://www.redfin.com/IL/Chicago/3412-W-Armitage-Ave/home/67890",
            address="3412 W Armitage Ave",
            city="Chicago",
            state="IL",
            zip_code="60647",
            neighborhood="Logan Square",
            asset_class="multifamily",
            units=8,
            sqft=7200,
            price=875_000,
            price_per_unit=109_375,
            days_on_market=7,
            raw={"stub": True},
        ),
    ]
