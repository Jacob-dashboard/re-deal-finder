"""
Crexi scraper — on-market multifamily/mixed-use in Cook County, IL.

Crexi uses a React SPA. This scraper uses Playwright (headless Chromium) to
render the page and parse the resulting HTML. Falls back to the undocumented
JSON API as a secondary attempt.
"""

import json
import logging
import os
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
CREXI_SEARCH_URL = "https://www.crexi.com/properties/IL/Cook/Multifamily?pageSize=60"

COOKIE_FILE = "config/crexi_cookies.json"

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
    """Parse a Crexi cui-card listing into a Deal.

    Field map (observed on /properties/IL/Cook/Multifamily):
      - div.cui-card-info-title       → price text ("$2,895,000" or "Unpriced")
      - h5.cui-card-info-subtitle     → listing name / street address
      - div.cui-card-info-description → details, e.g. "Multifamily • 7.42% CAP • 32 Units"
      - h4.cui-card-info-text         → full address with city + zip
      - span.cui-card-info-text-small → "City, ST ZIP"
    """
    try:
        deal = Deal(source="crexi", channel="on_market")

        link = (
            card.select_one("a.cui-card-cover-link")
            or card.select_one("a[href*='/properties/']")
        )
        if not link or not link.get("href"):
            return None
        href = link["href"]
        m = re.search(r"/properties/(\d+)", href)
        if not m:
            return None
        deal.external_id = m.group(1)
        deal.url = href if href.startswith("http") else f"https://www.crexi.com{href}"
        # Strip recommId tracking param
        deal.url = deal.url.split("?")[0]

        # Price
        title_el = card.select_one(".cui-card-info-title")
        if title_el:
            t = title_el.get_text(" ", strip=True)
            if "$" in t:
                deal.price = _parse_price(t)

        # Street address: prefer the subtitle (clean street name on its own).
        subtitle_el = card.select_one("h5.cui-card-info-subtitle, .cui-card-info-subtitle")
        if subtitle_el:
            sub = subtitle_el.get_text(" ", strip=True)
            # Subtitle is sometimes a marketing name ("Euclid Place Apartments")
            # rather than a street. That's still a useful identifier.
            deal.address = sub

        # City / state / zip — from the small text element, which is just "City, ST ZIP".
        small_el = card.select_one("span.cui-card-info-text-small, .cui-card-info-text-small")
        if small_el:
            small = small_el.get_text(" ", strip=True)
            zm = re.search(r"\b(\d{5})\b", small)
            if zm:
                deal.zip_code = zm.group(1)
            cm = re.match(r"([^,]+),\s*([A-Z]{2})", small)
            if cm:
                deal.city = cm.group(1).strip()
                deal.state = cm.group(2).strip()

        # If no subtitle but the h4 has a usable full address, fall back to it.
        if not deal.address:
            addr_el = card.select_one("h4.cui-card-info-text, .cui-card-info-text")
            if addr_el:
                deal.address = addr_el.get_text(" ", strip=True)

        # Details: "Multifamily • 7.42% CAP • 32 Units • $185,000/unit"
        desc_el = card.select_one(".cui-card-info-description")
        if desc_el:
            desc = desc_el.get_text(" ", strip=True)
            dl = desc.lower()
            if "multi" in dl or "apartment" in dl:
                deal.asset_class = "multifamily"
            elif "mixed" in dl:
                deal.asset_class = "mixed-use"
            # Units
            um = re.search(r"(\d[\d,]*)\s*(?:-\s*)?[Uu]nit", desc)
            if um:
                try:
                    deal.units = int(um.group(1).replace(",", ""))
                except ValueError:
                    pass
            # Cap rate
            cm2 = re.search(r"([\d.]+)\s*%\s*CAP", desc, re.IGNORECASE)
            if cm2:
                try:
                    deal.cap_rate = float(cm2.group(1))
                except ValueError:
                    pass

        # Price per unit
        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units

        if not deal.state:
            deal.state = "IL"
        return deal
    except Exception as e:
        logger.debug("Crexi: error parsing card: %s", e)
        return None


def _parse_listing_anchors(soup) -> list[Deal]:
    """Fallback: find every anchor whose href matches /properties/{numeric-id}/...
    and synthesize a minimal Deal. Used when the cui-card structure shifts."""
    seen: set[str] = set()
    deals: list[Deal] = []
    for a in soup.select("a[href*='/properties/']"):
        href = a.get("href", "")
        m = re.search(r"/properties/(\d+)/([a-zA-Z0-9\-]+)", href)
        if not m:
            continue
        ext_id = m.group(1)
        if ext_id in seen:
            continue
        seen.add(ext_id)

        deal = Deal(source="crexi", channel="on_market")
        deal.external_id = ext_id
        deal.url = href if href.startswith("http") else f"https://www.crexi.com{href}"
        # Slug → human-ish address
        slug = m.group(2).replace("-", " ").replace("illinois ", "").strip()
        deal.address = slug.title() if slug else None
        deal.city = "Chicago"
        deal.state = "IL"
        # Walk up to the nearest card container to grab price text if present
        card = a
        for _ in range(5):
            card = card.parent if card and card.parent else None
            if card is None:
                break
            if hasattr(card, "select_one"):
                price_el = card.select_one(".cui-card-info-description, .cui-card-info-title")
                if price_el:
                    deal.price = deal.price or _parse_price(price_el.get_text(" ", strip=True))
                    break
        deals.append(deal)
    return deals


# ---------------------------------------------------------------------------
# __NEXT_DATA__ JSON extraction (Crexi is a Next.js app)
# ---------------------------------------------------------------------------

def _walk_for_listings(node, found: list, depth: int = 0) -> None:
    """Recursively walk a JSON tree and collect listing-shaped dicts."""
    if depth > 12:
        return
    if isinstance(node, dict):
        # heuristic: a listing has at least an id + (address|name) + price-ish key
        keys = set(node.keys())
        looks_like_listing = (
            ("id" in keys or "assetId" in keys)
            and any(k in keys for k in ("askingPrice", "price", "primaryImageUrl"))
            and any(k in keys for k in ("name", "address", "displayAddress", "fullAddress"))
        )
        if looks_like_listing:
            found.append(node)
            return
        for v in node.values():
            _walk_for_listings(v, found, depth + 1)
    elif isinstance(node, list):
        for v in node:
            _walk_for_listings(v, found, depth + 1)


def _next_listing_to_deal(item: dict) -> Optional[Deal]:
    try:
        deal = Deal(source="crexi", channel="on_market")
        deal.external_id = str(item.get("id") or item.get("assetId") or "")
        if deal.external_id:
            deal.url = f"https://www.crexi.com/properties/{deal.external_id}"

        addr = item.get("address") or {}
        if isinstance(addr, str):
            deal.address = addr
        else:
            deal.address = (
                addr.get("street") or item.get("displayAddress")
                or item.get("fullAddress") or item.get("name") or ""
            )
            deal.city     = addr.get("city")  or "Chicago"
            deal.state    = addr.get("state") or "IL"
            deal.zip_code = addr.get("zip")   or addr.get("zipCode") or ""

        deal.neighborhood = item.get("neighborhood") or ""
        deal.latitude     = item.get("latitude")
        deal.longitude    = item.get("longitude")
        deal.asset_class  = (item.get("primaryUse") or item.get("propertyType") or "").lower() or None
        deal.units        = item.get("totalUnits") or item.get("units") or item.get("numberOfUnits")
        deal.sqft         = item.get("totalSqft")  or item.get("buildingSize") or item.get("squareFeet")
        deal.year_built   = item.get("yearBuilt")
        deal.price        = item.get("askingPrice") or item.get("price")
        deal.cap_rate     = item.get("capRate")
        deal.noi          = item.get("noi")
        deal.gross_rent   = item.get("grossAnnualRents") or item.get("grossRent")
        deal.days_on_market = item.get("daysOnMarket")
        deal.listing_date   = item.get("listDate", "") or item.get("activatedOn", "")

        broker = item.get("broker") or item.get("contact") or {}
        if isinstance(broker, dict):
            deal.broker       = broker.get("name") or ""
            deal.broker_phone = broker.get("phone") or ""
            deal.broker_email = broker.get("email") or ""

        if deal.price and deal.units and deal.units > 0:
            deal.price_per_unit = deal.price / deal.units
        if deal.price and deal.sqft and deal.sqft > 0:
            deal.price_per_sqft = deal.price / deal.sqft

        deal.raw = {}
        if not deal.url:
            return None
        return deal
    except Exception as e:
        logger.debug("Crexi: error parsing __NEXT_DATA__ item: %s", e)
        return None


def _parse_next_data(soup) -> list[Deal]:
    """Pull listings out of the embedded __NEXT_DATA__ / state JSON if present."""
    deals: list[Deal] = []
    seen_ids: set[str] = set()

    candidates = []
    el = soup.find("script", id="__NEXT_DATA__")
    if el and el.string:
        candidates.append(el.string)
    for s in soup.find_all("script"):
        txt = s.string or ""
        if "askingPrice" in txt and "{" in txt:
            candidates.append(txt)

    for txt in candidates:
        # Find the first balanced JSON object in the script body
        start = txt.find("{")
        if start < 0:
            continue
        try:
            data = json.loads(txt[start:])
        except Exception:
            # Try the leading {...} block only
            try:
                end = txt.rfind("}")
                if end > start:
                    data = json.loads(txt[start : end + 1])
                else:
                    continue
            except Exception:
                continue

        found: list = []
        _walk_for_listings(data, found)
        for item in found:
            d = _next_listing_to_deal(item)
            if d and d.external_id and d.external_id not in seen_ids:
                seen_ids.add(d.external_id)
                deals.append(d)

    return deals


# ---------------------------------------------------------------------------
# Playwright scrape
# ---------------------------------------------------------------------------

def _load_cookies() -> list[dict]:
    """Load Crexi cookies (extracted from Chrome) for Playwright. Empty if absent."""
    if not os.path.exists(COOKIE_FILE):
        return []
    try:
        with open(COOKIE_FILE) as f:
            raw = json.load(f) or []
    except Exception as e:
        logger.warning("Crexi: cookie file unreadable: %s", e)
        return []

    out = []
    for c in raw:
        # Skip empty values — Playwright rejects them
        if not c.get("name") or c.get("value") is None:
            continue
        ss = (c.get("sameSite") or "Lax").capitalize()
        if ss not in ("Strict", "Lax", "None"):
            ss = "Lax"
        cookie = {
            "name":     c["name"],
            "value":    str(c["value"]),
            "domain":   c.get("domain") or ".crexi.com",
            "path":     c.get("path") or "/",
            "secure":   bool(c.get("secure", True)),
            "httpOnly": bool(c.get("httpOnly", False)),
            "sameSite": ss,
        }
        out.append(cookie)
    return out


def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Scrape Crexi using headless Chromium via Playwright."""
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("Crexi: playwright not installed — run: pip install playwright && playwright install chromium")
        return []

    deals: list[Deal] = []
    max_pages = min(3, config.MAX_PAGES)

    cookies = _load_cookies()
    logger.info("Crexi: loaded %d cookies from %s", len(cookies), COOKIE_FILE)

    try:
        with sync_playwright() as p:
            launch_kwargs = dict(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                ],
            )
            try:
                browser = p.chromium.launch(channel="chrome", **launch_kwargs)
            except Exception:
                browser = p.chromium.launch(**launch_kwargs)
            ctx = browser.new_context(
                viewport=VIEWPORT,
                user_agent=USER_AGENT,
                locale="en-US",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            # Hide webdriver flag
            ctx.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
            )
            if cookies:
                # Try bulk first; if Playwright rejects the batch, apply one-by-one
                try:
                    ctx.add_cookies(cookies)
                except Exception as e:
                    logger.warning("Crexi: bulk cookie set failed (%s) — retrying individually", e)
                    ok = 0
                    for ck in cookies:
                        try:
                            ctx.add_cookies([ck])
                            ok += 1
                        except Exception:
                            pass
                    logger.info("Crexi: %d/%d cookies applied", ok, len(cookies))

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

                # Wait for cui-card listings to render. Crexi uses an Angular
                # SPA — we look for the cover-link anchor pattern.
                try:
                    page.wait_for_selector(
                        "a.cui-card-cover-link, a[href*='/properties/']",
                        timeout=25_000,
                    )
                except PWTimeout:
                    logger.warning("Crexi: no cards appeared on page %d — blocked or no results", page_num)
                    break

                # Scroll to trigger lazy loading of the listing list
                for _ in range(6):
                    page.mouse.wheel(0, 1200)
                    time.sleep(0.5)

                time.sleep(random.uniform(1.5, 2.5))

                html = page.content()
                soup = BeautifulSoup(html, "lxml")

                # 1) Crexi cui-card containers (preferred — has price/units inline)
                page_deals: list[Deal] = []
                seen_ids: set[str] = set()
                # Walk each cover-link up to the nearest <cui-card> (custom element)
                # or any ancestor containing the .cui-card-info-title.
                card_candidates = []
                for cover in soup.select("a.cui-card-cover-link"):
                    root = cover
                    for _ in range(8):
                        if root.parent and root.parent.name:
                            root = root.parent
                            if root.name == "cui-card":
                                break
                            if hasattr(root, "select_one") and root.select_one(".cui-card-info-title"):
                                break
                    card_candidates.append(root)

                for card in card_candidates:
                    deal = _parse_card(card)
                    if deal and deal.external_id and deal.external_id not in seen_ids:
                        seen_ids.add(deal.external_id)
                        page_deals.append(deal)

                # 2) Fallback: any /properties/{id}/{slug} anchor
                if not page_deals:
                    for d in _parse_listing_anchors(soup):
                        if d.external_id and d.external_id not in seen_ids:
                            seen_ids.add(d.external_id)
                            page_deals.append(d)

                # 3) Last resort: __NEXT_DATA__-style embedded JSON (older Crexi builds)
                if not page_deals:
                    page_deals = _parse_next_data(soup)

                if not page_deals:
                    logger.info("Crexi: no cards parsed on page %d", page_num)
                    break

                for d in page_deals:
                    deals.append(d)
                    if len(deals) >= limit:
                        break

                logger.info("Crexi: page %d → %d listings (total: %d)", page_num, len(page_deals), len(deals))

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
