"""
LoopNet scraper — on-market multifamily/mixed-use in Cook County, IL.

Uses Playwright (real Chrome via the `chrome` channel when available,
plus playwright-stealth and a desktop-fingerprint context) to bypass JS
rendering and Akamai Bot Manager. Falls back gracefully with a warning
if blocked.

If `output/loopnet_cookies.json` exists, the cookies are injected into
the context before navigation — this lets a human-validated session
(produced by `scripts/extract_loopnet_cookies.py`) carry past Akamai's
IP/network-level challenges that headless code can't solve on its own.
"""

import json
import logging
import random
import re
import time
from pathlib import Path
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
COOKIES_PATH = Path(config.LOCAL_OUTPUT_DIR) / "loopnet_cookies.json"

# Realistic viewport + UA to avoid bot detection.
# Chrome 125+ string and 1920x1080 resolution match the most common desktop
# fingerprint; LoopNet's Akamai Bot Manager is suspicious of unusual sizes.
VIEWPORT = {"width": 1920, "height": 1080}
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125.0.0.0 Safari/537.36"
)

# Anti-bot launch flags. --disable-blink-features=AutomationControlled is the
# main one — it prevents `navigator.webdriver === true`, which is the cheapest
# bot signal.
LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]


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

def _is_blocked_page(page) -> bool:
    """Detect Akamai/CAPTCHA/access-denied responses by title and body text."""
    try:
        title = (page.title() or "").lower()
        if any(kw in title for kw in ("captcha", "challenge", "access denied", "just a moment", "blocked")):
            return True
        # Akamai Bot Manager often returns "Pardon Our Interruption" or empty
        # body with reference IDs.
        body_text = (page.evaluate("() => document.body && document.body.innerText || ''") or "").lower()
        for kw in ("pardon our interruption", "verify you are a human", "access to this page has been denied"):
            if kw in body_text:
                return True
    except Exception:
        pass
    return False


def _try_apply_stealth(page) -> None:
    """Best-effort: apply playwright-stealth patches if the package is installed."""
    try:
        from playwright_stealth import Stealth
        Stealth().apply_stealth_sync(page)
        logger.debug("LoopNet: playwright-stealth applied")
    except ImportError:
        logger.debug("LoopNet: playwright-stealth not installed — skipping")
    except Exception as e:
        logger.debug("LoopNet: stealth apply failed: %s", e)


def _launch_browser(p):
    """Launch real Chrome if available; fall back to bundled Chromium."""
    from playwright.sync_api import Error as PWError
    try:
        return p.chromium.launch(
            headless=True,
            channel="chrome",
            args=LAUNCH_ARGS,
        )
    except PWError as e:
        logger.info("LoopNet: Chrome channel unavailable (%s) — falling back to bundled Chromium", e)
        return p.chromium.launch(headless=True, args=LAUNCH_ARGS)


def _new_context(browser):
    """Create a context with a realistic desktop fingerprint + warmed cookies."""
    ctx = browser.new_context(
        viewport=VIEWPORT,
        user_agent=USER_AGENT,
        locale="en-US",
        timezone_id="America/Chicago",
        device_scale_factor=2,
        is_mobile=False,
        has_touch=False,
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                      "image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-Ch-Ua": '"Chromium";v="125", "Google Chrome";v="125", "Not.A/Brand";v="24"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
            "Upgrade-Insecure-Requests": "1",
        },
    )

    # Inject session cookies from a previous human-validated browser run.
    # File format: list of Playwright cookie dicts (name, value, domain, path,
    # expires, httpOnly, secure, sameSite). Generated by
    # scripts/extract_loopnet_cookies.py.
    if COOKIES_PATH.exists():
        try:
            cookies = json.loads(COOKIES_PATH.read_text())
            ctx.add_cookies(cookies)
            logger.info("LoopNet: loaded %d cookies from %s", len(cookies), COOKIES_PATH)
        except Exception as e:
            logger.warning("LoopNet: could not load cookies from %s: %s", COOKIES_PATH, e)

    return ctx


def _fetch_search_page_html(limit: int) -> Optional[str]:
    """Open Akamai-protected search page once and return rendered HTML.

    Returns None on bot-block / timeout. Retries once with a longer warm-up
    if the first attempt is challenged.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        logger.error("LoopNet: playwright not installed — run: pip install playwright && playwright install chromium")
        return None

    for attempt in (1, 2):
        try:
            with sync_playwright() as p:
                browser = _launch_browser(p)
                ctx = _new_context(browser)
                page = ctx.new_page()
                _try_apply_stealth(page)

                # Block heavy assets to speed up — but keep CSS/JS so Akamai's
                # client-side challenge actually completes.
                page.route(
                    "**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf}",
                    lambda r: r.abort(),
                )

                # Random pre-request warm-up so we don't hit the URL the
                # millisecond the browser comes up.
                time.sleep(random.uniform(2.0, 5.0))

                logger.info("LoopNet: navigating to search page (attempt %d)", attempt)
                try:
                    page.goto(SEARCH_URL, wait_until="domcontentloaded", timeout=45_000)
                except PWTimeout:
                    logger.warning("LoopNet: page load timed out (attempt %d)", attempt)
                    browser.close()
                    if attempt == 1:
                        time.sleep(10)
                        continue
                    return None

                if _is_blocked_page(page):
                    logger.warning("LoopNet: bot-detection page on attempt %d", attempt)
                    browser.close()
                    if attempt == 1:
                        time.sleep(10)
                        continue
                    return None

                # Wait for cards
                try:
                    page.wait_for_selector(
                        "article.listingCard, [data-testid='listing-card'], "
                        ".listing-card, li.placard",
                        timeout=20_000,
                    )
                except PWTimeout:
                    logger.warning("LoopNet: no cards rendered on attempt %d", attempt)
                    browser.close()
                    if attempt == 1:
                        time.sleep(10)
                        continue
                    return None

                # Human-like dwell
                time.sleep(random.uniform(3.0, 8.0))

                html = page.content()
                browser.close()
                return html
        except Exception as e:
            logger.warning("LoopNet: Playwright error on attempt %d: %s", attempt, e)
            if attempt == 1:
                time.sleep(10)
                continue
            return None

    return None


def _scrape_with_playwright(limit: int) -> list[Deal]:
    """Scrape LoopNet using a stealth-patched Chrome via Playwright.

    Currently scrapes page 1 only, with retry on bot-block. Pagination over
    Akamai-protected pages tends to re-challenge each click anyway; safer to
    rely on the first page's listings (sorted by recency on LoopNet).
    """
    html = _fetch_search_page_html(limit)
    if html is None:
        logger.warning("LoopNet: no HTML retrieved — site is blocking headless browsers")
        return []

    soup = BeautifulSoup(html, "lxml")
    cards = (
        soup.select("article.listingCard")
        or soup.select("[data-testid='listing-card']")
        or soup.select(".listing-card")
        or soup.select("li.placard")
    )

    deals: list[Deal] = []
    for card in cards:
        if len(deals) >= limit:
            break
        deal = _parse_listing_card(card)
        if deal and deal.url:
            deals.append(deal)

    logger.info("LoopNet: parsed %d cards → %d deals", len(cards), len(deals))
    return deals


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def scrape(dry_run: bool = False, limit: int = 50) -> list[Deal]:
    """
    Main entry point. Returns list of Deal objects from LoopNet.
    dry_run=True returns synthetic stub data instead of hitting the network.

    LoopNet's Akamai EdgeSuite is currently IP-blocking this host (returns
    HTTP "Access Denied" before any rendering). To avoid burning ~60s of
    retries on every scan, the live path short-circuits unless either:

      - a session cookie file exists at ``output/loopnet_cookies.json``
        (produced by ``scripts/extract_loopnet_cookies.py``), OR
      - the env var ``LOOPNET_FORCE=1`` is set (e.g. for testing from a
        new IP / VPN / proxy).

    Revisit this when an IP rotation, residential proxy, or fresh cookies
    are available.
    """
    import os

    if dry_run:
        logger.info("LoopNet: dry-run mode — returning stub deals")
        return _stub_deals()

    if not COOKIES_PATH.exists() and not os.environ.get("LOOPNET_FORCE"):
        logger.warning(
            "LoopNet: skipped — Akamai is IP-blocking this host. "
            "Run scripts/extract_loopnet_cookies.py from a real Chrome "
            "session, or set LOOPNET_FORCE=1 to override."
        )
        return []

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
