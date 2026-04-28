#!/usr/bin/env python3
"""
Extract a fresh LoopNet session into output/loopnet_cookies.json.

Why this exists:
LoopNet is fronted by Akamai Bot Manager + EdgeSuite. Headless requests
from a residential IP routinely return "Access Denied" before the page
even renders — there is nothing the scraper can do at the HTTP layer
to bypass it. The reliable workaround is to launch a *visible* Chrome
window, let a human pass any challenge, then dump the cookies that
LoopNet has set on this machine.

The main scraper (scraper/loopnet.py) automatically loads
output/loopnet_cookies.json into its Playwright context if the file
exists, so a refreshed session immediately unblocks the next scan.

Usage:
    python3 scripts/extract_loopnet_cookies.py

Steps performed:
1. Open a real Chrome window (non-headless).
2. Navigate to LoopNet's Cook County multifamily search.
3. Wait until you (the human) close the window — solve any CAPTCHA in
   the meantime.
4. Save all cookies set on the loopnet.com domain to JSON.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import config  # noqa: E402

OUTPUT_PATH = Path(config.LOCAL_OUTPUT_DIR) / "loopnet_cookies.json"
SEARCH_URL = (
    "https://www.loopnet.com/search/multifamily-properties/"
    "cook-county-il/for-sale/"
)


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("ERROR: playwright not installed — run: pip install playwright && playwright install chrome")
        return 2

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=False, channel="chrome")
        except Exception:
            print("WARN: Chrome channel unavailable — falling back to bundled Chromium")
            browser = p.chromium.launch(headless=False)

        ctx = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/Chicago",
        )
        page = ctx.new_page()
        print(f"Opening {SEARCH_URL} — solve any CAPTCHA, then close the window.")
        page.goto(SEARCH_URL, wait_until="domcontentloaded")

        # Block until the user closes the window
        try:
            page.wait_for_event("close", timeout=0)
        except Exception:
            pass

        cookies = ctx.cookies()
        loopnet_cookies = [c for c in cookies if "loopnet.com" in (c.get("domain") or "")]

        OUTPUT_PATH.write_text(json.dumps(loopnet_cookies, indent=2))
        print(f"Saved {len(loopnet_cookies)} loopnet.com cookies → {OUTPUT_PATH}")
        browser.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
