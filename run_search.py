#!/usr/bin/env python3
"""
RE Deal Finder — Main Runner
Chicago multifamily/mixed-use deal pipeline.

Usage:
  python run_search.py [OPTIONS]

Options:
  --channel   on-market | off-market | all        (default: all)
  --limit     max deals per scraper               (default: 50)
  --dry-run   use stub data, don't hit real sites  (default: False)
  --no-alert  skip notifications and file output   (default: False)
  --verbose   show filtered-out deal reasons       (default: False)
  --slot      morning | evening (for output filenames, default: auto-detect)
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# Bootstrap logging before anything else
# ---------------------------------------------------------------------------
import config  # noqa: E402 (needed before sub-imports)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOG_FILE, mode="a"),
    ],
)
logger = logging.getLogger("run_search")

# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------
from scraper.loopnet       import scrape as scrape_loopnet
from scraper.crexi         import scrape as scrape_crexi
from scraper.redfin        import scrape as scrape_redfin
from scraper.realtor       import scrape as scrape_realtor
from scraper.cook_county   import scrape as scrape_cook_county
from scraper.chicago_data  import scrape as scrape_chicago_data
from scraper.foreclosure   import scrape as scrape_foreclosure
from scraper.fsbo          import scrape as scrape_fsbo

# Pipeline
from pipeline.filter import apply as filter_and_dedup
from pipeline.scorer import score_deals
from pipeline.alert  import run_alerts


# ---------------------------------------------------------------------------
# Scraper registry
# ---------------------------------------------------------------------------
ON_MARKET_SCRAPERS = {
    "redfin":  scrape_redfin,
    "realtor": scrape_realtor,
    "loopnet": scrape_loopnet,
    "crexi":   scrape_crexi,
}

OFF_MARKET_SCRAPERS = {
    "cook_county":  scrape_cook_county,
    "chicago_data": scrape_chicago_data,
    "foreclosure":  scrape_foreclosure,
    "fsbo":         scrape_fsbo,
}


def _run_scraper(name: str, fn, dry_run: bool, limit: int) -> list:
    """Run a single scraper with error isolation — never crash the pipeline."""
    try:
        logger.info("▶ Running scraper: %s", name)
        t0 = time.time()
        results = fn(dry_run=dry_run, limit=limit)
        elapsed = time.time() - t0
        logger.info("✓ %s: %d deals in %.1fs", name, len(results), elapsed)
        return results
    except Exception as e:
        logger.error("✗ %s: scraper failed with exception: %s", name, e, exc_info=True)
        return []


def run(
    channel: str = "all",
    limit: int = 50,
    dry_run: bool = False,
    no_alert: bool = False,
    verbose: bool = False,
    slot: str = None,
) -> list:
    """
    Full pipeline run. Returns scored+filtered deal list.
    """
    start = datetime.now()
    logger.info(
        "=" * 60 + "\nRE Deal Finder — %s scan | channel=%s | dry_run=%s\n" + "=" * 60,
        start.strftime("%Y-%m-%d %H:%M"),
        channel,
        dry_run,
    )

    # ---- Collect raw deals from scrapers ----
    raw_deals = []

    if channel in ("on-market", "all"):
        logger.info("--- Channel A: On-Market ---")
        for name, fn in ON_MARKET_SCRAPERS.items():
            raw_deals.extend(_run_scraper(name, fn, dry_run, limit))

    if channel in ("off-market", "all"):
        logger.info("--- Channel B: Off-Market ---")
        for name, fn in OFF_MARKET_SCRAPERS.items():
            raw_deals.extend(_run_scraper(name, fn, dry_run, limit))

    logger.info("Raw deals collected: %d (across all scrapers)", len(raw_deals))

    # ---- Filter + dedup ----
    filtered = filter_and_dedup(raw_deals, verbose=verbose)

    # ---- Score ----
    scored = score_deals(filtered)

    # ---- Summary to console ----
    _print_summary(scored)

    # ---- Alerts / output ----
    if not no_alert and scored:
        run_alerts(scored, slot=slot)
    elif not scored:
        logger.info("No qualifying deals found — skipping output")

    elapsed_total = (datetime.now() - start).total_seconds()
    logger.info("Pipeline complete in %.1fs. %d deals surfaced.", elapsed_total, len(scored))
    return scored


def _print_summary(deals: list) -> None:
    """Pretty-print top deals to console."""
    if not deals:
        print("\n── No qualifying deals found ──\n")
        return

    width = 100
    print("\n" + "=" * width)
    print(f"  TOP DEALS — {len(deals)} qualifying | Ranked by Opportunity Score")
    print("=" * width)
    header = f"{'#':<3} {'Score':>5}  {'Neighborhood':<20} {'Price':>10}  {'Units':>5}  {'Cap%':>5}  {'Source':<18}  Address"
    print(header)
    print("-" * width)

    for i, d in enumerate(deals[:20], 1):
        price  = f"${d.price/1e6:.2f}M" if d.price and d.price >= 1e6 else (f"${d.price:,.0f}" if d.price else "?")
        units  = str(d.units) if d.units else "?"
        cap    = f"{d.cap_rate:.1f}%" if d.cap_rate else "?"
        nbhd   = (d.neighborhood or "?")[:20]
        source = d.source[:18]
        addr   = (d.address or d.url or "—")[:40]

        marker = " *" if d.off_market else "  "
        print(f"{i:<3} {d.score:>5.1f}{marker} {nbhd:<20} {price:>10}  {units:>5}  {cap:>5}  {source:<18}  {addr}")

    if len(deals) > 20:
        print(f"  ... and {len(deals) - 20} more (see output/ for full list)")
    print("=" * width)
    print("  * = off-market source")
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Chicago RE multifamily deal-sourcing pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--channel",
        choices=["on-market", "off-market", "all"],
        default="all",
        help="Which channels to run (default: all)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max deals per scraper (default: 50)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Use stub data — don't hit real sites",
    )
    parser.add_argument(
        "--no-alert",
        action="store_true",
        default=False,
        help="Skip file output and notifications",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Log reasons for filtered-out deals",
    )
    parser.add_argument(
        "--slot",
        choices=["morning", "evening"],
        default=None,
        help="Override output slot (default: auto from time of day)",
    )

    args = parser.parse_args()

    deals = run(
        channel=args.channel,
        limit=args.limit,
        dry_run=args.dry_run,
        no_alert=args.no_alert,
        verbose=args.verbose,
        slot=args.slot,
    )

    # Exit code: 0 = deals found, 1 = no deals (useful for shell scripts)
    sys.exit(0 if deals else 1)


if __name__ == "__main__":
    main()
