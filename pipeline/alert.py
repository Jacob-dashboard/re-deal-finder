"""
Alert & output module.

Actions on each scan:
  1. Save full deal list to output/deals_YYYY-MM-DD_{slot}.json + .csv
  2. Maintain output/seen_deals.json — track NEW / UPDATED / EXISTING across scans
  3. Write a human-readable summary to output/deals_YYYY-MM-DD_{slot}_summary.md
  4. Mirror summary to ~/Documents/Models/RealEstate/Pipeline/latest_scan.md
  5. Fire a macOS native notification via osascript (+ terminal-notifier if available)
  6. Clone proforma template for top-scoring deals
"""

import csv
import json
import logging
import os
import shutil
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from scraper import Deal
import config

logger = logging.getLogger(__name__)

OUTPUT_DIR      = Path(config.LOCAL_OUTPUT_DIR)
SEEN_FILE       = OUTPUT_DIR / "seen_deals.json"
PIPELINE_DIR    = Path(config.PIPELINE_OUTPUT_DIR)
PROFORMA_DIR    = Path(config.PROFORMA_TEMPLATE_DIR)

# ---------------------------------------------------------------------------
# Deduplication / change tracking across scans
# ---------------------------------------------------------------------------

def load_seen_deals() -> dict:
    """
    Load the seen-deals registry from disk.
    Structure: { deal_key: { "first_seen": ISO, "last_price": float|null, "score": float } }
    """
    if SEEN_FILE.exists():
        try:
            with open(SEEN_FILE) as f:
                return json.load(f)
        except Exception as e:
            logger.warning("alert: could not load seen_deals.json: %s", e)
    return {}


def save_seen_deals(registry: dict) -> None:
    """Persist the seen-deals registry to disk."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(SEEN_FILE, "w") as f:
        json.dump(registry, f, indent=2)


def _deal_key(deal: Deal) -> str:
    """Stable unique key for a deal (URL preferred, else normalized address)."""
    if deal.url:
        return deal.url.strip().lower().rstrip("/")
    addr = deal.address.upper().strip() if deal.address else ""
    return f"{addr}|{deal.city}|{deal.state}"


def classify_deals(deals: list[Deal], registry: dict) -> dict[str, list[Deal]]:
    """
    Classify each deal as NEW / UPDATED / EXISTING.
    Also updates the registry in-place.
    Returns: { "new": [...], "updated": [...], "existing": [...] }
    """
    new_deals      = []
    updated_deals  = []
    existing_deals = []

    now_iso = datetime.utcnow().isoformat()

    for deal in deals:
        key = _deal_key(deal)
        if key not in registry:
            # Brand new
            registry[key] = {
                "first_seen": now_iso,
                "last_price":  deal.price,
                "last_score":  deal.score,
                "address":     deal.address,
                "source":      deal.source,
            }
            new_deals.append(deal)
        else:
            prev = registry[key]
            price_changed = (
                deal.price is not None
                and prev.get("last_price") is not None
                and abs(deal.price - prev["last_price"]) > 1000
            )
            score_changed = abs(deal.score - prev.get("last_score", 0)) > 5

            if price_changed or score_changed:
                prev["last_price"] = deal.price
                prev["last_score"] = deal.score
                updated_deals.append(deal)
            else:
                existing_deals.append(deal)

    return {"new": new_deals, "updated": updated_deals, "existing": existing_deals}


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------

def _scan_slot() -> str:
    """Return 'morning' or 'evening' based on current hour (CT)."""
    # Rough: just use local time; launchd fires at correct CT times
    hour = datetime.now().hour
    return "morning" if hour < 15 else "evening"


def _base_filename(slot: str = None) -> str:
    today = date.today().isoformat()
    slot  = slot or _scan_slot()
    return f"deals_{today}_{slot}"


def write_json(deals: list[Deal], path: Path) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump([d.to_dict() for d in deals], f, indent=2, default=str)
    logger.info("alert: wrote JSON → %s (%d deals)", path, len(deals))


def write_csv(deals: list[Deal], path: Path) -> None:
    if not deals:
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = list(deals[0].to_dict().keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in deals:
            writer.writerow(d.to_dict())
    logger.info("alert: wrote CSV → %s", path)


def write_summary_md(
    deals: list[Deal],
    classified: dict,
    slot: str,
    path: Path,
) -> None:
    """Write a human-readable Markdown scan summary."""
    today     = date.today().isoformat()
    n_new     = len(classified["new"])
    n_updated = len(classified["updated"])
    n_total   = len(deals)

    lines = [
        f"# RE Deal Scan — {today} ({slot.title()})",
        f"",
        f"**{n_new} NEW** | **{n_updated} UPDATED** | {n_total} total qualifying deals",
        f"",
        f"---",
        f"",
    ]

    def _deal_section(header: str, deal_list: list[Deal]) -> list[str]:
        if not deal_list:
            return [f"## {header}\n_None this scan._\n"]
        section = [f"## {header}\n"]
        for i, d in enumerate(deal_list[:20], 1):
            price_str  = f"${d.price:,.0f}" if d.price else "Price unknown"
            units_str  = f"{d.units} units" if d.units else "units unknown"
            cap_str    = f"{d.cap_rate:.1f}% cap" if d.cap_rate else ""
            score_str  = f"Score: **{d.score:.0f}/100**"
            nbhd_str   = d.neighborhood or "neighborhood unknown"
            source_str = d.source.replace("_", " ").title()
            addr_str   = d.address or d.url or "—"

            parts = [p for p in [price_str, units_str, cap_str] if p]
            url_part = f" — [{addr_str}]({d.url})" if d.url else f" — {addr_str}"

            section.append(
                f"{i}. **{nbhd_str}** | {' · '.join(parts)} | {score_str} | _{source_str}_{url_part}"
            )
        return section + [""]

    lines += _deal_section("🆕 New Deals", classified["new"])
    lines += _deal_section("📈 Updated Deals", classified["updated"])

    if deals:
        lines += ["## Top 10 Overall (All Deals)\n"]
        for i, d in enumerate(deals[:10], 1):
            price_str = f"${d.price:,.0f}" if d.price else "?"
            units_str = f"{d.units}u" if d.units else "?u"
            cap_str   = f"{d.cap_rate:.1f}%" if d.cap_rate else "cap?"
            nbhd_str  = d.neighborhood or "?"
            addr_str  = d.address or "?"
            url_part  = f"[{addr_str}]({d.url})" if d.url else addr_str
            lines.append(
                f"{i}. **{d.score:.0f}** — {nbhd_str} | {price_str} · {units_str} · {cap_str} | {url_part}"
            )
        lines.append("")

    lines += [
        "---",
        f"_Generated by re-deal-finder · {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}_",
    ]

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(lines))
    logger.info("alert: wrote summary → %s", path)


# ---------------------------------------------------------------------------
# macOS notifications
# ---------------------------------------------------------------------------

def _notify_osascript(title: str, message: str, subtitle: str = "") -> None:
    """Fire a macOS notification via osascript (works on all macOS)."""
    subtitle_part = f', subtitle:"{subtitle}"' if subtitle else ""
    script = (
        f'display notification "{message}"{subtitle_part} '
        f'with title "{title}" sound name "Glass"'
    )
    try:
        subprocess.run(["osascript", "-e", script], timeout=5, capture_output=True)
        logger.info("alert: macOS notification fired")
    except Exception as e:
        logger.warning("alert: osascript notification failed: %s", e)


def _notify_terminal_notifier(title: str, message: str) -> None:
    """Use terminal-notifier for richer notifications (if installed)."""
    try:
        result = subprocess.run(
            ["which", "terminal-notifier"], capture_output=True, text=True
        )
        if result.returncode != 0:
            return  # not installed
        subprocess.run(
            [
                "terminal-notifier",
                "-title", title,
                "-message", message,
                "-sound", "Glass",
                "-contentImage", "",
                "-group", "re-deal-finder",
            ],
            timeout=5,
            capture_output=True,
        )
        logger.info("alert: terminal-notifier fired")
    except Exception as e:
        logger.debug("alert: terminal-notifier failed: %s", e)


def send_notification(classified: dict, total: int) -> None:
    """Send macOS notification summarizing the scan."""
    n_new     = len(classified["new"])
    n_updated = len(classified["updated"])

    if n_new == 0 and n_updated == 0:
        title   = "RE Scan Complete"
        message = f"{total} existing deals — no new/updated listings"
    else:
        parts = []
        if n_new:
            parts.append(f"{n_new} NEW")
        if n_updated:
            parts.append(f"{n_updated} UPDATED")
        title   = f"🏢 RE Deal Alert — {' · '.join(parts)}"
        message = f"{total} total qualifying deals in target Chicago neighborhoods"

    # Try terminal-notifier first (richer), fall back to osascript
    _notify_terminal_notifier(title, message)
    _notify_osascript(title, message, subtitle="re-deal-finder")


# ---------------------------------------------------------------------------
# Proforma template cloning
# ---------------------------------------------------------------------------

def clone_proforma(deal: Deal, rank: int) -> Optional[Path]:
    """
    Clone the proforma template for a top-scoring deal.
    Looks for any .xlsx file in PROFORMA_TEMPLATE_DIR.
    Pre-populates the filename with deal info.
    """
    PIPELINE_DIR.mkdir(parents=True, exist_ok=True)

    # Find template
    templates = list(PROFORMA_DIR.glob("*.xlsx")) + list(PROFORMA_DIR.glob("*.xls"))
    if not templates:
        logger.debug("Proforma: no template found in %s", PROFORMA_DIR)
        return None

    template = templates[0]
    nbhd  = (deal.neighborhood or "unknown").replace(" ", "_")
    addr  = re.sub(r"[^\w\s-]", "", deal.address or "deal").replace(" ", "_")[:30]
    price = f"${deal.price:,.0f}" if deal.price else "price_unknown"
    fname = f"Rank{rank:02d}_{nbhd}_{addr}_{price}.xlsx"
    dest  = PIPELINE_DIR / fname

    try:
        shutil.copy2(template, dest)
        logger.info("Proforma: cloned template → %s", dest)
        return dest
    except Exception as e:
        logger.warning("Proforma: clone failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_alerts(
    deals: list[Deal],
    slot: str = None,
    clone_proformas: bool = True,
) -> dict:
    """
    Full alert pipeline:
      1. Load seen-deals registry
      2. Classify deals as new/updated/existing
      3. Write JSON, CSV, Markdown summary
      4. Mirror summary to Pipeline/latest_scan.md
      5. Fire macOS notification
      6. Clone proformas for top deals
    Returns the classified dict.
    """
    import re as _re

    slot = slot or _scan_slot()
    base = _base_filename(slot)

    # 1. Load registry + classify
    registry = load_seen_deals()
    classified = classify_deals(deals, registry)
    save_seen_deals(registry)

    # 2. Write outputs
    json_path    = OUTPUT_DIR / f"{base}.json"
    csv_path     = OUTPUT_DIR / f"{base}.csv"
    summary_path = OUTPUT_DIR / f"{base}_summary.md"

    write_json(deals, json_path)
    write_csv(deals, csv_path)
    write_summary_md(deals, classified, slot, summary_path)

    # 3. Mirror summary to Pipeline dir
    try:
        PIPELINE_DIR.mkdir(parents=True, exist_ok=True)
        latest_path = PIPELINE_DIR / "latest_scan.md"
        shutil.copy2(summary_path, latest_path)
        logger.info("alert: mirrored summary → %s", latest_path)
    except Exception as e:
        logger.warning("alert: could not mirror summary to pipeline dir: %s", e)

    # 4. macOS notification
    send_notification(classified, len(deals))

    # 5. Clone proformas for top deals
    if clone_proformas:
        for rank, deal in enumerate(deals[:config.TOP_DEALS_TO_CLONE], 1):
            clone_proforma(deal, rank)

    logger.info(
        "alert: scan complete — %d new, %d updated, %d existing",
        len(classified["new"]), len(classified["updated"]), len(classified["existing"]),
    )
    return classified


# Needed for re.sub in clone_proforma
import re
