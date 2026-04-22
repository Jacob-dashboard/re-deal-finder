# re-deal-finder

Autonomous Chicago multifamily deal-sourcing pipeline. Scrapes on-market listings (LoopNet, Crexi) and off-market sources (tax delinquency, code violations, foreclosures, FSBO), filters against deal criteria, scores opportunities, and fires macOS notifications on new/updated deals.

## Deal criteria (edit `config.py` to adjust)

| Criteria | Default |
|---|---|
| Geography | Cook County, IL |
| Target neighborhoods | Avondale, Little Village, Pilsen, Logan Square, Humboldt Park, Belmont Cragin, Brighton Park, Gage Park, Archer Heights, Hermosa, West Town |
| Asset class | Multifamily (5+ units) or Mixed-use |
| Max price | $1,200,000 |
| Min cap rate | 7% |

## Project structure

```
re-deal-finder/
├── config.py               # All criteria, neighborhoods, scoring weights
├── scraper/
│   ├── __init__.py         # Deal dataclass (shared schema)
│   ├── loopnet.py          # LoopNet on-market scraper
│   ├── crexi.py            # Crexi on-market scraper
│   ├── cook_county.py      # Cook County tax delinquency (off-market)
│   ├── chicago_data.py     # Chicago Data Portal — code violations (off-market)
│   ├── foreclosure.py      # Sheriff sales + HUD homes (off-market)
│   └── fsbo.py             # Craigslist + FSBO.com (off-market)
├── pipeline/
│   ├── filter.py           # Criteria filter + cross-source deduplication
│   ├── scorer.py           # 0-100 opportunity scorer
│   └── alert.py            # Output, dedup tracking, macOS notifications
├── schedule/
│   ├── com.jacob.deal-finder.plist   # macOS launchd job (10am + 10pm CT)
│   └── install.sh                    # One-command launchd installer
├── output/                 # Deal reports land here (gitignored)
├── run_search.py           # Main entry point
├── run_search.sh           # Shell wrapper (used by launchd)
└── requirements.txt
```

## Quickstart

```bash
cd ~/Projects/re-deal-finder

# Install dependencies (one-time)
pip install -r requirements.txt

# Dry run — stub data, no network
python run_search.py --dry-run

# Live run — all channels
python run_search.py --channel all --limit 50

# On-market only
python run_search.py --channel on-market

# Off-market only
python run_search.py --channel off-market

# Chicago Data Portal violations only (clean public API — good first live test)
python -c "
from scraper.chicago_data import scrape_live_sample
from pipeline.scorer import score_deals
deals = scrape_live_sample(zip_code='60623', limit=10)
scored = score_deals(deals)
for d in scored:
    print(d.score, d.address, d.neighborhood, d.violation_count)
"
```

## Scheduled scanning (macOS launchd)

Runs automatically at **10:00 AM and 10:00 PM CT** every day.

```bash
# Install (run once)
bash schedule/install.sh

# Verify it's registered
launchctl list | grep deal-finder

# Trigger immediately (for testing)
launchctl start com.jacob.deal-finder

# Uninstall
launchctl unload ~/Library/LaunchAgents/com.jacob.deal-finder.plist
rm ~/Library/LaunchAgents/com.jacob.deal-finder.plist
```

Logs → `output/launchd_stdout.log`

## Output files

Each scan produces:

| File | Description |
|---|---|
| `output/deals_YYYY-MM-DD_{slot}.json` | Full deal list with scores |
| `output/deals_YYYY-MM-DD_{slot}.csv` | Same, spreadsheet-friendly |
| `output/deals_YYYY-MM-DD_{slot}_summary.md` | Human-readable ranked summary |
| `output/seen_deals.json` | Cross-scan registry (NEW / UPDATED / EXISTING) |
| `~/Documents/Models/RealEstate/Pipeline/latest_scan.md` | Latest summary (ask Claude: "what came in on the latest scan?") |

Where `slot` = `morning` (AM run) or `evening` (PM run).

## Notifications

Each scan fires a **macOS native notification** via `osascript`. For richer notifications:

```bash
brew install terminal-notifier
```

The pipeline auto-detects and uses `terminal-notifier` when available.

## Scraper status

| Scraper | Status | Notes |
|---|---|---|
| `loopnet.py` | **Functional / may be blocked** | LoopNet uses Cloudflare; consider Playwright for reliable access |
| `crexi.py` | **Functional / may be blocked** | Crexi API endpoint observed via devtools — may require auth token on new builds |
| `cook_county.py` | **Functional** | Pulls annual tax sale CSV; URL changes yearly — update `TREASURER_CSV_URLS` |
| `chicago_data.py` | **Fully functional** | Chicago Data Portal open API — most reliable source |
| `foreclosure.py` | **Partial** | Sheriff sale page is JS-heavy; HUD search works; Playwright needed for full Sheriff data |
| `fsbo.py` | **Functional** | Craigslist + FSBO.com; keyword-based matching |

## Opportunity scoring (0-100)

| Factor | Weight | Notes |
|---|---|---|
| Cap rate | 20% | 10%+ = 100pts; 7% = 40pts |
| Price per unit | 20% | <$40k/unit = 100pts; >$150k = 0pts |
| Unit count | 15% | 20+ = 100pts; 5 = 20pts |
| Neighborhood | 15% | Target = 100pts; Excluded = 0pts |
| Days on market | 10% | <7 days = 100pts; 120+ = 0pts |
| Rent upside | 10% | Estimated below-market rent gap |
| Off-market bonus | 10% | +10pts flat for off-market sources |

## Next steps

### Telegram alerting
```python
# In pipeline/alert.py — add to run_alerts():
import requests
if config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID:
    msg = f"*RE Scan — {n_new} NEW deals*\n" + summary_text
    requests.post(
        f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
        json={"chat_id": config.TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}
    )
```

Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in `config.py` or as env vars.

### Email alerting
Use `smtplib` or SendGrid SDK. Set `ALERT_EMAIL` in `config.py`.

### Playwright for blocked scrapers
```bash
pip install playwright
playwright install chromium
```
Replace `requests.get(...)` in `loopnet.py` / `foreclosure.py` with Playwright async page fetch.

### Assessor cross-reference
`cook_county.py` has a stub for calling the Cook County Assessor DataCatalog API to look up property class codes per PIN — expand `_fetch_assessor_data()` for full multifamily classification.
