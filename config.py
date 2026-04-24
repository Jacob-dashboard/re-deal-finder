"""
RE Deal Finder — Chicago Configuration
All deal criteria, neighborhoods, scoring weights, and runtime settings.
Edit this file to tune filters without touching scraper logic.
"""

# ---------------------------------------------------------------------------
# Geography
# ---------------------------------------------------------------------------
COOK_COUNTY_STATE = "IL"
COOK_COUNTY_NAME  = "Cook County"
CHICAGO_CITY      = "Chicago"

TARGET_NEIGHBORHOODS = [
    "Avondale",
    "Little Village",
    "South Lawndale",     # same area as Little Village
    "Pilsen",
    "Lower West Side",    # same area as Pilsen
    "Logan Square",
    "Humboldt Park",
    "Belmont Cragin",
    "Brighton Park",
    "Gage Park",
    "Archer Heights",
    "Hermosa",
    "West Town",
]

EXCLUDE_NEIGHBORHOODS = [
    "Englewood",
    "West Englewood",
    "Auburn Gresham",
    "Greater Grand Crossing",
    "Washington Park",
    "Woodlawn",
    "West Garfield Park",
    "East Garfield Park",
    "North Lawndale",
    "Austin",
    "Riverdale",
    "Pullman",
    "West Pullman",
    "Roseland",
    "Chatham",
    "South Shore",
]

# Chicago ZIP codes that map to target neighborhoods (supplemental filter)
TARGET_ZIP_CODES = [
    "60618",  # Avondale
    "60623",  # Little Village / South Lawndale
    "60608",  # Pilsen / Lower West Side
    "60647",  # Logan Square / Bucktown
    "60651",  # Humboldt Park
    "60641",  # Belmont Cragin
    "60632",  # Brighton Park
    "60629",  # Gage Park / West Lawn
    "60632",  # Archer Heights
    "60639",  # Hermosa / Belmont Cragin
    "60622",  # West Town / Ukrainian Village
    "60612",  # West Town adjacent
]

# ---------------------------------------------------------------------------
# Deal Criteria
# ---------------------------------------------------------------------------
MAX_PRICE       = 1_200_000   # $1.2M hard cap
MIN_CAP_RATE    = 7.0         # percent
MIN_UNITS       = 5           # multifamily threshold
ASSET_CLASSES   = ["multifamily", "mixed-use", "apartment", "multi-family"]

# ---------------------------------------------------------------------------
# Scoring Weights (must sum to 1.0)
# ---------------------------------------------------------------------------
SCORE_WEIGHTS = {
    "cap_rate":          0.20,   # higher = better
    "price_per_unit":    0.20,   # lower = better
    "unit_count":        0.15,   # more = better
    "neighborhood":      0.15,   # target vs caution vs unknown
    "days_on_market":    0.10,   # lower = hotter / less competition
    "rent_upside":       0.10,   # market rent vs asking rent gap
    "off_market_bonus":  0.10,   # extra credit for off-market sourcing
}

# Neighborhood tier scores (max 30 pts for target)
NEIGHBORHOOD_SCORE_TARGET   = 30
NEIGHBORHOOD_SCORE_CAUTION  = 15
NEIGHBORHOOD_SCORE_UNKNOWN  = 10
NEIGHBORHOOD_SCORE_EXCLUDED = 0

# Cap rate scoring thresholds
CAP_RATE_EXCELLENT = 10.0   # 100% score above this
CAP_RATE_GOOD      = 8.0
CAP_RATE_MINIMUM   = 7.0    # MIN_CAP_RATE

# Price-per-unit scoring (lower is better)
PPU_EXCELLENT = 40_000    # $40k/unit — 100% score at/below
PPU_GOOD      = 80_000
PPU_HIGH      = 150_000   # 0% score at/above

# Unit count scoring
UNIT_EXCELLENT = 20       # 100% score at/above
UNIT_GOOD      = 10
UNIT_MINIMUM   = MIN_UNITS

# Days on market (lower = hotter)
DOM_HOT  = 7    # < 1 week = 100%
DOM_WARM = 30
DOM_COLD = 120  # 0% at/above

# ---------------------------------------------------------------------------
# Proforma / Alert Settings
# ---------------------------------------------------------------------------
PROFORMA_TEMPLATE_DIR = "/Users/jacob/Documents/Models/RealEstate/"
PIPELINE_OUTPUT_DIR   = "/Users/jacob/Documents/Models/RealEstate/Pipeline/"
LOCAL_OUTPUT_DIR      = "output"
TOP_DEALS_TO_CLONE    = 5

# Telegram
TELEGRAM_BOT_TOKEN  = "8632113390:AAGayabSa8J18r5Vt-2BTNeBGLItfYIFtE4"
TELEGRAM_CHAT_ID    = None  # Discovered at runtime via getUpdates; message @Vortex100_bot first

# Email (future)
ALERT_EMAIL = ""

# ---------------------------------------------------------------------------
# Scraper Settings
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT    = 15     # seconds
REQUEST_DELAY_MIN  = 2.0    # seconds between requests (min)
REQUEST_DELAY_MAX  = 4.0    # seconds between requests (max)
MAX_RETRIES        = 3
MAX_PAGES          = 10     # max pagination depth per scraper

# Chicago Data Portal
CHICAGO_DATA_PORTAL_BASE  = "https://data.cityofchicago.org/resource"
CHICAGO_VIOLATIONS_DATASET = "22u3-xenr.json"
CHICAGO_APP_TOKEN         = ""  # optional but increases rate limits

# Cook County Assessor API
COOK_COUNTY_ASSESSOR_BASE = "https://datacatalog.cookcountyil.gov/resource"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = "INFO"
LOG_FILE  = "re_deal_finder.log"
