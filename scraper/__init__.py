"""
Scraper package — exports all scrapers and the shared Deal dataclass.
Each scraper returns a list[Deal].
"""

from dataclasses import dataclass, field
from typing import Optional
from datetime import date


@dataclass
class Deal:
    """Normalized deal record — common schema across all sources."""
    # Identification
    source: str                      # "loopnet", "crexi", "cook_county", etc.
    channel: str                     # "on_market" | "off_market"
    url: str = ""
    external_id: str = ""
    sources: list = field(default_factory=list)  # populated post-dedup with all sources where this deal appeared

    # Location
    address: str = ""
    city: str = ""
    state: str = "IL"
    zip_code: str = ""
    neighborhood: str = ""
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    # Property basics
    asset_class: str = ""            # "multifamily", "mixed-use", etc.
    units: Optional[int] = None
    sqft: Optional[int] = None
    year_built: Optional[int] = None
    lot_size: Optional[str] = None

    # Financials
    price: Optional[float] = None
    price_per_unit: Optional[float] = None
    price_per_sqft: Optional[float] = None
    cap_rate: Optional[float] = None        # percent, e.g. 7.5
    noi: Optional[float] = None
    gross_rent: Optional[float] = None
    market_rent_est: Optional[float] = None

    # Listing metadata
    days_on_market: Optional[int] = None
    listing_date: Optional[str] = None
    broker: str = ""
    broker_phone: str = ""
    broker_email: str = ""

    # Off-market specific
    tax_delinquency_amount: Optional[float] = None
    violation_count: Optional[int] = None
    foreclosure_date: Optional[str] = None
    foreclosure_case: str = ""

    # Scoring (populated by pipeline)
    score: float = 0.0
    score_breakdown: dict = field(default_factory=dict)
    off_market: bool = False

    # Raw data
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        import dataclasses
        d = dataclasses.asdict(self)
        d.pop("raw", None)
        return d


__all__ = ["Deal"]
