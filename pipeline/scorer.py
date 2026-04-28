"""
Deal scorer — rank deals 0-100 based on weighted opportunity factors.

Scoring model (weights from config.py):
  - cap_rate        (20%): higher = better
  - price_per_unit  (20%): lower = better
  - unit_count      (15%): more = better
  - neighborhood    (15%): target > unknown > excluded
  - days_on_market  (10%): fewer = hotter
  - rent_upside     (10%): gap between market rent and asking
  - off_market_bonus(10%): +10pts flat for off-market sourcing

Each component produces a 0-100 sub-score; final score is weighted sum.
"""

import logging
from typing import Optional

from scraper import Deal
import config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Sub-scorers — each returns 0.0 to 100.0
# ---------------------------------------------------------------------------

def _score_cap_rate(cap_rate: Optional[float]) -> float:
    """Higher cap rate = better score."""
    if cap_rate is None:
        return 50.0  # neutral / unknown
    if cap_rate >= config.CAP_RATE_EXCELLENT:
        return 100.0
    if cap_rate >= config.CAP_RATE_GOOD:
        # Linear interpolation: [8, 10] → [70, 100]
        return 70.0 + 30.0 * (cap_rate - config.CAP_RATE_GOOD) / (config.CAP_RATE_EXCELLENT - config.CAP_RATE_GOOD)
    if cap_rate >= config.CAP_RATE_MINIMUM:
        # [7, 8] → [40, 70]
        return 40.0 + 30.0 * (cap_rate - config.CAP_RATE_MINIMUM) / (config.CAP_RATE_GOOD - config.CAP_RATE_MINIMUM)
    # Below minimum — should have been filtered, but score it low
    return max(0.0, 40.0 * (cap_rate / config.CAP_RATE_MINIMUM))


def _score_price_per_unit(ppu: Optional[float]) -> float:
    """Lower price per unit = better score."""
    if ppu is None:
        return 50.0  # neutral
    if ppu <= config.PPU_EXCELLENT:
        return 100.0
    if ppu <= config.PPU_GOOD:
        # [40k, 80k] → [70, 100]
        return 70.0 + 30.0 * (config.PPU_GOOD - ppu) / (config.PPU_GOOD - config.PPU_EXCELLENT)
    if ppu <= config.PPU_HIGH:
        # [80k, 150k] → [0, 70]
        return 70.0 * (config.PPU_HIGH - ppu) / (config.PPU_HIGH - config.PPU_GOOD)
    return 0.0


def _score_unit_count(units: Optional[int]) -> float:
    """More units = better (economies of scale)."""
    if units is None:
        return 40.0  # slight penalty for unknown
    if units >= config.UNIT_EXCELLENT:
        return 100.0
    if units >= config.UNIT_GOOD:
        return 60.0 + 40.0 * (units - config.UNIT_GOOD) / (config.UNIT_EXCELLENT - config.UNIT_GOOD)
    if units >= config.UNIT_MINIMUM:
        return 20.0 + 40.0 * (units - config.UNIT_MINIMUM) / (config.UNIT_GOOD - config.UNIT_MINIMUM)
    return 0.0


def _score_neighborhood(neighborhood: str) -> float:
    """Target neighborhoods get full score; excluded get 0."""
    if not neighborhood:
        return float(config.NEIGHBORHOOD_SCORE_UNKNOWN) / 30.0 * 100.0

    nbhd_lower = neighborhood.lower()
    for n in config.TARGET_NEIGHBORHOODS:
        if n.lower() in nbhd_lower or nbhd_lower in n.lower():
            return 100.0  # TARGET = full 30pts → normalized to 100
    for n in config.EXCLUDE_NEIGHBORHOODS:
        if n.lower() in nbhd_lower or nbhd_lower in n.lower():
            return 0.0

    # Unknown neighborhood
    return float(config.NEIGHBORHOOD_SCORE_UNKNOWN) / float(config.NEIGHBORHOOD_SCORE_TARGET) * 100.0


def _score_days_on_market(dom: Optional[int]) -> float:
    """Fewer days = hotter deal (less competition / more motivated)."""
    if dom is None:
        return 50.0  # neutral
    if dom <= config.DOM_HOT:
        return 100.0
    if dom <= config.DOM_WARM:
        return 60.0 + 40.0 * (config.DOM_WARM - dom) / (config.DOM_WARM - config.DOM_HOT)
    if dom < config.DOM_COLD:
        return 60.0 * (config.DOM_COLD - dom) / (config.DOM_COLD - config.DOM_WARM)
    return 0.0


def _score_rent_upside(
    gross_rent: Optional[float],
    market_rent_est: Optional[float],
    price: Optional[float],
    units: Optional[int],
) -> float:
    """
    Estimate rent upside: how far below market is current rent?
    If no data, return neutral 50.
    """
    # If market rent is known
    if gross_rent and market_rent_est and gross_rent > 0:
        ratio = gross_rent / market_rent_est
        if ratio <= 0.70:
            return 100.0   # 30%+ below market = huge upside
        if ratio <= 0.85:
            return 70.0 + 30.0 * (0.85 - ratio) / (0.85 - 0.70)
        if ratio <= 1.0:
            return 30.0 + 40.0 * (1.0 - ratio) / (1.0 - 0.85)
        return 0.0  # at or above market

    # Proxy: price-per-unit vs. Chicago multifamily benchmarks
    # At target price-per-unit levels, rent upside is usually significant
    if price and units and units > 0:
        ppu = price / units
        if ppu < 60_000:
            return 80.0   # low PPU = likely below-market rents
        if ppu < 100_000:
            return 60.0
        if ppu < 150_000:
            return 40.0
        return 20.0

    return 50.0  # neutral when no data


def _score_off_market(off_market: bool) -> float:
    """Binary: off-market sourcing gets bonus."""
    return 100.0 if off_market else 0.0


# ---------------------------------------------------------------------------
# Main scorer
# ---------------------------------------------------------------------------

def score_deal(deal: Deal) -> Deal:
    """
    Compute composite 0-100 score for a deal.
    Populates deal.score and deal.score_breakdown.
    Returns the deal (mutated in place).
    """
    w = config.SCORE_WEIGHTS

    sub_scores = {
        "cap_rate":       _score_cap_rate(deal.cap_rate),
        "price_per_unit": _score_price_per_unit(deal.price_per_unit),
        "unit_count":     _score_unit_count(deal.units),
        "neighborhood":   _score_neighborhood(deal.neighborhood),
        "days_on_market": _score_days_on_market(deal.days_on_market),
        "rent_upside":    _score_rent_upside(
            deal.gross_rent, deal.market_rent_est, deal.price, deal.units
        ),
        "off_market_bonus": _score_off_market(deal.off_market),
    }

    # Weighted sum
    total = sum(sub_scores[k] * w[k] for k in w)

    # Cross-source confidence boost: +10 pts for each additional source the
    # same deal showed up on (reflects validation that the listing is real
    # and actively marketed across multiple channels).
    extra_sources = max(0, len(deal.sources or []) - 1)
    cross_source_bonus = 10.0 * extra_sources
    total += cross_source_bonus

    deal.score = round(total, 1)
    deal.score_breakdown = {k: round(v, 1) for k, v in sub_scores.items()}
    if cross_source_bonus:
        deal.score_breakdown["cross_source_bonus"] = round(cross_source_bonus, 1)
    return deal


def score_deals(deals: list[Deal]) -> list[Deal]:
    """Score all deals and return them sorted by score descending."""
    scored = [score_deal(d) for d in deals]
    scored.sort(key=lambda d: d.score, reverse=True)
    logger.info(
        "Scorer: %d deals scored. Top score: %.1f, Median: %.1f",
        len(scored),
        scored[0].score if scored else 0,
        scored[len(scored) // 2].score if scored else 0,
    )
    return scored
