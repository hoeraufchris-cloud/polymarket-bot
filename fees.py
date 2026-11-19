"""
fees.py - Polymarket taker-fee helpers for the bot.

Fee model (per Polymarket docs, Fee Structure V2):
    fee_usdc = shares * rate * p * (1 - p)
    with shares = stake / p, this simplifies to:
    fee_usdc = stake * rate * (1 - p)

Only TAKERS pay. Resting limit orders that fill as maker pay 0 (and may earn rebates).
Geopolitics / world-events markets are fee-free.

Rates below are defaults; override per category with env vars, e.g.
    POLY_FEE_RATE_SPORTS=0.05
or force one rate for everything with POLY_FEE_RATE_OVERRIDE.
Check the live per-market value at clob.polymarket.com/fee-rate if you can; rates have
changed several times in 2026 (sports went 0.03 -> 0.05 in July).

Fee is modelled as USDC on top of stake (conservative: if Polymarket takes it in shares
on buys, the economics are the same to within rounding).
"""
import os

DEFAULT_RATES = {
    "sports": 0.05,
    "crypto": 0.07,
    "finance": 0.04,
    "politics": 0.04,
    "tech": 0.04,
    "mentions": 0.04,
    "economics": 0.05,
    "culture": 0.05,
    "weather": 0.05,
    "other": 0.05,
    "geopolitics": 0.0,
}


def fee_rate(category="sports"):
    override = os.getenv("POLY_FEE_RATE_OVERRIDE")
    if override not in (None, ""):
        try:
            return float(override)
        except ValueError:
            pass
    cat = str(category or "sports").strip().lower()
    env = os.getenv(f"POLY_FEE_RATE_{cat.upper()}")
    if env not in (None, ""):
        try:
            return float(env)
        except ValueError:
            pass
    return DEFAULT_RATES.get(cat, DEFAULT_RATES["other"])


def fee_per_share(price, rate=None, category="sports"):
    """USDC fee per share for a taker buy at `price` (0-1)."""
    p = float(price)
    if not 0.0 < p < 1.0:
        return 0.0
    r = fee_rate(category) if rate is None else float(rate)
    return r * p * (1.0 - p)


def all_in_price(price, rate=None, category="sports"):
    """What one share really costs you, fee included."""
    return float(price) + fee_per_share(price, rate, category)


def breakeven_prob(price, rate=None, category="sports"):
    """Win probability needed to break even after the fee (a $1 payout share)."""
    return all_in_price(price, rate, category)


def effective_decimal_odds(price, rate=None, category="sports"):
    return 1.0 / all_in_price(price, rate, category)


def fee_pct_of_stake(price, rate=None, category="sports"):
    p = float(price)
    return fee_per_share(p, rate, category) / p if p > 0 else 0.0


def net_edge_pct(win_prob, price, rate=None, category="sports"):
    """Expected profit per $1 staked after fee, in percent, given your win probability."""
    cost = all_in_price(price, rate, category)
    return (float(win_prob) / cost - 1.0) * 100.0


def bet_net_pnl(stake, price, won, rate=None, category="sports"):
    """Net USDC P&L of a resolved taker bet. stake = USDC spent on shares (ex fee)."""
    stake = float(stake)
    p = float(price)
    if stake <= 0 or not 0.0 < p < 1.0:
        return 0.0
    shares = stake / p
    fee = shares * fee_per_share(p, rate, category)
    gross = (shares - stake) if won else -stake
    return gross - fee


def summarize_roi(rows, category_key=None):
    """
    rows: iterable of dicts with 'stake', 'price', 'won' (bool) [, category_key].
    Returns dict with gross/net profit, stake, and gross/net ROI %.
    """
    stake_total = gross = net = fees = 0.0
    n = 0
    for r in rows:
        try:
            stake = float(r["stake"])
            price = float(r["price"])
            won = bool(r["won"])
        except (KeyError, TypeError, ValueError):
            continue
        if stake <= 0 or not 0.0 < price < 1.0:
            continue
        cat = (r.get(category_key) if category_key else None) or "sports"
        g = bet_net_pnl(stake, price, won, rate=0.0)
        nn = bet_net_pnl(stake, price, won, category=cat)
        stake_total += stake
        gross += g
        net += nn
        fees += g - nn
        n += 1
    return {
        "n": n,
        "stake": stake_total,
        "gross_profit": gross,
        "net_profit": net,
        "fees": fees,
        "gross_roi_pct": (gross / stake_total * 100.0) if stake_total else 0.0,
        "net_roi_pct": (net / stake_total * 100.0) if stake_total else 0.0,
    }


def alert_price_line(price, category="sports", rate=None):
    """One line for bet alerts, e.g. '49.0c -> 50.3c all-in | breakeven 50.3% | fee 2.6% of stake'."""
    p = float(price)
    aip = all_in_price(p, rate, category)
    return (
        f"{p*100:.1f}c -> {aip*100:.1f}c all-in | breakeven {aip*100:.1f}% | "
        f"fee {fee_pct_of_stake(p, rate, category)*100:.1f}% of stake"
    )
