"""
rolling_roi.py - trailing-window wallet ROI (net of Polymarket taker fees) and
guardrail tier decisions with hysteresis.

Pure functions only (no network, no globals), so they can be unit tested on
their own. analyze_activity.py supplies the data and the thresholds.

Rolling ROI statistic
---------------------
Over a wallet's resolved WIN/LOSS tracked bets whose alert_ts falls inside the
last `window_days` days, weighted by stake_pct (dollar-weighted, like the sheet):

    win:  profit = weight * min(1/price - 1, max_win_units)
    loss: profit = -weight
    fee:  weight * fee_pct_of_stake(price)       (paid on every bet, win or lose)

    roi_gross = sum(profit) / sum(weight)
    roi_net   = (sum(profit) - sum(fee)) / sum(weight)

A single longshot win is capped at `max_win_units` (same idea as the trailing
dynamic block) so one 8x result cannot carry a small window.
"""
import time


def compute_rolling_roi(
    bets,
    now_ts=None,
    window_days=14,
    max_win_units=3.0,
    fee_pct_fn=None,
):
    """
    bets: iterable of tracked-bet dicts (wallet, alert_ts, resolved, result,
          current_price_at_alert, stake_pct).
    fee_pct_fn: callable(price) -> taker fee as PERCENT of stake (e.g. 2.5), or
          None to leave the result gross (net == gross).

    Returns {wallet: {"n", "wins", "losses", "stake_weight", "roi_gross",
                      "roi_net", "window_days"}}. window_days in the result is
    the age in days of the oldest bet in the window (<= the window length).
    """
    now_ts = float(now_ts if now_ts is not None else time.time())
    cutoff = now_ts - float(window_days) * 86400.0
    rows = bets.values() if isinstance(bets, dict) else bets

    acc = {}
    for bet in rows:
        if not isinstance(bet, dict):
            continue
        if not bet.get("resolved"):
            continue
        result = str(bet.get("result", "") or "").strip().upper()
        if result not in ("WIN", "LOSS"):
            continue
        wallet = str(bet.get("wallet", "") or "").strip().lower()
        if not wallet:
            continue
        try:
            ts = float(bet.get("alert_ts", 0) or 0)
        except (TypeError, ValueError):
            continue
        if ts < cutoff or ts > now_ts + 86400.0:
            continue
        try:
            price = float(bet.get("current_price_at_alert"))
        except (TypeError, ValueError):
            continue
        if not (0.02 <= price <= 0.98):
            continue
        try:
            weight = float(bet.get("stake_pct") or 1.0)
        except (TypeError, ValueError):
            weight = 1.0
        if weight <= 0:
            weight = 1.0

        if result == "WIN":
            units = min(1.0 / price - 1.0, float(max_win_units))
        else:
            units = -1.0

        fee_pct = 0.0
        if fee_pct_fn is not None:
            try:
                fee_pct = float(fee_pct_fn(price) or 0.0)
            except Exception:
                fee_pct = 0.0

        a = acc.setdefault(
            wallet,
            {"n": 0, "wins": 0, "losses": 0, "w": 0.0, "profit": 0.0, "fee": 0.0, "first_ts": ts},
        )
        a["n"] += 1
        a["wins" if result == "WIN" else "losses"] += 1
        a["w"] += weight
        a["profit"] += units * weight
        a["fee"] += (fee_pct / 100.0) * weight
        a["first_ts"] = min(a["first_ts"], ts)

    out = {}
    for wallet, a in acc.items():
        if a["w"] <= 0:
            continue
        out[wallet] = {
            "n": a["n"],
            "wins": a["wins"],
            "losses": a["losses"],
            "stake_weight": a["w"],
            "roi_gross": a["profit"] / a["w"] * 100.0,
            "roi_net": (a["profit"] - a["fee"]) / a["w"] * 100.0,
            "window_days": round(min(float(window_days), (now_ts - a["first_ts"]) / 86400.0), 1),
        }
    return out


def decide_tier(
    roi,
    n,
    prev,
    *,
    suppress_enter,
    suppress_exit,
    cap_enter,
    cap_exit,
    trusted_enter,
    trusted_exit,
    min_n_suppress,
    min_n_cap,
    min_n_trusted,
    trusted_history_ok=True,
    hysteresis=True,
):
    """
    Returns "suppress", "cap_lean", "trusted" or "allow".

    A wallet ENTERS a tier at the *_enter threshold and only LEAVES it once ROI
    crosses the (looser) *_exit threshold, so a wallet hovering around a
    threshold does not flip every refresh. `prev` is the wallet's previous tier.
    With hysteresis=False only the enter thresholds are used (this reproduces the
    original lifetime-ROI behavior when enter thresholds equal the old ones).

    Leaving suppress lands in cap_lean while ROI is still <= cap_exit; leaving
    cap_lean lands in allow (or trusted, if it qualifies).
    """
    if roi is None:
        return "allow"
    if not hysteresis:
        prev = None

    if n >= min_n_suppress and (roi <= suppress_enter or (prev == "suppress" and roi <= suppress_exit)):
        return "suppress"

    if n >= min_n_cap and (roi <= cap_enter or (prev in ("suppress", "cap_lean") and roi <= cap_exit)):
        return "cap_lean"

    if (
        n >= min_n_trusted
        and trusted_history_ok
        and (roi >= trusted_enter or (prev == "trusted" and roi >= trusted_exit))
    ):
        return "trusted"

    return "allow"
