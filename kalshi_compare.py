"""
kalshi_compare.py - SHADOW-LOG ONLY Kalshi price lookup for bet alerts.

For a Polymarket sports bet (slug like 'nba-lal-bos-2026-11-20', outcome = team name) this finds
the matching Kalshi game market and logs both prices. It never changes alerts or bets.
Kalshi market data is public (no key). Set KALSHI_SHADOW_ENABLED=0 to switch off.

Log line:  [KALSHI] match=ok|none pm_slug=... pm_outcome=... pm_price=0.49c kalshi_ticker=... \
           kalshi_ask=0.51 kalshi_bid=0.49 diff_c=+2.0 reason=...
Kalshi prices were not verified against live responses when this was written, so parsing is
defensive and every miss logs a reason.
"""
import os
import re
import time
from datetime import datetime

import requests

KALSHI_SHADOW_ENABLED = os.getenv("KALSHI_SHADOW_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}
KALSHI_BASE = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2").rstrip("/")
CACHE_TTL = int(os.getenv("KALSHI_CACHE_TTL_SECONDS", "90"))
TIMEOUT = 8

# Polymarket slug prefix -> Kalshi game-winner series ticker (best guess; override via env JSON-less
# pairs like KALSHI_SERIES_NBA=KXNBAGAME). Misses are logged so these can be corrected.
DEFAULT_SERIES = {
    "nba": "KXNBAGAME",
    "nfl": "KXNFLGAME",
    "nhl": "KXNHLGAME",
    "mlb": "KXMLBGAME",
    "cfb": "KXNCAAFGAME",
    "cbb": "KXNCAAMBGAME",
    "wnba": "KXWNBAGAME",
}

_cache = {}  # series -> (ts, events list)
_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def _series_for(prefix):
    return os.getenv(f"KALSHI_SERIES_{prefix.upper()}") or DEFAULT_SERIES.get(prefix)


def parse_slug(slug):
    """'nba-lal-bos-2026-11-20' -> ('nba', ['lal','bos'], date) or None."""
    m = re.match(r"^([a-z0-9]+)-([a-z0-9]{2,5})-([a-z0-9]{2,5})-(\d{4})-(\d{2})-(\d{2})$", str(slug or "").lower())
    if not m:
        return None
    try:
        d = datetime(int(m.group(4)), int(m.group(5)), int(m.group(6)))
    except ValueError:
        return None
    return m.group(1), [m.group(2), m.group(3)], d


def _kalshi_date_token(d):
    return f"{d.year % 100:02d}{_MONTHS[d.month - 1]}{d.day:02d}"


def _fetch_events(series):
    now = time.time()
    hit = _cache.get(series)
    if hit and now - hit[0] < CACHE_TTL:
        return hit[1]
    events, cursor = [], None
    for _ in range(5):
        params = {"series_ticker": series, "status": "open", "with_nested_markets": "true", "limit": 200}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(f"{KALSHI_BASE}/events", params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        events.extend(data.get("events") or [])
        cursor = data.get("cursor")
        if not cursor:
            break
    _cache[series] = (now, events)
    return events


def _price(m, name):
    """Read a price in dollars from '<name>_dollars' or cents-style '<name>'."""
    v = m.get(f"{name}_dollars")
    if v not in (None, ""):
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    v = m.get(name)
    if v not in (None, ""):
        try:
            f = float(v)
            return f / 100.0 if f > 1 else f
        except (TypeError, ValueError):
            pass
    return None


def find_match(slug, outcome):
    """Returns (info dict | None, reason)."""
    parsed = parse_slug(slug)
    if not parsed:
        return None, "slug_not_game_format"
    prefix, codes, d = parsed
    series = _series_for(prefix)
    if not series:
        return None, f"no_series_for_{prefix}"
    token = _kalshi_date_token(d)
    events = _fetch_events(series)
    cands = [
        e for e in events
        if token in str(e.get("event_ticker", "")).upper()
        and all(c.upper() in str(e.get("event_ticker", "")).upper() for c in codes)
    ]
    if not cands:
        return None, f"no_event(series={series},token={token},open_events={len(events)})"
    ev = cands[0]
    out = str(outcome or "").strip().lower()
    out_last = out.split()[-1] if out else ""
    for m in ev.get("markets") or []:
        label = " ".join(str(m.get(k, "") or "") for k in ("yes_sub_title", "subtitle", "title")).lower()
        tail = str(m.get("ticker", "")).rsplit("-", 1)[-1].lower()
        if (out and out in label) or (out_last and out_last in label) or tail in codes and tail and tail in out.replace(" ", ""):
            return {
                "ticker": m.get("ticker"),
                "ask": _price(m, "yes_ask"),
                "bid": _price(m, "yes_bid"),
                "last": _price(m, "last_price"),
            }, "ok"
    labels = "; ".join(
        f"{m.get('ticker')}='{m.get('yes_sub_title') or m.get('subtitle') or m.get('title')}' "
        f"ask={_price(m, 'yes_ask')}"
        for m in (ev.get("markets") or [])
    )
    return None, f"no_market_for_outcome(event={ev.get('event_ticker')}) labels: {labels}"


def log_comparison(g, pm_price):
    """Log-only. Never raises."""
    if not KALSHI_SHADOW_ENABLED:
        return
    try:
        slug = g.get("slug") or g.get("market_slug")
        outcome = g.get("outcome")
        info, reason = find_match(slug, outcome)
        if not info:
            print(f"[KALSHI] match=none pm_slug={slug} pm_outcome={outcome} reason={reason}")
            return
        ask = info.get("ask")
        diff = f"{(ask - float(pm_price)) * 100:+.1f}" if ask is not None and pm_price else "n/a"
        print(
            f"[KALSHI] match=ok pm_slug={slug} pm_outcome={outcome} pm_price={float(pm_price) * 100:.1f}c "
            f"kalshi_ticker={info['ticker']} kalshi_ask={info['ask']} kalshi_bid={info['bid']} "
            f"kalshi_last={info['last']} ask_minus_pm_c={diff}"
        )
    except Exception as exc:
        print(f"[KALSHI] error {type(exc).__name__}: {exc}")
