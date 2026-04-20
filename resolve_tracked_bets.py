import json
import time
from datetime import datetime
from urllib.parse import quote
import urllib.request
import ssl
import os

try:
    import truststore
except Exception:
    truststore = None

RESOLVE_TRUSTSTORE_INJECTED = False

def configure_native_truststore():
    global RESOLVE_TRUSTSTORE_INJECTED

    if RESOLVE_TRUSTSTORE_INJECTED:
        return True

    if truststore is None:
        return False

    try:
        truststore.inject_into_ssl()
        RESOLVE_TRUSTSTORE_INJECTED = True
        print("[SSL] resolve_tracked_bets.py using native macOS trust store via truststore")
        return True
    except Exception as e:
        print(f"[SSL truststore warning] {repr(e)}")
        return False

configure_native_truststore()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir("/data"):
    DATA_DIR = "/data"
else:
    DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

INPUT_FILE = os.path.join(DATA_DIR, "tracked_bets.json")
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS = 0.08


def load_tracked_bets():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("tracked_bets.json must contain a top-level JSON object.")

    return data


def save_tracked_bets(data):
    with open(INPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def parse_iso_to_ts(value):
    if not value:
        return None

    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def safe_json_loads(value):
    if isinstance(value, (dict, list)):
        return value

    if not isinstance(value, str):
        return None

    try:
        return json.loads(value)
    except Exception:
        return None


def normalize_outcome_name(value):
    if value is None:
        return None

    text = str(value).strip().lower()
    text = text.replace("’", "'")
    text = " ".join(text.split())
    return text

def fetch_json_url(url):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        },
    )

    ssl_context = ssl.create_default_context()

    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT, context=ssl_context) as response:
        raw = response.read().decode("utf-8")

    return json.loads(raw)

def fetch_market_by_slug(slug):
    """
    Try a few public Gamma endpoints because response shapes vary.
    Returns a single market dict when found, else None.
    """
    encoded_slug = quote(slug, safe="")
    urls = [
        f"https://gamma-api.polymarket.com/markets/slug/{encoded_slug}",
        f"https://gamma-api.polymarket.com/markets?slug={encoded_slug}",
    ]

    last_error = None
    slug_normalized = str(slug or "").strip().lower()

    for url in urls:
        try:
            payload = fetch_json_url(url)

            if isinstance(payload, dict):
                if str(payload.get("slug", "") or "").strip().lower() == slug_normalized:
                    return payload

                markets = payload.get("markets")
                if isinstance(markets, list):
                    for item in markets:
                        if (
                            isinstance(item, dict)
                            and str(item.get("slug", "") or "").strip().lower() == slug_normalized
                        ):
                            return item

                data = payload.get("data")
                if isinstance(data, list):
                    for item in data:
                        if (
                            isinstance(item, dict)
                            and str(item.get("slug", "") or "").strip().lower() == slug_normalized
                        ):
                            return item

            if isinstance(payload, list):
                for item in payload:
                    if (
                        isinstance(item, dict)
                        and str(item.get("slug", "") or "").strip().lower() == slug_normalized
                    ):
                        return item

        except Exception as e:
            last_error = e

    if last_error:
        raise last_error

    return None


def extract_winning_outcome(market_data):
    """
    Try several fields Polymarket may expose for winner/resolution.
    """
    direct_fields = [
        "winning_outcome",
        "winningOutcome",
        "winner",
        "outcome",
    ]

    for field in direct_fields:
        value = market_data.get(field)
        if value not in (None, ""):
            return value

    outcome_prices = safe_json_loads(market_data.get("outcomePrices"))
    outcomes = safe_json_loads(market_data.get("outcomes"))

    if isinstance(outcomes, list) and isinstance(outcome_prices, list) and len(outcomes) == len(outcome_prices):
        try:
            numeric_prices = [float(x) for x in outcome_prices]
            max_idx = max(range(len(numeric_prices)), key=lambda i: numeric_prices[i])
            if numeric_prices[max_idx] >= 0.999:
                return outcomes[max_idx]
        except Exception:
            pass

    return None


def extract_resolution_price(market_data, winning_outcome):
    """
    Best-effort resolution price extraction.
    """
    direct_fields = [
        "resolution",
        "resolution_price",
        "resolutionPrice",
        "settlement_price",
        "settlementPrice",
    ]

    for field in direct_fields:
        value = market_data.get(field)
        if value not in (None, ""):
            try:
                return float(value)
            except Exception:
                return value

    outcome_prices = safe_json_loads(market_data.get("outcomePrices"))
    outcomes = safe_json_loads(market_data.get("outcomes"))

    winning_norm = normalize_outcome_name(winning_outcome)

    if isinstance(outcomes, list) and isinstance(outcome_prices, list) and len(outcomes) == len(outcome_prices):
        for outcome, price in zip(outcomes, outcome_prices):
            if normalize_outcome_name(outcome) == winning_norm:
                try:
                    return float(price)
                except Exception:
                    return price

    return None


def is_market_closed(market_data):
    closed_fields = [
        "closed",
        "resolved",
        "archived",
        "isResolved",
        "isClosed",
    ]

    for field in closed_fields:
        value = market_data.get(field)
        if isinstance(value, bool) and value:
            return True

    winning_outcome = extract_winning_outcome(market_data)
    if winning_outcome not in (None, ""):
        return True

    return False


def choose_result(bet, market_data):
    """
    Returns:
        resolved (bool)
        result ("WIN"/"LOSS"/None)
        winning_outcome (str|None)
        resolution_price (float|None)
        resolved_ts (int|None)
    """
    if not isinstance(market_data, dict):
        return False, None, None, None, None

    winning_outcome = extract_winning_outcome(market_data)
    resolution_price = extract_resolution_price(market_data, winning_outcome)

    resolved_ts = (
        parse_iso_to_ts(market_data.get("closedTime"))
        or parse_iso_to_ts(market_data.get("endDate"))
        or parse_iso_to_ts(market_data.get("gameStartTime"))
        or parse_iso_to_ts(market_data.get("endDateIso"))
        or parse_iso_to_ts(market_data.get("resolveTime"))
        or parse_iso_to_ts(market_data.get("resolvedTime"))
    )

    closed = is_market_closed(market_data)

    if not closed:
        return False, None, winning_outcome, resolution_price, resolved_ts

    bet_outcome = normalize_outcome_name(
        bet.get("outcome") or bet.get("bet") or bet.get("normalized_outcome_key")
    )
    winning_norm = normalize_outcome_name(winning_outcome)

    if winning_norm and bet_outcome:
        result = "WIN" if bet_outcome == winning_norm else "LOSS"
        return True, result, winning_outcome, resolution_price, resolved_ts

    outcomes_raw = safe_json_loads(market_data.get("outcomes"))
    if (
        isinstance(outcomes_raw, list)
        and len(outcomes_raw) == 2
        and resolution_price is not None
    ):
        try:
            resolution_float = float(resolution_price)

            if resolution_float == 1.0:
                inferred_winner = outcomes_raw[0]
            elif resolution_float == 0.0:
                inferred_winner = outcomes_raw[1]
            else:
                inferred_winner = None

            inferred_norm = normalize_outcome_name(inferred_winner)
            if inferred_norm and bet_outcome:
                result = "WIN" if bet_outcome == inferred_norm else "LOSS"
                return True, result, inferred_winner, resolution_float, resolved_ts
        except Exception:
            pass

    return True, None, winning_outcome, resolution_price, resolved_ts


def iter_bets(data):
    """
    tracked_bets.json is a dict keyed like:
    "slug||outcome||wallet||timestamp": {...bet object...}
    """
    for key, bet in data.items():
        if isinstance(bet, dict):
            yield key, bet


def main():
    data = load_tracked_bets()

    total = 0
    updated = 0
    resolved_count = 0
    unresolved_count = 0
    failed = 0

    slug_cache = {}

    for key, bet in iter_bets(data):
        total += 1
        slug = bet.get("slug")

        if not slug:
            failed += 1
            continue

        try:
            if slug not in slug_cache:
                slug_cache[slug] = fetch_market_by_slug(slug)
                time.sleep(SLEEP_BETWEEN_CALLS)

            market_data = slug_cache[slug]

            if not market_data:
                failed += 1
                continue

            is_resolved, result, winning_outcome, resolution_price, resolved_ts = choose_result(
                bet, market_data
            )

            bet["resolved"] = bool(is_resolved)
            bet["result"] = result
            bet["winning_outcome"] = winning_outcome
            bet["resolution_price"] = resolution_price
            bet["resolved_ts"] = resolved_ts

            updated += 1

            if is_resolved:
                resolved_count += 1
            else:
                unresolved_count += 1

        except Exception as e:
            failed += 1
            print(f"[Resolve lookup error] key={key} slug={slug} error={repr(e)}")

        if total % 25 == 0:
            print(
                f"Processed {total} bets - updated: {updated}, "
                f"resolved: {resolved_count}, unresolved: {unresolved_count}, failed: {failed}"
            )

    save_tracked_bets(data)

    populated_result = sum(
        1 for bet in data.values()
        if isinstance(bet, dict) and bet.get("result") not in (None, "")
    )
    populated_winner = sum(
        1 for bet in data.values()
        if isinstance(bet, dict) and bet.get("winning_outcome") not in (None, "")
    )
    populated_resolution_price = sum(
        1 for bet in data.values()
        if isinstance(bet, dict) and bet.get("resolution_price") not in (None, "")
    )
    populated_resolved_ts = sum(
        1 for bet in data.values()
        if isinstance(bet, dict) and bet.get("resolved_ts") not in (None, "")
    )

    print("")
    print("RESOLUTION SUMMARY")
    print("============================================================")
    print(f"Total bets processed:    {total}")
    print(f"Updated bets:            {updated}")
    print(f"Resolved bets:           {resolved_count}")
    print(f"Still unresolved:        {unresolved_count}")
    print(f"Failed lookups:          {failed}")
    print(f"Result populated:        {populated_result}")
    print(f"Winning outcome pop.:    {populated_winner}")
    print(f"Resolution price pop.:   {populated_resolution_price}")
    print(f"Resolved ts pop.:        {populated_resolved_ts}")
    print(f"Saved back to:           {INPUT_FILE}")


if __name__ == "__main__":
    main()