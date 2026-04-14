import json
import time
from datetime import datetime, timezone
from urllib.parse import quote

import requests


INPUT_FILE = "data/tracked_bets.json"
REQUEST_TIMEOUT = 20
SLEEP_BETWEEN_CALLS = 0.08


def load_tracked_bets():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("tracked_bets.txt must contain a top-level JSON object.")

    return data


def save_tracked_bets(data):
    with open(INPUT_FILE, "w") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def parse_iso_to_ts(value):
    if not value:
        return None

    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def fetch_market_by_slug(slug):
    """
    Tries both documented slug endpoints because Polymarket has returned
    slightly different shapes depending on endpoint/version.
    """
    encoded_slug = quote(slug, safe="")

    urls = [
        f"https://gamma-api.polymarket.com/markets/slug/{encoded_slug}",
        f"https://gamma-api.polymarket.com/markets?slug={encoded_slug}",
    ]

    last_error = None

    for url in urls:
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()

            # Endpoint 1: direct market object
            if isinstance(payload, dict) and payload.get("slug") == slug:
                return payload

            # Endpoint 2: list response
            if isinstance(payload, list) and payload:
                for item in payload:
                    if isinstance(item, dict) and item.get("slug") == slug:
                        return item

            # Sometimes wrapped
            if isinstance(payload, dict):
                markets = payload.get("markets")
                if isinstance(markets, list):
                    for item in markets:
                        if isinstance(item, dict) and item.get("slug") == slug:
                            return item

        except Exception as e:
            last_error = e

    if last_error:
        raise last_error

    return None


def normalize_outcome_name(value):
    if value is None:
        return None
    return str(value).strip().lower()


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

    closed = bool(market_data.get("closed"))
    winning_outcome = market_data.get("winning_outcome")
    resolution_price = market_data.get("resolution")
    resolved_ts = (
        parse_iso_to_ts(market_data.get("closedTime"))
        or parse_iso_to_ts(market_data.get("endDate"))
        or parse_iso_to_ts(market_data.get("gameStartTime"))
        or parse_iso_to_ts(market_data.get("endDateIso"))
    )

    # If market isn't closed, do not mark resolved
    if not closed:
        return False, None, winning_outcome, resolution_price, resolved_ts

    bet_outcome = normalize_outcome_name(
        bet.get("outcome") or bet.get("bet") or bet.get("normalized_outcome_key")
    )
    winning_norm = normalize_outcome_name(winning_outcome)

    # Straight winner match
    if winning_norm and bet_outcome:
        result = "WIN" if bet_outcome == winning_norm else "LOSS"
        return True, result, winning_outcome, resolution_price, resolved_ts

    # Fallback for binary yes/no-style markets
    outcomes_raw = market_data.get("outcomes")
    if isinstance(outcomes_raw, str):
        try:
            outcomes_raw = json.loads(outcomes_raw)
        except Exception:
            outcomes_raw = None

    outcome_prices_raw = market_data.get("outcomePrices")
    if isinstance(outcome_prices_raw, str):
        try:
            outcome_prices_raw = json.loads(outcome_prices_raw)
        except Exception:
            outcome_prices_raw = None

    # If there are exactly two outcomes and a resolution price of 1 or 0,
    # infer the winner from the first/second side.
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

    # Market closed, but could not determine winner cleanly
    return True, None, winning_outcome, resolution_price, resolved_ts


def iter_bets(data):
    """
    tracked_bets.txt is a dict keyed like:
    "slug||outcome||wallet||timestamp": {...bet object...}
    """
    for key, bet in data.items():
        if isinstance(bet, dict):
            yield key, bet


def main():
    data = load_tracked_bets()

    total = 0
    updated = 0
    resolved = 0
    unresolved = 0
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

            # Always backfill useful resolution fields when available
            bet["resolved"] = bool(is_resolved)
            bet["result"] = result
            bet["winning_outcome"] = winning_outcome
            bet["resolution_price"] = resolution_price
            bet["resolved_ts"] = resolved_ts

            updated += 1

            if is_resolved:
                resolved += 1
            else:
                unresolved += 1

        except Exception:
            failed += 1

        if total % 25 == 0:
            print(
                f"Processed {total} bets – updated: {updated}, "
                f"resolved: {resolved}, unresolved: {unresolved}, failed: {failed}"
            )

    save_tracked_bets(data)

    print("\nRESOLUTION SUMMARY")
    print("============================================================")
    print(f"Total bets processed: {total}")
    print(f"Updated bets:         {updated}")
    print(f"Resolved bets:        {resolved}")
    print(f"Still unresolved:     {unresolved}")
    print(f"Failed lookups:       {failed}")
    print(f"Saved back to:        {INPUT_FILE}")


if __name__ == "__main__":
    main()