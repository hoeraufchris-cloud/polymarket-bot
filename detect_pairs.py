import json
from collections import defaultdict
from pathlib import Path

def is_live_position(pos):
    """
    Return True only for positions that still appear to have a live market price
    and non-zero current value.
    """
    cur_price = float(pos.get("curPrice", 0) or 0)
    current_value = float(pos.get("currentValue", 0) or 0)

    return cur_price > 0 and current_value > 0

def is_actionable_position(pos):
    """
    Return True only for positions in markets that are still reasonably tradable.
    Exclude markets priced too close to 0 or 1.
    """
    current_price = float(pos.get("curPrice", 0) or 0)
    return 0.05 <= current_price <= 0.95

def load_positions(file_path: str):
    """Load positions JSON from disk and return raw, live, and actionable positions."""
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Could not find file: {file_path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError("Expected positions JSON to be a list of position objects.")

    raw_data = data
    live_data = [pos for pos in raw_data if is_live_position(pos)]
    actionable_data = [pos for pos in live_data if is_actionable_position(pos)]

    return raw_data, live_data, actionable_data

def score_unpaired_market_from_clv(position_details):
    """
    Score an unpaired market using the best positive CLV among its positions.
    For now, this is a simple v1 scoring rule based only on CLV.
    """

    best_clv = max((p["clv_cents"] for p in position_details), default=0)

    # Strict timing rule
    if best_clv <= 0:
        return {
            "score": 0,
            "label": "PASS",
            "stake_pct": 0,
            "reason": "No positive CLV"
        }
    elif best_clv <= 1:
        return {
            "score": 55,
            "label": "LEAN",
            "stake_pct": 30,
            "reason": "Positive CLV, early timing"
        }
    elif best_clv <= 2:
        return {
            "score": 72,
            "label": "BET",
            "stake_pct": 55,
            "reason": "Strong positive CLV"
        }
    elif best_clv <= 3:
        return {
            "score": 52,
            "label": "LEAN",
            "stake_pct": 25,
            "reason": "Positive CLV, but getting late"
        }
    else:
        return {
            "score": 0,
            "label": "PASS",
            "stake_pct": 0,
            "reason": "Too late"
        }

def detect_paired_markets(positions):
    """
    Group positions by (wallet, market slug) and determine whether
    multiple outcomes are held in the same market.
    Also calculate per-position CLV in cents.
    """
    grouped = defaultdict(list)

    for pos in positions:
        wallet = pos.get("proxyWallet", "")
        market_slug = pos.get("slug", "")
        grouped[(wallet, market_slug)].append(pos)

    results = []

    for (wallet, market_slug), group in grouped.items():
        outcomes = sorted({g.get("outcome", "") for g in group if g.get("outcome")})
        is_paired = len(outcomes) > 1

        total_initial_value = sum(float(g.get("initialValue", 0) or 0) for g in group)
        total_current_value = sum(float(g.get("currentValue", 0) or 0) for g in group)
        title = group[0].get("title", "")
        event_slug = group[0].get("eventSlug", "")

        position_details = []
        for g in group:
            avg_price = float(g.get("avgPrice", 0) or 0)
            current_price = float(g.get("curPrice", 0) or 0)
            clv_cents = round((current_price - avg_price) * 100, 2)

            if clv_cents <= 0:
                clv_status = "No positive CLV"
            elif clv_cents <= 1:
                clv_status = "Good timing"
            elif clv_cents <= 2:
                clv_status = "Strong timing"
            elif clv_cents <= 3:
                clv_status = "Getting late"
            else:
                clv_status = "Too late - auto pass"

            position_details.append(
                {
                    "outcome": g.get("outcome", ""),
                    "size": float(g.get("size", 0) or 0),
                    "avg_price": avg_price,
                    "current_price": current_price,
                    "clv_cents": clv_cents,
                    "clv_status": clv_status,
                    "initial_value": round(float(g.get("initialValue", 0) or 0), 4),
                    "current_value": round(float(g.get("currentValue", 0) or 0), 4),
                }
            )
        if is_paired:
            market_decision = {
                "label": "PASS",
                "score": 0,
                "stake_pct": 0,
                "reason": "Paired market"
            }
        else:
            market_decision = score_unpaired_market_from_clv(position_details)
        
        results.append(
            {
                "wallet": wallet,
                "title": title,
                "market_slug": market_slug,
                "event_slug": event_slug,
                "outcomes_held": outcomes,
                "paired": is_paired,
                "num_positions": len(group),
                "total_initial_value": round(total_initial_value, 4),
                "total_current_value": round(total_current_value, 4),
                "positions": position_details,
                "label": market_decision["label"],
                "score": market_decision["score"],
                "stake_pct": market_decision["stake_pct"],
                "reason": market_decision["reason"],
            }
        )

    return results
    """
    Group positions by (wallet, market slug) and determine whether
    multiple outcomes are held in the same market.
    """
    grouped = defaultdict(list)

    for pos in positions:
        wallet = pos.get("proxyWallet", "")
        market_slug = pos.get("slug", "")
        grouped[(wallet, market_slug)].append(pos)

    results = []

    for (wallet, market_slug), group in grouped.items():
        outcomes = sorted({g.get("outcome", "") for g in group if g.get("outcome")})
        is_paired = len(outcomes) > 1

        total_initial_value = sum(float(g.get("initialValue", 0) or 0) for g in group)
        total_current_value = sum(float(g.get("currentValue", 0) or 0) for g in group)
        title = group[0].get("title", "")
        event_slug = group[0].get("eventSlug", "")

        results.append(
            {
                "wallet": wallet,
                "title": title,
                "market_slug": market_slug,
                "event_slug": event_slug,
                "outcomes_held": outcomes,
                "paired": is_paired,
                "num_positions": len(group),
                "total_initial_value": round(total_initial_value, 4),
                "total_current_value": round(total_current_value, 4),
            }
        )

    return results


def print_results(results):
    """Pretty-print results."""
    if not results:
        print("No grouped markets found.")
        return

    for r in results:
        print("-" * 80)
        print(f"Wallet:              {r['wallet']}")
        print(f"Title:               {r['title']}")
        print(f"Market slug:         {r['market_slug']}")
        print(f"Event slug:          {r['event_slug']}")
        print(f"Outcomes held:       {', '.join(r['outcomes_held'])}")
        print(f"Paired market:       {r['paired']}")
        print(f"Number of positions: {r['num_positions']}")
        print(f"Initial value total: ${r['total_initial_value']}")
        print(f"Current value total: ${r['total_current_value']}")
        print(f"Label:               {r['label']}")
        print(f"Score:               {r['score']}")
        print(f"Stake %:             {r['stake_pct']}")
        print(f"Reason:              {r['reason']}")
        print("Position details:")

        for p in r["positions"]:
            print(f"  - Outcome:         {p['outcome']}")
            print(f"    Size:            {p['size']}")
            print(f"    Avg price:       {p['avg_price']}")
            print(f"    Current price:   {p['current_price']}")
            print(f"    CLV (cents):     {p['clv_cents']}")
            print(f"    CLV status:      {p['clv_status']}")
            print(f"    Initial value:   ${p['initial_value']}")
            print(f"    Current value:   ${p['current_value']}")


if __name__ == "__main__":
    raw_positions, live_positions, actionable_positions = load_positions("positions.json")

    print("=" * 80)
    print("PIPELINE SUMMARY")
    print(f"Raw positions:        {len(raw_positions)}")
    print(f"Live positions:       {len(live_positions)}")
    print(f"Actionable positions: {len(actionable_positions)}")
    print("=" * 80)

    results = detect_paired_markets(actionable_positions)
    print_results(results)