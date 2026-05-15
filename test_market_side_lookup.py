import json
import os

from execution import get_polymarket_client


TEST_MARKET_SLUG = os.getenv(
    "TEST_POLYMARKET_MARKET_SLUG",
    "nba-sas-min-2026-05-15-total-210pt5",
)

SEARCH_TERMS = [
    "spurs",
    "timberwolves",
    "sas",
    "min",
    "210.5",
    "210pt5",
    "total",
]


def market_text_blob(market):
    parts = [
        market.get("question"),
        market.get("title"),
        market.get("subtitle"),
        market.get("slug"),
        market.get("description"),
        market.get("outcomes"),
        market.get("outcomePrices"),
        market.get("marketType"),
        market.get("sportsMarketTypeV2"),
    ]

    for side in market.get("marketSides", []):
        parts.append(side.get("description"))
        parts.append(side.get("identifier"))

        team = side.get("team") or {}
        parts.append(team.get("name"))
        parts.append(team.get("abbreviation"))
        parts.append(team.get("alias"))
        parts.append(team.get("safeName"))

    return " | ".join(str(part or "") for part in parts).lower()


def is_candidate_market(market):
    blob = market_text_blob(market)

    if TEST_MARKET_SLUG.lower() in blob:
        return True

    has_spurs = any(term in blob for term in ["spurs", "sas", "san antonio"])
    has_wolves = any(term in blob for term in ["timberwolves", "wolves", "min", "minnesota"])
    has_total = any(term in blob for term in ["210.5", "210pt5", "total", "over", "under"])

    return has_spurs and has_wolves and has_total


def print_market_summary(market):
    print("-" * 80)
    print("id:", market.get("id"))
    print("question:", market.get("question"))
    print("title:", market.get("title"))
    print("slug:", market.get("slug"))
    print("active:", market.get("active"))
    print("closed:", market.get("closed"))
    print("archived:", market.get("archived"))
    print("marketType:", market.get("marketType"))
    print("sportsMarketTypeV2:", market.get("sportsMarketTypeV2"))
    print("outcomes:", market.get("outcomes"))
    print("outcomePrices:", market.get("outcomePrices"))

    for side in market.get("marketSides", []):
        print(
            "side:",
            side.get("description"),
            "| long:",
            side.get("long"),
            "| price:",
            side.get("price"),
            "| identifier:",
            side.get("identifier"),
        )


def main():
    client = get_polymarket_client()

    print("=" * 80)
    print(f"Trying retrieve_by_slug: {TEST_MARKET_SLUG}")
    print("=" * 80)

    try:
        market_response = client.markets.retrieve_by_slug(TEST_MARKET_SLUG)
        print(json.dumps(market_response, indent=2, default=str))
        return
    except Exception as e:
        print("retrieve_by_slug failed")
        print("Error type:", type(e).__name__)
        print("Error:", e)

    matches = []
    limit = 100
    max_offsets = 100

    for page in range(max_offsets):
        offset = page * limit

        markets_response = client.markets.list({
            "limit": limit,
            "offset": offset,
            "active": True,
            "closed": False,
            "archived": False,
        })

        markets = markets_response.get("markets", [])

        print(f"Scanned offset={offset}, returned={len(markets)}")

        if not markets:
            break

        for market in markets:
            if is_candidate_market(market):
                matches.append(market)

    print("=" * 80)
    print(f"Candidate matches found: {len(matches)}")
    print("=" * 80)

    for market in matches:
        print_market_summary(market)


if __name__ == "__main__":
    main()