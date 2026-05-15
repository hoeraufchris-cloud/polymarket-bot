import json

from execution import get_polymarket_client


def main():
    client = get_polymarket_client()

    markets = client.markets.list({
        "limit": 25,
        "offset": 0,
        "active": True,
        "closed": False,
        "archived": False,
        "categories": ["sports"],
    })

    open_markets = []

    for market in markets.get("markets", []):
        if market.get("closed") is True:
            continue

        if market.get("archived") is True:
            continue

        if market.get("active") is not True:
            continue

        open_markets.append({
            "id": market.get("id"),
            "question": market.get("question"),
            "slug": market.get("slug"),
            "active": market.get("active"),
            "closed": market.get("closed"),
            "archived": market.get("archived"),
            "marketType": market.get("marketType"),
            "sportsMarketTypeV2": market.get("sportsMarketTypeV2"),
            "bestBid": market.get("bestBid"),
            "bestAsk": market.get("bestAsk"),
            "lastTradePrice": market.get("lastTradePrice"),
            "liquidity": market.get("liquidity"),
            "volume": market.get("volume"),
            "outcomes": market.get("outcomes"),
            "outcomePrices": market.get("outcomePrices"),
        })

    print(json.dumps({
        "raw_count": len(markets.get("markets", [])),
        "open_count": len(open_markets),
        "open_markets": open_markets,
    }, indent=2, default=str))


if __name__ == "__main__":
    main()