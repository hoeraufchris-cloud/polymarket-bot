import os
import json

from execution import preview_order


TEST_MARKET_SLUG = os.getenv("TEST_POLYMARKET_MARKET_SLUG")
TEST_OUTCOME = os.getenv("TEST_POLYMARKET_OUTCOME", "yes")
TEST_PRICE = os.getenv("TEST_POLYMARKET_PRICE")
TEST_MAX_ORDER_USD = os.getenv("TEST_MAX_ORDER_USD", "5")


def main():
    if not TEST_MARKET_SLUG:
        raise RuntimeError("Missing TEST_POLYMARKET_MARKET_SLUG env var")

    if not TEST_PRICE:
        raise RuntimeError("Missing TEST_POLYMARKET_PRICE env var")

    try:
        result = preview_order(
            market_slug=TEST_MARKET_SLUG,
            outcome=TEST_OUTCOME,
            price=TEST_PRICE,
            max_order_usd=TEST_MAX_ORDER_USD,
        )

        print(json.dumps(result, indent=2, default=str))

    except Exception as e:
        print("ORDER PREVIEW FAILED")
        print("Market slug:", TEST_MARKET_SLUG)
        print("Outcome:", TEST_OUTCOME)
        print("Price:", TEST_PRICE)
        print("Max order USD:", TEST_MAX_ORDER_USD)
        print("Error type:", type(e).__name__)
        print("Error:", e)
        raise


if __name__ == "__main__":
    main()