import json
import os
import re
from difflib import SequenceMatcher

from execution import get_polymarket_client, preview_order


FAILED_MARKET_SLUG = os.getenv("FAILED_MARKET_SLUG", "").strip()
FAILED_MARKET_TITLE = os.getenv("FAILED_MARKET_TITLE", "").strip()
FAILED_OUTCOME = os.getenv("FAILED_OUTCOME", "").strip()
FAILED_PRICE = os.getenv("FAILED_PRICE", "").strip()
FAILED_MAX_ORDER_USD = os.getenv("FAILED_MAX_ORDER_USD", "5").strip()

SCAN_LIMIT = int(os.getenv("SLUG_RESOLVER_SCAN_LIMIT", "100"))
MAX_PAGES = int(os.getenv("SLUG_RESOLVER_MAX_PAGES", "50"))
MIN_SCORE_TO_PRINT = float(os.getenv("SLUG_RESOLVER_MIN_SCORE", "20"))
TOP_CANDIDATES_TO_PREVIEW = int(os.getenv("SLUG_RESOLVER_TOP_PREVIEW", "10"))


def normalize_text(value):
    text = str(value or "").lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def slug_terms(slug):
    raw_terms = normalize_text(slug).split()

    stop_terms = {
        "atp",
        "wta",
        "nba",
        "mlb",
        "wnba",
        "mls",
        "epl",
        "lal",
        "mex",
        "chi",
        "total",
        "spread",
        "home",
        "away",
        "yes",
        "no",
        "over",
        "under",
        "2026",
        "2025",
        "2024",
        "05",
        "06",
        "07",
        "08",
        "09",
        "10",
        "11",
        "12",
        "01",
        "02",
        "03",
        "04",
    }

    useful_terms = []

    for term in raw_terms:
        if term in stop_terms:
            continue

        if len(term) <= 1:
            continue

        if re.fullmatch(r"\d+", term):
            continue

        useful_terms.append(term)

    return useful_terms


def market_text_blob(market):
    parts = [
        market.get("id"),
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
        parts.append(team.get("displayAbbreviation"))

    return normalize_text(" ".join(str(part or "") for part in parts))


def score_market(market, target_terms, target_title, target_outcome):
    blob = market_text_blob(market)
    slug = normalize_text(market.get("slug"))
    title = normalize_text(market.get("title") or market.get("question"))

    score = 0.0
    matched_terms = []

    for term in target_terms:
        if term in blob:
            score += 12
            matched_terms.append(term)

    outcome_clean = normalize_text(target_outcome)

    if outcome_clean and outcome_clean in blob:
        score += 40
        matched_terms.append(f"outcome:{outcome_clean}")

    target_title_clean = normalize_text(target_title)

    if target_title_clean and title:
        score += SequenceMatcher(None, target_title_clean, title).ratio() * 40

    if FAILED_MARKET_SLUG and normalize_text(FAILED_MARKET_SLUG) == slug:
        score += 100
        matched_terms.append("exact_slug")

    return score, matched_terms


def print_market_summary(rank, score, matched_terms, market):
    print("-" * 100)
    print(f"Rank: {rank}")
    print(f"Score: {score:.2f}")
    print(f"Matched: {matched_terms}")
    print("id:", market.get("id"))
    print("slug:", market.get("slug"))
    print("title:", market.get("title"))
    print("question:", market.get("question"))
    print("active:", market.get("active"))
    print("closed:", market.get("closed"))
    print("archived:", market.get("archived"))
    print("marketType:", market.get("marketType"))
    print("sportsMarketTypeV2:", market.get("sportsMarketTypeV2"))
    print("outcomes:", market.get("outcomes"))
    print("outcomePrices:", market.get("outcomePrices"))

    for side in market.get("marketSides", []):
        team = side.get("team") or {}

        print(
            "side:",
            side.get("description"),
            "| long:",
            side.get("long"),
            "| price:",
            side.get("price"),
            "| team:",
            team.get("name") or team.get("alias") or team.get("safeName"),
        )


def main():
    if not FAILED_MARKET_SLUG:
        raise RuntimeError("Missing FAILED_MARKET_SLUG env var")

    if not FAILED_OUTCOME:
        raise RuntimeError("Missing FAILED_OUTCOME env var")

    if not FAILED_PRICE:
        raise RuntimeError("Missing FAILED_PRICE env var")

    client = get_polymarket_client()

    target_terms = slug_terms(FAILED_MARKET_SLUG)

    print("=" * 100)
    print("SLUG RESOLVER INPUT")
    print("=" * 100)
    print("failed_slug:", FAILED_MARKET_SLUG)
    print("failed_title:", FAILED_MARKET_TITLE)
    print("failed_outcome:", FAILED_OUTCOME)
    print("failed_price:", FAILED_PRICE)
    print("target_terms:", target_terms)

    candidates = []

    for page in range(MAX_PAGES):
        offset = page * SCAN_LIMIT

        markets_response = client.markets.list({
            "limit": SCAN_LIMIT,
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
            score, matched_terms = score_market(
                market=market,
                target_terms=target_terms,
                target_title=FAILED_MARKET_TITLE,
                target_outcome=FAILED_OUTCOME,
            )

            if score >= MIN_SCORE_TO_PRINT:
                candidates.append({
                    "score": score,
                    "matched_terms": matched_terms,
                    "market": market,
                })

    candidates.sort(key=lambda item: item["score"], reverse=True)

    print("=" * 100)
    print(f"CANDIDATES FOUND: {len(candidates)}")
    print("=" * 100)

    for index, candidate in enumerate(candidates[:25], start=1):
        print_market_summary(
            rank=index,
            score=candidate["score"],
            matched_terms=candidate["matched_terms"],
            market=candidate["market"],
        )

    print("=" * 100)
    print(f"PREVIEW TESTING TOP {TOP_CANDIDATES_TO_PREVIEW}")
    print("=" * 100)

    for index, candidate in enumerate(candidates[:TOP_CANDIDATES_TO_PREVIEW], start=1):
        market = candidate["market"]
        candidate_slug = market.get("slug")

        print("-" * 100)
        print(f"Preview candidate #{index}: {candidate_slug}")

        try:
            preview = preview_order(
                market_slug=candidate_slug,
                outcome=FAILED_OUTCOME,
                price=FAILED_PRICE,
                max_order_usd=FAILED_MAX_ORDER_USD,
            )

            preview_order_data = preview.get("preview", {}).get("order", {})

            print("PREVIEW_OK")
            print("candidate_slug:", candidate_slug)
            print("mode:", preview.get("mode"))
            print("intent:", preview.get("payload", {}).get("intent"))
            print("marketMetadata.outcome:", preview_order_data.get("marketMetadata", {}).get("outcome"))
            print("action:", preview_order_data.get("action"))
            print("outcomeSide:", preview_order_data.get("outcomeSide"))
            print("quantity:", preview_order_data.get("quantity"))
            print(json.dumps(preview.get("payload"), indent=2, default=str))

        except Exception as e:
            print("PREVIEW_FAILED")
            print("candidate_slug:", candidate_slug)
            print("error:", e)


if __name__ == "__main__":
    main()