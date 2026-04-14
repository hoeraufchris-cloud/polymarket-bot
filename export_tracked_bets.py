import json
import csv
from datetime import datetime

INPUT_FILE = "data/tracked_bets.json"
OUTPUT_FILE = "tracked_bets_export.csv"


def load_lines():
    with open(INPUT_FILE, "r") as f:
        data = json.load(f)

    bets = []

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                bets.append(item)
        return bets

    if not isinstance(data, dict):
        return bets

    for _, val in data.items():
        if not isinstance(val, dict):
            continue

        if (
            "market" in val
            or "outcome" in val
            or "slug" in val
            or "score" in val
            or "result" in val
        ):
            bets.append(val)
            continue

        for _, inner_val in val.items():
            if not isinstance(inner_val, dict):
                continue
            if (
                "market" in inner_val
                or "outcome" in inner_val
                or "slug" in inner_val
                or "score" in inner_val
                or "result" in inner_val
            ):
                bets.append(inner_val)

    return bets


def format_event_start_date(event_start_time):
    if not event_start_time:
        return None

    try:
        dt = datetime.fromisoformat(event_start_time.replace("Z", "+00:00"))
        return f"{dt.month}/{dt.day}/{dt.year}"
    except Exception:
        return event_start_time


def dedupe_bets(bets):
    seen = {}

    for bet in bets:
        key = (
            bet.get("slug"),
            bet.get("outcome"),
            bet.get("wallet"),
            bet.get("alert_ts"),
        )

        if key not in seen:
            seen[key] = bet

    return list(seen.values())


def to_row(bet):
    result = bet.get("result")

    if result is None:
        if bet.get("resolved") is True:
            winning_outcome = bet.get("winning_outcome")
            outcome = bet.get("outcome")

            if winning_outcome is not None and outcome is not None:
                result = "WIN" if str(winning_outcome) == str(outcome) else "LOSS"
            elif bet.get("resolution_price") is not None:
                try:
                    result = "WIN" if float(bet.get("resolution_price")) >= 0.5 else "LOSS"
                except (TypeError, ValueError):
                    result = None

    return {
        "Date": bet.get("event_start_time") or bet.get("timestamp") or bet.get("alert_ts"),
        "Market": bet.get("market"),
        "Bet": bet.get("outcome"),
        "Market Phase": bet.get("market_phase"),
        "Alert Price": bet.get("current_price_at_alert") or bet.get("price_at_alert"),
        "Fair Price": bet.get("fair_price_at_alert"),
        "Edge %": bet.get("edge_pct_at_alert") or bet.get("edge_at_alert"),
        "Movement (c)": bet.get("market_movement_cents_at_alert") or bet.get("market_movement_cents"),
        "Consensus Score": bet.get("consensus_score"),
        "Consensus Type": bet.get("consensus_type"),
        "Score": bet.get("score"),
        "Sequence Role": bet.get("sequence_role"),
        "Size Ratio": bet.get("size_ratio"),
        "Stake %": bet.get("stake_pct") or bet.get("stake_percentage"),
        "Buy Count": bet.get("buy_count"),
        "Total Size": bet.get("total_size"),
        "Wallet": bet.get("wallet"),
        "Instant CLV (c)": bet.get("instant_clv_cents_at_alert") or bet.get("instant_clv_at_alert"),
        "Resolved": bet.get("resolved"),
        "Result": bet.get("result"),
        "Winning Outcome": bet.get("winning_outcome"),
        "Resolution Price": bet.get("resolution_price"),
        "Resolved TS": bet.get("resolved_ts"),
    }


def write_csv(rows):
    if not rows:
        print("No data to write.")
        return

    fieldnames = [
        "Date",
        "Market",
        "Bet",
        "Market Phase",
        "Alert Price",
        "Fair Price",
        "Edge %",
        "Movement (c)",
        "Consensus Score",
        "Consensus Type",
        "Score",
        "Sequence Role",
        "Size Ratio",
        "Stake %",
        "Buy Count",
        "Total Size",
        "Wallet",
        "Instant CLV (c)",
        "Resolved",
        "Result",
        "Winning Outcome",
        "Resolution Price",
        "Resolved TS",
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main():
    bets = load_lines()
    deduped = dedupe_bets(bets)
    rows = [to_row(bet) for bet in deduped]

    write_csv(rows)

    resolved_count = sum(1 for row in rows if row.get("Resolved") not in (None, "", False, "FALSE"))
    result_count = sum(1 for row in rows if row.get("Result") not in (None, ""))
    winning_outcome_count = sum(1 for row in rows if row.get("Winning Outcome") not in (None, ""))
    resolution_price_count = sum(1 for row in rows if row.get("Resolution Price") not in (None, ""))

    print(f"Raw rows: {len(bets)}")
    print(f"Deduped rows: {len(rows)}")
    print(f"Resolved populated: {resolved_count}")
    print(f"Result populated: {result_count}")
    print(f"Winning Outcome populated: {winning_outcome_count}")
    print(f"Resolution Price populated: {resolution_price_count}")
    print(f"Exported to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()