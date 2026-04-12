import json
import os
from pathlib import Path


DATA_DIR = os.environ.get("DATA_DIR", "/data")
TRACKED_BETS_PATH = Path(f"{DATA_DIR}/tracked_bets.json")


def load_tracked_bets():
    if not TRACKED_BETS_PATH.exists():
        print(f"File not found: {TRACKED_BETS_PATH.resolve()}")
        return []

    try:
        with open(TRACKED_BETS_PATH, "r") as f:
            tracked_bets = json.load(f)
    except Exception as e:
        print(f"Failed to read {TRACKED_BETS_PATH}: {e}")
        return []

    if isinstance(tracked_bets, dict):
        tracked_bets = list(tracked_bets.values())
    elif not isinstance(tracked_bets, list):
        print("tracked_bets.json is not a list or dict.")
        return []

    return tracked_bets


def build_wallet_rollup(tracked_bets):
    by_wallet = {}

    for row in tracked_bets:
        if not isinstance(row, dict):
            continue

        wallet = str(row.get("wallet", "") or "").strip().lower()
        if not wallet:
            continue

        if wallet not in by_wallet:
            by_wallet[wallet] = {
                "wallet": wallet,
                "tracked_bets": 0,
                "resolved": 0,
                "wins": 0,
                "losses": 0,
                "edge_sum": 0.0,
                "edge_count": 0,
                "instant_clv_sum": 0.0,
                "instant_clv_count": 0,
            }

        stats = by_wallet[wallet]
        stats["tracked_bets"] += 1

        resolved = bool(row.get("resolved"))
        won = row.get("won")
        result = str(row.get("result", "") or "").strip().upper()

        if resolved:
            stats["resolved"] += 1
            if won is True or result == "WIN":
                stats["wins"] += 1
            elif won is False or result == "LOSS":
                stats["losses"] += 1

        edge = row.get("edge_pct_at_alert")
        if edge is None:
            edge = row.get("edge_pct")

        try:
            edge = float(edge)
            stats["edge_sum"] += edge
            stats["edge_count"] += 1
        except Exception:
            pass

        instant_clv = row.get("instant_clv_cents")
        if instant_clv is None:
            instant_clv = row.get("snapshot_clv_cents")

        try:
            instant_clv = float(instant_clv)
            stats["instant_clv_sum"] += instant_clv
            stats["instant_clv_count"] += 1
        except Exception:
            pass

    rows = []
    for wallet, stats in by_wallet.items():
        resolved = stats["resolved"]
        wins = stats["wins"]
        losses = stats["losses"]

        win_rate = round((wins / resolved) * 100, 1) if resolved > 0 else None
        avg_edge = round(stats["edge_sum"] / stats["edge_count"], 2) if stats["edge_count"] > 0 else None
        avg_instant_clv = (
            round(stats["instant_clv_sum"] / stats["instant_clv_count"], 2)
            if stats["instant_clv_count"] > 0 else None
        )

        rows.append({
            "wallet": wallet,
            "tracked_bets": stats["tracked_bets"],
            "resolved": resolved,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "avg_edge_at_alert": avg_edge,
            "avg_instant_clv_at_alert": avg_instant_clv,
        })

    rows.sort(
        key=lambda r: (
            int(r.get("resolved", 0) or 0),
            int(r.get("wins", 0) or 0),
            int(r.get("tracked_bets", 0) or 0),
        ),
        reverse=True,
    )

    return rows


def print_summary(tracked_bets, wallet_rows):
    resolved_bets = 0
    wins = 0
    losses = 0

    for row in tracked_bets:
        if not isinstance(row, dict):
            continue

        resolved = bool(row.get("resolved"))
        won = row.get("won")
        result = str(row.get("result", "") or "").strip().upper()

        if not resolved:
            continue

        resolved_bets += 1
        if won is True or result == "WIN":
            wins += 1
        elif won is False or result == "LOSS":
            losses += 1

    print("=" * 80)
    print("TRACKED BET AUDIT")
    print("=" * 80)
    print(f"Tracked bets:      {len(tracked_bets)}")
    print(f"Resolved bets:     {resolved_bets}")
    print(f"Wins:              {wins}")
    print(f"Losses:            {losses}")
    print(f"Wallets found:     {len(wallet_rows)}")
    print("-" * 80)

    for row in wallet_rows[:25]:
        print(f"Wallet:            {row['wallet']}")
        print(f"Tracked bets:      {row['tracked_bets']}")
        print(f"Resolved bets:     {row['resolved']}")
        print(f"Wins:              {row['wins']}")
        print(f"Losses:            {row['losses']}")
        print(f"Win rate:          {row['win_rate'] if row['win_rate'] is not None else 'N/A'}")
        print(f"Avg edge at alert: {row['avg_edge_at_alert'] if row['avg_edge_at_alert'] is not None else 'N/A'}")
        print(
            f"Avg instant CLV:   "
            f"{row['avg_instant_clv_at_alert'] if row['avg_instant_clv_at_alert'] is not None else 'N/A'}"
        )
        print("-" * 80)


def main():
    tracked_bets = load_tracked_bets()
    wallet_rows = build_wallet_rollup(tracked_bets)
    print_summary(tracked_bets, wallet_rows)


if __name__ == "__main__":
    main()