import json
import os
from collections import defaultdict
from datetime import datetime, timezone


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if os.path.isdir("/data"):
    DATA_DIR = "/data"
else:
    DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(DATA_DIR, exist_ok=True)

SIGNAL_METRICS_HISTORY_PATH = os.path.join(DATA_DIR, "signal_metrics_history.json")
MARKET_MODEL_OUTPUT_PATH = os.path.join(DATA_DIR, "market_model_output.json")

MODEL_MIN_MINUTES_TO_START = 0
MODEL_MAX_MINUTES_TO_START = 180


def load_signal_metrics_history():
    try:
        with open(SIGNAL_METRICS_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []


def parse_ts(ts_value):
    try:
        return int(ts_value)
    except Exception:
        return None


def parse_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def parse_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def parse_event_start_time(event_start_time):
    if not event_start_time:
        return None

    try:
        return datetime.fromisoformat(str(event_start_time).replace("Z", "+00:00"))
    except Exception:
        return None


def get_minutes_to_start(row):
    if not isinstance(row, dict):
        return None

    existing = row.get("minutes_to_start")
    if existing is not None:
        try:
            return int(existing)
        except Exception:
            pass

    event_dt = parse_event_start_time(row.get("event_start_time"))
    row_ts = parse_ts(row.get("ts"))

    if event_dt is None or row_ts is None:
        return None

    alert_dt = datetime.fromtimestamp(row_ts, tz=timezone.utc)
    return int((event_dt - alert_dt).total_seconds() / 60)


def get_model_window_bucket(minutes_to_start):
    if minutes_to_start is None:
        return "unknown"

    m = int(minutes_to_start)

    if m < 0:
        return "started"
    if m <= 10:
        return "00-10m"
    if m <= 30:
        return "10-30m"
    if m <= 60:
        return "30-60m"
    if m <= 180:
        return "01-03h"
    if m <= 360:
        return "03-06h"
    if m <= 720:
        return "06-12h"
    return "12h+"

def is_in_model_window(minutes_to_start):
    if minutes_to_start is None:
        return False

    try:
        m = int(minutes_to_start)
    except Exception:
        return False

    return MODEL_MIN_MINUTES_TO_START <= m <= MODEL_MAX_MINUTES_TO_START

def group_rows_by_market_outcome(rows):
    grouped = defaultdict(list)

    for row in rows:
        if not isinstance(row, dict):
            continue

        slug = str(row.get("slug", "") or "").strip()
        outcome = str(row.get("outcome", "") or "").strip()

        if not slug or not outcome:
            continue

        grouped[(slug, outcome)].append(row)

    for key in grouped:
        grouped[key] = sorted(
            grouped[key],
            key=lambda x: parse_ts(x.get("ts")) or 0,
        )

    return grouped


def build_market_snapshot(rows):
    if not rows:
        return None

    latest = rows[-1]

    wallets = set()
    total_notional = 0.0
    total_size = 0.0
    size_ratios = []
    scores = []
    stake_pcts = []
    edges = []
    market_moves = []
    followers = []
    consensus_scores = []
    leaderboard_rois = []
    wallet_counts_by_role = defaultdict(int)

    for row in rows:
        wallet = str(row.get("wallet", "") or "").strip().lower()
        if wallet:
            wallets.add(wallet)

        total_notional += parse_float(row.get("total_notional"), 0.0)
        total_size += parse_float(row.get("total_size"), 0.0)

        size_ratio = parse_float(row.get("size_ratio"), None)
        if size_ratio is not None:
            size_ratios.append(size_ratio)

        score = parse_int(row.get("score"), None)
        if score is not None:
            scores.append(score)

        stake_pct = parse_int(row.get("stake_pct"), None)
        if stake_pct is not None:
            stake_pcts.append(stake_pct)

        edge_pct = parse_float(row.get("edge_pct"), None)
        if edge_pct is not None:
            edges.append(edge_pct)

        market_movement_cents = parse_float(row.get("market_movement_cents"), None)
        if market_movement_cents is not None:
            market_moves.append(market_movement_cents)

        follower_count = parse_int(row.get("followers"), None)
        if follower_count is not None:
            followers.append(follower_count)

        consensus_score = parse_int(row.get("consensus_score"), None)
        if consensus_score is not None:
            consensus_scores.append(consensus_score)

        leaderboard_roi = row.get("leaderboard_roi")
        if leaderboard_roi is not None:
            try:
                leaderboard_rois.append(float(leaderboard_roi))
            except Exception:
                pass

        sequence_role = str(row.get("sequence_role", "") or "").strip().lower()
        if sequence_role:
            wallet_counts_by_role[sequence_role] += 1

    minutes_to_start = get_minutes_to_start(latest)
    time_bucket = get_model_window_bucket(minutes_to_start)

    snapshot = {
        "slug": str(latest.get("slug", "") or "").strip(),
        "market": str(latest.get("market", "") or latest.get("title", "") or "").strip(),
        "outcome": str(latest.get("outcome", "") or "").strip(),
        "market_phase": str(latest.get("market_phase", "") or "").strip(),
        "event_start_time": latest.get("event_start_time"),
        "minutes_to_start": minutes_to_start,
        "time_to_start_bucket": time_bucket,
        "alert_count": len(rows),
        "unique_wallet_count": len(wallets),
        "leader_count": wallet_counts_by_role.get("leader", 0),
        "early_count": wallet_counts_by_role.get("early", 0),
        "follower_count": wallet_counts_by_role.get("follower", 0),
        "total_notional": round(total_notional, 2),
        "total_size": round(total_size, 4),
        "max_size_ratio": round(max(size_ratios), 4) if size_ratios else None,
        "avg_size_ratio": round(sum(size_ratios) / len(size_ratios), 4) if size_ratios else None,
        "max_score": max(scores) if scores else None,
        "avg_score": round(sum(scores) / len(scores), 2) if scores else None,
        "max_stake_pct": max(stake_pcts) if stake_pcts else None,
        "avg_stake_pct": round(sum(stake_pcts) / len(stake_pcts), 2) if stake_pcts else None,
        "avg_edge_pct": round(sum(edges) / len(edges), 4) if edges else None,
        "avg_market_movement_cents": round(sum(market_moves) / len(market_moves), 4) if market_moves else None,
        "max_followers": max(followers) if followers else 0,
        "max_consensus_score": max(consensus_scores) if consensus_scores else 0,
        "avg_leaderboard_roi": round(sum(leaderboard_rois) / len(leaderboard_rois), 6) if leaderboard_rois else None,
        "latest_current_price": latest.get("current_price"),
        "latest_wallet_entry_price": latest.get("wallet_entry_price"),
        "latest_edge_pct": latest.get("edge_pct"),
        "latest_score": latest.get("score"),
        "latest_stake_pct": latest.get("stake_pct"),
        "latest_size_ratio": latest.get("size_ratio"),
        "latest_total_notional": latest.get("total_notional"),
        "latest_followers": latest.get("followers"),
        "latest_consensus_score": latest.get("consensus_score"),
        "latest_sequence_role": latest.get("sequence_role"),
        "latest_ts": latest.get("ts"),
    }

    return snapshot


def score_market_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        return None

    score = 0
    reasons = []

    total_notional = parse_float(snapshot.get("total_notional"), 0.0)
    max_size_ratio = parse_float(snapshot.get("max_size_ratio"), 0.0)
    avg_size_ratio = parse_float(snapshot.get("avg_size_ratio"), 0.0)
    unique_wallet_count = parse_int(snapshot.get("unique_wallet_count"), 0)
    avg_leaderboard_roi = snapshot.get("avg_leaderboard_roi")
    latest_edge_pct = parse_float(snapshot.get("latest_edge_pct"), 0.0)
    minutes_to_start = snapshot.get("minutes_to_start")
    max_followers = parse_int(snapshot.get("max_followers"), 0)
    max_consensus_score = parse_int(snapshot.get("max_consensus_score"), 0)

    if total_notional >= 25000:
        score += 25
        reasons.append("very strong total notional")
    elif total_notional >= 10000:
        score += 18
        reasons.append("strong total notional")
    elif total_notional >= 5000:
        score += 12
        reasons.append("good total notional")
    elif total_notional >= 1000:
        score += 6
        reasons.append("acceptable total notional")

    if max_size_ratio >= 5:
        score += 20
        reasons.append("elite size ratio")
    elif max_size_ratio >= 3:
        score += 14
        reasons.append("strong size ratio")
    elif max_size_ratio >= 1.5:
        score += 8
        reasons.append("solid size ratio")

    if unique_wallet_count >= 2 and avg_size_ratio >= 2:
        score += 8
        reasons.append("multiple strong wallets")
    elif avg_size_ratio >= 1.2:
        score += 4
        reasons.append("average ratio above baseline")

    if unique_wallet_count >= 3:
        score += 15
        reasons.append("multi-wallet support")
    elif unique_wallet_count >= 2:
        score += 8
        reasons.append("two-wallet support")

    if max_followers >= 2:
        score += 6
        reasons.append("follower support")
    elif max_followers >= 1:
        score += 3
        reasons.append("some follower support")

    if max_consensus_score >= 70:
        score += 10
        reasons.append("strong consensus score")
    elif max_consensus_score >= 50:
        score += 5
        reasons.append("moderate consensus score")

    if avg_leaderboard_roi is not None:
        if avg_leaderboard_roi >= 0.05:
            score += 12
            reasons.append("strong wallet ROI")
        elif avg_leaderboard_roi >= 0.02:
            score += 6
            reasons.append("acceptable wallet ROI")
        elif avg_leaderboard_roi < 0.01:
            score -= 6
            reasons.append("weak wallet ROI")

    if latest_edge_pct >= 3:
        score += 10
        reasons.append("strong current edge")
    elif latest_edge_pct >= 1:
        score += 5
        reasons.append("positive current edge")
    elif latest_edge_pct < -1:
        score -= 8
        reasons.append("edge too far gone")

    if minutes_to_start is not None:
        if 0 <= minutes_to_start <= 10:
            score += 12
            reasons.append("very close to start")
        elif 10 < minutes_to_start <= 30:
            score += 10
            reasons.append("close to start")
        elif 30 < minutes_to_start <= 60:
            score += 6
            reasons.append("within one hour")
        elif 60 < minutes_to_start <= 180:
            score -= 4
            reasons.append("too early (1–3h)")
        elif 180 < minutes_to_start <= 360:
            score -= 10
            reasons.append("very early (3–6h)")
        elif minutes_to_start > 360:
            score -= 15
            reasons.append("extremely early (6h+)")
        elif minutes_to_start < 0:
            score -= 20
            reasons.append("market already started")

    recommendation = "PASS"
    stake_pct = 0

    if unique_wallet_count >= 3 and score >= 65:
        recommendation = "BET"
        stake_pct = 100
    elif unique_wallet_count >= 2 and score >= 45:
        recommendation = "LEAN"
        stake_pct = 50
    elif unique_wallet_count == 1 and score >= 55:
        recommendation = "LEAN"
        stake_pct = 25

    return {
        "model_score": score,
        "recommendation": recommendation,
        "recommended_stake_pct": stake_pct,
        "reasons": reasons,
    }

def classify_signal_stage(snapshot, model_output):
    if not isinstance(snapshot, dict) or not isinstance(model_output, dict):
        return "discard"

    unique_wallet_count = parse_int(snapshot.get("unique_wallet_count"), 0)
    total_notional = parse_float(snapshot.get("total_notional"), 0.0)
    max_size_ratio = parse_float(snapshot.get("max_size_ratio"), 0.0)
    max_followers = parse_int(snapshot.get("max_followers"), 0)
    max_consensus_score = parse_int(snapshot.get("max_consensus_score"), 0)
    minutes_to_start = parse_int(snapshot.get("minutes_to_start"), -999999)
    model_score = parse_int(model_output.get("model_score"), 0)

    if (
        unique_wallet_count >= 2
        or max_followers >= 1
        or max_consensus_score >= 50
    ):
        if model_score >= 30 and 0 <= minutes_to_start <= MODEL_MAX_MINUTES_TO_START:
            return "confirmed"

    if (
        unique_wallet_count == 1
        and total_notional >= 5000
        and max_size_ratio >= 3
        and model_score >= 30
        and 60 < minutes_to_start <= MODEL_MAX_MINUTES_TO_START
    ):
        return "early_watch"

    return "discard"

def build_recommendations(rows):
    grouped = group_rows_by_market_outcome(rows)
    recommendations = []

    for _, market_rows in grouped.items():
        snapshot = build_market_snapshot(market_rows)
        if not snapshot:
            continue

        raw_minutes = snapshot.get("minutes_to_start")

        try:
            minutes_to_start = int(raw_minutes)
        except Exception:
            continue

        if minutes_to_start < MODEL_MIN_MINUTES_TO_START:
            continue

        if minutes_to_start > MODEL_MAX_MINUTES_TO_START:
            continue

        snapshot["minutes_to_start"] = minutes_to_start

        model_output = score_market_snapshot(snapshot)
        if not model_output:
            continue

        signal_stage = classify_signal_stage(snapshot, model_output)
        if signal_stage == "discard":
            continue

        row = dict(snapshot)
        row.update(model_output)
        row["signal_stage"] = signal_stage
        recommendations.append(row)

    recommendations.sort(
        key=lambda x: (
            parse_int(x.get("model_score"), 0),
            parse_float(x.get("total_notional"), 0.0),
            parse_int(x.get("unique_wallet_count"), 0),
        ),
        reverse=True,
    )

    return recommendations


def save_recommendations_json(recommendations):
    early_watch_count = 0
    confirmed_count = 0

    for row in recommendations:
        stage = str(row.get("signal_stage", "") or "").strip().lower()
        if stage == "early_watch":
            early_watch_count += 1
        elif stage == "confirmed":
            confirmed_count += 1

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "recommendation_count": len(recommendations),
        "early_watch_count": early_watch_count,
        "confirmed_count": confirmed_count,
        "recommendations": recommendations,
    }

    try:
        with open(MARKET_MODEL_OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"[Market model output save error] {repr(e)}")


def print_recommendations(recommendations, limit=25):
    print("=" * 80)
    print("MARKET MODEL RECOMMENDATIONS")
    print("=" * 80)

    if not recommendations:
        print("No recommendations available.")
        return

    early_watch_count = sum(
        1 for row in recommendations
        if str(row.get("signal_stage", "") or "").lower() == "early_watch"
    )
    confirmed_count = sum(
        1 for row in recommendations
        if str(row.get("signal_stage", "") or "").lower() == "confirmed"
    )

    print(f"Early watch count: {early_watch_count}")
    print(f"Confirmed count: {confirmed_count}")

    for row in recommendations[:limit]:
        print("-" * 80)
        print(f"Market: {row.get('market', 'N/A')}")
        print(f"Outcome: {row.get('outcome', 'N/A')}")
        print(f"Recommendation: {row.get('recommendation', 'N/A')}")
        print(f"Signal stage: {row.get('signal_stage', 'N/A')}")
        print(f"Model score: {row.get('model_score', 'N/A')}")
        print(f"Recommended stake %: {row.get('recommended_stake_pct', 'N/A')}")
        print(f"Minutes to start: {row.get('minutes_to_start', 'N/A')}")
        print(f"Time bucket: {row.get('time_to_start_bucket', 'N/A')}")
        print(f"Unique wallets: {row.get('unique_wallet_count', 'N/A')}")
        print(f"Leader/Early/Follower: {row.get('leader_count', 0)}/{row.get('early_count', 0)}/{row.get('follower_count', 0)}")
        print(f"Total notional: {row.get('total_notional', 'N/A')}")
        print(f"Max size ratio: {row.get('max_size_ratio', 'N/A')}")
        print(f"Avg size ratio: {row.get('avg_size_ratio', 'N/A')}")
        print(f"Avg leaderboard ROI: {row.get('avg_leaderboard_roi', 'N/A')}")
        print(f"Latest edge %: {row.get('latest_edge_pct', 'N/A')}")
        print(f"Max followers: {row.get('max_followers', 'N/A')}")
        print(f"Max consensus score: {row.get('max_consensus_score', 'N/A')}")
        print(f"Reasons: {', '.join(row.get('reasons', []))}")


if __name__ == "__main__":
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_DIR in use: {DATA_DIR}")
    print(f"SIGNAL_METRICS_HISTORY_PATH: {SIGNAL_METRICS_HISTORY_PATH}")
    print(f"MARKET_MODEL_OUTPUT_PATH: {MARKET_MODEL_OUTPUT_PATH}")
    print(
        f"MODEL WINDOW: {MODEL_MIN_MINUTES_TO_START} to "
        f"{MODEL_MAX_MINUTES_TO_START} minutes before start"
    )

    signal_metrics_history = load_signal_metrics_history()

    print(f"Loaded signal metrics rows: {len(signal_metrics_history)}")

    recommendations = build_recommendations(signal_metrics_history)
    save_recommendations_json(recommendations)
    print_recommendations(recommendations)