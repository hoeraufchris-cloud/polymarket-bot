import json
import os
import time
from decimal import Decimal, ROUND_DOWN

from polymarket_us import PolymarketUS


ENABLE_REAL_MONEY_ORDERS = os.getenv("ENABLE_REAL_MONEY_ORDERS", "false").lower() == "true"
POLYMARKET_KEY_ID = os.getenv("POLYMARKET_KEY_ID")
POLYMARKET_SECRET_KEY = os.getenv("POLYMARKET_SECRET_KEY")

DEFAULT_MAX_ORDER_USD = Decimal(os.getenv("MAX_ORDER_USD", "25"))
DEFAULT_ORDER_TYPE = os.getenv("POLYMARKET_ORDER_TYPE", "ORDER_TYPE_LIMIT")
DEFAULT_TIME_IN_FORCE = os.getenv("POLYMARKET_TIME_IN_FORCE", "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL")

LIVE_ORDER_CREATE_CONFIRMATION = os.getenv("LIVE_ORDER_CREATE_CONFIRMATION", "") == "I_UNDERSTAND_THIS_PLACES_REAL_BETS"
LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS = int(os.getenv("LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS", "120"))
LIVE_ORDER_MIN_PRICE = Decimal(os.getenv("LIVE_ORDER_MIN_PRICE", "0.05"))
LIVE_ORDER_MAX_PRICE = Decimal(os.getenv("LIVE_ORDER_MAX_PRICE", "0.95"))
LIVE_ORDER_MAX_USD = Decimal(os.getenv("LIVE_ORDER_MAX_USD", "2"))

LIVE_ORDER_MAX_CHASE_DRIFT_CENTS = Decimal(os.getenv("LIVE_ORDER_MAX_CHASE_DRIFT_CENTS", "2.5"))
LIVE_ORDER_HARD_MAX_CHASE_DRIFT_CENTS = Decimal(os.getenv("LIVE_ORDER_HARD_MAX_CHASE_DRIFT_CENTS", "5"))

EXECUTION_LEDGER_PATH = os.getenv(
    "EXECUTION_LEDGER_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "execution_orders.json"),
)
EXECUTION_DEDUPE_TTL_SECONDS = int(os.getenv("EXECUTION_DEDUPE_TTL_SECONDS", "1800"))
EXECUTION_LEDGER_MAX_ROWS = int(os.getenv("EXECUTION_LEDGER_MAX_ROWS", "5000"))

EXECUTION_SLUG_ALIAS_PATH = os.getenv(
    "EXECUTION_SLUG_ALIAS_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "execution_slug_aliases.json"),
)



def get_polymarket_client():
    if not POLYMARKET_KEY_ID:
        raise RuntimeError("Missing POLYMARKET_KEY_ID env var")

    if not POLYMARKET_SECRET_KEY:
        raise RuntimeError("Missing POLYMARKET_SECRET_KEY env var")

    return PolymarketUS(
        key_id=POLYMARKET_KEY_ID,
        secret_key=POLYMARKET_SECRET_KEY,
    )


def normalize_price(price):
    price_decimal = Decimal(str(price))

    if price_decimal <= 0 or price_decimal >= 1:
        raise ValueError(f"Invalid Polymarket price: {price}")

    return str(price_decimal.quantize(Decimal("0.001"), rounding=ROUND_DOWN))


def calculate_quantity(max_order_usd, price):
    max_order_decimal = Decimal(str(max_order_usd))
    price_decimal = Decimal(str(price))

    if max_order_decimal <= 0:
        raise ValueError(f"Invalid max order amount: {max_order_usd}")

    quantity = max_order_decimal / price_decimal

    return int(quantity)


def map_outcome_to_order_intent(outcome, market_slug=None):
    outcome_clean = str(outcome).strip().lower()

    if outcome_clean in {"yes", "long", "over"}:
        return "ORDER_INTENT_BUY_LONG"

    if outcome_clean in {"no", "short", "under"}:
        return "ORDER_INTENT_BUY_SHORT"

    if market_slug:
        client = get_polymarket_client()
        market_response = client.markets.retrieve_by_slug(market_slug)
        market = market_response.get("market", market_response)

        for side in market.get("marketSides", []):
            side_values = [
                side.get("description"),
            ]

            team = side.get("team") or {}
            side_values.extend([
                team.get("name"),
                team.get("abbreviation"),
                team.get("alias"),
                team.get("safeName"),
                team.get("displayAbbreviation"),
            ])

            normalized_values = {
                str(value or "").strip().lower()
                for value in side_values
                if value
            }

            if outcome_clean in normalized_values:
                if side.get("long") is True:
                    return "ORDER_INTENT_BUY_LONG"

                if side.get("long") is False:
                    return "ORDER_INTENT_BUY_SHORT"

    raise ValueError(f"Unsupported outcome for automated order mapping: {outcome}")

def load_execution_slug_aliases():
    if not os.path.exists(EXECUTION_SLUG_ALIAS_PATH):
        return {}

    try:
        with open(EXECUTION_SLUG_ALIAS_PATH, "r") as f:
            aliases = json.load(f)

        if isinstance(aliases, dict):
            return aliases

        return {}

    except Exception:
        return {}


def convert_feed_slug_to_us_slug(market_slug):
    slug = str(market_slug or "").strip()

    if not slug:
        return slug

    slug_aliases = load_execution_slug_aliases()

    if slug in slug_aliases:
        return slug_aliases[slug]

    converted = slug.replace("nba-sas-", "nba-sa-")
    converted = converted.replace("mlb-oak-", "mlb-ath-")
    converted = converted.replace("-oak-", "-ath-")

    if converted.startswith(("aec-", "tsc-", "atc-", "asc-")):
        return converted

    total_league_prefixes = (
        "nba-",
        "mlb-",
        "wnba-",
        "mls-",
        "lal-",
        "epl-",
        "sea-",
        "scop-",
        "chi-",
        "j100-",
        "j1100-",
        "j2100-",
    )

    moneyline_league_prefixes = (
        "nba-",
        "mlb-",
        "wnba-",
        "atp-",
        "wta-",
    )

    soccer_league_prefixes = (
        "mls-",
        "lal-",
        "epl-",
        "sea-",
        "scop-",
        "chi-",
    )

    tennis_league_prefixes = (
        "atp-",
        "wta-",
        "j100-",
        "j1100-",
        "j2100-",
    )

    if "-total-" in converted and converted.startswith(total_league_prefixes):
        converted = converted.replace("-total-", "-")
        return "tsc-" + converted

    if converted.startswith(("nba-", "mlb-", "wnba-")):
        return "aec-" + converted

    if converted.startswith(soccer_league_prefixes):
        return "atc-" + converted

    if converted.startswith(tennis_league_prefixes):
        return converted

    if converted.startswith(moneyline_league_prefixes):
        return "aec-" + converted

    return converted


def is_supported_execution_market(market_slug):
    slug = str(market_slug or "").strip().lower()

    if not slug:
        return False, "missing_slug"

    supported_league_prefixes = (
        "nba-",
        "mlb-",
        "wnba-",
    )

    if not slug.startswith(supported_league_prefixes):
        return False, "unsupported_league_or_prefix"

    unsupported_markers = [
        "-spread-",
        "-player-",
        "-props-",
        "-btts",
        "-btbs",
        "-draw",
    ]

    for marker in unsupported_markers:
        if marker in slug:
            return False, f"unsupported_market_type:{marker}"

    return True, "supported_execution_candidate"


def is_live_order_whitelisted_market(market_slug):
    slug = str(market_slug or "").strip().lower()

    if not slug:
        return False, "missing_slug"

    resolved_slug = convert_feed_slug_to_us_slug(slug)

    supported_live_prefixes = (
        "aec-nba-",
        "aec-mlb-",
        "aec-wnba-",
        "tsc-nba-",
        "tsc-mlb-",
        "tsc-wnba-",
    )

    if not resolved_slug.startswith(supported_live_prefixes):
        return False, "live_order_unsupported_league_or_prefix"

    unsupported_live_markers = [
        "-spread-",
        "-player-",
        "-props-",
        "-btts",
        "-btbs",
        "-draw",
        "-1h-",
        "-1q-",
        "-2q-",
        "-3q-",
        "-4q-",
        "-f5-",
    ]

    for marker in unsupported_live_markers:
        if marker in resolved_slug:
            return False, f"live_order_unsupported_market_type:{marker}"

    return True, "passed_live_order_market_whitelist"

def make_execution_key(market_slug, outcome, price):
    resolved_market_slug = convert_feed_slug_to_us_slug(market_slug)
    normalized_price = normalize_price(price)
    outcome_clean = str(outcome or "").strip().lower()

    return f"{resolved_market_slug}||{outcome_clean}||{normalized_price}"


def load_execution_ledger():
    if not os.path.exists(EXECUTION_LEDGER_PATH):
        return []

    try:
        with open(EXECUTION_LEDGER_PATH, "r") as f:
            rows = json.load(f)

        if isinstance(rows, list):
            return rows

        return []

    except Exception:
        return []


def save_execution_ledger(rows):
    os.makedirs(os.path.dirname(EXECUTION_LEDGER_PATH), exist_ok=True)

    rows_to_save = rows[-EXECUTION_LEDGER_MAX_ROWS:]

    with open(EXECUTION_LEDGER_PATH, "w") as f:
        json.dump(rows_to_save, f, indent=2, default=str)


def get_recent_execution_record(market_slug, outcome, price):
    execution_key = make_execution_key(market_slug, outcome, price)
    now_ts = time.time()

    blocking_statuses = {
        "PREVIEW_OK",
        "LIVE_ORDER_PLACED",
        "LIVE_ORDER_BLOCKED_SAFETY_CHECK",
        "LIVE_ORDER_BLOCKED_CONFIRMATION_MISSING",
        "LIVE_ORDER_BLOCKED_REAL_MONEY_DISABLED",
    }

    non_blocking_statuses = {
        "PREVIEW_FAILED",
        "PREVIEW_SKIPPED",
    }

    for row in reversed(load_execution_ledger()):
        try:
            row_ts = float(row.get("timestamp", 0))
        except Exception:
            row_ts = 0

        if now_ts - row_ts > EXECUTION_DEDUPE_TTL_SECONDS:
            continue

        row_status = str(row.get("status") or "").upper()
        row_mode = str(row.get("mode") or "").upper()
        row_effective_status = row_status if row_status else row_mode

        if row_effective_status in non_blocking_statuses:
            continue

        if row_effective_status and row_effective_status not in blocking_statuses:
            continue

        if row.get("execution_key") == execution_key:
            return row

    return None


def record_execution_attempt(
    market_slug,
    outcome,
    price,
    mode,
    status,
    live_safe=None,
    live_safety_reason=None,
    payload=None,
    preview=None,
    order=None,
    error=None,
):
    resolved_market_slug = convert_feed_slug_to_us_slug(market_slug)
    execution_key = make_execution_key(market_slug, outcome, price)

    preview_order = {}

    if isinstance(preview, dict):
        preview_order = preview.get("order", {}) or {}

    row = {
        "timestamp": time.time(),
        "execution_key": execution_key,
        "feed_market_slug": market_slug,
        "resolved_market_slug": resolved_market_slug,
        "outcome": outcome,
        "price": str(price),
        "mode": mode,
        "status": status,
        "live_safe": live_safe,
        "live_safety_reason": live_safety_reason,
        "payload": payload,
        "preview_order_id": preview_order.get("id"),
        "preview_action": preview_order.get("action"),
        "preview_outcome_side": preview_order.get("outcomeSide"),
        "preview_quantity": preview_order.get("quantity"),
        "order": order,
        "error": str(error) if error else None,
    }

    rows = load_execution_ledger()
    rows.append(row)
    save_execution_ledger(rows)

    return row

def build_order_payload(
    market_slug,
    outcome,
    price,
    max_order_usd=None,
    time_in_force=None,
):
    normalized_price = normalize_price(price)
    order_usd = Decimal(str(max_order_usd)) if max_order_usd is not None else DEFAULT_MAX_ORDER_USD
    quantity = calculate_quantity(order_usd, normalized_price)
    resolved_market_slug = convert_feed_slug_to_us_slug(market_slug)
    intent = map_outcome_to_order_intent(outcome, resolved_market_slug)

    if quantity <= 0:
        raise ValueError(
            f"Order quantity calculated as 0. max_order_usd={order_usd}, price={normalized_price}"
        )

    return {
        "marketSlug": resolved_market_slug,
        "intent": intent,
        "type": DEFAULT_ORDER_TYPE,
        "price": {
            "value": normalized_price,
            "currency": "USD",
        },
        "quantity": quantity,
        "tif": time_in_force or DEFAULT_TIME_IN_FORCE,
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        "synchronousExecution": True,
        "maxBlockTime": "5",
    }


def preview_order(
    market_slug,
    outcome,
    price,
    max_order_usd=None,
):
    client = get_polymarket_client()

    payload = build_order_payload(
        market_slug=market_slug,
        outcome=outcome,
        price=price,
        max_order_usd=max_order_usd,
    )

    request_payload = {
        "request": payload,
    }

    preview = client.orders.preview(request_payload)

    return {
        "mode": "PREVIEW_ONLY",
        "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
        "payload": payload,
        "request_payload": request_payload,
        "preview": preview,
    }


def validate_live_order_safety(price, signal_context=None):
    signal_context = signal_context or {}
    price_decimal = Decimal(str(price))

    if price_decimal < LIVE_ORDER_MIN_PRICE:
        return False, f"price_below_min:{price_decimal}"

    if price_decimal > LIVE_ORDER_MAX_PRICE:
        return False, f"price_above_max:{price_decimal}"

    signal_age_seconds = signal_context.get("since_last_buy_s")

    if signal_age_seconds is not None:
        try:
            signal_age_seconds = int(float(signal_age_seconds))

            if signal_age_seconds > LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS:
                return False, f"signal_too_old:{signal_age_seconds}s"

        except Exception:
            return False, f"invalid_signal_age:{signal_age_seconds}"

    wallet_entry_price = (
        signal_context.get("wallet_entry_price")
        if signal_context.get("wallet_entry_price") is not None
        else signal_context.get("avg_trade_price")
    )

    if wallet_entry_price is None:
        return False, "missing_wallet_entry_price"

    try:
        wallet_entry_decimal = Decimal(str(wallet_entry_price))
    except Exception:
        return False, f"invalid_wallet_entry_price:{wallet_entry_price}"

    if wallet_entry_decimal <= 0 or wallet_entry_decimal >= 1:
        return False, f"invalid_wallet_entry_price:{wallet_entry_decimal}"

    chase_drift_decimal = price_decimal - wallet_entry_decimal
    chase_drift_cents = chase_drift_decimal * Decimal("100")

    hard_max_drift_decimal = LIVE_ORDER_HARD_MAX_CHASE_DRIFT_CENTS / Decimal("100")
    max_drift_decimal = LIVE_ORDER_MAX_CHASE_DRIFT_CENTS / Decimal("100")

    if chase_drift_decimal >= hard_max_drift_decimal:
        return False, f"hard_chase_drift_too_high:{chase_drift_cents:.2f}c"

    if chase_drift_decimal > max_drift_decimal:
        return False, f"chase_drift_too_high:{chase_drift_cents:.2f}c"

    return True, f"passed_live_order_safety:drift={chase_drift_cents:.2f}c"


def execute_order_safely(
    market_slug,
    outcome,
    price,
    max_order_usd=None,
    signal_context=None,
):
    client = get_polymarket_client()

    effective_max_order_usd = max_order_usd

    if ENABLE_REAL_MONEY_ORDERS:
        if effective_max_order_usd is None:
            effective_max_order_usd = LIVE_ORDER_MAX_USD
        else:
            effective_max_order_usd = min(
                Decimal(str(effective_max_order_usd)),
                LIVE_ORDER_MAX_USD,
            )

    payload = build_order_payload(
        market_slug=market_slug,
        outcome=outcome,
        price=price,
        max_order_usd=effective_max_order_usd,
    )

    request_payload = {
        "request": payload,
    }

    preview = client.orders.preview(request_payload)

    live_safe, live_safety_reason = validate_live_order_safety(
        price=payload["price"]["value"],
        signal_context=signal_context,
    )

    live_whitelisted, live_whitelist_reason = is_live_order_whitelisted_market(market_slug)

    if not live_whitelisted:
        live_safe = False
        live_safety_reason = live_whitelist_reason

    if not ENABLE_REAL_MONEY_ORDERS:
        return {
            "mode": "PREVIEW_ONLY_REAL_ORDER_DISABLED",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_order_max_usd": str(LIVE_ORDER_MAX_USD),
            "live_safe": live_safe,
            "live_safety_reason": live_safety_reason,
            "live_order_market_whitelisted": live_whitelisted,
            "live_order_market_whitelist_reason": live_whitelist_reason,
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    if not LIVE_ORDER_CREATE_CONFIRMATION:
        return {
            "mode": "LIVE_ORDER_BLOCKED_CONFIRMATION_MISSING",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_order_max_usd": str(LIVE_ORDER_MAX_USD),
            "live_safe": live_safe,
            "live_safety_reason": "missing_live_order_create_confirmation",
            "live_order_market_whitelisted": live_whitelisted,
            "live_order_market_whitelist_reason": live_whitelist_reason,
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    if not live_safe:
        return {
            "mode": "LIVE_ORDER_BLOCKED_SAFETY_CHECK",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_order_max_usd": str(LIVE_ORDER_MAX_USD),
            "live_safe": live_safe,
            "live_safety_reason": live_safety_reason,
            "live_order_market_whitelisted": live_whitelisted,
            "live_order_market_whitelist_reason": live_whitelist_reason,
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    order = client.orders.create(request_payload)

    return {
        "mode": "LIVE_ORDER_PLACED",
        "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
        "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
        "live_order_max_usd": str(LIVE_ORDER_MAX_USD),
        "live_safe": live_safe,
        "live_safety_reason": live_safety_reason,
        "live_order_market_whitelisted": live_whitelisted,
        "live_order_market_whitelist_reason": live_whitelist_reason,
        "payload": payload,
        "request_payload": request_payload,
        "preview": preview,
        "order": order,
    }


def place_order(
    market_slug,
    outcome,
    price,
    max_order_usd=None,
):
    return execute_order_safely(
        market_slug=market_slug,
        outcome=outcome,
        price=price,
        max_order_usd=max_order_usd,
        signal_context=None,
    )