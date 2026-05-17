import os
from decimal import Decimal, ROUND_DOWN

from polymarket_us import PolymarketUS


ENABLE_REAL_MONEY_ORDERS = os.getenv("ENABLE_REAL_MONEY_ORDERS", "false").lower() == "true"
POLYMARKET_KEY_ID = os.getenv("POLYMARKET_KEY_ID")
POLYMARKET_SECRET_KEY = os.getenv("POLYMARKET_SECRET_KEY")

DEFAULT_MAX_ORDER_USD = Decimal(os.getenv("MAX_ORDER_USD", "25"))
DEFAULT_ORDER_TYPE = os.getenv("POLYMARKET_ORDER_TYPE", "ORDER_TYPE_LIMIT")
DEFAULT_TIME_IN_FORCE = os.getenv("POLYMARKET_TIME_IN_FORCE", "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL")

LIVE_ORDER_CREATE_CONFIRMATION = os.getenv("LIVE_ORDER_CREATE_CONFIRMATION", "") == "I_UNDERSTAND_THIS_PLACES_REAL_BETS"
LIVE_ORDER_MIN_EDGE_PERCENT = Decimal(os.getenv("LIVE_ORDER_MIN_EDGE_PERCENT", "3"))
LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS = int(os.getenv("LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS", "120"))
LIVE_ORDER_MIN_PRICE = Decimal(os.getenv("LIVE_ORDER_MIN_PRICE", "0.05"))
LIVE_ORDER_MAX_PRICE = Decimal(os.getenv("LIVE_ORDER_MAX_PRICE", "0.95"))



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

def convert_feed_slug_to_us_slug(market_slug):
    slug = str(market_slug or "").strip()

    if not slug:
        return slug

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
        return "aec-" + converted

    if converted.startswith(moneyline_league_prefixes):
        return "aec-" + converted

    return converted

def is_supported_execution_market(market_slug):
    slug = str(market_slug or "").strip().lower()

    if not slug:
        return False, "missing_slug"

    unsupported_markers = [
        "-spread-",
        "-player-",
        "-props-",
    ]

    for marker in unsupported_markers:
        if marker in slug:
            return False, f"unsupported_market_type:{marker}"

    return True, "attempt_once_preview_candidate"

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

    edge_percent = signal_context.get("edge_percent")

    if edge_percent is None:
        return False, "missing_edge_percent"

    try:
        edge_decimal = Decimal(str(edge_percent))

        if edge_decimal < LIVE_ORDER_MIN_EDGE_PERCENT:
            return False, f"edge_below_min:{edge_decimal}%"

    except Exception:
        return False, f"invalid_edge:{edge_percent}"

    return True, "passed_live_order_safety"


def execute_order_safely(
    market_slug,
    outcome,
    price,
    max_order_usd=None,
    signal_context=None,
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

    live_safe, live_safety_reason = validate_live_order_safety(
        price=payload["price"]["value"],
        signal_context=signal_context,
    )

    if not ENABLE_REAL_MONEY_ORDERS:
        return {
            "mode": "PREVIEW_ONLY_REAL_ORDER_DISABLED",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_safe": live_safe,
            "live_safety_reason": live_safety_reason,
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    if not LIVE_ORDER_CREATE_CONFIRMATION:
        return {
            "mode": "LIVE_ORDER_BLOCKED_CONFIRMATION_MISSING",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_safe": live_safe,
            "live_safety_reason": "missing_live_order_create_confirmation",
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    if not live_safe:
        return {
            "mode": "LIVE_ORDER_BLOCKED_SAFETY_CHECK",
            "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
            "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
            "live_safe": live_safe,
            "live_safety_reason": live_safety_reason,
            "payload": payload,
            "request_payload": request_payload,
            "preview": preview,
        }

    order = client.orders.create(request_payload)

    return {
        "mode": "LIVE_ORDER_PLACED",
        "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
        "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
        "live_safe": live_safe,
        "live_safety_reason": live_safety_reason,
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