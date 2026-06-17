import json
import os
import time
from decimal import Decimal, ROUND_DOWN

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
LIVE_ORDER_TENNIS_MAX_USD = Decimal(os.getenv("LIVE_ORDER_TENNIS_MAX_USD", "1"))
LIVE_ORDER_TENNIS_MAX_SIGNAL_AGE_SECONDS = int(os.getenv("LIVE_ORDER_TENNIS_MAX_SIGNAL_AGE_SECONDS", "60"))
LIVE_ORDER_TENNIS_MAX_FAVORABLE_DRIFT_CENTS = Decimal(os.getenv("LIVE_ORDER_TENNIS_MAX_FAVORABLE_DRIFT_CENTS", "3"))

LIVE_ORDER_MAX_CHASE_DRIFT_CENTS = Decimal(os.getenv("LIVE_ORDER_MAX_CHASE_DRIFT_CENTS", "2.5"))
LIVE_ORDER_HARD_MAX_CHASE_DRIFT_CENTS = Decimal(os.getenv("LIVE_ORDER_HARD_MAX_CHASE_DRIFT_CENTS", "5"))
LIVE_ORDER_MIN_EDGE_PERCENT = Decimal(os.getenv("LIVE_ORDER_MIN_EDGE_PERCENT", "1.0"))

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
    try:
        from polymarket_us import PolymarketUS
    except Exception as e:
        raise RuntimeError(f"Missing polymarket_us client module: {e}")

    if not POLYMARKET_KEY_ID:
        raise RuntimeError("Missing POLYMARKET_KEY_ID env var")

    if not POLYMARKET_SECRET_KEY:
        raise RuntimeError("Missing POLYMARKET_SECRET_KEY env var")

    return PolymarketUS(
        key_id=POLYMARKET_KEY_ID,
        secret_key=POLYMARKET_SECRET_KEY,
    )


_POLYMARKET_MARKETS_METHODS_LOGGED = False


def log_polymarket_markets_methods_once(client):
    global _POLYMARKET_MARKETS_METHODS_LOGGED

    if _POLYMARKET_MARKETS_METHODS_LOGGED:
        return

    _POLYMARKET_MARKETS_METHODS_LOGGED = True

    try:
        import inspect

        markets_obj = getattr(client, "markets", None)
        methods = [
            name
            for name in dir(markets_obj)
            if not name.startswith("_")
        ] if markets_obj is not None else []

        method_signatures = {}

        for method_name in methods:
            method = getattr(markets_obj, method_name, None)

            if callable(method):
                try:
                    method_signatures[method_name] = str(inspect.signature(method))
                except Exception as signature_error:
                    method_signatures[method_name] = f"signature_error:{signature_error}"

        print(
            "[POLYMARKET CLIENT MARKETS METHODS] "
            f"methods={methods} "
            f"signatures={method_signatures}",
            flush=True,
        )

        try:
            from polymarket_us.types.markets import MarketsListParams

            params_signature = None
            params_annotations = None
            params_attrs = None

            try:
                params_signature = str(inspect.signature(MarketsListParams))
            except Exception as signature_error:
                params_signature = f"signature_error:{signature_error}"

            try:
                params_annotations = getattr(MarketsListParams, "__annotations__", None)
            except Exception as annotations_error:
                params_annotations = f"annotations_error:{annotations_error}"

            try:
                params_attrs = [
                    name
                    for name in dir(MarketsListParams)
                    if not name.startswith("_")
                ]
            except Exception as attrs_error:
                params_attrs = f"attrs_error:{attrs_error}"

            print(
                "[POLYMARKET CLIENT MARKETS LIST PARAMS] "
                f"signature={params_signature} "
                f"annotations={params_annotations} "
                f"attrs={params_attrs}",
                flush=True,
            )

        except Exception as params_error:
            print(
                "[POLYMARKET CLIENT MARKETS LIST PARAMS FAILED] "
                f"error_type={type(params_error).__name__} "
                f"error={params_error}",
                flush=True,
            )

        try:
            markets_response = markets_obj.list()
            response_type = type(markets_response).__name__
            response_attrs = [
                name
                for name in dir(markets_response)
                if not name.startswith("_")
            ]

            response_keys = None
            market_rows = None

            if isinstance(markets_response, dict):
                response_keys = list(markets_response.keys())

                for key in ("markets", "data", "items", "results"):
                    if isinstance(markets_response.get(key), list):
                        market_rows = markets_response.get(key)
                        break

                if market_rows is None:
                    for value in markets_response.values():
                        if isinstance(value, list):
                            market_rows = value
                            break
            else:
                for attr in ("markets", "data", "items", "results"):
                    value = getattr(markets_response, attr, None)
                    if isinstance(value, list):
                        market_rows = value
                        break

            sample_markets = []

            if market_rows:
                for market in market_rows[:10]:
                    if isinstance(market, dict):
                        sample_markets.append({
                            "id": market.get("id"),
                            "slug": market.get("slug"),
                            "title": market.get("title") or market.get("question") or market.get("name"),
                            "status": market.get("status"),
                        })
                    else:
                        sample_markets.append({
                            "id": getattr(market, "id", None),
                            "slug": getattr(market, "slug", None),
                            "title": (
                                getattr(market, "title", None)
                                or getattr(market, "question", None)
                                or getattr(market, "name", None)
                            ),
                            "status": getattr(market, "status", None),
                        })

            print(
                "[POLYMARKET CLIENT MARKETS LIST SAMPLE] "
                f"response_type={response_type} "
                f"response_attrs={response_attrs} "
                f"response_keys={response_keys} "
                f"sample_markets={sample_markets}",
                flush=True,
            )

        except Exception as list_error:
            print(
                "[POLYMARKET CLIENT MARKETS LIST SAMPLE FAILED] "
                f"error_type={type(list_error).__name__} "
                f"error={list_error}",
                flush=True,
            )

    except Exception as e:
        print(
            "[POLYMARKET CLIENT MARKETS METHODS FAILED] "
            f"error_type={type(e).__name__} "
            f"error={e}",
            flush=True,
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


EXECUTION_TEAM_ALIASES = {
    "ari": ["ari", "arizona", "diamondbacks", "arizona diamondbacks"],
    "ath": ["ath", "oak", "athletics", "oakland athletics"],
    "atl": ["atl", "atlanta", "braves", "atlanta braves", "dream", "atlanta dream"],
    "bal": ["bal", "baltimore", "orioles", "baltimore orioles"],
    "bos": ["bos", "boston", "red sox", "boston red sox", "celtics", "boston celtics"],
    "chc": ["chc", "cubs", "chicago cubs"],
    "cin": ["cin", "cincinnati", "reds", "cincinnati reds"],
    "cle": ["cle", "cleveland", "guardians", "cleveland guardians"],
    "col": ["col", "colorado", "rockies", "colorado rockies"],
    "conn": ["conn", "connecticut", "sun", "connecticut sun"],
    "dal": ["dal", "dallas", "wings", "dallas wings", "mavericks", "dallas mavericks"],
    "det": ["det", "detroit", "tigers", "detroit tigers"],
    "gsv": ["gsv", "golden state", "valkyries", "golden state valkyries"],
    "hou": ["hou", "houston", "astros", "houston astros", "rockets", "houston rockets"],
    "ind": ["ind", "indiana", "fever", "indiana fever", "pacers", "indiana pacers"],
    "kc": ["kc", "kansas city", "royals", "kansas city royals"],
    "la": ["la", "los angeles", "sparks", "los angeles sparks", "dodgers", "los angeles dodgers"],
    "laa": ["laa", "angels", "los angeles angels"],
    "lad": ["lad", "dodgers", "los angeles dodgers"],
    "mia": ["mia", "miami", "marlins", "miami marlins", "heat", "miami heat"],
    "mil": ["mil", "milwaukee", "brewers", "milwaukee brewers", "bucks", "milwaukee bucks"],
    "min": ["min", "minnesota", "twins", "minnesota twins", "lynx", "minnesota lynx", "timberwolves", "minnesota timberwolves"],
    "nyk": ["nyk", "new york", "knicks", "new york knicks"],
    "nym": ["nym", "mets", "new york mets"],
    "nyy": ["nyy", "yankees", "new york yankees"],
    "phx": ["phx", "phoenix", "mercury", "phoenix mercury", "suns", "phoenix suns"],
    "pit": ["pit", "pittsburgh", "pirates", "pittsburgh pirates"],
    "por": ["por", "portland", "fire", "portland fire", "portlandfire", "trail blazers", "portland trail blazers"],
    "sa": ["sa", "sas", "san antonio", "spurs", "san antonio spurs"],
    "sd": ["sd", "san diego", "padres", "san diego padres"],
    "sea": ["sea", "seattle", "mariners", "seattle mariners", "storm", "seattle storm"],
    "sf": ["sf", "san francisco", "giants", "san francisco giants"],
    "stl": ["stl", "st louis", "st. louis", "cardinals", "st louis cardinals", "st. louis cardinals"],
    "tb": ["tb", "tampa bay", "rays", "tampa bay rays"],
    "tex": ["tex", "texas", "rangers", "texas rangers"],
    "tor": ["tor", "toronto", "blue jays", "toronto blue jays", "tempo", "toronto tempo"],
    "wsh": ["wsh", "washington", "nationals", "washington nationals", "mystics", "washington mystics"],
}


def normalize_execution_text(value):
    return (
        str(value or "")
        .strip()
        .lower()
        .replace(".", "")
        .replace("-", " ")
        .replace("_", " ")
    )


def execution_slug_parts_for_side_mapping(market_slug):
    slug = str(market_slug or "").strip().lower()

    for prefix in ("aec-", "tsc-", "atc-", "asc-"):
        if slug.startswith(prefix):
            slug = slug[len(prefix):]
            break

    parts = slug.split("-")

    if len(parts) < 6:
        return None, None

    league = parts[0]

    date_index = None
    for i in range(1, len(parts) - 2):
        if (
            len(parts[i]) == 4
            and parts[i].isdigit()
            and len(parts[i + 1]) == 2
            and parts[i + 1].isdigit()
            and len(parts[i + 2]) == 2
            and parts[i + 2].isdigit()
        ):
            date_index = i
            break

    if date_index is None or date_index < 3:
        return None, None

    participant_parts = parts[1:date_index]

    if len(participant_parts) != 2:
        return None, None

    return league, participant_parts


def outcome_matches_slug_side(outcome_clean, side_token):
    side_token_clean = normalize_execution_text(side_token)

    possible_values = set(EXECUTION_TEAM_ALIASES.get(side_token_clean, []))
    possible_values.add(side_token_clean)

    for value in possible_values:
        value_clean = normalize_execution_text(value)

        if not value_clean:
            continue

        if outcome_clean == value_clean:
            return True

        if value_clean in outcome_clean.split():
            return True

        if value_clean in outcome_clean and len(value_clean) >= 4:
            return True

        if side_token_clean in outcome_clean and len(side_token_clean) >= 4:
            return True

    return False


def infer_order_intent_from_slug(outcome, market_slug):
    outcome_clean = normalize_execution_text(outcome)
    league, participant_parts = execution_slug_parts_for_side_mapping(market_slug)

    if not league or not participant_parts:
        return None

    side_a = participant_parts[0]
    side_b = participant_parts[1]

    side_a_match = outcome_matches_slug_side(outcome_clean, side_a)
    side_b_match = outcome_matches_slug_side(outcome_clean, side_b)

    if side_a_match and not side_b_match:
        return "ORDER_INTENT_BUY_LONG"

    if side_b_match and not side_a_match:
        return "ORDER_INTENT_BUY_SHORT"

    return None


def map_outcome_to_order_intent(outcome, market_slug=None):
    outcome_clean = normalize_execution_text(outcome)

    if outcome_clean in {"yes", "long", "over"}:
        return "ORDER_INTENT_BUY_LONG"

    if outcome_clean in {"no", "short", "under"}:
        return "ORDER_INTENT_BUY_SHORT"

    if market_slug:
        inferred_intent = infer_order_intent_from_slug(outcome, market_slug)

        if inferred_intent:
            print(
                "[ORDER INTENT INFERRED FROM SLUG] "
                f"market={market_slug} "
                f"outcome={outcome_clean} "
                f"intent={inferred_intent}",
                flush=True,
            )

            return inferred_intent

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
                normalize_execution_text(value)
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
    converted = converted.replace("-sas-", "-sa-")
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
        return converted

    if converted.startswith(soccer_league_prefixes):
        return "atc-" + converted

    if converted.startswith(tennis_league_prefixes):
        return converted

    if converted.startswith(moneyline_league_prefixes):
        return converted

    return converted

def build_execution_slug_candidates(market_slug):
    slug = str(market_slug or "").strip().lower()


    if not slug:
        return []




    candidates = []




    def add_candidate(candidate):
        candidate = str(candidate or "").strip().lower()
        if candidate and candidate not in candidates:
            candidates.append(candidate)




    converted_slug = convert_feed_slug_to_us_slug(slug)
    add_candidate(converted_slug)




    tennis_raw_slug = slug
    for prefix in ("aec-",):
        if tennis_raw_slug.startswith(prefix):
            tennis_raw_slug = tennis_raw_slug[len(prefix):]
            break


    if tennis_raw_slug.startswith(("atp-", "wta-", "j100-", "j1100-", "j2100-")):
        add_candidate(tennis_raw_slug)
        add_candidate("aec-" + tennis_raw_slug)
        return candidates




    parts = slug.split("-")
    if len(parts) < 6:
        return candidates




    league = parts[0]




    date_index = None
    for i in range(1, len(parts) - 2):
        if (
            len(parts[i]) == 4
            and parts[i].isdigit()
            and len(parts[i + 1]) == 2
            and parts[i + 1].isdigit()
            and len(parts[i + 2]) == 2
            and parts[i + 2].isdigit()
        ):
            date_index = i
            break




    if date_index is None:
        return candidates




    if date_index < 3:
        return candidates




    if league not in {"nba", "mlb", "wnba"}:
        return candidates




    team_parts = parts[1:date_index]
    date_parts = parts[date_index:date_index + 3]
    suffix_parts = parts[date_index + 3:]




    if len(team_parts) != 2:
        return candidates




    team_a = team_parts[0]
    team_b = team_parts[1]




    reversed_feed_slug_parts = [league, team_b, team_a] + date_parts + suffix_parts
    reversed_feed_slug = "-".join(reversed_feed_slug_parts)




    add_candidate(convert_feed_slug_to_us_slug(reversed_feed_slug))




    return candidates

def is_supported_execution_market(market_slug):
    slug = str(market_slug or "").strip().lower()

    if not slug:
        return False, "missing_slug"

    supported_league_prefixes = (
        "nba-",
        "mlb-",
        "wnba-",
        "atp-",
        "wta-",
        "j100-",
        "j1100-",
        "j2100-",
    )

    if not slug.startswith(supported_league_prefixes):
        return False, "unsupported_league_or_prefix"

    tennis_prefixes = (
        "atp-",
        "wta-",
        "j100-",
        "j1100-",
        "j2100-",
    )

    if slug.startswith(tennis_prefixes) and "-total-" in slug:
        return False, "unsupported_tennis_total"

    unsupported_markers = [
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
        "nba-",
        "mlb-",
        "wnba-",
        "aec-nba-",
        "aec-mlb-",
        "aec-wnba-",
        "tsc-nba-",
        "tsc-mlb-",
        "tsc-wnba-",
        "atp-",
        "wta-",
        "j100-",
        "j1100-",
        "j2100-",
        "aec-atp-",
        "aec-wta-",
        "aec-j100-",
        "aec-j1100-",
        "aec-j2100-",
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

    try:
        intent = map_outcome_to_order_intent(outcome, resolved_market_slug)
    except Exception as e:
        print(
            "[ORDER INTENT MAP FAILED] "
            f"input_market={market_slug} "
            f"resolved_market={resolved_market_slug} "
            f"outcome={str(outcome).strip().lower()} "
            f"price={price} "
            f"quantity={quantity} "
            f"error_type={type(e).__name__} "
            f"error={e}",
            flush=True,
        )
        raise

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

    market_slug = str(signal_context.get("market_slug") or "").strip().lower()
    resolved_market_slug = convert_feed_slug_to_us_slug(market_slug)
    is_tennis_market = resolved_market_slug.startswith((
        "atp-",
        "wta-",
        "j100-",
        "j1100-",
        "j2100-",
        "aec-atp-",
        "aec-wta-",
        "aec-j100-",
        "aec-j1100-",
        "aec-j2100-",
    ))

    if price_decimal < LIVE_ORDER_MIN_PRICE:
        return False, f"price_below_min:{price_decimal}"

    if price_decimal > LIVE_ORDER_MAX_PRICE:
        return False, f"price_above_max:{price_decimal}"

    edge_percent = signal_context.get("edge_percent")

    trusted_no_edge_auto_bet_allowed = bool(
        signal_context.get("trusted_no_edge_auto_bet_allowed", False)
    )

    sharp_entry_proxy_allowed = bool(
        signal_context.get("sharp_entry_proxy_allowed", False)
    )

    if edge_percent is None and not trusted_no_edge_auto_bet_allowed and not sharp_entry_proxy_allowed:
        return False, "missing_edge_percent"

    if edge_percent is not None:
        try:
            edge_percent_decimal = Decimal(str(edge_percent))
        except Exception:
            return False, f"invalid_edge_percent:{edge_percent}"

        if edge_percent_decimal < LIVE_ORDER_MIN_EDGE_PERCENT and not sharp_entry_proxy_allowed:
            return False, f"edge_below_min:{edge_percent_decimal}%"

    signal_age_seconds = signal_context.get("since_last_buy_s")
    max_signal_age_seconds = LIVE_ORDER_TENNIS_MAX_SIGNAL_AGE_SECONDS if is_tennis_market else LIVE_ORDER_MAX_SIGNAL_AGE_SECONDS

    if signal_age_seconds is not None:
        try:
            signal_age_seconds = int(float(signal_age_seconds))

            if signal_age_seconds > max_signal_age_seconds:
                return False, f"signal_too_old:{signal_age_seconds}s,max={max_signal_age_seconds}s"

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

    if is_tennis_market and chase_drift_cents < (LIVE_ORDER_TENNIS_MAX_FAVORABLE_DRIFT_CENTS * Decimal("-1")):
        return False, f"tennis_favorable_drift_too_large:{chase_drift_cents:.2f}c"

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
    log_polymarket_markets_methods_once(client)
    signal_context = signal_context or {}


    effective_max_order_usd = max_order_usd


    if ENABLE_REAL_MONEY_ORDERS:
        initial_resolved_market_slug = convert_feed_slug_to_us_slug(market_slug)
        is_tennis_market = initial_resolved_market_slug.startswith((
            "atp-",
            "wta-",
            "j100-",
            "j1100-",
            "j2100-",
            "aec-atp-",
            "aec-wta-",
            "aec-j100-",
            "aec-j1100-",
            "aec-j2100-",
        ))


        if effective_max_order_usd is None:
            effective_max_order_usd = LIVE_ORDER_MAX_USD
        else:
            effective_max_order_usd = min(
                Decimal(str(effective_max_order_usd)),
                LIVE_ORDER_MAX_USD,
            )


        if is_tennis_market:
            effective_max_order_usd = min(
                Decimal(str(effective_max_order_usd)),
                LIVE_ORDER_TENNIS_MAX_USD,
            )


    candidate_market_slugs = build_execution_slug_candidates(market_slug)


    if not candidate_market_slugs:
        candidate_market_slugs = [convert_feed_slug_to_us_slug(market_slug)]


    payload = None
    request_payload = None
    preview = None
    preview_error = None
    resolved_market_slug_used = None
    preview_debug_failures = []


    for candidate_market_slug in candidate_market_slugs:
        candidate_payload = None
        candidate_request_payload = None

        try:
            candidate_payload = build_order_payload(
                market_slug=candidate_market_slug,
                outcome=outcome,
                price=price,
                max_order_usd=effective_max_order_usd,
            )


            candidate_request_payload = {
                "request": candidate_payload,
            }


            candidate_preview = client.orders.preview(candidate_request_payload)


            payload = candidate_payload
            request_payload = candidate_request_payload
            preview = candidate_preview
            resolved_market_slug_used = candidate_payload.get("marketSlug") or candidate_market_slug
            preview_error = None
            break


        except Exception as e:
            preview_error = e

            preview_debug_failure = {
                "candidate_market_slug": candidate_market_slug,
                "outcome": outcome,
                "price": str(price),
                "max_order_usd": str(effective_max_order_usd),
                "payload": candidate_payload,
                "error_type": type(e).__name__,
                "error": str(e),
            }

            preview_debug_failures.append(preview_debug_failure)

            print(
                "[ORDER PREVIEW CANDIDATE FAILED] "
                f"market={market_slug} "
                f"candidate={candidate_market_slug} "
                f"outcome={str(outcome).strip().lower()} "
                f"price={price} "
                f"max_order_usd={effective_max_order_usd} "
                f"error_type={type(e).__name__} "
                f"error={e} "
                f"payload={candidate_payload}",
                flush=True,
            )

            continue


    if preview is None:
        print(
            "[ORDER PREVIEW ALL CANDIDATES FAILED] "
            f"market={market_slug} "
            f"outcome={str(outcome).strip().lower()} "
            f"price={price} "
            f"candidates={candidate_market_slugs} "
            f"failures={preview_debug_failures}",
            flush=True,
        )

        raise preview_error or RuntimeError(
            f"Order preview failed for all candidate slugs: {candidate_market_slugs}"
        )


    safety_context = dict(signal_context)
    safety_context["market_slug"] = resolved_market_slug_used or market_slug


    live_safe, live_safety_reason = validate_live_order_safety(
        price=payload["price"]["value"],
        signal_context=safety_context,
    )


    live_whitelisted, live_whitelist_reason = is_live_order_whitelisted_market(
        resolved_market_slug_used or market_slug
    )


    if not live_whitelisted:
        live_safe = False
        live_safety_reason = live_whitelist_reason


    common_response = {
        "real_money_orders_enabled": ENABLE_REAL_MONEY_ORDERS,
        "live_order_create_confirmation": LIVE_ORDER_CREATE_CONFIRMATION,
        "live_order_max_usd": str(LIVE_ORDER_MAX_USD),
        "live_safe": live_safe,
        "live_safety_reason": live_safety_reason,
        "live_order_market_whitelisted": live_whitelisted,
        "live_order_market_whitelist_reason": live_whitelist_reason,
        "resolved_market_slug_used": resolved_market_slug_used,
        "candidate_market_slugs": candidate_market_slugs,
        "payload": payload,
        "request_payload": request_payload,
        "preview": preview,
    }


    if not ENABLE_REAL_MONEY_ORDERS:
        return {
            **common_response,
            "mode": "PREVIEW_ONLY_REAL_ORDER_DISABLED",
        }


    if not LIVE_ORDER_CREATE_CONFIRMATION:
        return {
            **common_response,
            "mode": "LIVE_ORDER_BLOCKED_CONFIRMATION_MISSING",
            "live_safety_reason": "missing_live_order_create_confirmation",
        }


    if not live_safe:
        return {
            **common_response,
            "mode": "LIVE_ORDER_BLOCKED_SAFETY_CHECK",
        }


    order = client.orders.create(request_payload)


    return {
        **common_response,
        "mode": "LIVE_ORDER_PLACED",
        "order": order,
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