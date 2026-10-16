import os
import csv
import json
from datetime import datetime, timezone
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if os.path.isdir("/data"):
    DATA_DIR = "/data"
else:
    DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

INSTANCE_LABEL = str(
    os.environ.get("BOT_INSTANCE_LABEL")
    or ("RAILWAY" if os.path.isdir("/data") else "LOCAL")
).strip()
if not INSTANCE_LABEL:
    INSTANCE_LABEL = "UNKNOWN"

ALLOW_NON_RAILWAY_TRACKED_BET_IO = str(
    os.environ.get("ALLOW_NON_RAILWAY_TRACKED_BET_IO", "") or ""
).strip().lower() in {"1", "true", "yes"}

INPUT_FILE = os.environ.get("EXPORT_INPUT_FILE") or os.path.join(DATA_DIR, "tracked_bets.json")
SHADOW_MODE = str(os.environ.get("EXPORT_SHADOW_MODE", "") or "").strip() == "1"
CLV_TRACKER_FILE = os.path.join(DATA_DIR, "clv_tracker.json")
SNAPSHOT_CLV_COLUMN = "AD"
OUTPUT_FILE = "shadow_bets_export.csv" if SHADOW_MODE else "tracked_bets_export.csv"
SERVICE_ACCOUNT_FILE = "service_account.json"
SERVICE_ACCOUNT_JSON_ENV = "GOOGLE_SERVICE_ACCOUNT_JSON"
SPREADSHEET_ID = "11JsBWkSWN5RTEnaEQx9uUJzEjNt9aeIU43gPoxfkYLY"
SHEET_NAME = os.environ.get("EXPORT_SHEET_NAME") or "Sheet1"
LAST_COL = "V" if SHADOW_MODE else "U"


RAW_HEADERS = [
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
]
if SHADOW_MODE:
    RAW_HEADERS = RAW_HEADERS + ["Shadow Reason"]


def load_tracked_bets():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError("tracked_bets.json must contain a top-level JSON object.")

    return data


def load_clv_tracker():
    try:
        with open(CLV_TRACKER_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def get_snapshot_clv_cents(bet, clv_tracker):
    """Frozen 5-minute snapshot CLV in cents (positive = price rose after the alert)."""
    slug = str(bet.get("slug", "") or "").strip()
    outcome = str(bet.get("outcome", "") or "").strip()
    wallet = str(bet.get("wallet", "") or "").strip().lower()
    if not slug or not outcome or not wallet:
        return None

    entry = clv_tracker.get(f"{slug}||{outcome}||{wallet}")
    if not isinstance(entry, dict):
        return None

    frozen = entry.get("snapshot_clv_frozen")
    if frozen is None:
        return None

    try:
        return round(float(frozen) * 100, 2)
    except (TypeError, ValueError):
        return None


def normalize_date(value):
    if value in (None, "", 0):
        return ""

    try:
        ts = int(float(value))
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%-m/%-d/%Y")
    except Exception:
        return str(value)


def dedupe_bets(bets):
    seen = {}

    for bet in bets:
        key = (
            str(bet.get("slug", "") or ""),
            str(bet.get("outcome", "") or ""),
            str(bet.get("wallet", "") or ""),
            str(bet.get("alert_ts", "") or ""),
        )

        if key not in seen:
            seen[key] = bet

    return list(seen.values())


def to_row(bet):
    row = _to_row_base(bet)
    if SHADOW_MODE:
        row["Shadow Reason"] = bet.get("shadow_reason", "")
    return row


def _to_row_base(bet):
    return {
        "Date": normalize_date(bet.get("alert_ts")),
        "Market": bet.get("market", ""),
        "Bet": bet.get("outcome", ""),
        "Market Phase": bet.get("market_phase", ""),
        "Alert Price": bet.get("current_price_at_alert", bet.get("current_price", "")),
        "Fair Price": bet.get("fair_price_at_alert", ""),
        "Edge %": bet.get("edge_pct_at_alert", bet.get("edge_pct", "")),
        "Movement (c)": bet.get(
            "market_movement_cents_at_alert",
            bet.get("market_movement_cents", "")
        ),
        "Consensus Score": bet.get("consensus_score", ""),
        "Consensus Type": bet.get("consensus_type", ""),
        "Score": bet.get("score", ""),
        "Sequence Role": bet.get("sequence_role", ""),
        "Size Ratio": bet.get("size_ratio", ""),
        "Stake %": bet.get("stake_pct", ""),
        "Buy Count": bet.get("buy_count", ""),
        "Total Size": bet.get("total_size", ""),
        "Wallet": bet.get("wallet", ""),
        "Instant CLV (c)": bet.get(
            "instant_clv_cents_at_alert",
            bet.get("instant_clv_cents", "")
        ),
        "Resolved": bet.get("resolved", ""),
        "Result": bet.get("result", ""),
        "Winning Outcome": bet.get("winning_outcome", ""),
    }


def row_to_values(row_dict):
    return [row_dict.get(header, "") for header in RAW_HEADERS]


def normalize_sheet_key_value(value):
    return " ".join(str(value or "").strip().lower().split())


def make_sheet_row_key_from_dict(row_dict):
    return "||".join([
        normalize_sheet_key_value(row_dict.get("Date", "")),
        normalize_sheet_key_value(row_dict.get("Market", "")),
        normalize_sheet_key_value(row_dict.get("Bet", "")),
        normalize_sheet_key_value(row_dict.get("Market Phase", "")),
        normalize_sheet_key_value(row_dict.get("Wallet", "")),
    ])


def make_sheet_row_key_from_list(row_values, header_index):
    def get_value(header_name):
        idx = header_index.get(header_name)
        if idx is None or idx >= len(row_values):
            return ""
        return row_values[idx]

    return "||".join([
        normalize_sheet_key_value(get_value("Date")),
        normalize_sheet_key_value(get_value("Market")),
        normalize_sheet_key_value(get_value("Bet")),
        normalize_sheet_key_value(get_value("Market Phase")),
        normalize_sheet_key_value(get_value("Wallet")),
    ])

def load_google_sheets_credentials(scopes):
    raw_service_account_json = str(
        os.environ.get(SERVICE_ACCOUNT_JSON_ENV, "") or ""
    ).strip()

    if raw_service_account_json:
        return Credentials.from_service_account_info(
            json.loads(raw_service_account_json),
            scopes=scopes,
        )

    return Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=scopes,
    )


def push_to_google_sheets(rows, snapshot_clv_by_row_key=None):
    snapshot_clv_by_row_key = snapshot_clv_by_row_key or {}
    print("=== GOOGLE SHEETS PUSH START ===")
    print(f"Spreadsheet ID: {SPREADSHEET_ID}")
    print(f"Sheet name: {SHEET_NAME}")
    print(f"Row count being sent: {len(rows)}")

    if not rows:
        print("No rows to send")
        return

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = load_google_sheets_credentials(scopes)

    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    row_dicts = rows
    row_values = [row_to_values(row_dict) for row_dict in row_dicts]

    # Ensure raw-data headers exist in row 1
    sheet.values().update(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A1:{LAST_COL}1",
        valueInputOption="RAW",
        body={"values": [RAW_HEADERS]},
    ).execute()

    # Pull existing raw rows from A:U
    existing_response = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=f"{SHEET_NAME}!A:{LAST_COL}",
    ).execute()

    existing_values = existing_response.get("values", [])
    existing_rows = existing_values[1:] if len(existing_values) > 1 else []
    header_index = {header: idx for idx, header in enumerate(RAW_HEADERS)}

    existing_key_to_row_number = {}

    for sheet_row_number, existing_row in enumerate(existing_rows, start=2):
        existing_key = make_sheet_row_key_from_list(existing_row, header_index)
        if existing_key:
            existing_key_to_row_number[existing_key] = sheet_row_number

    updates = []
    appends = []

    for row_dict, values in zip(row_dicts, row_values):
        row_key = make_sheet_row_key_from_dict(row_dict)

        if row_key in existing_key_to_row_number:
            sheet_row_number = existing_key_to_row_number[row_key]
            updates.append({
                "range": f"{SHEET_NAME}!A{sheet_row_number}:{LAST_COL}{sheet_row_number}",
                "values": [values],
            })
            snapshot_cents = snapshot_clv_by_row_key.get(row_key)
            if snapshot_cents is not None:
                updates.append({
                    "range": f"{SHEET_NAME}!{SNAPSHOT_CLV_COLUMN}{sheet_row_number}",
                    "values": [[snapshot_cents]],
                })
        else:
            appends.append(values)

    if updates:
        sheet.values().batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "valueInputOption": "RAW",
                "data": updates,
            },
        ).execute()
        print(f"Updated existing rows: {len(updates)}")
    else:
        print("Updated existing rows: 0")

    if appends:
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=f"{SHEET_NAME}!A:{LAST_COL}",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": appends},
        ).execute()
        print(f"Appended new rows: {len(appends)}")
    else:
        print("Appended new rows: 0")

    print("Google Sheets update complete.")
    print("=== GOOGLE SHEETS PUSH END ===")


def write_csv(rows):
    if not rows:
        print("No data to write.")
        return

    print(f"Writing CSV to: {OUTPUT_FILE}")
    print(f"Row count being written: {len(rows)}")

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print("CSV write complete.")


def main():
    print(f"RUNNING FILE: {os.path.abspath(__file__)}")
    print(f"WORKING DIRECTORY: {os.getcwd()}")
    print(f"INSTANCE_LABEL: {INSTANCE_LABEL}")

    if INSTANCE_LABEL != "RAILWAY" and not ALLOW_NON_RAILWAY_TRACKED_BET_IO:
        print(
            f"Refusing to export tracked bets on non-source instance: "
            f"{INSTANCE_LABEL}. "
            f"Set ALLOW_NON_RAILWAY_TRACKED_BET_IO=1 to override intentionally."
        )
        return

    tracked = load_tracked_bets()
    bets = [bet for bet in tracked.values() if isinstance(bet, dict)]
    deduped = dedupe_bets(bets)
    resolved_deduped = [bet for bet in deduped if bet.get("resolved") is True]
    rows = [to_row(bet) for bet in resolved_deduped]

    clv_tracker = {} if SHADOW_MODE else load_clv_tracker()
    snapshot_clv_by_row_key = {}
    for bet in resolved_deduped:
        snapshot_cents = get_snapshot_clv_cents(bet, clv_tracker)
        if snapshot_cents is None:
            continue
        snapshot_row_key = make_sheet_row_key_from_dict(to_row(bet))
        if snapshot_row_key:
            snapshot_clv_by_row_key[snapshot_row_key] = snapshot_cents
    print(f"Snapshot CLV values available: {len(snapshot_clv_by_row_key)}")
    if not SHADOW_MODE:
        _frozen_total = sum(
            1 for _r in clv_tracker.values()
            if isinstance(_r, dict) and _r.get("snapshot_clv_frozen") is not None
        )
        print(f"CLV tracker: {len(clv_tracker)} entries, {_frozen_total} frozen")

    row_key_map = {}
    for row in rows:
        row_key = make_sheet_row_key_from_dict(row)
        if row_key:
            row_key_map[row_key] = row
    rows = list(row_key_map.values())

    print(f"About to write {len(rows)} resolved rows to {OUTPUT_FILE}")
    write_csv(rows)
    print("Returned from write_csv(rows)")

    push_to_google_sheets(rows, snapshot_clv_by_row_key)
    print("Returned from push_to_google_sheets(rows)")

    resolved_count = sum(1 for b in deduped if b.get("resolved") is True)
    result_count = sum(1 for b in deduped if b.get("result") not in (None, ""))
    winning_outcome_count = sum(1 for b in deduped if b.get("winning_outcome") not in (None, ""))
    resolution_price_count = sum(1 for b in deduped if b.get("resolution_price") not in (None, ""))

    print(f"Raw rows: {len(bets)}")
    print(f"Deduped rows: {len(deduped)}")
    print(f"Resolved deduped rows exported: {len(resolved_deduped)}")
    print(f"Resolved populated: {resolved_count}")
    print(f"Result populated: {result_count}")
    print(f"Winning outcome populated: {winning_outcome_count}")
    print(f"Resolution Price populated: {resolution_price_count}")
    print(f"Exported to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
