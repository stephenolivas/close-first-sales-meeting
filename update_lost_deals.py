"""
update_lost_deals.py

Reassigns leads to John Kirk when:
  - First Sales Call Booked Date falls in the lookback window (Pacific time)
  - Lead Status = 💔 Lost

Lookback window:
  - Monday: Friday + Saturday + Sunday (covers the weekend)
  - Tue-Fri: yesterday only
  - Sat/Sun: yesterday only (the scheduler skips weekends; manual triggers
    still behave sensibly)

For each matched lead, sets:
  - Lead Owner (custom field) → John Kirk
  - Lane 2 Handraiser → "Prior Day Lost Deals"
  - Creates a Close task for John, due today

Idempotent: skips leads whose Lead Owner is already John Kirk.

Environment variables:
  CLOSE_API_KEY   (required)
  DRY_RUN         (optional, set to "true" to log without writing)
"""

import json
import os
import sys
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
DRY_RUN = os.environ.get("DRY_RUN", "false").lower() == "true"

BASE = "https://api.close.com/api/v1"
AUTH = (CLOSE_API_KEY, "")

JOHN_KIRK_USER_ID       = "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL"
FIRST_SALES_CALL_FIELD  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
LEAD_OWNER_FIELD        = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
LANE_2_HANDRAISER_FIELD = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
LOST_STATUS_LABEL       = "💔 Lost"
HANDRAISER_VALUE        = "Prior Day Lost Deals"

# Close's GET /lead/{id}/ returns custom fields under `custom`, keyed by
# display name (not cf_xxx ID). Writes still use cf_xxx in PUT payloads.
# If any of these field names get renamed in Close, this dict needs updating.
FIELD_DISPLAY_NAMES = {
    FIRST_SALES_CALL_FIELD: "First Sales Call Booked Date",
    LEAD_OWNER_FIELD:        "Lead Owner",
}

PACIFIC = ZoneInfo("America/Los_Angeles")


def get_lookback_dates():
    """Monday → Fri/Sat/Sun. Every other day → yesterday only."""
    today = datetime.now(PACIFIC).date()
    weekday = today.weekday()  # Mon=0 ... Sun=6
    offsets = (3, 2, 1) if weekday == 0 else (1,)
    return [(today - timedelta(days=n)).strftime("%Y-%m-%d") for n in offsets]


def get_lost_status_id():
    r = requests.get(f"{BASE}/status/lead/", auth=AUTH)
    r.raise_for_status()
    for s in r.json()["data"]:
        if s["label"] == LOST_STATUS_LABEL:
            return s["id"]
    sys.exit(f"Could not find lead status: {LOST_STATUS_LABEL}")


def get_custom_field(lead, field_id):
    """Look up a custom field's value. Primary path: lead.custom[<display name>].
    Falls back to cf_xxx-keyed paths in case the response shape ever changes."""
    custom = lead.get("custom") or {}
    name = FIELD_DISPLAY_NAMES.get(field_id)
    if name and name in custom:
        return custom[name]
    # Fallback paths
    if field_id in lead:
        return lead[field_id]
    if field_id in custom:
        return custom[field_id]
    return None


def format_call_date(iso_str):
    """Convert '2026-05-28' → 'Thu May 28'. Falls back gracefully."""
    try:
        d = datetime.strptime(iso_str, "%Y-%m-%d").date()
        return d.strftime("%a %b %d")
    except (ValueError, TypeError):
        return iso_str or "recently"


def date_clause(date_str):
    return {
        "type": "field_condition",
        "field": {
            "type": "custom_field",
            "custom_field_id": FIRST_SALES_CALL_FIELD,
        },
        "condition": {"type": "term", "values": [date_str]},
    }


def build_query(dates, lost_status_id):
    """Structured Close search query — verified via diagnose_query.py."""
    if len(dates) == 1:
        date_filter = date_clause(dates[0])
    else:
        date_filter = {
            "type": "or",
            "queries": [date_clause(d) for d in dates],
        }

    status_filter = {
        "type": "field_condition",
        "field": {
            "type": "regular_field",
            "object_type": "lead",
            "field_name": "status_id",
        },
        "condition": {"type": "term", "values": [lost_status_id]},
    }

    return {"type": "and", "queries": [date_filter, status_filter]}


def search_lead_ids(query):
    """Search returns lead IDs only — we fetch each in full separately."""
    ids, cursor = [], None
    while True:
        payload = {
            "query": query,
            "_fields": {"lead": ["id"]},
            "results_limit": 100,
        }
        if cursor:
            payload["cursor"] = cursor
        r = requests.post(f"{BASE}/data/search/", json=payload, auth=AUTH)
        r.raise_for_status()
        data = r.json()
        ids.extend(lead["id"] for lead in data.get("data", []))
        cursor = data.get("cursor")
        if not cursor:
            break
    return ids


def get_lead(lead_id):
    """Fetch a single lead with all fields, including custom."""
    r = requests.get(f"{BASE}/lead/{lead_id}/", auth=AUTH)
    r.raise_for_status()
    return r.json()


def get_current_owner_id(lead):
    """User-type custom fields can return ID string or {id, name}."""
    val = get_custom_field(lead, LEAD_OWNER_FIELD)
    if isinstance(val, dict):
        return val.get("id")
    return val


def update_lead(lead_id):
    if DRY_RUN:
        return
    payload = {
        LEAD_OWNER_FIELD: JOHN_KIRK_USER_ID,
        LANE_2_HANDRAISER_FIELD: HANDRAISER_VALUE,
    }
    r = requests.put(f"{BASE}/lead/{lead_id}/", json=payload, auth=AUTH)
    r.raise_for_status()


def create_task(lead_id, lead_name, sales_call_date_iso):
    if DRY_RUN:
        return
    today = datetime.now(PACIFIC).strftime("%Y-%m-%d")
    pretty_date = format_call_date(sales_call_date_iso)
    payload = {
        "_type": "lead",
        "lead_id": lead_id,
        "assigned_to": JOHN_KIRK_USER_ID,
        "date": today,
        "text": (
            f"Prior Day Lost Deal — {lead_name}. "
            f"First sales call was {pretty_date}. "
            f"Lead reassigned to you for review/outreach."
        ),
    }
    r = requests.post(f"{BASE}/task/", json=payload, auth=AUTH)
    r.raise_for_status()


def main():
    if DRY_RUN:
        print("=" * 60)
        print("  DRY RUN MODE — no writes will be made")
        print("=" * 60)

    dates = get_lookback_dates()
    today_name = datetime.now(PACIFIC).strftime("%A")
    print(f"Today is {today_name}. Lookback window: {dates}")

    lost_status_id = get_lost_status_id()
    query = build_query(dates, lost_status_id)

    if DRY_RUN:
        print("Query JSON:")
        print(json.dumps(query, indent=2))
        print()

    lead_ids = search_lead_ids(query)
    print(f"Found {len(lead_ids)} matching leads\n")

    processed = skipped = 0
    for lead_id in lead_ids:
        lead = get_lead(lead_id)

        name = lead.get("display_name", "(no name)")
        sales_call_iso = get_custom_field(lead, FIRST_SALES_CALL_FIELD)
        pretty_date = format_call_date(sales_call_iso)
        current_owner = get_current_owner_id(lead)

        if current_owner == JOHN_KIRK_USER_ID:
            print(f"  SKIP   {name} — Lead Owner is already John")
            skipped += 1
            continue

        if DRY_RUN:
            print(f"  WOULD  {name}  (current owner: {current_owner or 'none'})")
            print(f"           → Lead Owner: John Kirk")
            print(f"           → Lane 2 Handraiser: {HANDRAISER_VALUE}")
            print(f"           → task: 'First sales call was {pretty_date}'")
        else:
            update_lead(lead_id)
            create_task(lead_id, name, sales_call_iso)
            print(f"  DONE   {name}")
        processed += 1

    verb = "Would process" if DRY_RUN else "Processed"
    print(f"\n{verb}: {processed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
