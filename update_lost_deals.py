"""
update_lost_deals.py

Reassigns leads to John Kirk when:
  - First Sales Call Booked Date falls in the lookback window (Pacific time)
  - Lead Status = 💔 Lost

Lookback window:
  - Monday: Friday + Saturday + Sunday (covers the weekend)
  - Tue-Fri: yesterday only
  - Sat/Sun: yesterday only (the scheduler skips weekends, but a manual
    trigger will still behave sensibly)

For each matched lead, sets:
  - lead_owner_id → John Kirk
  - Lane 2 Handraiser → "Prior Day Lost Deals"
  - Creates a Close task for John, due today

Idempotent: skips leads already owned by John.

Environment variables:
  CLOSE_API_KEY   (required)
  DRY_RUN         (optional, set to "true" to log without writing)
"""

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
LANE_2_HANDRAISER_FIELD = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
LOST_STATUS_LABEL       = "💔 Lost"
HANDRAISER_VALUE        = "Prior Day Lost Deals"

PACIFIC = ZoneInfo("America/Los_Angeles")


def get_lookback_dates():
    """
    Build the list of date strings to search.
    Monday → Fri/Sat/Sun. Every other day → yesterday only.
    """
    today = datetime.now(PACIFIC).date()
    weekday = today.weekday()  # Mon=0 ... Sun=6
    if weekday == 0:  # Monday
        offsets = (3, 2, 1)  # Fri, Sat, Sun
    else:
        offsets = (1,)
    return [(today - timedelta(days=n)).strftime("%Y-%m-%d") for n in offsets]


def get_lost_status_id():
    r = requests.get(f"{BASE}/status/lead/", auth=AUTH)
    r.raise_for_status()
    for s in r.json()["data"]:
        if s["label"] == LOST_STATUS_LABEL:
            return s["id"]
    sys.exit(f"Could not find lead status: {LOST_STATUS_LABEL}")


def search_matching_leads(dates, lost_status_id):
    """Multi-value `term` acts as OR across the supplied dates."""
    query = {
        "query": {
            "type": "and",
            "queries": [
                {
                    "type": "field_condition",
                    "field": {
                        "type": "custom_field",
                        "custom_field_id": FIRST_SALES_CALL_FIELD.removeprefix("cf_"),
                    },
                    "condition": {"type": "term", "values": dates},
                },
                {
                    "type": "field_condition",
                    "field": {
                        "type": "regular_field",
                        "object_type": "lead",
                        "field_name": "status_id",
                    },
                    "condition": {"type": "term", "values": [lost_status_id]},
                },
            ],
        },
        "_fields": {
            "lead": [
                "id",
                "display_name",
                "lead_owner_id",
                FIRST_SALES_CALL_FIELD,
            ]
        },
        "results_limit": 200,
    }
    r = requests.post(f"{BASE}/data/search/", json=query, auth=AUTH)
    r.raise_for_status()
    return r.json().get("data", [])


def update_lead(lead_id):
    if DRY_RUN:
        return
    payload = {
        "lead_owner_id": JOHN_KIRK_USER_ID,
        LANE_2_HANDRAISER_FIELD: HANDRAISER_VALUE,
    }
    r = requests.put(f"{BASE}/lead/{lead_id}/", json=payload, auth=AUTH)
    r.raise_for_status()


def create_task(lead_id, lead_name, sales_call_date):
    if DRY_RUN:
        return
    today = datetime.now(PACIFIC).strftime("%Y-%m-%d")
    payload = {
        "_type": "lead",
        "lead_id": lead_id,
        "assigned_to": JOHN_KIRK_USER_ID,
        "date": today,
        "text": (
            f"Prior Day Lost Deal — {lead_name}. "
            f"First sales call was {sales_call_date}. "
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
    leads = search_matching_leads(dates, lost_status_id)
    print(f"Found {len(leads)} matching leads\n")

    processed = skipped = 0
    for lead in leads:
        name = lead.get("display_name", "(no name)")
        sales_call_date = lead.get(FIRST_SALES_CALL_FIELD) or "recently"

        if lead.get("lead_owner_id") == JOHN_KIRK_USER_ID:
            print(f"  SKIP   {name} — already owned by John")
            skipped += 1
            continue

        if DRY_RUN:
            print(f"  WOULD  {name}")
            print(f"           → lead_owner_id: John Kirk")
            print(f"           → Lane 2 Handraiser: {HANDRAISER_VALUE}")
            print(f"           → create task for John (first call: {sales_call_date})")
        else:
            update_lead(lead["id"])
            create_task(lead["id"], name, sales_call_date)
            print(f"  DONE   {name}")
        processed += 1

    verb = "Would process" if DRY_RUN else "Processed"
    print(f"\n{verb}: {processed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
