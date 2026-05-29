"""
update_lost_deals.py

Reassigns leads to John Kirk when:
  - First Sales Call Booked Date = yesterday (Pacific)
  - Lead Status = 💔 Lost

Also sets Lane 2 Handraiser = "Prior Day Lost Deals" and creates a Close task
for John on each lead. Idempotent: skips leads already owned by John.
"""

import os
import sys
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
BASE = "https://api.close.com/api/v1"
AUTH = (CLOSE_API_KEY, "")

JOHN_KIRK_USER_ID       = "user_5pAfnzGONQLUVLKqFQVpQ3570YV1gurVCTp1MMgfCDL"
FIRST_SALES_CALL_FIELD  = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
LANE_2_HANDRAISER_FIELD = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
LOST_STATUS_LABEL       = "💔 Lost"
HANDRAISER_VALUE        = "Prior Day Lost Deals"

PACIFIC = ZoneInfo("America/Los_Angeles")


def get_lost_status_id():
    r = requests.get(f"{BASE}/status/lead/", auth=AUTH)
    r.raise_for_status()
    for s in r.json()["data"]:
        if s["label"] == LOST_STATUS_LABEL:
            return s["id"]
    sys.exit(f"Could not find lead status: {LOST_STATUS_LABEL}")


def search_matching_leads(yesterday_str, lost_status_id):
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
                    "condition": {"type": "term", "values": [yesterday_str]},
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
        "_fields": {"lead": ["id", "display_name", "lead_owner_id"]},
        "results_limit": 200,
    }
    r = requests.post(f"{BASE}/data/search/", json=query, auth=AUTH)
    r.raise_for_status()
    return r.json().get("data", [])


def update_lead(lead_id):
    payload = {
        "lead_owner_id": JOHN_KIRK_USER_ID,
        LANE_2_HANDRAISER_FIELD: HANDRAISER_VALUE,
    }
    r = requests.put(f"{BASE}/lead/{lead_id}/", json=payload, auth=AUTH)
    r.raise_for_status()


def create_task(lead_id, lead_name, sales_call_date):
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
    yesterday = (datetime.now(PACIFIC) - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Looking for 💔 Lost leads with First Sales Call = {yesterday}")

    lost_status_id = get_lost_status_id()
    leads = search_matching_leads(yesterday, lost_status_id)
    print(f"Found {len(leads)} matching leads")

    processed = skipped = 0
    for lead in leads:
        if lead.get("lead_owner_id") == JOHN_KIRK_USER_ID:
            print(f"  SKIP  {lead['display_name']} — already owned by John")
            skipped += 1
            continue
        update_lead(lead["id"])
        create_task(lead["id"], lead["display_name"], yesterday)
        print(f"  DONE  {lead['display_name']}")
        processed += 1

    print(f"\nProcessed: {processed}, Skipped: {skipped}")


if __name__ == "__main__":
    main()
