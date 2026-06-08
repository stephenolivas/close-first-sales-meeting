"""
update_lane2_reassignment.py

Daily (weekday) round-robin reassignment of two Lane 2 lead buckets in Close CRM.

Buckets (driven by Close Smart Views, owner-filtered to Lane 1 reps):
  - Bucket 1  "L2 Handoff: 14-days No Comms"  -> Lane 2 Handraiser = "No Activity / Past 14 Days"
  - Bucket 2  "L2 Handoff: 30 Days since Booking" -> Lane 2 Handraiser = "30 Day Aged Deals"

For each lead pulled from a view, the script:
  1. Assigns the lead to the next Lane 2 rep in a round-robin rotation
     (each bucket keeps its OWN rotation pointer, persisted in lane2_state_cache.json).
  2. Sets the Lane 2 Handraiser custom field for that bucket.
  3. Reassigns every ACTIVE opportunity on the lead to the same rep (opp.user_id).
  4. Creates a HIGH-PRIORITY task for the rep, due today, with the bucket's text.

Why no churn / idempotency cache is needed:
  Both Smart Views are filtered to leads owned by Lane 1 reps. The moment a lead is
  reassigned to a Lane 2 rep it drops off the view, so it can't be picked up again.
  That makes re-runs on the same day safe (a second run finds the view already cleared)
  and means failed leads are simply retried on the next run.

Bucket precedence:
  Bucket 2 is processed FIRST. Any lead that somehow appears in both views is handled
  by Bucket 2 and skipped in Bucket 1. (By design the views shouldn't overlap.)

Modes (CLI flags or env vars set by the GitHub workflow):
  --dry-run   / DRY_RUN=true    Read + report only. No writes, no tasks. Shows the
                                exact round-robin distribution. Does NOT persist pointers.
  --skip-tasks/ SKIP_TASKS=true Do the field + opportunity updates, but create no tasks
                                (useful for backfilling without alerting reps).
"""

import os
import sys
import json
import argparse
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
BASE = "https://api.close.com/api/v1"
AUTH = (CLOSE_API_KEY, "")
PACIFIC = ZoneInfo("America/Los_Angeles")

STATE_FILE = "lane2_state_cache.json"

# Lane 2 reps, in round-robin order.
REPS = [
    ("Cameron Caswell", "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW"),
    ("Elvis Ellis",     "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr"),
    ("Jason Aaron",     "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj"),
    ("Lyle Hubbard",    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG"),
    ("Kelley Schrader", "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7"),
]
REP_IDS = {rid for _, rid in REPS}

# Custom field API IDs.
LEAD_OWNER_FIELD       = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"
LANE2_HANDRAISER_FIELD = "cf_Q1hRv8It46xsAEmpv4PRKdI1y0sPJnrnQrgRbIlF8uL"
LEAD_OWNER_DISPLAY     = "Lead Owner"  # how Close keys it on reads

# Bucket definitions. Processed in PROCESS_ORDER (Bucket 2 wins overlaps).
BUCKETS = {
    "bucket1": {
        "label":         "14-Day No Activity",
        "smart_view_id": "save_usGcGnOy1f5wIxt9jKGkACrfuQXgsKRwbzZSWhV2T8q",
        "handraiser":    "No Activity / Past 14 Days",
        "task_text":     "New Lead Assigned: No Activity within Past 14 days- Please Review",
        "index_key":     "bucket1_index",
    },
    "bucket2": {
        "label":         "30-Day Aged",
        "smart_view_id": "save_vUj7qzI7VqAcOj0kiYJVoSPGtTQVRXB9nqFNjPfMxXU",
        "handraiser":    "30 Day Aged Deals",
        "task_text":     "New Lead Assigned: 30 Day Aged Deal - Please Review",
        "index_key":     "bucket2_index",
    },
}
PROCESS_ORDER = ["bucket2", "bucket1"]  # Bucket 2 takes precedence on overlap


# ---------------------------------------------------------------------------
# State cache (round-robin pointers)
# ---------------------------------------------------------------------------

def load_state():
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
    except FileNotFoundError:
        s = {}
    s.setdefault("bucket1_index", 0)
    s.setdefault("bucket2_index", 0)
    return s


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ---------------------------------------------------------------------------
# Close API helpers
# ---------------------------------------------------------------------------

def get_saved_search_query(smart_view_id):
    """Fetch a Smart View and return its query in /data/search/ form."""
    r = requests.get(f"{BASE}/saved_search/{smart_view_id}/", auth=AUTH)
    r.raise_for_status()
    data = r.json()
    # Newer Smart Views store a structured query under s_query; older ones a string in query.
    return data.get("s_query") or data.get("query")


def search_lead_ids(query):
    """
    Run a Smart View query and return matching lead IDs.
    Handles structured queries via /data/search/ and legacy string queries via /lead/.
    """
    # Legacy string query -> classic lead listing endpoint.
    if isinstance(query, str):
        lead_ids, skip = [], 0
        while True:
            r = requests.get(
                f"{BASE}/lead/",
                params={"query": query, "_fields": "id", "_limit": 100, "_skip": skip},
                auth=AUTH,
            )
            r.raise_for_status()
            j = r.json()
            data = j.get("data", [])
            lead_ids += [d["id"] for d in data]
            if not j.get("has_more"):
                return lead_ids
            skip += len(data)

    # Structured query -> /data/search/. Some saved searches embed the full
    # request (with its own "query" key); unwrap if so.
    base = query if (isinstance(query, dict) and "query" in query) else {"query": query}

    lead_ids, cursor = [], None
    while True:
        body = dict(base)
        body["_fields"] = {"lead": ["id", "display_name"]}
        body["_limit"] = 200
        if cursor:
            body["cursor"] = cursor
        r = requests.post(f"{BASE}/data/search/", json=body, auth=AUTH)
        r.raise_for_status()
        j = r.json()
        lead_ids += [row["id"] for row in j.get("data", [])]
        cursor = j.get("cursor")
        if not cursor:
            return lead_ids


def get_lead(lead_id):
    r = requests.get(f"{BASE}/lead/{lead_id}/", auth=AUTH)
    r.raise_for_status()
    return r.json()


def read_lead_owner(lead):
    """Close returns custom fields keyed several ways depending on endpoint/version."""
    for key in (f"custom.{LEAD_OWNER_FIELD}",):
        if key in lead:
            return lead[key]
    cust = lead.get("custom", {}) or {}
    return cust.get(LEAD_OWNER_FIELD) or cust.get(LEAD_OWNER_DISPLAY)


def get_active_opportunities(lead_id):
    r = requests.get(
        f"{BASE}/opportunity/",
        params={"lead_id": lead_id, "_fields": "id,user_id,status_type"},
        auth=AUTH,
    )
    r.raise_for_status()
    return [o for o in r.json().get("data", []) if o.get("status_type") == "active"]


def reassign_lead(lead_id, rep_id, handraiser_value):
    """PUT the Lead Owner + Lane 2 Handraiser, then verify the owner actually took."""
    payload = {
        f"custom.{LEAD_OWNER_FIELD}":       rep_id,
        f"custom.{LANE2_HANDRAISER_FIELD}": handraiser_value,
    }
    r = requests.put(f"{BASE}/lead/{lead_id}/", json=payload, auth=AUTH)
    r.raise_for_status()
    new_owner = read_lead_owner(r.json())
    if new_owner != rep_id:
        raise RuntimeError(
            f"Lead {lead_id}: owner update did not take "
            f"(got {new_owner!r}, expected {rep_id!r}). Aborting before task creation."
        )


def reassign_opportunity(opp_id, rep_id):
    r = requests.put(f"{BASE}/opportunity/{opp_id}/", json={"user_id": rep_id}, auth=AUTH)
    r.raise_for_status()


def create_task(lead_id, rep_id, text):
    today = datetime.now(PACIFIC).strftime("%Y-%m-%d")
    payload = {
        "_type":       "lead",
        "lead_id":     lead_id,
        "assigned_to": rep_id,
        "date":        today,       # due date
        "text":        text,
        "priority":    "high",      # the "high priority" checkbox
    }
    r = requests.post(f"{BASE}/task/", json=payload, auth=AUTH)
    r.raise_for_status()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-tasks", action="store_true")
    args = parser.parse_args()

    dry_run    = args.dry_run    or os.environ.get("DRY_RUN", "").lower() == "true"
    skip_tasks = args.skip_tasks or os.environ.get("SKIP_TASKS", "").lower() == "true"

    print("=" * 64)
    if dry_run:
        print("  DRY RUN MODE — no writes will be made")
    elif skip_tasks:
        print("  SKIP-TASKS MODE — fields/opps updated, no tasks created")
    else:
        print("  LIVE RUN")
    print("=" * 64)

    state = load_state()
    handled = set()                 # lead IDs already assigned this run (overlap guard)
    totals = {}

    for key in PROCESS_ORDER:
        b = BUCKETS[key]
        idx_key = b["index_key"]
        print(f"\n--- {b['label']}  (view {b['smart_view_id']}) ---")

        query = get_saved_search_query(b["smart_view_id"])
        lead_ids = search_lead_ids(query)
        print(f"View returned {len(lead_ids)} lead(s)")

        assigned = skipped_overlap = skipped_owned = 0

        for lead_id in lead_ids:
            if lead_id in handled:
                skipped_overlap += 1
                print(f"  SKIP (overlap, Bucket 2 won)  {lead_id}")
                continue

            lead = get_lead(lead_id)
            name = lead.get("display_name", lead_id)

            # Safety belt: if the view ever returns a lead already on a Lane 2 rep,
            # don't reassign it and don't consume a rotation slot.
            if read_lead_owner(lead) in REP_IDS:
                skipped_owned += 1
                print(f"  SKIP (already a Lane 2 rep)  {name}")
                continue

            rep_name, rep_id = REPS[state[idx_key] % len(REPS)]
            opps = get_active_opportunities(lead_id)

            if dry_run:
                print(f"  WOULD  {name}")
                print(f"           -> Lead Owner: {rep_name}")
                print(f"           -> Lane 2 Handraiser: {b['handraiser']}")
                print(f"           -> Active opps to reassign: {len(opps)}")
                print(f"           -> Task ({'skipped' if skip_tasks else 'high priority'}): "
                      f"{b['task_text']!r}")
            else:
                reassign_lead(lead_id, rep_id, b["handraiser"])
                for opp in opps:
                    if opp.get("user_id") != rep_id:
                        reassign_opportunity(opp["id"], rep_id)
                if not skip_tasks:
                    create_task(lead_id, rep_id, b["task_text"])
                print(f"  DONE   {name} -> {rep_name} "
                      f"({len(opps)} opp(s)"
                      f"{', task created' if not skip_tasks else ', no task'})")

            state[idx_key] += 1   # advance rotation only on a real assignment
            handled.add(lead_id)
            assigned += 1

        totals[key] = assigned
        print(f"  {b['label']}: assigned {assigned}, "
              f"overlap-skipped {skipped_overlap}, already-owned-skipped {skipped_owned}")

    if not dry_run:
        save_state(state)
        print(f"\nState cache saved: {state}")
    else:
        print(f"\n(dry run — pointers NOT persisted; preview ending state would be {state})")

    print(f"\nSummary: " + ", ".join(f"{BUCKETS[k]['label']}={v}" for k, v in totals.items()))


if __name__ == "__main__":
    main()
