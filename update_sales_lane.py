#!/usr/bin/env python3
"""
Sales Lane — Close CRM Field Updater
====================================

Sets the lead's "Sales Lane" dropdown (Lane 1 / Lane 2) based ENTIRELY on who
the lead's *Lead Owner* is.

  - "Lead Owner" is the CUSTOM field cf_gOfS... (NOT Close's built-in
    `assigned_to`, which controls opportunity/task ownership in this org).
  - If the owner is a Lane 1 rep  -> Sales Lane = "Lane 1"
  - If the owner is a Lane 2 rep  -> Sales Lane = "Lane 2"
  - If the owner is anyone else (or unset) -> the lead is left untouched.

Idempotent: reads the current Sales Lane value and only PATCHes leads whose
value is wrong/blank. No state cache needed — the owner-to-lane map IS the
desired state, recomputed every run.

Runs independently of update_field.py. It has nothing to do with meeting
classification, so it is its own script on its own schedule.

CLI
---
    python update_sales_lane.py            # live run
    python update_sales_lane.py --dry-run  # report only, zero writes
    python update_sales_lane.py --limit 50 # only scan first N leads (testing)

Requires env var: CLOSE_API_KEY
"""

import argparse
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
BASE_URL = "https://api.close.com/api/v1"

# Field being written
SALES_LANE_FIELD = "cf_UD9Hm3dpLGtcUd37tX8Y9GAK1Lhc3BdtDX769ffFvyB"  # "Sales Lane"
# Field being read (the deciding factor)
LEAD_OWNER_FIELD = "cf_gOfS9pFwext58oberEegLyix8hZzeHrxhCZOVh3P3rd"  # "Lead Owner"

# Dropdown option values to write — must match the field's options in Close exactly.
LANE_1_VALUE = "Lane 1"
LANE_2_VALUE = "Lane 2"

# ---------------------------------------------------------------------------
# Rep rosters  — EDIT THESE when reps move lanes / join / leave.
# Names are only for log readability; the user_id is what matters.
# ---------------------------------------------------------------------------

LANE_1_REPS = {
    "user_F0VeLnOQlWpkDncNW8rBl1V2QJ08fnDt6DcUjNATUJK": "Scott Seymour",
    "user_7F059xEinVentOEvkRMP77fWZyvwUiTRTUOuhD11J0e": "Robin Perkins",
    "user_wF5aATmDljO6g6AHqehRPVmfCmH5j9VszbO6Q6Pjzm4": "Eric Piccione",
    "user_1xDZSeOa8omjfxHXD80twTf8OieXfQ6tNCaYbVygtv1": "Dubem Adindu",
    "user_fYWHvOuCKDuaQxSp6lROlv2rmvZZYq1kzjGvaF7OrAL": "Jake Skinner",
    "user_Ap8we63okFA5Cw9pvr5xgccvqDlIfisKVtFKt6oBe6p": "Luis Galarza",
    "user_XEbPgLixZy4dhuLp34WogOzCIChkKEnrffDnHlxOnA7": "Danny Santolaya",
    "user_7HSxi55O8q5jO11khvrTcAGoL2nlcoa3kZ6loAY6i78": "Joe Vaughn",
    "user_1TKtkacQ7ZMKkcqnmCERikTYWwGltp5XUjEE9Hshple": "Shreya Bechra",
    "user_wHm1vcLde4RExd3vv9UOjnms5Oz8ssXg8600mQuxMPb": "Christian Hartwell",
    "user_lUjlATIIgFg8mELa0GFzZUj0lG4Cs7PwQsxbi34I6Su": "Joe Dysert",
}

LANE_2_REPS = {
    "user_MrBLkl5wCqTm7QxHxPo2ydNV5KxMllg6YZDVc12Aqzj": "Jason Aaron",
    "user_WquWudQN7dghZsAPiNY80eJUmg1EadQg2UCQdvgbif7": "Kelly Schrader",
    "user_Bov31jjnHhENBy8uWNTTL8KKax8VX7o6DugLzBYOHBG": "Lyle Hubbard",
    "user_I0cHZ04mBXXBvbFcnwmsc2KrcMsLsKxqjW8DtJ783Hr": "Elvis Ellis",
    "user_UpJb11fzX2TuFHf7fFyWpfXr84lg2Ui7i7p5CtQkIaW": "Cameron Caswell",
}

# Flatten into a single owner -> desired-lane map.
OWNER_TO_LANE = {uid: LANE_1_VALUE for uid in LANE_1_REPS}
OWNER_TO_LANE.update({uid: LANE_2_VALUE for uid in LANE_2_REPS})

PAGE_SIZE = 200  # Close data/search max

# ---------------------------------------------------------------------------
# Close API helpers
# ---------------------------------------------------------------------------


def make_session():
    if not CLOSE_API_KEY:
        sys.exit("ERROR: CLOSE_API_KEY environment variable is not set.")
    s = requests.Session()
    s.auth = (CLOSE_API_KEY, "")  # Close uses HTTP Basic: key as username, blank password
    s.headers.update({"Content-Type": "application/json"})
    return s


def get_custom(lead, cf_id, display_name=None):
    """
    Defensive accessor for a custom field value on a lead payload.

    Close is inconsistent about how custom fields come back depending on
    endpoint/params:
      - nested under "custom" keyed by cf id:   lead["custom"]["cf_xxx"]
      - flattened top-level:                    lead["custom.cf_xxx"]
      - nested under "custom" keyed by display: lead["custom"]["Sales Lane"]
    This checks all of them and returns the first hit (or None).
    """
    flat_key = f"custom.{cf_id}"
    if flat_key in lead:
        return lead[flat_key]

    custom = lead.get("custom") or {}
    if cf_id in custom:
        return custom[cf_id]
    if display_name and display_name in custom:
        return custom[display_name]
    return None


def iter_all_leads(session, hard_limit=None):
    """
    Paginate every lead in the org via POST /data/search/.

    We request the two custom fields we care about. A bare 'all leads' query is
    the simplest thing Close will reliably honor, so we filter by owner in
    Python rather than relying on a custom-field search condition.
    """
    url = f"{BASE_URL}/data/search/"
    cursor = None
    fetched = 0

    while True:
        body = {
            "query": {
                "type": "and",
                "queries": [{"type": "object_type", "object_type": "lead"}],
            },
            "_fields": {"lead": ["id", "display_name", "custom"]},
            "_limit": PAGE_SIZE,
        }
        if cursor:
            body["_cursor"] = cursor

        resp = session.post(url, json=body)
        if resp.status_code != 200:
            sys.exit(f"ERROR: lead search failed ({resp.status_code}): {resp.text[:500]}")

        payload = resp.json()
        for lead in payload.get("data", []):
            yield lead
            fetched += 1
            if hard_limit and fetched >= hard_limit:
                return

        cursor = payload.get("cursor")
        if not cursor:
            return
        time.sleep(0.15)  # be gentle on the API


def update_sales_lane(session, lead_id, desired_value):
    """PUT the Sales Lane value. Returns True on confirmed write."""
    url = f"{BASE_URL}/lead/{lead_id}/"
    payload = {f"custom.{SALES_LANE_FIELD}": desired_value}
    resp = session.put(url, json=payload)
    if resp.status_code != 200:
        print(f"  ! FAILED {lead_id} ({resp.status_code}): {resp.text[:200]}")
        return False

    # Verify the write actually took.
    written = get_custom(resp.json(), SALES_LANE_FIELD, "Sales Lane")
    if written != desired_value:
        print(f"  ! VERIFY MISMATCH {lead_id}: wanted {desired_value!r}, got {written!r}")
        return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(dry_run=False, limit=None):
    session = make_session()

    stats = {
        "scanned": 0,
        "updated": 0,
        "unchanged": 0,
        "no_match_owner": 0,
        "failed": 0,
    }
    changes = []  # (lead_id, name, owner_name, from_value, to_value)

    mode = "DRY RUN — no writes" if dry_run else "LIVE"
    print(f"Sales Lane updater starting [{mode}]")
    print(f"Lane 1 reps: {len(LANE_1_REPS)} | Lane 2 reps: {len(LANE_2_REPS)}\n")

    for lead in iter_all_leads(session, hard_limit=limit):
        stats["scanned"] += 1
        lead_id = lead.get("id")
        name = lead.get("display_name", "?")

        owner = get_custom(lead, LEAD_OWNER_FIELD, "Lead Owner")
        desired = OWNER_TO_LANE.get(owner) if owner else None

        if desired is None:
            # Owner isn't a known Lane 1/2 rep (or no owner) -> leave the lead alone.
            stats["no_match_owner"] += 1
            continue

        current = get_custom(lead, SALES_LANE_FIELD, "Sales Lane")
        if current == desired:
            stats["unchanged"] += 1
            continue

        owner_name = LANE_1_REPS.get(owner) or LANE_2_REPS.get(owner) or owner
        changes.append((lead_id, name, owner_name, current, desired))

        if dry_run:
            print(f"  WOULD SET {desired} <- ({current!r}) | {name} | owner: {owner_name}")
            stats["updated"] += 1
        else:
            if update_sales_lane(session, lead_id, desired):
                print(f"  SET {desired} <- ({current!r}) | {name} | owner: {owner_name}")
                stats["updated"] += 1
            else:
                stats["failed"] += 1

    # ---- Summary ----
    print("\n" + "=" * 52)
    print("SUMMARY")
    print("=" * 52)
    print(f"  Leads scanned        : {stats['scanned']}")
    print(f"  Updated{' (would)' if dry_run else '        '}      : {stats['updated']}")
    print(f"  Already correct      : {stats['unchanged']}")
    print(f"  Owner not in roster  : {stats['no_match_owner']} (left untouched)")
    print(f"  Failed writes        : {stats['failed']}")
    print("=" * 52)

    if stats["failed"]:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Set Sales Lane from Lead Owner.")
    parser.add_argument("--dry-run", action="store_true", help="Report only, no writes.")
    parser.add_argument("--limit", type=int, default=None, help="Scan only first N leads (testing).")
    args = parser.parse_args()
    run(dry_run=args.dry_run, limit=args.limit)


if __name__ == "__main__":
    main()
