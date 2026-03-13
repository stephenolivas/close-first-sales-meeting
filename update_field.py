"""
First SALES Meeting Field Updater
----------------------------------
Scans all meetings in Close CRM, applies title-based classification rules,
and writes the earliest qualifying first sales meeting date to the
"First Sales Call Booked Date" custom field on each lead.

Custom field: cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq

Logic:
- Fetch all meetings via pagination
- Group by lead_id in memory
- Classify titles using the same rules as the Call Capacity / MTD dashboards
- For each lead: find earliest qualifying meeting date (Pacific time)
- Only write to Close if the field value would change
"""

import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
BASE_URL = "https://api.close.com/api/v1"
CUSTOM_FIELD_ID = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
CUSTOM_FIELD_KEY = f"custom.{CUSTOM_FIELD_ID}"
PACIFIC = ZoneInfo("America/Los_Angeles")
SLEEP_BETWEEN_CALLS = 0.5  # seconds — keep well under Close's rate limit

# ─────────────────────────────────────────────
# User IDs
# ─────────────────────────────────────────────

# These users' meetings are completely ignored (owners, internal)
EXCLUDED_OWNERS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

# These are setter/discovery users — their meetings are NOT sales meetings
SETTER_OWNERS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# ─────────────────────────────────────────────
# Classification Rules
# ─────────────────────────────────────────────

# Step 1 — Hard excludes: these title patterns are never a first sales meeting

RE_CANCELED = re.compile(r"^canceled[\s:]?", re.IGNORECASE)

RE_FOLLOWUP_PATTERNS = re.compile(
    r"follow[\-\s]?up|fallow\s+up|f/u\b|next\s+steps|reschedul",
    re.IGNORECASE,
)

RE_ENROLLMENT_PATTERNS = re.compile(
    r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE,
)

# Step 2 — Setter/discovery title patterns (NOT sales meetings)

RE_DISCOVERY = re.compile(r"vending\s+quick\s+discovery", re.IGNORECASE)

# Step 3 — Qualifying first sales meeting titles (ONLY these count)

QUALIFYING_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
]


def is_qualifying_meeting(meeting: dict) -> bool:
    """
    Returns True if this meeting counts as a qualifying first sales meeting.
    Applies classification rules in order — first match wins.
    """
    title = (meeting.get("title") or "").strip()
    user_id = meeting.get("user_id") or ""

    # User exclusions — completely ignore these owners
    if user_id in EXCLUDED_OWNERS:
        return False

    # Setter/discovery owners — not a sales meeting regardless of title
    if user_id in SETTER_OWNERS:
        return False

    # Step 1 — Hard excludes
    if RE_CANCELED.match(title):
        return False

    if RE_FOLLOWUP_PATTERNS.search(title):
        return False

    # Exclude titles containing BOTH "Anthony" AND "Q&A" (group Q&A sessions)
    if re.search(r"\banthony\b", title, re.IGNORECASE) and re.search(
        r"\bq&a\b", title, re.IGNORECASE
    ):
        return False

    if RE_ENROLLMENT_PATTERNS.search(title):
        return False

    # Step 2 — Setter/discovery title patterns
    if RE_DISCOVERY.search(title):
        return False

    # Step 3 — Must match at least one qualifying pattern
    for pattern in QUALIFYING_PATTERNS:
        if pattern.search(title):
            return True

    # Step 4 — Default: does not qualify
    return False


# ─────────────────────────────────────────────
# API Helpers
# ─────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")


def api_get(path: str, params: dict = None, retry: int = 5) -> dict:
    """GET with retry on 429 and sleep throttle."""
    url = f"{BASE_URL}{path}"
    for attempt in range(retry):
        time.sleep(SLEEP_BETWEEN_CALLS)
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            print(f"  [rate limit] sleeping {wait}s ...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"GET {path} failed after {retry} attempts")


def api_put(path: str, payload: dict, retry: int = 5) -> dict:
    """PUT with retry on 429 and sleep throttle."""
    url = f"{BASE_URL}{path}"
    for attempt in range(retry):
        time.sleep(SLEEP_BETWEEN_CALLS)
        resp = session.put(url, json=payload, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            print(f"  [rate limit] sleeping {wait}s ...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"PUT {path} failed after {retry} attempts")


# ─────────────────────────────────────────────
# Step 1: Fetch all meetings
# ─────────────────────────────────────────────

def fetch_all_meetings() -> dict:
    """
    Paginates through ALL meeting activities in the org.
    Returns a dict: { lead_id: [list of qualifying meetings] }

    Close API ignores date filters on meeting endpoint — must fetch everything
    and filter in Python.
    """
    print("Fetching all meetings from Close...", flush=True)
    meetings_by_lead = defaultdict(list)
    skip = 0
    limit = 100
    total_fetched = 0
    total_qualifying = 0

    while True:
        data = api_get("/activity/meeting/", params={"_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        if not batch:
            break

        for meeting in batch:
            lead_id = meeting.get("lead_id")
            if not lead_id:
                continue
            if is_qualifying_meeting(meeting):
                meetings_by_lead[lead_id].append(meeting)
                total_qualifying += 1

        total_fetched += len(batch)
        print(
            f"  Fetched {total_fetched} meetings so far "
            f"({total_qualifying} qualifying) ...",
            flush=True,
        )

        if not data.get("has_more"):
            break
        skip += limit

    print(
        f"\nDone fetching. Total meetings: {total_fetched} | "
        f"Qualifying meetings: {total_qualifying} | "
        f"Leads with qualifying meetings: {len(meetings_by_lead)}",
        flush=True,
    )
    return meetings_by_lead


# ─────────────────────────────────────────────
# Step 2: Resolve earliest qualifying date per lead
# ─────────────────────────────────────────────

def earliest_pacific_date(meetings: list) -> str | None:
    """
    Given a list of qualifying meetings for a lead, returns the date string
    (YYYY-MM-DD) of the earliest one in Pacific time, or None if list is empty.
    """
    earliest = None
    for meeting in meetings:
        starts_at = meeting.get("starts_at")
        if not starts_at:
            continue
        # Parse UTC timestamp from Close
        dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
        dt_pacific = dt_utc.astimezone(PACIFIC)
        date_str = dt_pacific.strftime("%Y-%m-%d")
        if earliest is None or date_str < earliest:
            earliest = date_str
    return earliest


# ─────────────────────────────────────────────
# Step 3: Fetch current field value & update if needed
# ─────────────────────────────────────────────

def process_leads(meetings_by_lead: dict) -> None:
    """
    For each lead with qualifying meetings:
    1. Fetch the lead's current field value (using _fields for minimal payload)
    2. Compare to the calculated earliest qualifying date
    3. Write only if different
    """
    lead_ids = list(meetings_by_lead.keys())
    print(f"\nProcessing {len(lead_ids)} leads with qualifying meetings...", flush=True)

    updated = 0
    skipped_correct = 0
    errors = 0

    for i, lead_id in enumerate(lead_ids, 1):
        try:
            # Minimal fetch — only grab the field we care about
            lead_data = api_get(
                f"/lead/{lead_id}/",
                params={"_fields": f"id,display_name,{CUSTOM_FIELD_KEY}"},
            )

            current_value = lead_data.get(CUSTOM_FIELD_KEY)
            lead_name = lead_data.get("display_name", lead_id)
            calculated_date = earliest_pacific_date(meetings_by_lead[lead_id])

            # Normalize current_value to just YYYY-MM-DD for comparison
            # Close returns dates as "YYYY-MM-DD" strings for date fields
            if current_value and len(current_value) > 10:
                current_value = current_value[:10]

            if current_value == calculated_date:
                skipped_correct += 1
                continue  # Already correct — no write needed

            # Write the update
            payload = {CUSTOM_FIELD_KEY: calculated_date}
            api_put(f"/lead/{lead_id}/", payload)

            updated += 1
            print(
                f"  [{i}/{len(lead_ids)}] Updated: {lead_name} | "
                f"{current_value or 'blank'} → {calculated_date or 'cleared'}",
                flush=True,
            )

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(lead_ids)}] ERROR on lead {lead_id}: {e}", flush=True)

    print(
        f"\n─────────────────────────────────────────\n"
        f"Run complete.\n"
        f"  Updated:         {updated}\n"
        f"  Already correct: {skipped_correct}\n"
        f"  Errors:          {errors}\n"
        f"─────────────────────────────────────────",
        flush=True,
    )


# ─────────────────────────────────────────────
# Step 4: Clear field for leads with NO qualifying meetings
# ─────────────────────────────────────────────

def clear_stale_fields(meetings_by_lead: dict) -> None:
    """
    Finds any leads that currently HAVE a value in the field but have NO
    qualifying meetings in the current scan — and clears them.

    This handles edge cases like a meeting being reclassified or deleted.

    Uses the Close search API to find leads where the custom field is set,
    then cross-references against our qualifying leads dict.
    """
    print("\nChecking for leads with stale field values to clear...", flush=True)
    cleared = 0
    skip = 0
    limit = 100

    while True:
        # Search for leads that have this custom field populated
        data = api_get(
            "/lead/",
            params={
                "query": f"custom.{CUSTOM_FIELD_ID}:*",
                "_fields": f"id,display_name,{CUSTOM_FIELD_KEY}",
                "_skip": skip,
                "_limit": limit,
            },
        )
        batch = data.get("data", [])
        if not batch:
            break

        for lead in batch:
            lead_id = lead.get("id")
            if lead_id in meetings_by_lead:
                continue  # Has qualifying meetings — handled in process_leads()

            lead_name = lead.get("display_name", lead_id)
            current_value = lead.get(CUSTOM_FIELD_KEY)

            if not current_value:
                continue  # Already blank

            try:
                api_put(f"/lead/{lead_id}/", {CUSTOM_FIELD_KEY: None})
                cleared += 1
                print(
                    f"  Cleared stale value on: {lead_name} (was {current_value})",
                    flush=True,
                )
            except Exception as e:
                print(f"  ERROR clearing {lead_id}: {e}", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    print(f"  Cleared {cleared} stale field(s).", flush=True)


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    start = datetime.now(timezone.utc)
    print(
        f"═══════════════════════════════════════════\n"
        f"First SALES Meeting Field Updater\n"
        f"Started: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Custom field: {CUSTOM_FIELD_KEY}\n"
        f"═══════════════════════════════════════════\n",
        flush=True,
    )

    # 1. Fetch and classify all meetings
    meetings_by_lead = fetch_all_meetings()

    # 2. Update leads that have qualifying meetings
    if meetings_by_lead:
        process_leads(meetings_by_lead)
    else:
        print("No qualifying meetings found — nothing to update.", flush=True)

    # 3. Clear any leads that had the field set but no longer have qualifying meetings
    clear_stale_fields(meetings_by_lead)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\nTotal runtime: {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
