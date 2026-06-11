#!/usr/bin/env python3
"""
update_funnel_name.py
=====================
Force the lead custom field **Funnel Name DEAL (Opp)** to "Reactivation Scrapers"
for every lead that was booked by a setter onto a Lane 1 rep's calendar.

Goal
----
Whenever a setter books a scraper "Next Steps" call, the lead's funnel name is
ALWAYS overwritten to "Reactivation Scrapers" — re-asserted on every run, with
no date cutoff. This is the aggressive/always-overwrite counterpart to the
write that `update_field.py` already does under a 2026-04-06 gate.

Trigger (both conditions, by design — see REQUIRE_LIVE_MEETING)
--------------------------------------------------------------
1. The lead's "Reactivation - Setter Name" field is populated, AND
2. The lead currently has a meeting matching the scraper "Next Steps" pattern.

NOTE on the field:
  "Funnel Name DEAL (Opp)" is a SHARED custom field that IS defined on the LEAD
  object (confirmed in Close: Settings > Custom Fields > Leads). The "(Opp)" is
  only part of the display name. We therefore write it via the LEAD endpoint.

Conventions reused from the existing automations (verify against update_field.py
and update_lost_deals.py if you refactor to share helpers):
  - Reads:  GET  /lead/{id}/  -> custom fields come back keyed by DISPLAY NAME
            under lead["custom"].
  - Writes: PUT  /lead/{id}/  -> custom fields written keyed by API ID, prefixed
            with "custom." (e.g. {"custom.cf_xxx": "..."}).
  - The script verifies the write took before counting it as success.

Usage
-----
  python update_funnel_name.py            # live
  python update_funnel_name.py --dry-run  # report only, zero writes
"""

import argparse
import os
import re
import sys

import requests

# --------------------------------------------------------------------------- #
# Config
# --------------------------------------------------------------------------- #

CLOSE_API_KEY = os.environ.get("CLOSE_API_KEY")
if not CLOSE_API_KEY:
    sys.exit("CLOSE_API_KEY environment variable is required.")

BASE = "https://api.close.com/api/v1"

# Field API IDs (writes use these, prefixed with "custom.")
FUNNEL_FIELD_ID = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"      # Funnel Name DEAL (Opp)
SETTER_NAME_FIELD_ID = "cf_vz6kNiu4ItFxRA8Y9HKlWIoQMq3TsdaQqKekQ2YuxVk"  # Reactivation - Setter Name

# Field DISPLAY NAMES (reads come back keyed by these under lead["custom"]).
# Brittle to renames: if either field is renamed in Close, update these two.
# (You mentioned you plan to drop the "(Opp)" from the funnel field name —
#  when you do, update FUNNEL_DISPLAY_NAME to match or the read will miss.)
FUNNEL_DISPLAY_NAME = "Funnel Name DEAL (Opp)"
SETTER_NAME_DISPLAY_NAME = "Reactivation - Setter Name"

TARGET_VALUE = "Reactivation Scrapers"

# Require BOTH the setter-name field AND a live qualifying meeting.
#   True  -> stricter: a lead whose setter-name is set but whose meeting was
#            later canceled/retitled is NOT overwritten (guards a stale field).
#   False -> "Reactivation - Setter Name" populated alone drives the overwrite,
#            which is the closer reading of "ALWAYS". One-line flip.
REQUIRE_LIVE_MEETING = True

SESSION = requests.Session()
SESSION.auth = (CLOSE_API_KEY, "")
SESSION.headers.update({"Content-Type": "application/json"})

# --------------------------------------------------------------------------- #
# "Next Steps" scraper meeting matcher
# --------------------------------------------------------------------------- #
# The four canonical scraper titles all contain "Next Steps" AND a
# vendingpreneur token. This mirrors the Step-2 matcher in update_field.py:
#   - case-insensitive
#   - misspelling-tolerant (Vendingprenuers, Vendingprenurs, ...)
#   - tolerant of Calendly name suffixes ("... with John Smith")
# Canceled / Rescheduled titles are excluded just like the upstream classifier.

_NEXT_STEPS_RE = re.compile(r"next\s*steps?", re.I)
_VENDINGPRENEUR_RE = re.compile(r"vendingpren[eu]+rs?", re.I)
_CANCELED_RE = re.compile(r"^\s*canceled", re.I)
_RESCHEDULE_RE = re.compile(r"reschedul", re.I)


def is_scraper_next_steps(title: str) -> bool:
    if not title:
        return False
    if _CANCELED_RE.search(title) or _RESCHEDULE_RE.search(title):
        return False
    return bool(_NEXT_STEPS_RE.search(title) and _VENDINGPRENEUR_RE.search(title))


# --------------------------------------------------------------------------- #
# Close API helpers
# --------------------------------------------------------------------------- #

def paginate_meetings():
    """Yield every meeting activity in the org.

    Matches the documented behavior of update_field.py: Close ignores date
    filters on this endpoint, so we page through everything. ~130 calls / ~65s.
    """
    skip, limit = 0, 100
    while True:
        resp = SESSION.get(
            f"{BASE}/activity/meeting/",
            params={"_skip": skip, "_limit": limit},
        )
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if not data:
            break
        for meeting in data:
            yield meeting
        if len(data) < limit:
            break
        skip += limit


def leads_with_live_next_steps_meeting():
    """Return the set of lead IDs that currently have a qualifying meeting."""
    lead_ids = set()
    for meeting in paginate_meetings():
        if is_scraper_next_steps(meeting.get("title", "")):
            lead_id = meeting.get("lead_id")
            if lead_id:
                lead_ids.add(lead_id)
    return lead_ids


def get_lead(lead_id: str) -> dict:
    resp = SESSION.get(f"{BASE}/lead/{lead_id}/")
    resp.raise_for_status()
    return resp.json()


def custom_value(lead: dict, display_name: str):
    """Read a custom field off a lead by display name (Close read convention)."""
    custom = lead.get("custom") or {}
    return custom.get(display_name)


def set_funnel(lead_id: str) -> dict:
    """Write the funnel field on the LEAD and verify the value stuck."""
    payload = {f"custom.{FUNNEL_FIELD_ID}": TARGET_VALUE}
    resp = SESSION.put(f"{BASE}/lead/{lead_id}/", json=payload)
    resp.raise_for_status()
    updated = resp.json()
    if custom_value(updated, FUNNEL_DISPLAY_NAME) != TARGET_VALUE:
        raise RuntimeError(
            f"Funnel write did not take on lead {lead_id} "
            f"(got {custom_value(updated, FUNNEL_DISPLAY_NAME)!r})."
        )
    return updated


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def run(dry_run: bool):
    # 1. Build the candidate set.
    if REQUIRE_LIVE_MEETING:
        candidate_ids = leads_with_live_next_steps_meeting()
        print(f"Leads with a live scraper 'Next Steps' meeting: {len(candidate_ids)}")
    else:
        # Setter-name-only path. Implement a lead search on
        # SETTER_NAME_FIELD_ID != empty here if you flip the toggle.
        sys.exit(
            "REQUIRE_LIVE_MEETING=False path not wired up. Add a lead search on "
            f"{SETTER_NAME_FIELD_ID} (not empty) to populate candidate_ids."
        )

    written = skipped_no_setter = unchanged = errors = 0

    # 2. Per candidate: confirm setter-name populated, overwrite funnel if needed.
    for lead_id in sorted(candidate_ids):
        try:
            lead = get_lead(lead_id)
        except requests.HTTPError as e:
            print(f"  ! fetch failed for {lead_id}: {e}")
            errors += 1
            continue

        setter = custom_value(lead, SETTER_NAME_DISPLAY_NAME)
        if not setter or not str(setter).strip():
            skipped_no_setter += 1
            continue  # setter-name not populated -> not a setter-booked lead

        current = custom_value(lead, FUNNEL_DISPLAY_NAME)
        name = lead.get("display_name") or lead.get("name") or lead_id

        if current == TARGET_VALUE:
            unchanged += 1
            continue

        if dry_run:
            print(f"  [dry-run] would set {name}: {current!r} -> {TARGET_VALUE!r} (setter: {setter})")
            written += 1
            continue

        try:
            set_funnel(lead_id)
            print(f"  set {name}: {current!r} -> {TARGET_VALUE!r} (setter: {setter})")
            written += 1
        except (requests.HTTPError, RuntimeError) as e:
            print(f"  ! write failed for {name}: {e}")
            errors += 1

    print("\nSummary")
    print(f"  {'would write' if dry_run else 'written'}: {written}")
    print(f"  unchanged (already correct): {unchanged}")
    print(f"  skipped (no setter name): {skipped_no_setter}")
    print(f"  errors: {errors}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="report only, no writes")
    args = parser.parse_args()
    run(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
