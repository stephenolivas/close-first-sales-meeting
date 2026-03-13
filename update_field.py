"""
First SALES Meeting Field Updater
----------------------------------
Scans all meetings in Close CRM, applies title-based classification rules,
and writes the earliest qualifying first sales meeting date to the
"First Sales Call Booked Date" custom field on each lead.

Custom field: cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq

Checkpoint support:
- Saves progress to checkpoint.json every 200 leads
- On restart after a timeout/cancel, skips already-processed leads
- Checkpoint is committed to the repo so it survives across workflow runs
- Checkpoint is automatically deleted after a clean full run completes
"""

import json
import os
import re
import subprocess
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

CHECKPOINT_FILE = "checkpoint.json"
CHECKPOINT_EVERY = 200  # commit progress to repo every N leads

# ─────────────────────────────────────────────
# User IDs
# ─────────────────────────────────────────────

EXCLUDED_OWNERS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

SETTER_OWNERS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# ─────────────────────────────────────────────
# Classification Rules
# ─────────────────────────────────────────────

RE_CANCELED = re.compile(r"^canceled[\s:]?", re.IGNORECASE)
RE_FOLLOWUP_PATTERNS = re.compile(
    r"follow[\-\s]?up|fallow\s+up|f/u\b|next\s+steps|reschedul",
    re.IGNORECASE,
)
RE_ENROLLMENT_PATTERNS = re.compile(
    r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment",
    re.IGNORECASE,
)
RE_DISCOVERY = re.compile(r"vending\s+quick\s+discovery", re.IGNORECASE)

QUALIFYING_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
]


def is_qualifying_meeting(meeting: dict) -> bool:
    title = (meeting.get("title") or "").strip()
    user_id = meeting.get("user_id") or ""

    if user_id in EXCLUDED_OWNERS:
        return False
    if user_id in SETTER_OWNERS:
        return False
    if RE_CANCELED.match(title):
        return False
    if RE_FOLLOWUP_PATTERNS.search(title):
        return False
    if re.search(r"\banthony\b", title, re.IGNORECASE) and re.search(
        r"\bq&a\b", title, re.IGNORECASE
    ):
        return False
    if RE_ENROLLMENT_PATTERNS.search(title):
        return False
    if RE_DISCOVERY.search(title):
        return False
    for pattern in QUALIFYING_PATTERNS:
        if pattern.search(title):
            return True
    return False


# ─────────────────────────────────────────────
# Checkpoint helpers
# ─────────────────────────────────────────────

def load_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        processed = set(data.get("processed_lead_ids", []))
        saved_at = data.get("saved_at", "unknown")
        print(
            f"Checkpoint loaded: {len(processed)} leads already processed "
            f"(saved at {saved_at}). Skipping these.",
            flush=True,
        )
        return processed
    except Exception as e:
        print(f"Warning: could not load checkpoint ({e}), starting fresh.", flush=True)
        return set()


def save_checkpoint(processed_ids: set) -> None:
    data = {
        "processed_lead_ids": list(processed_ids),
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "count": len(processed_ids),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)

    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(
            ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
            check=True,
        )
        subprocess.run(["git", "add", CHECKPOINT_FILE], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(
                ["git", "commit", "-m", f"checkpoint: {len(processed_ids)} leads processed"],
                check=True,
            )
            subprocess.run(["git", "push"], check=True)
            print(
                f"  [checkpoint] Saved & committed: {len(processed_ids)} leads processed.",
                flush=True,
            )
    except subprocess.CalledProcessError as e:
        print(f"  [checkpoint] Warning: git commit failed ({e}), continuing.", flush=True)


def clear_checkpoint() -> None:
    if not os.path.exists(CHECKPOINT_FILE):
        return
    try:
        os.remove(CHECKPOINT_FILE)
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(
            ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
            check=True,
        )
        subprocess.run(["git", "add", CHECKPOINT_FILE], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(
                ["git", "commit", "-m", "checkpoint: cleared after successful full run"],
                check=True,
            )
            subprocess.run(["git", "push"], check=True)
        print("Checkpoint cleared after successful run.", flush=True)
    except Exception as e:
        print(f"Warning: could not clear checkpoint ({e}).", flush=True)


# ─────────────────────────────────────────────
# API Helpers
# ─────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")


def api_get(path: str, params: dict = None, retry: int = 5) -> dict:
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
    earliest = None
    for meeting in meetings:
        starts_at = meeting.get("starts_at")
        if not starts_at:
            continue
        dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
        dt_pacific = dt_utc.astimezone(PACIFIC)
        date_str = dt_pacific.strftime("%Y-%m-%d")
        if earliest is None or date_str < earliest:
            earliest = date_str
    return earliest


# ─────────────────────────────────────────────
# Step 3: Process leads (with checkpoint support)
# ─────────────────────────────────────────────

def process_leads(meetings_by_lead: dict, already_processed: set) -> set:
    lead_ids = list(meetings_by_lead.keys())
    remaining = [lid for lid in lead_ids if lid not in already_processed]

    print(
        f"\nLeads with qualifying meetings: {len(lead_ids)} total | "
        f"{len(already_processed)} already processed (checkpoint) | "
        f"{len(remaining)} to process this run",
        flush=True,
    )

    updated = 0
    skipped_correct = 0
    errors = 0
    processed_this_run = set()

    for i, lead_id in enumerate(remaining, 1):
        try:
            lead_data = api_get(
                f"/lead/{lead_id}/",
                params={"_fields": f"id,display_name,{CUSTOM_FIELD_KEY}"},
            )

            current_value = lead_data.get(CUSTOM_FIELD_KEY)
            lead_name = lead_data.get("display_name", lead_id)
            calculated_date = earliest_pacific_date(meetings_by_lead[lead_id])

            if current_value and len(current_value) > 10:
                current_value = current_value[:10]

            if current_value == calculated_date:
                skipped_correct += 1
            else:
                api_put(f"/lead/{lead_id}/", {CUSTOM_FIELD_KEY: calculated_date})
                updated += 1
                print(
                    f"  [{i}/{len(remaining)}] Updated: {lead_name} | "
                    f"{current_value or 'blank'} → {calculated_date or 'cleared'}",
                    flush=True,
                )

            processed_this_run.add(lead_id)

            # Save checkpoint every N leads
            if len(processed_this_run) % CHECKPOINT_EVERY == 0:
                all_processed = already_processed | processed_this_run
                save_checkpoint(all_processed)

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(remaining)}] ERROR on lead {lead_id}: {e}", flush=True)

    print(
        f"\n─────────────────────────────────────────\n"
        f"Lead processing complete.\n"
        f"  Updated:         {updated}\n"
        f"  Already correct: {skipped_correct}\n"
        f"  Errors:          {errors}\n"
        f"─────────────────────────────────────────",
        flush=True,
    )

    return already_processed | processed_this_run


# ─────────────────────────────────────────────
# Step 4: Clear stale fields
# ─────────────────────────────────────────────

def clear_stale_fields(meetings_by_lead: dict) -> None:
    print("\nChecking for leads with stale field values to clear...", flush=True)
    cleared = 0
    skip = 0
    limit = 100

    while True:
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
                continue

            lead_name = lead.get("display_name", lead_id)
            current_value = lead.get(CUSTOM_FIELD_KEY)

            if not current_value:
                continue

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

    # Load checkpoint — resume from any previous interrupted run
    already_processed = load_checkpoint()

    # 1. Fetch and classify all meetings
    meetings_by_lead = fetch_all_meetings()

    # 2. Update leads with qualifying meetings (respects checkpoint)
    if meetings_by_lead:
        all_processed = process_leads(meetings_by_lead, already_processed)
    else:
        print("No qualifying meetings found — nothing to update.", flush=True)
        all_processed = already_processed

    # 3. Clear any leads where field is set but no qualifying meetings exist
    clear_stale_fields(meetings_by_lead)

    # 4. Clean up checkpoint — full run completed successfully
    clear_checkpoint()

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\nTotal runtime: {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
