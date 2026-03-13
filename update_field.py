"""
First SALES Meeting Field Updater
----------------------------------
Scans all meetings in Close CRM, applies title-based classification rules,
and writes the earliest qualifying first sales meeting date to the
"First Sales Call Booked Date" custom field on each lead.

Custom field: cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq

Performance strategy:
- Always paginate ALL meetings (~107 API calls, ~53 seconds — unavoidable,
  Close API ignores date filters on meeting endpoint)
- Cache known lead→date state in state_cache.json (committed to repo)
- On each run, compare recalculated dates against cache IN MEMORY
- Only hit Close API for leads where something actually changed
- Routine runs: ~1-2 minutes. Backfill: one-time ~80 minutes (uses checkpoint)
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
SLEEP_BETWEEN_CALLS = 0.5

CHECKPOINT_FILE = "checkpoint.json"   # Temporary — only exists during backfill
STATE_CACHE_FILE = "state_cache.json" # Permanent — tracks known lead→date state
CHECKPOINT_EVERY = 200

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
# Git helper
# ─────────────────────────────────────────────

def git_commit_and_push(filename: str, message: str) -> None:
    """Commit a single file to the repo and push."""
    try:
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=True)
        subprocess.run(
            ["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"],
            check=True,
        )
        subprocess.run(["git", "add", filename], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"  [git] Warning: commit failed ({e}), continuing.", flush=True)


# ─────────────────────────────────────────────
# State cache — permanent, survives across runs
# ─────────────────────────────────────────────

def load_state_cache() -> dict:
    """
    Load the known lead→date state from previous runs.
    Format: { lead_id: "YYYY-MM-DD" or None }
    This lets us skip leads where nothing has changed without hitting the Close API.
    """
    if not os.path.exists(STATE_CACHE_FILE):
        print("No state cache found — this must be the initial backfill run.", flush=True)
        return {}
    try:
        with open(STATE_CACHE_FILE) as f:
            data = json.load(f)
        cache = data.get("state", {})
        saved_at = data.get("saved_at", "unknown")
        print(
            f"State cache loaded: {len(cache)} leads cached (saved at {saved_at}).",
            flush=True,
        )
        return cache
    except Exception as e:
        print(f"Warning: could not load state cache ({e}), rebuilding.", flush=True)
        return {}


def save_state_cache(state: dict) -> None:
    """Save the full lead→date state to the repo after a successful run."""
    data = {
        "state": state,
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "count": len(state),
    }
    with open(STATE_CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    git_commit_and_push(
        STATE_CACHE_FILE,
        f"state cache: {len(state)} leads tracked",
    )
    print(f"State cache saved: {len(state)} leads.", flush=True)


# ─────────────────────────────────────────────
# Checkpoint — temporary, only during backfill
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
            f"(saved at {saved_at}). Resuming from here.",
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
    git_commit_and_push(
        CHECKPOINT_FILE,
        f"checkpoint: {len(processed_ids)} leads processed",
    )
    print(f"  [checkpoint] {len(processed_ids)} leads processed.", flush=True)


def clear_checkpoint() -> None:
    if not os.path.exists(CHECKPOINT_FILE):
        return
    try:
        os.remove(CHECKPOINT_FILE)
        git_commit_and_push(CHECKPOINT_FILE, "checkpoint: cleared after successful full run")
        print("Checkpoint cleared.", flush=True)
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
    """
    Paginates ALL meetings and returns { lead_id: [qualifying meetings] }.
    Close API silently ignores date filters — must fetch everything and filter in Python.
    ~107 API calls, ~53 seconds. Unavoidable.
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
            f"  Fetched {total_fetched} meetings ({total_qualifying} qualifying) ...",
            flush=True,
        )

        if not data.get("has_more"):
            break
        skip += limit

    print(
        f"\nDone. Total: {total_fetched} meetings | "
        f"{total_qualifying} qualifying | "
        f"{len(meetings_by_lead)} leads with qualifying meetings",
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


def calculate_desired_state(meetings_by_lead: dict) -> dict:
    """
    Build the full desired state: { lead_id: "YYYY-MM-DD" or None }
    for every lead that has qualifying meetings.
    Pure Python — zero API calls.
    """
    return {
        lead_id: earliest_pacific_date(meetings)
        for lead_id, meetings in meetings_by_lead.items()
    }


# ─────────────────────────────────────────────
# Step 3a: Routine run — cache-based, fast
# ─────────────────────────────────────────────

def routine_update(desired_state: dict, cached_state: dict) -> dict:
    """
    For each lead in desired_state, compare against cache.
    Only hit Close API for leads where the date has changed.
    Returns the updated full state (desired + any cached leads not in desired).
    """
    # Find leads where desired != cached
    changed = {
        lead_id: date
        for lead_id, date in desired_state.items()
        if cached_state.get(lead_id) != date
    }

    # Also find leads that WERE in cache but have NO qualifying meetings now
    # (meeting was reclassified/deleted) — need to clear the field
    stale = {
        lead_id: None
        for lead_id in cached_state
        if lead_id not in desired_state and cached_state[lead_id] is not None
    }

    all_changes = {**changed, **stale}

    print(
        f"\nCache comparison: {len(desired_state)} leads with qualifying meetings | "
        f"{len(changed)} changed | {len(stale)} stale (need clearing) | "
        f"{len(desired_state) - len(changed)} already correct (skipped)",
        flush=True,
    )

    if not all_changes:
        print("Nothing to update — all leads already correct.", flush=True)
        return {**cached_state, **desired_state}

    updated = 0
    errors = 0

    for i, (lead_id, new_date) in enumerate(all_changes.items(), 1):
        try:
            lead_data = api_get(
                f"/lead/{lead_id}/",
                params={"_fields": f"id,display_name,{CUSTOM_FIELD_KEY}"},
            )
            lead_name = lead_data.get("display_name", lead_id)
            current_value = lead_data.get(CUSTOM_FIELD_KEY)
            if current_value and len(current_value) > 10:
                current_value = current_value[:10]

            if current_value == new_date:
                # Cache was stale but Close already has the right value — just update cache
                pass
            else:
                api_put(f"/lead/{lead_id}/", {CUSTOM_FIELD_KEY: new_date})
                updated += 1
                print(
                    f"  [{i}/{len(all_changes)}] {lead_name} | "
                    f"{current_value or 'blank'} → {new_date or 'cleared'}",
                    flush=True,
                )

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(all_changes)}] ERROR on {lead_id}: {e}", flush=True)

    print(
        f"\nRoutine run complete. Updated: {updated} | Errors: {errors}",
        flush=True,
    )

    # Merge desired state into cached state for saving
    new_cache = dict(cached_state)
    new_cache.update(desired_state)
    for lead_id in stale:
        new_cache[lead_id] = None
    return new_cache


# ─────────────────────────────────────────────
# Step 3b: Initial backfill — no cache, must fetch every lead
# ─────────────────────────────────────────────

def backfill(desired_state: dict, already_processed: set) -> tuple[dict, set]:
    """
    First-time run: no state cache exists. Must fetch every lead from Close
    to get its current field value before writing.
    Uses checkpoint to survive timeouts.
    Returns (built_cache, all_processed_ids).
    """
    lead_ids = list(desired_state.keys())
    remaining = [lid for lid in lead_ids if lid not in already_processed]

    print(
        f"\nBackfill mode: {len(lead_ids)} leads total | "
        f"{len(already_processed)} already done (checkpoint) | "
        f"{len(remaining)} to process",
        flush=True,
    )

    updated = 0
    skipped_correct = 0
    errors = 0
    processed_this_run = set()
    built_cache = {}

    for i, lead_id in enumerate(remaining, 1):
        new_date = desired_state[lead_id]
        try:
            lead_data = api_get(
                f"/lead/{lead_id}/",
                params={"_fields": f"id,display_name,{CUSTOM_FIELD_KEY}"},
            )
            lead_name = lead_data.get("display_name", lead_id)
            current_value = lead_data.get(CUSTOM_FIELD_KEY)
            if current_value and len(current_value) > 10:
                current_value = current_value[:10]

            if current_value == new_date:
                skipped_correct += 1
            else:
                api_put(f"/lead/{lead_id}/", {CUSTOM_FIELD_KEY: new_date})
                updated += 1
                print(
                    f"  [{i}/{len(remaining)}] Updated: {lead_name} | "
                    f"{current_value or 'blank'} → {new_date or 'cleared'}",
                    flush=True,
                )

            built_cache[lead_id] = new_date
            processed_this_run.add(lead_id)

            if len(processed_this_run) % CHECKPOINT_EVERY == 0:
                save_checkpoint(already_processed | processed_this_run)

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(remaining)}] ERROR on {lead_id}: {e}", flush=True)

    print(
        f"\nBackfill complete. Updated: {updated} | "
        f"Already correct: {skipped_correct} | Errors: {errors}",
        flush=True,
    )

    all_processed = already_processed | processed_this_run
    return built_cache, all_processed


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    start = datetime.now(timezone.utc)
    print(
        f"═══════════════════════════════════════════\n"
        f"First SALES Meeting Field Updater\n"
        f"Started: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"═══════════════════════════════════════════\n",
        flush=True,
    )

    # 1. Load state cache (empty dict if first run)
    cached_state = load_state_cache()
    is_backfill = len(cached_state) == 0 and not os.path.exists(CHECKPOINT_FILE)

    # Edge case: checkpoint exists but no cache = interrupted backfill, resume it
    is_resuming_backfill = os.path.exists(CHECKPOINT_FILE)

    # 2. Fetch and classify ALL meetings (always required — Close API won't filter by date)
    meetings_by_lead = fetch_all_meetings()

    # 3. Calculate desired state in Python — zero API calls
    desired_state = calculate_desired_state(meetings_by_lead)

    # 4. Update Close — strategy depends on whether we have a state cache
    if cached_state and not is_resuming_backfill:
        # ── Routine run: fast path ──────────────────────────────────────────
        print("\nMode: ROUTINE (state cache found — skipping unchanged leads)", flush=True)
        new_cache = routine_update(desired_state, cached_state)
        save_state_cache(new_cache)

    else:
        # ── Backfill: first run or resuming interrupted run ─────────────────
        print(
            f"\nMode: {'RESUMING BACKFILL' if is_resuming_backfill else 'INITIAL BACKFILL'}",
            flush=True,
        )
        already_processed = load_checkpoint()
        built_cache, all_processed = backfill(desired_state, already_processed)

        # Only save state cache + clear checkpoint if ALL leads were processed
        if len(all_processed) >= len(desired_state):
            save_state_cache(built_cache)
            clear_checkpoint()
            print(
                "\nBackfill complete — state cache saved. "
                "Future runs will use fast routine mode.",
                flush=True,
            )
        else:
            save_checkpoint(all_processed)
            print(
                f"\nBackfill incomplete ({len(all_processed)}/{len(desired_state)} done). "
                f"Re-run to continue.",
                flush=True,
            )

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(f"\nTotal runtime: {elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
