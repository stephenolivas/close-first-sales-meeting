"""
Close CRM Field Updater
------------------------
Updates three custom fields on each lead:

1. "First Sales Call Booked Date" (cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq)
   — Date of the earliest qualifying first sales (closer) meeting, in Pacific time.

2. "Closer / Setter Call" (cf_6yy8dqzeiBIQD2dhDfaVeiCmEhTW6ycmM4SVQ5sO6CG)
   — Dropdown: "Closer" | "Setter" | blank
   — Rules:
       • Any qualifying sales meeting on the lead → "Closer"
       • Any setter/discovery meeting (Spencer, Kristin, or "Vending Quick Discovery")
         and no closer meeting → "Setter"
       • No meetings of either type → blank
       • NEVER downgrade from "Closer" — once set, it stays.

3. "Scraper Funnel" (cf_69vb5dGu6FcBrnLGJFeHQviYQTkk7zpnLRgMPW2vipd)
   — Dropdown: "YES" | blank
   — Rules:
       • Any meeting title matching "Vendingpreneur Next Steps" → "YES"
       • NEVER cleared once set to "YES".

Performance:
- Always paginates ALL meetings (~107 API calls — Close ignores date filters)
- State cache (state_cache.json, committed to repo) tracks known field values
- Routine runs compare desired vs cached IN MEMORY — only changed leads hit the API
- Routine run time: ~1-2 minutes. Backfill: one-time ~80 minutes.
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
PACIFIC = ZoneInfo("America/Los_Angeles")
SLEEP_BETWEEN_CALLS = 0.5

# Custom fields
FIELD_DATE_ID        = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_CALLTYPE_ID    = "cf_6yy8dqzeiBIQD2dhDfaVeiCmEhTW6ycmM4SVQ5sO6CG"
FIELD_SCRAPER_ID     = "cf_69vb5dGu6FcBrnLGJFeHQviYQTkk7zpnLRgMPW2vipd"
FIELD_POSTWEBINAR_ID = "cf_inRBDlgKLV9CgE7gBgzoQB0CAhwwuOoTHWclHxZoZQW"
FIELD_DATE_KEY       = f"custom.{FIELD_DATE_ID}"
FIELD_CALLTYPE_KEY   = f"custom.{FIELD_CALLTYPE_ID}"
FIELD_SCRAPER_KEY    = f"custom.{FIELD_SCRAPER_ID}"
FIELD_POSTWEBINAR_KEY = f"custom.{FIELD_POSTWEBINAR_ID}"
FIELDS_PARAM         = f"id,display_name,{FIELD_DATE_KEY},{FIELD_CALLTYPE_KEY},{FIELD_SCRAPER_KEY},{FIELD_POSTWEBINAR_KEY}"

CHECKPOINT_FILE  = "checkpoint.json"
STATE_CACHE_FILE = "state_cache.json"
CHECKPOINT_EVERY = 200

# ─────────────────────────────────────────────
# User IDs
# ─────────────────────────────────────────────

# Meetings from these users are completely ignored
EXCLUDED_OWNERS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",  # Stephen Olivas
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",  # Ahmad Bukhari
}

# Setter/discovery reps — their meetings count as "Setter" (not Closer)
SETTER_OWNERS = {
    "user_EmhqCmaHERTfgfWnPADiLGEqQw3ENvRYd3u1VEmblIp",  # Kristin Nelson
    "user_4sfuKGMbv0LQZ4hpS8ipASv406kKTSNP5Xx79jOwSqM",  # Spencer Reynolds
}

# ─────────────────────────────────────────────
# Classification Rules
# ─────────────────────────────────────────────

RE_CANCELED          = re.compile(r"^canceled[\s:]?", re.IGNORECASE)
RE_FOLLOWUP          = re.compile(r"follow[\-\s]?up|fallow\s+up|f/u\b|next\s+steps|reschedul", re.IGNORECASE)
RE_ENROLLMENT        = re.compile(r"enrollment|silver\s+start\s*up|bronze\s+enrollment|questions\s+on\s+enrollment", re.IGNORECASE)
RE_DISCOVERY_TITLE   = re.compile(r"vending\s+quick\s+discovery", re.IGNORECASE)
RE_SCRAPER_TITLE     = re.compile(r"vendingpren[eu]+r\s+next\s+steps", re.IGNORECASE)
RE_POSTWEBINAR_TITLE = re.compile(r"post\s+masterclass\s+strategy\s+call", re.IGNORECASE)

CLOSER_PATTERNS = [
    re.compile(r"vending\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+consultation", re.IGNORECASE),
    re.compile(r"vendingpren[eu]+rs?\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"new\s+vendingpren[eu]+r\s+strategy\s+call", re.IGNORECASE),
    re.compile(r"vending\s+consult\b", re.IGNORECASE),
    re.compile(r"post\s+masterclass\s+strategy\s+call", re.IGNORECASE),
]


def _is_hard_excluded(meeting: dict) -> bool:
    """Returns True if this meeting should be ignored entirely regardless of type."""
    user_id = meeting.get("user_id") or ""
    if user_id in EXCLUDED_OWNERS:
        return True
    title = (meeting.get("title") or "").strip()
    if RE_CANCELED.match(title):
        return True
    if RE_FOLLOWUP.search(title):
        return True
    if re.search(r"\banthony\b", title, re.IGNORECASE) and re.search(r"\bq&a\b", title, re.IGNORECASE):
        return True
    if RE_ENROLLMENT.search(title):
        return True
    return False


def classify_meeting(meeting: dict) -> str | None:
    """
    Returns the tier of this meeting:
      "closer"       — qualifying first sales meeting
      "setter"       — discovery/setter meeting
      "scraper"      — Vendingpreneur Next Steps meeting
      "post_webinar" — Post Masterclass Strategy Call (also counts as closer)
      None           — irrelevant, ignore

    NOTE: Scraper is checked BEFORE hard excludes because "Vendingpreneur Next Steps"
    contains "Next Steps" which would otherwise be caught by the followup hard exclude.
    """
    user_id = meeting.get("user_id") or ""
    title   = (meeting.get("title") or "").strip()

    # Scraper check first — before hard excludes
    if RE_SCRAPER_TITLE.search(title):
        if user_id not in EXCLUDED_OWNERS:
            return "scraper"

    if _is_hard_excluded(meeting):
        return None

    # Setter by owner — Kristin or Spencer's meetings are always setter
    if user_id in SETTER_OWNERS:
        return "setter"

    # Setter by title — Vending Quick Discovery (any owner)
    if RE_DISCOVERY_TITLE.search(title):
        return "setter"

    # Post Masterclass Strategy Call — closer AND sets post-webinar flag
    if RE_POSTWEBINAR_TITLE.search(title):
        return "post_webinar"

    # Closer — must match a qualifying pattern
    for pattern in CLOSER_PATTERNS:
        if pattern.search(title):
            return "closer"

    return None


# ─────────────────────────────────────────────
# Per-lead desired state calculation
# ─────────────────────────────────────────────

def calculate_desired_state(all_meetings: list) -> dict:
    """
    Given all meetings from the org, returns:
    {
      lead_id: {
        "date":      "YYYY-MM-DD" or None,  # earliest closer meeting date
        "call_type": "Closer" | "Setter" | None,
        "scraper":   "YES" | None
      }
    }

    Tier hierarchy: Closer > Setter > None
    Scraper is independent — a lead can be both Closer and scraper=YES.
    Zero API calls — pure Python.
    """
    # Group meetings by lead
    by_lead = defaultdict(list)
    for m in all_meetings:
        lead_id = m.get("lead_id")
        if lead_id:
            by_lead[lead_id].append(m)

    desired = {}
    for lead_id, meetings in by_lead.items():
        closer_dates   = []
        has_setter     = False
        has_scraper    = False
        has_postwebinar = False

        for m in meetings:
            tier = classify_meeting(m)
            if tier in ("closer", "post_webinar"):
                starts_at = m.get("starts_at")
                if starts_at:
                    dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
                    dt_pac = dt_utc.astimezone(PACIFIC)
                    closer_dates.append(dt_pac.strftime("%Y-%m-%d"))
                if tier == "post_webinar":
                    has_postwebinar = True
            elif tier == "setter":
                has_setter = True
            elif tier == "scraper":
                has_scraper = True

        if closer_dates:
            call_type = "Closer"
        elif has_setter:
            call_type = "Setter"
        else:
            call_type = None

        if call_type is not None or has_scraper or has_postwebinar:
            desired[lead_id] = {
                "date":         min(closer_dates) if closer_dates else None,
                "call_type":    call_type,
                "scraper":      "YES" if has_scraper else None,
                "post_webinar": "YES" if has_postwebinar else None,
            }

    return desired


# ─────────────────────────────────────────────
# Git helper
# ─────────────────────────────────────────────

def git_commit_and_push(filename: str, message: str) -> None:
    try:
        subprocess.run(["git", "config", "user.name",  "github-actions[bot]"], check=True)
        subprocess.run(["git", "config", "user.email", "github-actions[bot]@users.noreply.github.com"], check=True)
        subprocess.run(["git", "add", filename], check=True)
        result = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
        if result.returncode != 0:
            subprocess.run(["git", "commit", "-m", message], check=True)
            subprocess.run(["git", "push"],                   check=True)
    except subprocess.CalledProcessError as e:
        print(f"  [git] Warning: commit failed ({e}), continuing.", flush=True)


# ─────────────────────────────────────────────
# State cache
# ─────────────────────────────────────────────

def load_state_cache() -> dict:
    """
    Load { lead_id: { "date": ..., "call_type": ... } } from state_cache.json.
    Returns empty dict if missing or incompatible format (triggers backfill).
    """
    if not os.path.exists(STATE_CACHE_FILE):
        print("No state cache found — running backfill.", flush=True)
        return {}
    try:
        with open(STATE_CACHE_FILE) as f:
            data = json.load(f)
        raw = data.get("state", {})

        # Detect old format (values were plain date strings, not dicts)
        sample = next(iter(raw.values()), None) if raw else None
        if sample is not None and not isinstance(sample, dict):
            print("State cache format has changed — rebuilding (this is a one-time backfill).", flush=True)
            return {}

        print(f"State cache loaded: {len(raw)} leads (saved {data.get('saved_at', '?')}).", flush=True)
        return raw
    except Exception as e:
        print(f"Warning: could not load state cache ({e}) — rebuilding.", flush=True)
        return {}


def save_state_cache(state: dict) -> None:
    data = {
        "state":    state,
        "saved_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "count":    len(state),
    }
    with open(STATE_CACHE_FILE, "w") as f:
        json.dump(data, f, indent=2)
    git_commit_and_push(STATE_CACHE_FILE, f"state cache: {len(state)} leads tracked")
    print(f"State cache saved: {len(state)} leads.", flush=True)


# ─────────────────────────────────────────────
# Checkpoint (backfill only)
# ─────────────────────────────────────────────

def load_checkpoint() -> set:
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    try:
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        processed = set(data.get("processed_lead_ids", []))
        print(f"Checkpoint: {len(processed)} leads already done (saved {data.get('saved_at','?')}). Resuming.", flush=True)
        return processed
    except Exception as e:
        print(f"Warning: checkpoint unreadable ({e}), starting fresh.", flush=True)
        return set()


def save_checkpoint(processed_ids: set) -> None:
    data = {
        "processed_lead_ids": list(processed_ids),
        "saved_at":           datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "count":              len(processed_ids),
    }
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f, indent=2)
    git_commit_and_push(CHECKPOINT_FILE, f"checkpoint: {len(processed_ids)} leads processed")
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
# API helpers
# ─────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")


def api_get(path: str, params: dict = None, retry: int = 5) -> dict:
    url = f"{BASE_URL}{path}"
    for _ in range(retry):
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
    for _ in range(retry):
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
# Fetch all meetings
# ─────────────────────────────────────────────

def fetch_all_meetings() -> list:
    """
    Returns a flat list of all meeting activity dicts.
    Close API ignores date filters on this endpoint — must paginate everything.
    ~107 API calls, ~53 seconds. Unavoidable.
    """
    print("Fetching all meetings from Close...", flush=True)
    all_meetings = []
    skip = 0
    limit = 100

    while True:
        data  = api_get("/activity/meeting/", params={"_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        if not batch:
            break
        all_meetings.extend(batch)
        print(f"  Fetched {len(all_meetings)} meetings ...", flush=True)
        if not data.get("has_more"):
            break
        skip += limit

    print(f"Done. {len(all_meetings)} total meetings fetched.", flush=True)
    return all_meetings


# ─────────────────────────────────────────────
# Write fields to a single lead
# ─────────────────────────────────────────────

def write_lead(lead_id: str, lead_name: str, current: dict, desired: dict) -> dict | None:
    """
    Compares current vs desired state for one lead and writes only what changed.

    current / desired format:
      { "date": ..., "call_type": ..., "scraper": ..., "post_webinar": ... }

    Rules:
    - Never downgrade call_type from "Closer"
    - Never clear scraper or post_webinar once set to "YES"
    - Only write fields that actually changed

    Returns the final state written (or None if nothing changed).
    """
    payload = {}

    # ── Date field ──────────────────────────────────────────────────────────
    cur_date = (current.get("date") or "")[:10] or None
    new_date = desired.get("date")
    if cur_date != new_date:
        payload[FIELD_DATE_KEY] = new_date

    # ── Call type field ─────────────────────────────────────────────────────
    cur_type = current.get("call_type")
    new_type = desired.get("call_type")

    # Never downgrade from Closer
    if cur_type == "Closer" and new_type != "Closer":
        new_type = "Closer"

    if cur_type != new_type:
        payload[FIELD_CALLTYPE_KEY] = new_type

    # ── Scraper Funnel field ────────────────────────────────────────────────
    cur_scraper = current.get("scraper")
    new_scraper = desired.get("scraper")

    # Never clear once set to YES
    if cur_scraper == "YES":
        new_scraper = "YES"

    if cur_scraper != new_scraper:
        payload[FIELD_SCRAPER_KEY] = new_scraper

    # ── Post-Webinar Sales Booked field ─────────────────────────────────────
    cur_postwebinar = current.get("post_webinar")
    new_postwebinar = desired.get("post_webinar")

    # Never clear once set to YES
    if cur_postwebinar == "YES":
        new_postwebinar = "YES"

    if cur_postwebinar != new_postwebinar:
        payload[FIELD_POSTWEBINAR_KEY] = new_postwebinar

    if not payload:
        return None  # Nothing to write

    api_put(f"/lead/{lead_id}/", payload)

    changes = []
    if FIELD_DATE_KEY in payload:
        changes.append(f"date: {cur_date or 'blank'} → {new_date or 'cleared'}")
    if FIELD_CALLTYPE_KEY in payload:
        changes.append(f"type: {cur_type or 'blank'} → {new_type or 'cleared'}")
    if FIELD_SCRAPER_KEY in payload:
        changes.append(f"scraper: {cur_scraper or 'blank'} → {new_scraper or 'cleared'}")
    if FIELD_POSTWEBINAR_KEY in payload:
        changes.append(f"post-webinar: {cur_postwebinar or 'blank'} → {new_postwebinar or 'cleared'}")

    print(f"  Updated: {lead_name} | {' | '.join(changes)}", flush=True)

    return {
        "date":         new_date if FIELD_DATE_KEY in payload else cur_date,
        "call_type":    new_type if FIELD_CALLTYPE_KEY in payload else cur_type,
        "scraper":      new_scraper if FIELD_SCRAPER_KEY in payload else cur_scraper,
        "post_webinar": new_postwebinar if FIELD_POSTWEBINAR_KEY in payload else cur_postwebinar,
    }


# ─────────────────────────────────────────────
# Routine run (fast path — cache exists)
# ─────────────────────────────────────────────

def routine_update(desired_state: dict, cached_state: dict) -> dict:
    """
    Compare desired vs cached in memory.
    Only fetch + update leads where something changed.
    """
    # Leads where desired differs from cache
    to_check = {
        lead_id: desired
        for lead_id, desired in desired_state.items()
        if cached_state.get(lead_id) != desired
    }

    # Leads cached as having a value but no longer in desired (stale)
    stale = {
        lead_id: {"date": None, "call_type": None, "scraper": None, "post_webinar": None}
        for lead_id, cached in cached_state.items()
        if lead_id not in desired_state
        and (cached.get("date") or cached.get("call_type"))
    }

    all_changes = {**to_check, **stale}

    closer_count = sum(1 for v in desired_state.values() if v.get("call_type") == "Closer")
    setter_count = sum(1 for v in desired_state.values() if v.get("call_type") == "Setter")

    print(
        f"\nDesired state: {closer_count} Closer | {setter_count} Setter | "
        f"{len(desired_state) - closer_count - setter_count} blank\n"
        f"Cache diff: {len(to_check)} changed | {len(stale)} stale | "
        f"{len(desired_state) - len(to_check)} already correct (skipped)",
        flush=True,
    )

    if not all_changes:
        print("Nothing to update.", flush=True)
        new_cache = {**cached_state, **desired_state}
        return new_cache

    updated = 0
    errors  = 0
    new_cache = dict(cached_state)
    new_cache.update(desired_state)

    for i, (lead_id, desired) in enumerate(all_changes.items(), 1):
        try:
            lead_data = api_get(f"/lead/{lead_id}/", params={"_fields": FIELDS_PARAM})
            lead_name = lead_data.get("display_name", lead_id)
            current   = {
                "date":         lead_data.get(FIELD_DATE_KEY),
                "call_type":    lead_data.get(FIELD_CALLTYPE_KEY),
                "scraper":      lead_data.get(FIELD_SCRAPER_KEY),
                "post_webinar": lead_data.get(FIELD_POSTWEBINAR_KEY),
            }

            result = write_lead(lead_id, lead_name, current, desired)
            if result:
                updated += 1
                new_cache[lead_id] = result
            else:
                new_cache[lead_id] = desired  # Cache was stale but Close already correct

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(all_changes)}] ERROR on {lead_id}: {e}", flush=True)

        if i % 100 == 0:
            print(f"  [{i}/{len(all_changes)}] still processing... ({updated} updated so far)", flush=True)

    print(f"\nRoutine run complete. Updated: {updated} | Errors: {errors}", flush=True)
    return new_cache


# ─────────────────────────────────────────────
# Backfill (first run or resuming interrupted run)
# ─────────────────────────────────────────────

def backfill(desired_state: dict, already_processed: set) -> tuple[dict, set]:
    """
    No state cache exists — must fetch every lead from Close to read current values.
    Uses checkpoint to survive timeouts/cancellations.
    """
    lead_ids  = list(desired_state.keys())
    remaining = [lid for lid in lead_ids if lid not in already_processed]

    print(
        f"\nBackfill: {len(lead_ids)} leads total | "
        f"{len(already_processed)} done (checkpoint) | "
        f"{len(remaining)} remaining",
        flush=True,
    )

    updated     = 0
    skipped     = 0
    errors      = 0
    built_cache = {}
    processed   = set()

    for i, lead_id in enumerate(remaining, 1):
        desired = desired_state[lead_id]
        try:
            lead_data = api_get(f"/lead/{lead_id}/", params={"_fields": FIELDS_PARAM})
            lead_name = lead_data.get("display_name", lead_id)
            current   = {
                "date":         lead_data.get(FIELD_DATE_KEY),
                "call_type":    lead_data.get(FIELD_CALLTYPE_KEY),
                "scraper":      lead_data.get(FIELD_SCRAPER_KEY),
                "post_webinar": lead_data.get(FIELD_POSTWEBINAR_KEY),
            }

            result = write_lead(lead_id, lead_name, current, desired)
            if result:
                updated += 1
                built_cache[lead_id] = result
            else:
                skipped += 1
                built_cache[lead_id] = desired

            processed.add(lead_id)

            if len(processed) % CHECKPOINT_EVERY == 0:
                save_checkpoint(already_processed | processed)

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(remaining)}] ERROR on {lead_id}: {e}", flush=True)

    print(
        f"\nBackfill pass complete. Updated: {updated} | "
        f"Already correct: {skipped} | Errors: {errors}",
        flush=True,
    )

    return built_cache, already_processed | processed


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    start = datetime.now(timezone.utc)
    print(
        f"═══════════════════════════════════════════\n"
        f"Close CRM Field Updater\n"
        f"Fields: First Sales Call Booked Date | Closer / Setter Call | Scraper Funnel\n"
        f"Started: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"═══════════════════════════════════════════\n",
        flush=True,
    )

    # 1. Load state cache
    cached_state         = load_state_cache()
    is_resuming_backfill = os.path.exists(CHECKPOINT_FILE)
    is_backfill          = not cached_state and not is_resuming_backfill

    # 2. Fetch ALL meetings (always required — Close ignores date filters)
    all_meetings = fetch_all_meetings()

    # 3. Calculate desired state in Python — zero API calls
    desired_state = calculate_desired_state(all_meetings)

    closer_count = sum(1 for v in desired_state.values() if v.get("call_type") == "Closer")
    setter_count = sum(1 for v in desired_state.values() if v.get("call_type") == "Setter")
    print(
        f"\nDesired state calculated: {closer_count} Closer | "
        f"{setter_count} Setter | {len(desired_state)} leads total",
        flush=True,
    )

    # 4. Update Close
    if cached_state and not is_resuming_backfill:
        # ── Fast routine path ───────────────────────────────────────────────
        print("\nMode: ROUTINE", flush=True)
        new_cache = routine_update(desired_state, cached_state)
        save_state_cache(new_cache)

    else:
        # ── Backfill ────────────────────────────────────────────────────────
        mode = "RESUMING BACKFILL" if is_resuming_backfill else "INITIAL BACKFILL"
        print(f"\nMode: {mode}", flush=True)
        already_processed = load_checkpoint()
        built_cache, all_processed = backfill(desired_state, already_processed)

        if len(all_processed) >= len(desired_state):
            save_state_cache(built_cache)
            clear_checkpoint()
            print("\nBackfill complete — switching to fast routine mode on next run.", flush=True)
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
