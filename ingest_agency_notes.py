#!/usr/bin/env python3
"""
ingest_agency_notes.py — Recurring (hourly)

Reads new agency-submitted prospect notes from a Google Sheet (backing a Google
Form) and posts each as a Note on the corresponding Close lead. Writes status
back to the Sheet.

Rows are considered "new" when their 'Ingested At' column is empty. Once
processed, a row is never re-processed unless the 'Ingested At' cell is cleared
manually. This is the idempotency mechanism.

Expected Sheet columns (headers in row 1, exact strings):
    A: Timestamp          (auto-populated by Google Forms)
    B: Prospect Email     (form field)
    C: Prospect Name      (form field)
    D: Source             (form field)
    E: Notes              (form field)
    F: Ingested At        (script-managed — you add this header)
    G: Status             (script-managed — you add this header)
    H: Lead ID            (script-managed — you add this header)

Env vars required:
    CLOSE_API_KEY                — existing GitHub secret
    GOOGLE_SERVICE_ACCOUNT_JSON  — existing pattern (see google-service-account-memo)
    AGENCY_NOTES_SHEET_ID        — the Sheet's ID from its URL

Usage:
    python ingest_agency_notes.py            # live
    python ingest_agency_notes.py --dry-run  # log what would happen, write nothing
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import gspread
import requests
from google.oauth2.service_account import Credentials

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
SHEET_ID = os.environ["AGENCY_NOTES_SHEET_ID"]
GOOGLE_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
PACIFIC = ZoneInfo("America/Los_Angeles")

# Column headers we expect / manage (exact strings — must match Sheet row 1)
COL_TIMESTAMP = "Timestamp"
COL_EMAIL = "Prospect Email"
COL_NAME = "Prospect Name"
COL_SOURCE = "Source"
COL_NOTES = "Notes"
COL_INGESTED = "Ingested At"
COL_STATUS = "Status"
COL_LEAD_ID = "Lead ID"

MANAGED_COLS = (COL_INGESTED, COL_STATUS, COL_LEAD_ID)


def get_sheet():
    info = json.loads(GOOGLE_JSON)
    creds = Credentials.from_service_account_info(info, scopes=SHEETS_SCOPES)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh.sheet1  # first tab — that's where Form responses land


def parse_form_date(ts_str: str) -> str:
    """Parse a Google Forms Timestamp into a Pacific YYYY-MM-DD.
    Falls back to today (Pacific) if the string is empty or unparseable."""
    if ts_str:
        for fmt in ("%m/%d/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"):
            try:
                return datetime.strptime(ts_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return datetime.now(PACIFIC).strftime("%Y-%m-%d")


def lookup_lead_by_email(email: str):
    """Return lead_id for the first lead whose contact has this email, or None."""
    r = requests.get(
        "https://api.close.com/api/v1/lead/",
        auth=(CLOSE_API_KEY, ""),
        params={"query": f"email_address:{email}", "_fields": "id"},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not data:
        return None
    if len(data) > 1:
        print(f"    ⚠ {len(data)} leads matched {email}, using first")
    return data[0]["id"]


def post_note(lead_id: str, body: str) -> None:
    r = requests.post(
        "https://api.close.com/api/v1/activity/note/",
        auth=(CLOSE_API_KEY, ""),
        json={"lead_id": lead_id, "note": body},
        timeout=30,
    )
    r.raise_for_status()


def column_letter(index_1based: int) -> str:
    """1 -> A, 2 -> B, 27 -> AA."""
    result = ""
    n = index_1based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    ws = get_sheet()
    all_rows = ws.get_all_values()
    if not all_rows:
        sys.exit("Sheet appears empty (no header row).")

    header = all_rows[0]
    data_rows = all_rows[1:]

    # Validate all expected headers exist. Case-sensitive, exact match.
    required = (COL_TIMESTAMP, COL_EMAIL, COL_NAME, COL_SOURCE, COL_NOTES,
                COL_INGESTED, COL_STATUS, COL_LEAD_ID)
    missing = [c for c in required if c not in header]
    if missing:
        sys.exit(f"Sheet is missing expected column(s): {missing}\nHeaders found: {header}")

    idx = {name: header.index(name) for name in required}
    letters = {c: column_letter(idx[c] + 1) for c in MANAGED_COLS}

    # Managed cols must be contiguous F/G/H for the one-shot range update below.
    contiguous = (idx[COL_STATUS] == idx[COL_INGESTED] + 1
                  and idx[COL_LEAD_ID] == idx[COL_STATUS] + 1)

    mode = "DRY RUN" if args.dry_run else "LIVE"
    print(f"[{mode}] {len(data_rows)} data rows in Sheet {SHEET_ID}")
    print(f"Managed columns: {COL_INGESTED}={letters[COL_INGESTED]}  "
          f"{COL_STATUS}={letters[COL_STATUS]}  {COL_LEAD_ID}={letters[COL_LEAD_ID]}")
    print()

    posted = no_match = errors = skipped = 0

    for i, row in enumerate(data_rows, start=2):  # row 2 is first data row
        row = row + [""] * (len(header) - len(row))  # right-pad empties

        # Fully-empty row (no timestamp) → skip silently
        if not row[idx[COL_TIMESTAMP]].strip():
            continue

        # Already processed → skip
        if row[idx[COL_INGESTED]].strip():
            skipped += 1
            continue

        email = row[idx[COL_EMAIL]].strip()
        name = row[idx[COL_NAME]].strip() or "(no name)"
        source = row[idx[COL_SOURCE]].strip() or "unknown"
        notes = row[idx[COL_NOTES]].strip()
        submitted_date = parse_form_date(row[idx[COL_TIMESTAMP]])
        ingested_at = datetime.now(PACIFIC).strftime("%Y-%m-%d %H:%M:%S %Z")

        try:
            if not email:
                status, lead_id_val = "error: no email", ""
                print(f"[row {i}] ❌ {name} — no email in row")
                errors += 1
            else:
                lead_id = lookup_lead_by_email(email)
                if not lead_id:
                    status, lead_id_val = "no_match", ""
                    print(f"[row {i}] ⚠  {name} ({email}) — no lead in Close")
                    no_match += 1
                else:
                    body = f"Agency Notes — {source} — {submitted_date}\n\n{notes}"
                    if not args.dry_run:
                        post_note(lead_id, body)
                    status = "posted (dry-run)" if args.dry_run else "posted"
                    lead_id_val = lead_id
                    icon = "👀" if args.dry_run else "✅"
                    print(f"[row {i}] {icon} {name} ({email}) → {lead_id}")
                    posted += 1

            if not args.dry_run:
                _write_status(ws, i, letters, idx, ingested_at, status, lead_id_val, contiguous)

        except Exception as e:
            print(f"[row {i}] ❌ {name} — {e}")
            errors += 1
            if not args.dry_run:
                try:
                    _write_status(ws, i, letters, idx, ingested_at,
                                  f"error: {str(e)[:200]}", "", contiguous)
                except Exception as inner:
                    print(f"    (also failed to write error back to sheet: {inner})")

        time.sleep(0.2)

    print()
    print(f"Summary: {posted} posted  |  {no_match} no-match  |  "
          f"{errors} errors  |  {skipped} already-processed (skipped)")
    if args.dry_run:
        print("(DRY RUN — no notes posted, no writes to sheet)")


def _write_status(ws, row_i, letters, idx, ingested_at, status, lead_id_val, contiguous):
    if contiguous:
        rng = f"{letters[COL_INGESTED]}{row_i}:{letters[COL_LEAD_ID]}{row_i}"
        ws.update(rng, [[ingested_at, status, lead_id_val]])
    else:
        ws.update_cell(row_i, idx[COL_INGESTED] + 1, ingested_at)
        ws.update_cell(row_i, idx[COL_STATUS] + 1, status)
        ws.update_cell(row_i, idx[COL_LEAD_ID] + 1, lead_id_val)


if __name__ == "__main__":
    main()
