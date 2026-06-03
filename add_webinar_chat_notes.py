#!/usr/bin/env python3
"""
add_webinar_chat_notes.py — Recurring

Reads a CSV of webinar attendees and posts a Note on each Close lead.

CSV format is flexible. The script looks for these columns:
  - Name (required, used for logging only)
  - Lead ID OR Email (at least one — if both present, Lead ID wins)
  - "Webinar Chat Notes" OR "Chat Responses" (the note body)

Usage:
  CLOSE_API_KEY=... python add_webinar_chat_notes.py <csv_path> --date YYYY-MM-DD [--dry-run]

  Or via env var:
  CLOSE_API_KEY=... NOTE_DATE=2026-06-02 python add_webinar_chat_notes.py <csv_path>

NOTE: Not idempotent. Re-running this on the same CSV creates duplicate notes.
Use --dry-run first to preview.
"""

import argparse
import csv
import os
import sys
import time
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
NOTE_COL_CANDIDATES = ("Webinar Chat Notes", "Chat Responses", "Notes")


def find_note_column(fieldnames):
    for c in NOTE_COL_CANDIDATES:
        if c in fieldnames:
            return c
    sys.exit(
        f"CSV must contain one of: {', '.join(NOTE_COL_CANDIDATES)}. "
        f"Got columns: {fieldnames}"
    )


def lookup_lead_by_email(email):
    """Return the lead_id for the first lead with a contact at this email, or None."""
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


def post_note(lead_id, body):
    r = requests.post(
        "https://api.close.com/api/v1/activity/note/",
        auth=(CLOSE_API_KEY, ""),
        json={"lead_id": lead_id, "note": body},
        timeout=30,
    )
    r.raise_for_status()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", nargs="?", default="webinar_chat_notes.csv")
    parser.add_argument("--date", default=os.environ.get("NOTE_DATE"),
                        help="Webinar date for the note header (YYYY-MM-DD)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview without posting")
    args = parser.parse_args()

    if not args.date:
        sys.exit("Provide --date YYYY-MM-DD (or set NOTE_DATE env var)")

    note_header = f"Webinar Chat Notes — {args.date}"

    with open(args.csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    note_col = find_note_column(fieldnames)
    has_lead_id = "Lead ID" in fieldnames
    has_email = "Email" in fieldnames
    if not (has_lead_id or has_email):
        sys.exit("CSV must contain 'Lead ID' or 'Email' column (or both)")

    mode = "DRY RUN — nothing will be posted" if args.dry_run else "LIVE"
    print(f"[{mode}] {len(rows)} rows from {args.csv_path}")
    print(f"Note column: '{note_col}'  |  Header: '{note_header}'")
    print(f"Lookup mode: {'Lead ID + Email fallback' if has_lead_id and has_email else ('Lead ID only' if has_lead_id else 'Email only')}")
    print()

    ok = looked_up = no_match = fail = 0

    for i, row in enumerate(rows, 1):
        name = row.get("Name", "").strip() or "(no name)"
        notes = row.get(note_col, "").strip()

        lead_id = row.get("Lead ID", "").strip() if has_lead_id else ""
        email = row.get("Email", "").strip() if has_email else ""
        resolved_via = "lead_id" if lead_id else None

        try:
            if not lead_id and email:
                lead_id = lookup_lead_by_email(email)
                if lead_id:
                    resolved_via = "email"
                    looked_up += 1
                else:
                    print(f"[{i}/{len(rows)}] ⚠  {name} — no lead found for {email}")
                    no_match += 1
                    continue

            if not lead_id:
                print(f"[{i}/{len(rows)}] ⚠  {name} — no Lead ID or Email in row")
                no_match += 1
                continue

            body = f"{note_header}\n\n{notes}"

            if args.dry_run:
                print(f"[{i}/{len(rows)}] 👀 {name} ({lead_id}, via {resolved_via})")
            else:
                post_note(lead_id, body)
                print(f"[{i}/{len(rows)}] ✅ {name} ({lead_id}, via {resolved_via})")
            ok += 1

        except Exception as e:
            print(f"[{i}/{len(rows)}] ❌ {name} — {e}")
            fail += 1

        time.sleep(0.15)

    print()
    print(f"Summary: {ok} {'previewed' if args.dry_run else 'posted'}"
          f"  |  {looked_up} resolved by email"
          f"  |  {no_match} no-match"
          f"  |  {fail} failed")
    if args.dry_run:
        print("(DRY RUN — nothing was actually posted. Re-run without --dry-run to send.)")


if __name__ == "__main__":
    main()
