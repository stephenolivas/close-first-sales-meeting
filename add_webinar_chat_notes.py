#!/usr/bin/env python3
"""
add_webinar_chat_notes.py — One-off

Reads a CSV of (Name, Webinar Chat Notes, Lead ID) and posts each row
as a Note activity on the corresponding lead in Close.

Usage:
    CLOSE_API_KEY=... python add_webinar_chat_notes.py [csv_path]

Defaults to ./webinar_chat_notes.csv in the repo root.

NOTE: This is not idempotent. If you re-run it on the same CSV, you'll
get duplicate notes. Only run once per CSV.
"""

import csv
import os
import sys
import time
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "webinar_chat_notes.csv"
NOTE_HEADER = "Webinar Chat Notes — 2026-05-19"


def add_note(lead_id: str, body: str) -> None:
    r = requests.post(
        "https://api.close.com/api/v1/activity/note/",
        auth=(CLOSE_API_KEY, ""),
        json={"lead_id": lead_id, "note": body},
        timeout=30,
    )
    r.raise_for_status()


def main() -> None:
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Loaded {len(rows)} rows from {CSV_PATH}")

    ok = fail = 0
    for i, row in enumerate(rows, 1):
        name = row["Name"].strip()
        lead_id = row["Lead ID"].strip()
        notes = row["Webinar Chat Notes"].strip()
        body = f"{NOTE_HEADER}\n\n{notes}"

        try:
            add_note(lead_id, body)
            print(f"[{i}/{len(rows)}] ✅ {name} ({lead_id})")
            ok += 1
        except Exception as e:
            print(f"[{i}/{len(rows)}] ❌ {name} ({lead_id}) — {e}")
            fail += 1

        # Gentle pacing; Close allows ~7 rps but no need to push it for 20 rows
        time.sleep(0.2)

    print(f"\nDone. {ok} succeeded, {fail} failed.")


if __name__ == "__main__":
    main()
