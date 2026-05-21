#!/usr/bin/env python3
"""
test_lead_attachments.py — One-off test

Inspects a single lead in Close and reports every file attachment found
across its Note and Email activities. The "Files" tab in the Close UI
aggregates attachments from these activity types — there is no separate
file resource on the lead itself.

Usage:
    CLOSE_API_KEY=... LEAD_ID=lead_xxx python3 test_lead_attachments.py

Or pass the lead ID as the first arg:
    CLOSE_API_KEY=... python3 test_lead_attachments.py lead_xxx

Output (stdout):
    - Lead name + status confirmation
    - Each Note / Email activity with attachment count, filenames,
      sizes, and content types
    - Summary count at the end

This is purely a read-only diagnostic — it does not modify any data.
"""

import os
import re
import sys
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
raw_input_id = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("LEAD_ID", "")).strip()

# Accept either a bare lead ID (lead_xxx) or a full Close URL pasted in.
# Extract the lead_... token regardless of what the user pastes.
match = re.search(r"lead_[A-Za-z0-9]+", raw_input_id)
LEAD_ID = match.group(0) if match else ""

if not LEAD_ID:
    print(f"❌ No valid lead ID found in input: {raw_input_id!r}")
    print("   Expected something like: lead_WjclPeKa9QGzQ27RBvIoUhE6JVFWAdLjSJOPnjweSsd")
    print("   (Full Close URLs are also fine — the script will extract the ID.)")
    sys.exit(1)

BASE = "https://api.close.com/api/v1"
AUTH = (CLOSE_API_KEY, "")


def fmt_size(num_bytes):
    if num_bytes is None:
        return "?"
    for unit in ("B", "KB", "MB", "GB"):
        if num_bytes < 1024:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.1f} TB"


def get_lead(lead_id):
    r = requests.get(f"{BASE}/lead/{lead_id}/", auth=AUTH, timeout=30)
    r.raise_for_status()
    return r.json()


def get_activities(lead_id, activity_type):
    """Paginate all activities of a given type for the lead."""
    results = []
    skip = 0
    limit = 100
    while True:
        r = requests.get(
            f"{BASE}/activity/{activity_type.lower()}/",
            auth=AUTH,
            params={"lead_id": lead_id, "_limit": limit, "_skip": skip},
            timeout=30,
        )
        r.raise_for_status()
        payload = r.json()
        results.extend(payload.get("data", []))
        if not payload.get("has_more"):
            break
        skip += limit
    return results


def describe_activity(activity, activity_type):
    """Print attachments for one activity."""
    attachments = activity.get("attachments") or []
    activity_id = activity.get("id", "?")
    date = activity.get("date_created", "?")[:10]
    user = activity.get("user_name") or activity.get("user_id") or "?"

    header = f"  [{activity_type}] {activity_id}  ·  {date}  ·  by {user}"
    if not attachments:
        print(f"{header}  →  no attachments")
        return 0

    print(f"{header}  →  {len(attachments)} attachment(s):")
    for i, att in enumerate(attachments, 1):
        filename = att.get("filename", "?")
        content_type = att.get("content_type", "?")
        size = fmt_size(att.get("size"))
        url = att.get("url", "")
        print(f"     {i}. {filename}")
        print(f"        type: {content_type}   size: {size}")
        if url:
            print(f"        url:  {url}")
    return len(attachments)


def main():
    print(f"🔍 Inspecting lead: {LEAD_ID}")
    print("=" * 70)

    # 1. Confirm the lead exists and show basic info
    try:
        lead = get_lead(LEAD_ID)
    except requests.HTTPError as e:
        print(f"❌ Could not fetch lead: {e}")
        sys.exit(1)

    print(f"Lead name:     {lead.get('display_name') or lead.get('name', '?')}")
    print(f"Lead status:   {lead.get('status_label', '?')}")
    print(f"URL:           {lead.get('url', '?')}")
    print()

    # 2. Pull Note activities
    print("📝 Note activities")
    print("-" * 70)
    notes = get_activities(LEAD_ID, "Note")
    note_attachment_count = 0
    if not notes:
        print("  (no notes on this lead)")
    else:
        for n in notes:
            note_attachment_count += describe_activity(n, "Note")
    print()

    # 3. Pull Email activities
    print("✉️  Email activities")
    print("-" * 70)
    emails = get_activities(LEAD_ID, "Email")
    email_attachment_count = 0
    if not emails:
        print("  (no emails on this lead)")
    else:
        for e in emails:
            email_attachment_count += describe_activity(e, "Email")
    print()

    # 4. Summary
    total = note_attachment_count + email_attachment_count
    print("=" * 70)
    print("📊 Summary")
    print(f"  Notes:        {len(notes)} ({note_attachment_count} attachments)")
    print(f"  Emails:       {len(emails)} ({email_attachment_count} attachments)")
    print(f"  TOTAL FILES:  {total}")
    print()

    if total == 0:
        print("⚠️  No attachments found on this lead.")
        print("   In a real alert scenario, this rep would get a 'missing contract' nudge.")
    else:
        print(f"✅ Found {total} attachment(s). Visible via API, filenames captured.")


if __name__ == "__main__":
    main()
