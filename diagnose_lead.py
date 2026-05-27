#!/usr/bin/env python3
"""
diagnose_lead.py — Deep diagnostic for one specific lead.

Walks through exactly what update_followups.py would see and do for a lead:
  1) All meetings on the lead (each with a classifier verdict)
  2) The earliest qualifying first sales call (if any)
  3) The follow-ups it would stamp (sorted by starts_at, earliest 3)
  4) All currently-populated custom fields on the lead with names

Usage:
    CLOSE_API_KEY=xxx python diagnose_lead.py <lead_id>
"""

import os
import sys
from datetime import datetime

import requests

# Pull the actual classifier from the live script
from update_followups import (
    is_followup_title,
    is_qualifying_first_sales_call,
    parse_iso,
    to_pacific_date,
    EXCLUDED_OWNERS,
    FIELD_FOLLOWUP_1, FIELD_FOLLOWUP_2, FIELD_FOLLOWUP_3,
)

LEAD_ID = sys.argv[1] if len(sys.argv) > 1 else "lead_pVI5lg5LgOnoiT8WjUIcZ68j1Avr2KSoi6thCDktYGi"

key = os.environ.get("CLOSE_API_KEY")
if not key:
    sys.exit("ERROR: set CLOSE_API_KEY")

BASE_URL = "https://api.close.com/api/v1"
s = requests.Session()
s.auth = (key, "")

# ---------------------------------------------------------------------------
# 1. User map for owner-name lookup
# ---------------------------------------------------------------------------
print(f"Lead: {LEAD_ID}")
print("=" * 90)
print("\n[1] Fetching org users...")
r = s.get(f"{BASE_URL}/user/", timeout=30)
r.raise_for_status()
user_map = {}
for u in r.json().get("data", []):
    name = (
        u.get("display_name")
        or " ".join(filter(None, [u.get("first_name"), u.get("last_name")])).strip()
        or u.get("email")
        or ""
    )
    user_map[u["id"]] = name
print(f"    {len(user_map)} users")

# ---------------------------------------------------------------------------
# 2. All meetings on the lead (scoped query)
# ---------------------------------------------------------------------------
print(f"\n[2] Fetching meetings for {LEAD_ID}...")
meetings = []
skip = 0
while True:
    r = s.get(
        f"{BASE_URL}/activity/meeting/",
        params={"lead_id": LEAD_ID, "_limit": 100, "_skip": skip},
        timeout=30,
    )
    r.raise_for_status()
    j = r.json()
    batch = j.get("data", [])
    meetings.extend(batch)
    if not j.get("has_more") or len(batch) < 100:
        break
    skip += 100
print(f"    {len(meetings)} meetings on this lead")

if not meetings:
    print("\n  ⚠  No meetings found on this lead at all.")
    sys.exit(0)

# Sort by starts_at for readability
meetings.sort(key=lambda m: m.get("starts_at") or "")

# ---------------------------------------------------------------------------
# 3. Classify each meeting
# ---------------------------------------------------------------------------
print(f"\n[3] Per-meeting classification:")
print(f"    {'STARTS_AT (UTC)':<28} {'OWNER':<22} {'FSC?':<6} {'F/U?':<6} TITLE")
print("    " + "-" * 110)

first_sales_dt = None
followup_times = []
for m in meetings:
    title = m.get("title", "") or ""
    owner_id = m.get("user_id", "")
    owner_name = user_map.get(owner_id, owner_id or "(none)")
    starts_at_raw = m.get("starts_at") or ""
    starts_at = parse_iso(starts_at_raw)

    is_fsc = is_qualifying_first_sales_call(title, owner_name)
    is_fu = (
        starts_at is not None
        and owner_name not in EXCLUDED_OWNERS
        and is_followup_title(title)
    )

    if is_fsc and starts_at and (first_sales_dt is None or starts_at < first_sales_dt):
        first_sales_dt = starts_at

    fsc_mark = "✓" if is_fsc else ""
    fu_mark = "✓" if is_fu else ""
    print(f"    {starts_at_raw:<28} {owner_name[:22]:<22} {fsc_mark:<6} {fu_mark:<6} {title!r}")

# ---------------------------------------------------------------------------
# 4. What would the script do?
# ---------------------------------------------------------------------------
print(f"\n[4] Script verdict:")
if first_sales_dt is None:
    print("    ✗ NO qualifying first sales call found → lead would be SKIPPED entirely")
else:
    print(f"    ✓ Earliest first sales call: {first_sales_dt.isoformat()}")
    # Collect followups after the FSC
    for m in meetings:
        title = m.get("title", "") or ""
        owner_name = user_map.get(m.get("user_id", ""), "")
        starts_at = parse_iso(m.get("starts_at"))
        if not starts_at:
            continue
        if owner_name in EXCLUDED_OWNERS:
            continue
        if not is_followup_title(title):
            continue
        if starts_at <= first_sales_dt:
            continue
        followup_times.append((starts_at, title))
    followup_times.sort(key=lambda x: x[0])
    if not followup_times:
        print("    ✗ No follow-ups after the first sales call → no fields would be stamped")
    else:
        print(f"    ✓ {len(followup_times)} follow-up(s) after first sales call:")
        for dt, title in followup_times[:3]:
            print(f"        • {to_pacific_date(dt)} (PT) — {title!r}")
        if len(followup_times) > 3:
            print(f"        … and {len(followup_times) - 3} more (would be ignored)")

# ---------------------------------------------------------------------------
# 5. Custom field schema & current values on the lead
# ---------------------------------------------------------------------------
print(f"\n[5] Custom field schema → lead value lookup")

schema = {}
skip = 0
while True:
    r = s.get(f"{BASE_URL}/custom_field/lead/", params={"_limit": 100, "_skip": skip}, timeout=30)
    r.raise_for_status()
    j = r.json()
    for f in j.get("data", []):
        schema[f["id"]] = {"name": f.get("name"), "type": f.get("type")}
    if not j.get("has_more") or len(j.get("data", [])) < 100:
        break
    skip += 100
print(f"    {len(schema)} custom fields defined on Lead\n")

r = s.get(f"{BASE_URL}/lead/{LEAD_ID}/", timeout=30)
r.raise_for_status()
lead = r.json()

# Investigate the actual key format Close uses for this lead's custom fields
print(f"    Total keys in lead response: {len(lead.keys())}")
cf_prefix = sum(1 for k in lead if k.startswith("cf_"))
custom_prefix = sum(1 for k in lead if k.startswith("custom."))
print(f"    Keys with 'cf_' prefix:      {cf_prefix}")
print(f"    Keys with 'custom.' prefix:  {custom_prefix}")

# Show a sample of each format that exists
samples_cf = sorted([k for k in lead if k.startswith("cf_")])[:5]
samples_custom = sorted([k for k in lead if k.startswith("custom.")])[:5]
if samples_cf:
    print(f"\n    Sample 'cf_*' keys:")
    for k in samples_cf:
        v = lead[k]
        if isinstance(v, str) and len(v) > 60:
            v = v[:60] + "..."
        print(f"      {k} = {v!r}")
if samples_custom:
    print(f"\n    Sample 'custom.*' keys:")
    for k in samples_custom:
        v = lead[k]
        if isinstance(v, str) and len(v) > 60:
            v = v[:60] + "..."
        print(f"      {k} = {v!r}")

# Look up the three target follow-up fields in BOTH possible formats
print(f"\n    Target follow-up field lookups (in both formats):")
target_ids = [(FIELD_FOLLOWUP_1, "FOLLOWUP_1"), (FIELD_FOLLOWUP_2, "FOLLOWUP_2"), (FIELD_FOLLOWUP_3, "FOLLOWUP_3")]
for fid, label in target_ids:
    meta = schema.get(fid, {})
    name = meta.get("name", "(not in schema)")
    ftype = meta.get("type", "?")
    val_cf = lead.get(fid)
    val_custom = lead.get(f"custom.{fid}")
    print(f"      {label}: {name!r} (type: {ftype})")
    print(f"        as '{fid}'         → {val_cf!r}")
    print(f"        as 'custom.{fid}'  → {val_custom!r}")

# Schema validity check
print()
for fid, label in target_ids:
    if fid not in schema:
        print(f"    ⚠  {label} ({fid}) is NOT a valid Lead custom field ID")
    else:
        meta = schema[fid]
        print(f"    ✓ {label} → {meta.get('name')!r} (type: {meta.get('type')})")
