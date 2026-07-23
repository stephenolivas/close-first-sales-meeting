#!/usr/bin/env python3
"""
parallel_check.py — Daily field-vs-outcome show-rate comparison
================================================================

Read-only. Computes, over the trailing WINDOW_DAYS of *completed* days
(Pacific), for first sales calls:

  field_rate    First Call Show Up (Opp) [+ Override]:  Yes / (Yes + No)
  outcome_rate  matched meeting outcome:  Completed / (Completed + No Show)
  coverage      % of matched first-call meetings with a terminal outcome
  agreement     where BOTH field and outcome exist: % that say the same thing

These are the cutover gates from claude/DECISION_meetingoutcomes.md:
coverage >= 95% and agreement >= 98% for 2 consecutive weeks.

Method: fetch meetings in window -> identify each lead's first-call meeting
on its FSCBD date (same matching rules as backfill_outcomes.py) -> pair the
lead's show field with that meeting's outcome. Leads whose FSCBD has no
matching meeting activity (data drift) are excluded and counted separately.

ENV: CLOSE_API_KEY (required) | WINDOW_DAYS (default 14)
Output: printed table + parallel_report.json (+ disagreements list w/ links)

Usage:
  python parallel_check.py
  python parallel_check.py --selftest
"""

import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

import requests

PACIFIC = ZoneInfo("America/Los_Angeles")
CLOSE_API = "https://api.close.com/api/v1"

OUTCOMES = {
    "scheduled":   "outcome_032DjlzDKpdXJZOzK4f7q3",
    "completed":   "outcome_032Djn4dfeNuEoCunojA7K",
    "rescheduled": "outcome_032Djo72GJ2Lvw3Q296wxH",
    "no_show":     "outcome_032DjoyPo9BgPBdOF6DzqH",
    "cancelled":   "outcome_032DjpoQ9otqb8rGb7SIYt",
}
ID_TO_KEY = {v: k for k, v in OUTCOMES.items()}

CF = {
    "fscbd":    "custom.cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq",
    "show":     "custom.cf_OPyvpU45RdvjLqfm8V1VWwNxrGKogEH2IBJmfCj0Uhq",
    "show_ovr": "custom.cf_CJMktLJShTyA86PdBqNUP59ZfJh0WpdB1tEt76Y3HEy",
}

# SYNC WITH backfill_outcomes.py / update_field.py
SALES_TITLE_RE = re.compile("|".join([
    r"vending strategy call",
    r"vendingpren[eu]+rs?\s+consultation",
    r"vendingpren[eu]+rs?\s+strategy call",
    r"new vendingpreneur strategy call",
    r"vending consult",
    r"post masterclass strategy call",
    r"vending route consultation",
    r"cash[- ]?flowing vending route advisory interview",
    r"vending route advisory call",
    r"vendingpren[eu]+rs?\s*-?\s*next steps",
    r"vendingpreneur next steps",
]), re.IGNORECASE)
FOLLOWUP_RE = re.compile(r"follow[\s-]?up|fallow up|f/u", re.IGNORECASE)

WINDOW_DAYS = int(os.environ.get("WINDOW_DAYS", "14"))

# ---------------------------------------------------------------------------

def close_session():
    s = requests.Session()
    s.auth = (os.environ["CLOSE_API_KEY"], "")
    s.headers["Content-Type"] = "application/json"
    return s


def close_get(s, path, params=None):
    for _ in range(5):
        r = s.get(f"{CLOSE_API}{path}", params=params, timeout=60)
        if r.status_code == 429:
            time.sleep(float(r.headers.get("Retry-After", 2)) + 0.5)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GET {path}: rate-limited")


def fetch_meetings(s, since_dt, until_dt):
    fields = "id,lead_id,title,starts_at,status,outcome_id"
    out, skip = [], 0
    while True:
        data = close_get(s, "/activity/meeting/",
                         {"_skip": skip, "_limit": 100, "_fields": fields})
        rows = data["data"]
        if not rows:
            break
        page_all_old = True
        for m in rows:
            st = parse_dt(m.get("starts_at"))
            if st and st >= since_dt:
                page_all_old = False
                if st <= until_dt:
                    out.append(m)
        if page_all_old or not data.get("has_more"):
            break
        skip += 100
    return out

# ---------------------------------------------------------------------------
# Pure logic (selftested)
# ---------------------------------------------------------------------------

def parse_dt(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def parse_date(v):
    if not v:
        return None
    try:
        return date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def pacific_date(m):
    st = parse_dt(m.get("starts_at"))
    return st.astimezone(PACIFIC).date() if st else None


def is_canceledish(m):
    t = (m.get("title") or "").strip().lower()
    return m.get("status") == "canceled" or t.startswith("canceled")


def first_call_meeting(fscbd, meetings):
    day = [m for m in meetings if pacific_date(m) == fscbd and not is_canceledish(m)]
    if not day:
        return None
    sales = [m for m in day if SALES_TITLE_RE.search(m.get("title") or "")
             and not FOLLOWUP_RE.search(m.get("title") or "")]
    pool = sales or ([day[0]] if len(day) == 1 else [])
    return min(pool, key=lambda m: m["starts_at"]) if pool else None


def field_verdict(lead_fields):
    for key in ("show_ovr", "show"):
        v = lead_fields.get(CF[key])
        if v is not None:
            v = str(v).strip().lower()
            if v in ("yes", "no"):
                return "completed" if v == "yes" else "no_show"
    return None


def outcome_verdict(meeting):
    key = ID_TO_KEY.get(meeting.get("outcome_id") or "")
    return key if key in ("completed", "no_show") else None
    # rescheduled/cancelled first-calls are excluded from show-rate math on
    # both sides (mirrors the EOD formula's reschedule exclusion)


def compare(pairs):
    """
    pairs: [{'day','field','outcome','terminal'}] one per matched first call.
    -> dict of totals + per-day rows.
    """
    def rate(xs, kind):
        num = sum(1 for x in xs if x[kind] == "completed")
        den = sum(1 for x in xs if x[kind] in ("completed", "no_show"))
        return (num / den * 100) if den else None, den

    both = [x for x in pairs if x["field"] and x["outcome"]]
    agree = sum(1 for x in both if x["field"] == x["outcome"])
    cov_den = len(pairs)
    cov_num = sum(1 for x in pairs if x["terminal"])

    days = defaultdict(list)
    for x in pairs:
        days[x["day"]].append(x)
    per_day = []
    for day in sorted(days):
        xs = days[day]
        fr, fd = rate(xs, "field")
        orate, od = rate(xs, "outcome")
        per_day.append({"day": str(day), "n": len(xs),
                        "field_rate": fr, "field_n": fd,
                        "outcome_rate": orate, "outcome_n": od,
                        "covered": sum(1 for x in xs if x["terminal"])})

    fr, fd = rate(pairs, "field")
    orate, od = rate(pairs, "outcome")
    return {
        "first_calls_matched": cov_den,
        "field_rate": fr, "field_denominator": fd,
        "outcome_rate": orate, "outcome_denominator": od,
        "coverage_pct": (cov_num / cov_den * 100) if cov_den else None,
        "agreement_pct": (agree / len(both) * 100) if both else None,
        "both_sides_n": len(both),
        "per_day": per_day,
    }

# ---------------------------------------------------------------------------

def run():
    now = datetime.now(timezone.utc)
    today_pac = now.astimezone(PACIFIC).date()
    end_day = today_pac - timedelta(days=1)          # completed days only
    start_day = end_day - timedelta(days=WINDOW_DAYS - 1)
    since = datetime.combine(start_day, datetime.min.time(), PACIFIC).astimezone(timezone.utc)
    until = datetime.combine(end_day + timedelta(days=1), datetime.min.time(), PACIFIC).astimezone(timezone.utc)

    s = close_session()
    print(f"parallel_check: {start_day}..{end_day} ({WINDOW_DAYS} days)")

    meetings = fetch_meetings(s, since, until)
    by_lead = defaultdict(list)
    for m in meetings:
        if m.get("lead_id"):
            by_lead[m["lead_id"]].append(m)

    fields_param = "id," + ",".join(CF.values())
    pairs, drift, disagreements = [], 0, []
    for lead_id, ms in by_lead.items():
        lead = close_get(s, f"/lead/{lead_id}/", {"_fields": fields_param})
        fscbd = parse_date(lead.get(CF["fscbd"]))
        if not fscbd or not (start_day <= fscbd <= end_day):
            continue
        m = first_call_meeting(fscbd, ms)
        if m is None:
            drift += 1
            continue
        f, o = field_verdict(lead), outcome_verdict(m)
        cur = m.get("outcome_id")
        pairs.append({"day": fscbd, "field": f, "outcome": o,
                      "terminal": bool(cur and cur != OUTCOMES["scheduled"])})
        if f and o and f != o:
            disagreements.append({"lead": lead_id, "meeting": m["id"],
                                  "day": str(fscbd), "field": f, "outcome": o,
                                  "url": f"https://app.close.com/lead/{lead_id}/"})

    result = compare(pairs)
    result["fscbd_without_meeting"] = drift
    result["disagreements"] = disagreements

    fmt = lambda v: f"{v:5.1f}%" if v is not None else "  n/a "
    print(f"\nfirst calls matched : {result['first_calls_matched']} "
          f"(+{drift} FSCBD-without-meeting excluded)")
    print(f"field show rate     : {fmt(result['field_rate'])}  (n={result['field_denominator']})")
    print(f"outcome show rate   : {fmt(result['outcome_rate'])}  (n={result['outcome_denominator']})")
    print(f"COVERAGE            : {fmt(result['coverage_pct'])}   (gate: >=95%)")
    print(f"AGREEMENT           : {fmt(result['agreement_pct'])}   (gate: >=98%, n={result['both_sides_n']})")
    print(f"disagreements       : {len(disagreements)}")
    for x in disagreements[:20]:
        print(f"  {x['day']} field={x['field']} outcome={x['outcome']} {x['url']}")
    print("\nper-day:")
    for r in result["per_day"]:
        print(f"  {r['day']}  n={r['n']:3d}  field={fmt(r['field_rate'])} "
              f" outcome={fmt(r['outcome_rate'])}  covered={r['covered']}/{r['n']}")

    with open("parallel_report.json", "w") as fh:
        json.dump({"generated_at": now.isoformat(),
                   "window": [str(start_day), str(end_day)], **result}, fh, indent=2)
    print("\nreport: parallel_report.json")
    return 0

# ---------------------------------------------------------------------------

def selftest():
    d1, d2 = date(2026, 7, 20), date(2026, 7, 21)
    pairs = [
        {"day": d1, "field": "completed", "outcome": "completed", "terminal": True},
        {"day": d1, "field": "no_show",   "outcome": "no_show",   "terminal": True},
        {"day": d1, "field": "completed", "outcome": None,        "terminal": False},  # blank outcome
        {"day": d2, "field": "completed", "outcome": "no_show",   "terminal": True},   # disagreement
        {"day": d2, "field": None,        "outcome": "completed", "terminal": True},   # field blank
        {"day": d2, "field": "no_show",   "outcome": None,        "terminal": True},   # resched outcome
    ]
    r = compare(pairs)
    checks = [
        ("coverage 5/6", abs(r["coverage_pct"] - 5/6*100) < 0.01),
        ("agreement 2/3", abs(r["agreement_pct"] - 2/3*100) < 0.01),
        ("field rate 3/5", abs(r["field_rate"] - 60.0) < 0.01),
        ("outcome rate 2/4", abs(r["outcome_rate"] - 50.0) < 0.01),
        ("two days", len(r["per_day"]) == 2),
        ("day1 n=3", r["per_day"][0]["n"] == 3),
    ]
    # matching helpers
    m1 = {"id": "m1", "title": "Vending Strategy Call",
          "starts_at": "2026-07-20T17:00:00+00:00", "status": "completed",
          "outcome_id": OUTCOMES["completed"]}
    fu = {"id": "m2", "title": "Vending Strategy Call Follow-Up",
          "starts_at": "2026-07-20T18:00:00+00:00", "status": "completed",
          "outcome_id": None}
    checks.append(("picks sales not fu",
                   first_call_meeting(d1, [fu, m1])["id"] == "m1"))
    checks.append(("outcome verdict completed", outcome_verdict(m1) == "completed"))
    m1r = dict(m1, outcome_id=OUTCOMES["rescheduled"])
    checks.append(("resched excluded", outcome_verdict(m1r) is None))
    checks.append(("override wins",
                   field_verdict({CF["show_ovr"]: "No", CF["show"]: "Yes"}) == "no_show"))

    failed = [n for n, ok in checks if not ok]
    for n, ok in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {n}")
    print(f"\n{len(checks) - len(failed)}/{len(checks)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    sys.exit(run())
