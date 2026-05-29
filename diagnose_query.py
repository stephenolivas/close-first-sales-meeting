"""
diagnose_query.py

One-time diagnostic: try multiple query formats against the Close API to find
which one correctly filters by the First Sales Call Booked Date custom field.

We know Thursday 2026-05-28 has 4 Lost leads. Whichever query returns 4 wins.

Run:
    CLOSE_API_KEY=your_key python diagnose_query.py
"""

import os
import sys
import requests

CLOSE_API_KEY = os.environ["CLOSE_API_KEY"]
BASE = "https://api.close.com/api/v1"
AUTH = (CLOSE_API_KEY, "")

FIELD = "cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq"
FIELD_ID = FIELD.removeprefix("cf_")
LOST_LABEL = "💔 Lost"
KNOWN_DATE_ISO = "2026-05-28"
KNOWN_DATE_US = "05/28/2026"
NEXT_DAY_ISO = "2026-05-29"


def get_lost_status_id():
    r = requests.get(f"{BASE}/status/lead/", auth=AUTH)
    r.raise_for_status()
    for s in r.json()["data"]:
        if s["label"] == LOST_LABEL:
            return s["id"]
    sys.exit("Could not find Lost status")


def try_dsl(label, query):
    """Try the GET /lead/?query=... endpoint with a DSL string."""
    try:
        r = requests.get(
            f"{BASE}/lead/",
            params={"query": query, "_fields": "id,display_name", "_limit": 50},
            auth=AUTH,
        )
        r.raise_for_status()
        data = r.json()
        count = data.get("total_results", len(data.get("data", [])))
        names = [d.get("display_name", "?") for d in data.get("data", [])[:6]]
        return count, names, None
    except requests.HTTPError as e:
        return None, [], f"{e.response.status_code} {e.response.text[:200]}"


def try_structured(label, query_payload):
    """Try the POST /data/search/ endpoint with a structured query."""
    try:
        r = requests.post(
            f"{BASE}/data/search/",
            json={
                "query": query_payload,
                "_fields": {"lead": ["id", "display_name"]},
                "results_limit": 50,
            },
            auth=AUTH,
        )
        r.raise_for_status()
        data = r.json()
        count = len(data.get("data", []))
        names = [d.get("display_name", "?") for d in data.get("data", [])[:6]]
        return count, names, None
    except requests.HTTPError as e:
        return None, [], f"{e.response.status_code} {e.response.text[:200]}"


def report(label, count, names, error):
    if error:
        print(f"  ❌ {label}: ERROR — {error}")
    elif count == 4:
        print(f"  ✅ {label}: {count} results  ← WINNER")
        print(f"      {names}")
    else:
        print(f"  ⚠️  {label}: {count} results")
        if names:
            print(f"      {names}")


def main():
    stat_id = get_lost_status_id()
    print(f"Lost status_id: {stat_id}")
    print(f"Looking for 4 Lost leads with first sales call = {KNOWN_DATE_ISO}\n")

    print("--- Sanity: just the Lost status filter (should be a lot of leads) ---")
    c, n, e = try_dsl("status only", f'lead_status_id:"{stat_id}"')
    report("status only (DSL)", c, n, e)
    print()

    print("--- DSL endpoint, various date formats ---")
    variants = [
        ("ISO + quotes",        f'custom.{FIELD}:"{KNOWN_DATE_ISO}" and lead_status_id:"{stat_id}"'),
        ("ISO no quotes",       f'custom.{FIELD}:{KNOWN_DATE_ISO} and lead_status_id:"{stat_id}"'),
        ("US + quotes",         f'custom.{FIELD}:"{KNOWN_DATE_US}" and lead_status_id:"{stat_id}"'),
        ("US no quotes",        f'custom.{FIELD}:{KNOWN_DATE_US} and lead_status_id:"{stat_id}"'),
        (">= and < range ISO",  f'custom.{FIELD}>="{KNOWN_DATE_ISO}" and custom.{FIELD}<"{NEXT_DAY_ISO}" and lead_status_id:"{stat_id}"'),
        ("range ..",            f'custom.{FIELD}:"{KNOWN_DATE_ISO}".."{NEXT_DAY_ISO}" and lead_status_id:"{stat_id}"'),
    ]
    for label, q in variants:
        c, n, e = try_dsl(label, q)
        report(label, c, n, e)

    print("\n--- Structured /data/search/ endpoint, condition variations ---")
    structured_variants = [
        (
            "term, bare ID",
            {
                "type": "and",
                "queries": [
                    {
                        "type": "field_condition",
                        "field": {"type": "custom_field", "custom_field_id": FIELD_ID},
                        "condition": {"type": "term", "values": [KNOWN_DATE_ISO]},
                    },
                    {
                        "type": "field_condition",
                        "field": {"type": "regular_field", "object_type": "lead", "field_name": "status_id"},
                        "condition": {"type": "term", "values": [stat_id]},
                    },
                ],
            },
        ),
        (
            "term, with cf_ prefix",
            {
                "type": "and",
                "queries": [
                    {
                        "type": "field_condition",
                        "field": {"type": "custom_field", "custom_field_id": FIELD},
                        "condition": {"type": "term", "values": [KNOWN_DATE_ISO]},
                    },
                    {
                        "type": "field_condition",
                        "field": {"type": "regular_field", "object_type": "lead", "field_name": "status_id"},
                        "condition": {"type": "term", "values": [stat_id]},
                    },
                ],
            },
        ),
        (
            "moment_range",
            {
                "type": "and",
                "queries": [
                    {
                        "type": "field_condition",
                        "field": {"type": "custom_field", "custom_field_id": FIELD_ID},
                        "condition": {
                            "type": "moment_range",
                            "on_or_after": {"type": "fixed_local_date", "value": KNOWN_DATE_ISO, "which": "start"},
                            "before": {"type": "fixed_local_date", "value": NEXT_DAY_ISO, "which": "start"},
                        },
                    },
                    {
                        "type": "field_condition",
                        "field": {"type": "regular_field", "object_type": "lead", "field_name": "status_id"},
                        "condition": {"type": "term", "values": [stat_id]},
                    },
                ],
            },
        ),
    ]
    for label, q in structured_variants:
        c, n, e = try_structured(label, q)
        report(label, c, n, e)


if __name__ == "__main__":
    main()
