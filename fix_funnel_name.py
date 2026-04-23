"""
Fix Funnel Name DEAL — One-Off Script
--------------------------------------
Targeted fix for 116 leads that have Scraper Funnel = YES but
Funnel Name DEAL was not updated to "Reactivation Scrapers" due to
the SCRAPER_FUNNEL_CUTOFF constant being missing from earlier deploys.

For each lead:
1. Fetch their meetings
2. Check if any scraper meeting exists on/after 2026-04-06
3. If yes, find the opportunity and write "Reactivation Scrapers"

Runtime: ~5-10 minutes
"""

import os
import re
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import requests

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

CLOSE_API_KEY         = os.environ["CLOSE_API_KEY"]
BASE_URL              = "https://api.close.com/api/v1"
PACIFIC               = ZoneInfo("America/Los_Angeles")
SLEEP                 = 0.5
SCRAPER_FUNNEL_CUTOFF = "2026-04-06"
OPP_FUNNEL_ID         = "cf_xqDQE8fkPsWa0RNEve7hcaxKblCe6489XeZGRDzyPdX"
OPP_FUNNEL_KEY        = f"custom.{OPP_FUNNEL_ID}"
OPP_FUNNEL_VALUE      = "Reactivation Scrapers"

# ─────────────────────────────────────────────
# All 116 leads with Scraper Funnel = YES
# but Funnel Name DEAL != "Reactivation Scrapers"
# ─────────────────────────────────────────────

LEAD_IDS = [
    "lead_fktuNBkdF7IZ54On4D0nqeTYWiJhQmgUhmVZBEKexKh",  # Tre
    "lead_wQKds5SOzkILjeqYTWcRNPM5zsPapkX13KIFTSmB4lv",  # Derrick Boddie
    "lead_ktVqbL9ApR6Q5wzZ4fDihfmOT8g1hQ4YKtnz3jMMkft",  # Stacie and Nina
    "lead_I3Ux81KDDApiG9JYBz6I4kRR3uhWJpgN5eGfQC1w7sK",  # Jeff Shiverdaker
    "lead_poE60toPDngR7JN5WL8h1FE1eGxj1xNERCtSmepqv33",  # Maria Campos
    "lead_iiD1tIQSKfDAjPEz162QpT9D2eIjt7mQbgkbzmrdEyC",  # Michael Eisner
    "lead_oKzjeRmkPO6bwju6NnNCSGjMeBa2j58deunzDlzRKlQ",  # Kevon Allen
    "lead_du5OnPABOvhbuiSGmfiz4btRijJ9u1klOh0GZH5KOK2",  # Patrick Vardaro
    "lead_RdpGxLywShgT9icE7SZHy8bAgmN5eCgVX9zLjOdGQgH",  # Santiago Leal
    "lead_sUZpJ4v32Zth8RPjeVj4stg7fjqY3Huc5JbwwwzilIo",  # Alex Parker
    "lead_atLZTgWlOsNGqjRRFpsrwISFWgQqFvmow9pBfByTNuD",  # Steven Forrester
    "lead_l68zkFVeRUQ9nt2e2xngjCB7I84sDDVPQ5o7uMGs4PZ",  # Stacie Refeld
    "lead_SZEpcd3VorU8TBQ6zOK6wGwvxiuhux6ya6gjFOkXpUx",  # Kaniya Allen
    "lead_S3um60JczmGu6wdoiya0R8ygib4M3gMxYj27bzEk3hd",  # Barbara Adway
    "lead_WqD60lkap99Qh9pmvrDhoBJeU15upIYCEoB3Q6hUUfz",  # Frank Rosales
    "lead_lUclJLUvCMDgp9BMBMxofUniSJSc2NZnVR1XLfJqwE1",  # Gregory Ayala
    "lead_GIb87B2aus3giyqbFr75xx1p6BVUWBpkkK2pwNGCKtZ",  # Rohin varghese
    "lead_wEmzKvXsXOm8Crx7tnud6Pb3kOZrJ1xuJxR1uyK2mWU",  # Adam McCrary
    "lead_1yrASadNiWuEC6BpAHn3StFV0TWEsVnLx6yw2Y4qGNX",  # Brandon Robinson
    "lead_C50F8gCns68Ix6yjN3DCMsF5c3sIxTKMlhaxokCRGOP",  # Thurmond
    "lead_EbaZfDz8c9vg1Zp0EIKl3qXkamvXzIyuIPHB6OO2OvH",  # Freddy
    "lead_fs2EGyeNM4ljbbFJjfdeFWhJcXiszfqz7VHmTFj8Hql",  # Ellis Hobbs
    "lead_M1sb0ZVGyQKeIpjWn8V0BPQu5fAljv0ZQ7oPJNJ8dpK",  # Dwayne OSullivan
    "lead_RYgu51fOLtCYZ7IopvRvl2nEeloPZb9H4zrIlefzBxz",  # Anissa & Daniel McCloud
    "lead_9k8phMjGFcMteg25n93oCH0exKA9NnOK3ngu9Fwpl9r",  # Gerald Ababio
    "lead_D8du61VZjZmKIHMfUiNySRU01HmwdDiUJrJU3HrjnL2",  # Jake Curtis
    "lead_C9ghUY4BGTyaTMqp0Fywg2g3ixOgdcJwuhcpSVzJ75q",  # Sabith Maliackal
    "lead_RsHct2E1ebnzkWKIMVu5cAfwvV4WmxgSzuJggA2KBLV",  # Darlene Torrence
    "lead_5HTwdvpd3dDhSvp8h6dj0FsTmOd3JYEcdkCFu0XZ7VX",  # Jenna Manavi
    "lead_KfDSumd5vJKHXJvQWSWCGQYNMABD6iFM51gukW5cmvu",  # Rozita Brown
    "lead_W1my151wL9VEv0PtYuGkM5jhhI99J5pejLbj6N8UzUC",  # Ashton Wonnick-gauthier
    "lead_iRnWIabr9jGegl07h4W03bDY7knFvzzWLtKqMKPGodf",  # Juwan May
    "lead_Z7HByb10xJXDkeRKDoCkZ1YIskJxBt7xSy9KduqQwrC",  # Elbert Belk
    "lead_wroSHhjftdFYeVijazfvyaxhtmDnY287jw97Ur4bI2T",  # Rengavittal Gururajan
    "lead_GTajKusoXiGhvLVoLxrkn9Lk4DfCzGItZ3C5nSH5czE",  # Omar Flores
    "lead_0xQP2dQFxkcAFfPniHFh8zuQGJP6xEzTD3z5JDd890p",  # Kenneth Thomas
    "lead_8ZoAoz70ATYdqykcMzN27bqSo4q6QM1L4c2wjItB5M5",  # Rich Cordero
    "lead_0HK1Eyn3pJPNBUobpqHMHIE5jlZXbkwO3OVISuAdQCW",  # Isaac Zetter
    "lead_aKDfYy8fmDMzBVhWkQm7An6n8nLfm06dGkKGHKoeFkn",  # Briana Connor
    "lead_30oFuAvf9vTa1jzcN1xnvOMKi2qPduiZ3ytqG06EBjI",  # Marisol Betancourt
    "lead_94iCGFZtr92dtK3qTZZ2Wm5zKQped4boXPmnNgdczjw",  # Olusola Owolabi
    "lead_nuY4q9cZ0WxsLhKpO300A4hyzAReQn5R0mQP0ivzH3d",  # Jared Miller
    "lead_gmVEz4xAjENI0yn2S3MUfL0h1cDf8xlhYiyRSLlkMfT",  # John Nava
    "lead_49dVfKnoAxjl4XNswqQxDM0qrrxgaIbgEAxMvyLcVXV",  # Marharyta
    "lead_JdIHH8fydSHzTWaMutGXNkmo5wzKa7wbI5y13cZExDS",  # Tobias Thomas
    "lead_ZLtGxai0fs8cTZVlTRf2hBaRI674vSNds2bAhH8jhbj",  # Peter Mounayer
    "lead_OZlYGiLfvyHDnM4ieBHUBl7cr94KKeW7ZGcwysGrqi8",  # Michael Monks
    "lead_kW4Mmcee9wBObMO8l99djL7GJoiZV5Bx6a2REbnoc24",  # Joe Dahl
    "lead_p2STtIghGEEkA1PpDBAYnDevILxHvP94hPSrhDcnmnH",  # Toll Bridge Company
    "lead_Ah4DnhpVc3Evx3PForjc1d0clvAlqr3PfJRgJScHaKP",  # Rebekah Walker
    "lead_e74FM0LZmusyIaZILdiZEY64TkAIKPDU0Wc3TONo9QE",  # Katrina Guidry
    "lead_RB3UWCc8NYSWKSjqkDpzs9M8Tw1QripZMKZslmtQK2w",  # Brent Parsley
    "lead_WRYCyxvLhihvpVUTwFJn4ezIGUrYz9pppFps8tPF7cd",  # John Rimmer
    "lead_xz5zLzSWxdl23D1nWix8W1fMBIPquyePO8kllp3MuNA",  # Julie Lurie
    "lead_fg0WAEAiV2DR767mTZjqRBGta7jBqMf2414Qa2hCI6w",  # Caelenthya Moore
    "lead_e0QCUgWi7Zpf9jypCsGkyj9KR5MP3vEfppOWR7nAHXP",  # Carrissa Brown
    "lead_2vJzrRVB2GGnmYgWn9BKuWVc2uyBObJnBgqTgr1EXqO",  # Yohanny Merette
    "lead_14d3NCodHHztBcKkTszjpD9Zhmc6z9ZBBUyJCqAskE1",  # Malik Gay
    "lead_ir4IPM1dOX1cK63meh2QxCjMLe5MYRIOlCf1xXanRFr",  # Mark Phillips
    "lead_lnRZtJlxAyK0V6MFLYniWuc4PAHpJqrdB92ebvPRp1M",  # Jaquasha Carter
    "lead_0HKfKyaaMUfWdt5mnuapKHw7nA3S4Ve52rMt0MemIdV",  # Darrell Starks
    "lead_LY4E57NXddysAyuEjLKo69WWVJMDU4Qs8ieQ3COA4np",  # Felicia Crawford
    "lead_q9uXsF23OQ41mQRZVikJB82akCFSSU7F6diyr3ys0L0",  # Alex Sha
    "lead_PeFj8E2k9p3DYbAqhJoQOR6vTmV4XQmKzqr5YjTF7Qd",  # Margaret Jackson
    "lead_P0NXDXye1gJZQAf6Y94eSFXazRiU3QrRRA0oT8bOgQ1",  # Am Yisrael
    "lead_RXAxB86CeNMSFXAxoWTmX2v9De5J77Kx2IvFoL0kNQn",  # Micah Burkett
    "lead_AFNVfZNkvNSJm8JUk3pn2DrC8Nel1KWwR2NwpRIcMA7",  # Tereasa Cummings
    "lead_20UxyqAGHlIUFYlkDwWGc6fGApaH5mXMg4qO0Ol0kMy",  # Kendy Deane
    "lead_DE3zVj0DgRsabAsw24NzHeSDTyTGgVoKgP5a7eyCNhR",  # Talia White
    "lead_NedFNrWQA4k0PWRA7lMk5vJUc192jPRnLDm5vxLGPfK",  # Gean Huston
    "lead_bKI35ecwK5VUYOPoctPIjUit3EHjAuc4xbcVz55pvtw",  # Earl Scott
    "lead_XCPEoqWP2otH1FBFlDSwsupLuD70n1glGEyYLvrddiT",  # Jimmy Dixon
    "lead_GRYQRxRyax1R2rOYLCdYb9MgyDJZo17EwnMWVknRcY7",  # Edith Bryant
    "lead_xpuAc3pFNHZIdtUgnYLtaMaNeYVgNHvJK9IJo01v85c",  # Aniyah Braggs
    "lead_vs3OtFXX7tQLG7fmc9I2Zwj8mIfYDkzuNTOZfBMNMoa",  # Lucas Perez
    "lead_FG9ttuYVzYpBfiHqmgKHDsSVgMtops1KOoZGOOw0Y9s",  # William Stanton
    "lead_i9sxPl4UITPz30tlcf6oLojntgicqcbGKQ894AmFGzs",  # Javier Zetter Kick
    "lead_Gu3R6K9lwEnuJ3Kxzu3bGm4qSvmGio0onK1TkZugqnw",  # Tanner Morris
    "lead_Agr9LdZBAezNL7wrCDvwZbK0u60iEYYV8FbjUQ5skiS",  # Chris Wyller
    "lead_LR63cS9SAUCvpmrvRwdDBY36hF2GgvhZx2iUeBmDfHo",  # Kelly Forrest
    "lead_cE8G6sE0BT2IYPhUxlhEekEfom1Rk7D1dEbzdyEpfW2",  # Jade Salomon
    "lead_Ow7jNne1OD5jirUI6B3w0ZjrvYmKzvXLPOh11PjG9sf",  # Devean Toler
    "lead_O6ksnq4qsJA0liT8aVhgebBnvVAcJhDs1xo64duV8qf",  # Alpha School
    "lead_R6GLDvU3uVbHU1qGe49Qwc7lfzCIPrzzaH6SmI0vBiF",  # Ridia Quewon
    "lead_5FO3IdDbZ8HczUrd7FZ8kaIvpsIE7WdAdcnKJBVTNlT",  # William Thomas
    "lead_FGqZM3xgbGoRKUuOCbV0sYFHI0UFsfH4VLWDgpWhBrQ",  # Sean Randall
    "lead_01F04xk8jM4b1f3eo3mTLPm0cCg0I1vfe6nzkddQzcG",  # Olawale Olateju
    "lead_ILnCBe3mcaH9tk4PjUptr04A6QfCNfqnW5trTP6fYSQ",  # John Rimmer (2)
    "lead_FQvK2INENkMxnb1e5BZ4PkP4xeew5ZzAGDMPJK9x6GM",  # Ronnie Jabet
    "lead_wXKWcSWa89t2mcMu1kUyD8dmKTMzHxfBvqs13f3z3XH",  # Marcus Kidd
    "lead_XiAG2Lb07r2SFNwlog9Czl07pCcM6BFBviwXVqV6xFB",  # Seth Tamisiea
    "lead_yXzoFO8iwHWtJM61yXCHhaxXotL11ek6WmcMITLFqW0",  # Fernando Larrazabal
    "lead_mVFIQc8aR0u4FRliHd7wTduDOzHN23JWDK3jVn6Cd6F",  # Ronaldo
    "lead_qTnDlC5t3EzdXILxzPPF2LsjVvG0kBk16Sqpcs4R33I",  # Carmela Roberts
    "lead_tqgSKXVoMeODCoCnZoOPGecwdwey1Sk4Vl5MaFCG55H",  # Karim Arafa
    "lead_HvF8wdvHBD8ikGs6MsNylm1TR0AnzUWhsod8IJ7fXfg",  # Don Keating
    "lead_f3xUrFFJPkfuJd6QQT8TuFJwnMxELUZJNCsiSobyk92",  # Olasoji Ogunya
    "lead_vGLangQL3Rp2E6dbeXlSknIAV1CG9oFEas44KIm8RlX",  # Joel Gabriel
    "lead_iTyrufjHDMlsqVPELT8LCMcnVwm7XpY128XVCuUxRh6",  # Jesse Pulido
    "lead_LyaylUAWNPVPvVwlWwCdD3Z4FhOS2XfZGZwQTw8ZeiG",  # Daniel McFadden
    "lead_ZV1zjIwJX0AG3BjevHXzKWjrvYa3Zxzd94n6mM4P8ej",  # Luke Michael
    "lead_VYXG8SCYzLNWIk3aUMszAuqjUUfAkIzeJbZ8Z6HA7ye",  # Al Bowman
    "lead_BLsDDNJfWHnM0t3Qb6LJ1jhEvfXZqTlb4iFnGeLQVxl",  # Jeff Timmington
    "lead_Cn1e9XtJU4N89VIOBZZHlxZajqilWfjxx8K4CFzImTp",  # Yunus Hopkinson
    "lead_q09kkhdzZCkMm4QCNBqAG9RlrGlSsXdngi9h1PRakXb",  # Samuel Jewell
    "lead_EEZgsP0msQ5pBzF0IeyDhaMf3NmAwTYaZHJmVIgcmod",  # George Schwenzfeger
    "lead_7CVhrcDF1zB13khCMdTAyWKErZNyoxN6J9ncgpjwM7I",  # Jaimin Surti
    "lead_D5Z1TC6jfEl0Q2e4kE3vJWSeb83AifqHNqctLgAZ6XK",  # Cari Guzman
    "lead_WquIaqx00mmzjg0z1ULREH1tWpD8N4q8vwXAH6Vv0RT",  # Mike Rustemeyer
    "lead_upgt1veNHK4IZJkEKM5QhEjLjP8FbSuitUUMDtL3359",  # Ben Hession
    "lead_PCYvQAv3WC7uu3GTf1B8uZm8Fw0hfWBO4BbmyvSuZ7S",  # Elayna Eben
    "lead_nL0npF6MHgjOTwlJAhQnh535Kwsp2Q6io4tmQaHVmpR",  # Amanda Burns
    "lead_ehNgb2DyjxwlE0kbCqoeT4ecYmmaA15xtTPZ5CYe27T",  # Christina Lutz
    "lead_WiYwaiXx7JdLD4srECfXVHNW9KteLnI8aed0nwMNC0s",  # Myres CPA
    "lead_r4UDAFcBtIC83rbDrLaNpkU9sitU2uohVOGQiUXhhoh",  # John McLeod
    "lead_FxcjLf7MCqcoZM83RvPoexdxcYiwcmknfoYtR0t3k3l",  # Demerlin Nuesi
]

# ─────────────────────────────────────────────
# Scraper title patterns (same as update_field.py)
# ─────────────────────────────────────────────

EXCLUDED_OWNERS = {
    "user_5cZRqXu8kb4O1IeBVA98UMcMEhYZUhx1fnCHfSL0YMV",
    "user_yRF070m26JE67J6CJqzkAB3IqY7btNm1K5RisCglKa6",
}

SCRAPER_TITLE_MAP = [
    (re.compile(r"vendingpren[eu]+rs?\s+-\s+next\s+steps\s+call", re.IGNORECASE),       "Kristin Nelson"),
    (re.compile(r"vendingpren[eu]+rs?\s+next\s+steps\s+call", re.IGNORECASE),           "Vince Bartolini"),
    (re.compile(r"vendingpren[eu]+rs?\s+-\s+next\s+steps(?!\s+call)", re.IGNORECASE),   "Spencer Reynolds"),
    (re.compile(r"vendingpren[eu]+r\s+next\s+steps", re.IGNORECASE),                    "Mallory Kent"),
]


def is_scraper_meeting(title: str, user_id: str) -> bool:
    if user_id in EXCLUDED_OWNERS:
        return False
    for pattern, _ in SCRAPER_TITLE_MAP:
        if pattern.search(title):
            return True
    return False


def pacific_date(starts_at: str) -> str:
    dt_utc = datetime.fromisoformat(starts_at.replace("Z", "+00:00"))
    return dt_utc.astimezone(PACIFIC).strftime("%Y-%m-%d")


# ─────────────────────────────────────────────
# API
# ─────────────────────────────────────────────

session = requests.Session()
session.auth = (CLOSE_API_KEY, "")


def api_get(path, params=None, retry=5):
    url = f"{BASE_URL}{path}"
    for _ in range(retry):
        time.sleep(SLEEP)
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            print(f"  [rate limit] sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"GET {path} failed")


def api_put(path, payload, retry=5):
    url = f"{BASE_URL}{path}"
    for _ in range(retry):
        time.sleep(SLEEP)
        resp = session.put(url, json=payload, timeout=30)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 10))
            print(f"  [rate limit] sleeping {wait}s", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"PUT {path} failed")


def get_active_opportunity(lead_id: str) -> str | None:
    try:
        data = api_get("/opportunity/", params={
            "lead_id": lead_id,
            "_order_by": "-date_updated",
            "_fields": "id,status_type",
            "_limit": 10,
        })
        opps = data.get("data", [])
        for opp in opps:
            if opp.get("status_type") not in ("won", "lost"):
                return opp["id"]
        return opps[0]["id"] if opps else None
    except Exception as e:
        print(f"  WARNING: could not fetch opp: {e}", flush=True)
        return None


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    start = datetime.now(timezone.utc)
    print(
        f"═══════════════════════════════════════\n"
        f"Fix Funnel Name DEAL — {len(LEAD_IDS)} leads\n"
        f"Cutoff: {SCRAPER_FUNNEL_CUTOFF}\n"
        f"Started: {start.strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"═══════════════════════════════════════\n",
        flush=True,
    )

    updated      = 0
    skipped_date = 0
    skipped_noopp = 0
    skipped_nomtg = 0
    errors       = 0

    for i, lead_id in enumerate(LEAD_IDS, 1):
        try:
            # Fetch lead name for logging
            lead_data = api_get(f"/lead/{lead_id}/", params={"_fields": "id,display_name"})
            lead_name = lead_data.get("display_name", lead_id)

            # Fetch meetings for this lead
            mtg_data = api_get("/activity/meeting/", params={
                "lead_id": lead_id,
                "_fields": "id,title,starts_at,user_id",
                "_limit": 100,
            })
            meetings = mtg_data.get("data", [])

            # Find earliest qualifying scraper meeting on/after cutoff
            qualifying_dates = []
            for m in meetings:
                title    = (m.get("title") or "").strip()
                user_id  = m.get("user_id") or ""
                starts_at = m.get("starts_at")
                if not starts_at:
                    continue
                date = pacific_date(starts_at)
                if is_scraper_meeting(title, user_id) and date >= SCRAPER_FUNNEL_CUTOFF:
                    qualifying_dates.append(date)

            if not qualifying_dates:
                # Check if they have scraper meetings but all before cutoff
                has_any_scraper = any(
                    is_scraper_meeting((m.get("title") or ""), m.get("user_id") or "")
                    for m in meetings
                )
                if has_any_scraper:
                    skipped_date += 1
                    print(f"  [{i}/{len(LEAD_IDS)}] SKIP (before cutoff): {lead_name}", flush=True)
                else:
                    skipped_nomtg += 1
                    print(f"  [{i}/{len(LEAD_IDS)}] SKIP (no scraper meeting found): {lead_name}", flush=True)
                continue

            # Find opportunity
            opp_id = get_active_opportunity(lead_id)
            if not opp_id:
                skipped_noopp += 1
                print(f"  [{i}/{len(LEAD_IDS)}] SKIP (no opportunity): {lead_name}", flush=True)
                continue

            # Write Funnel Name DEAL
            api_put(f"/opportunity/{opp_id}/", {OPP_FUNNEL_KEY: OPP_FUNNEL_VALUE})
            updated += 1
            print(
                f"  [{i}/{len(LEAD_IDS)}] ✓ Updated: {lead_name} "
                f"| earliest scraper meeting: {min(qualifying_dates)}",
                flush=True,
            )

        except Exception as e:
            errors += 1
            print(f"  [{i}/{len(LEAD_IDS)}] ERROR on {lead_id}: {e}", flush=True)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    print(
        f"\n═══════════════════════════════════════\n"
        f"Done.\n"
        f"  Updated:                {updated}\n"
        f"  Skipped (before Apr 6): {skipped_date}\n"
        f"  Skipped (no opp):       {skipped_noopp}\n"
        f"  Skipped (no mtg found): {skipped_nomtg}\n"
        f"  Errors:                 {errors}\n"
        f"  Runtime:                {elapsed:.1f}s\n"
        f"═══════════════════════════════════════",
        flush=True,
    )


if __name__ == "__main__":
    main()
