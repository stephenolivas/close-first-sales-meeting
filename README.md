# First SALES Meeting Field Updater

Populates the **"First Sales Call Booked Date"** custom field on Close CRM leads with the date of each lead's earliest qualifying first sales meeting.

Runs every 30 minutes via GitHub Actions + cron-job.org.

---

## What It Does

Our current custom "First Call Booked Date" field is unreliable — it updates on reschedules and includes non-sales meetings. This script writes a clean, filtered date to a custom field using the same title classification logic that powers the Call Capacity and MTD Funnel dashboards.

**Result:** Marketing (and anyone else) can filter leads in Close with:
- `First Sales Call Booked Date = Yesterday` + `Funnel Name Deal = Instagram`

...and get an accurate count of leads that had a first sales meeting booked — no external dashboard required.

---

## Classification Rules (applied in order)

### Always excluded
- Title starts with `Canceled` (with or without colon)
- Title contains follow-up/reschedule patterns: `follow-up`, `follow up`, `fallow up`, `F/U`, `Next Steps`, `Rescheduled`, `reschedule`
- Title contains both `Anthony` and `Q&A` (group Q&A sessions)
- Title contains enrollment patterns: `enrollment`, `Silver Start up`, `Bronze enrollment`, `questions on enrollment`

### Not a sales meeting
- Title contains `Vending Quick Discovery`
- Meeting owned by Kristin Nelson or Spencer Reynolds

### Excluded owners (meetings fully ignored)
- Stephen Olivas
- Ahmad Bukhari

### Qualifying titles (ONLY these count)
- `Vending Strategy Call`
- `Vendingprenuers Consultation` (handles misspellings)
- `Vendingprenuers Strategy Call` (handles misspellings)
- `New Vendingpreneur Strategy Call`
- `Vending Consult` (partial match, e.g. "Vending Consult w/Dillan")

---

## Custom Field

| Property | Value |
|----------|-------|
| Field name | First Sales Call Booked Date |
| Field ID | `cf_LFdYEQ6bsgp49YjZzefypDmdVx8iwuakWDSLPLpVrBq` |
| Type | Date |
| Object | Lead |

---

## Setup

### 1. Add the API key secret

In your GitHub repo → Settings → Secrets → Actions:

| Secret | Value |
|--------|-------|
| `CLOSE_API_KEY` | Your Close CRM API key |

### 2. Set up cron-job.org (primary trigger)

Create a job at [cron-job.org](https://cron-job.org) to hit the GitHub Actions `workflow_dispatch` endpoint every 30 minutes:

```
URL: https://api.github.com/repos/YOUR_ORG/YOUR_REPO/actions/workflows/update-field.yml/dispatches
Method: POST
Headers:
  Authorization: Bearer YOUR_GITHUB_PAT
  Accept: application/vnd.github+json
Body: {"ref": "main"}
Schedule: Every 30 minutes
```

The workflow also has a built-in `schedule` cron as a backup.

### 3. Run the initial backfill

Trigger the workflow manually via GitHub Actions → Run workflow. The first run will scan all meetings and populate the field for every lead that has a qualifying meeting. Expect 10-15 minutes.

---

## Performance

| Step | API calls |
|------|-----------|
| Paginate all meetings (~10,600) | ~107 GET calls |
| Fetch + update changed leads | varies (initial: hundreds; ongoing: ~10-50) |
| Clear stale field values | minimal |

- All calls throttled at 0.5s sleep between requests
- No threading or concurrency (avoids rate limit errors)
- Uses `_fields` parameter to minimize payload on lead fetches
- Runtime: 3-10 min depending on update volume

---

## Related Repos

- [call-capacity-dashboard](https://github.com/stephenolivas/call-capacity-dashboard)
- [mtd-funnel-reporting](https://github.com/stephenolivas/mtd-funnel-reporting)
