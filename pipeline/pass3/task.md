# Pass 3 — Same-Date Event Merging Task

## What This Script Does

Takes `merged_pass2_5.json` — a sorted timeline where the same date can
appear multiple times as separate objects — and produces a final timeline
where every date appears exactly once, with all events under that date
merged and flags detected.

---

## Input File

`merged_pass2_5.json`

Structure:
```json
{
  "events_by_date": [
    {
      "event_date": "YYYY-MM-DD",
      "events": [
        {
          "event_tags": ["tag1"],
          "event_details": ["fact 1", "fact 2"],
          "event_flags": []
        }
      ]
    }
  ]
}
```

**Known issue in input:** The same date can appear as multiple separate
objects. Example — `2021-02-22` appears twice with different events.
This is expected and is what Pass 3 resolves.

---

## Output File

`pass3_output.json`

Same structure as input. One object per date. No duplicate dates.

---

## What The Script Must Do

### Step 1 — Group by date (pure Python, no LLM)

Scan `events_by_date` and group all event objects that share the same
`event_date` into a dict keyed by date.

```python
from collections import defaultdict
grouped = defaultdict(list)
for entry in data["events_by_date"]:
    for event in entry["events"]:
        grouped[entry["event_date"]].append(event)
```

After this step, each date key maps to a flat list of all event objects
from all documents for that date.

### Step 2 — Skip dates with only one event (no LLM needed)

If a date has exactly one event object after grouping → pass it through
directly to output. No LLM call needed.

### Step 3 — Merge dates with multiple events via LLM

If a date has 2 or more event objects → send to LLM for merging.

**Chunking rule for large event sets:**
If a date has more than 10 event objects, do NOT send all at once.
Use pairwise binary tree merging as we did earlier:

```
[e1, e2, e3, e4, e5, e6, e7, e8]
  → Level 1: merge(e1,e2), merge(e3,e4), merge(e5,e6), merge(e7,e8)  [parallel]
  → Level 2: merge(r1,r2), merge(r3,r4)                               [parallel]
  → Level 3: merge(r5,r6)                                             [final]
```

Each merge call receives exactly 2 event objects and returns 1 merged
event object (which may itself contain multiple events as a list).

**Semaphore:** Cap concurrent LLM calls at MAX_PARALLEL = 5.
Use `asyncio.Semaphore(MAX_PARALLEL)`.

### Step 4 — Sort and write output

After all dates are processed, sort by date ascending and write to
`pass3_output.json`.

---

## LLM Call Details

- Load prompt from `pass3_prompt.md`
- Replace `{{EVENTS_JSON}}` placeholder with the JSON of events being merged
- Parse response as JSON directly — LLM returns only a JSON object


---

## Pre-sort Events Before Merging

Before sending events into the pairwise tree, sort them by tag so that
events of the same type are grouped together in the same merge call.
This improves dedup quality.

```python
TAG_ORDER = [
    "admission", "injury", "diagnosis", "imaging",
    "procedure", "surgery", "medication", "checkup",
    "test", "discharge", "other"
]
---

