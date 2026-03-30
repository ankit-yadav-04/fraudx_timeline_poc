# Task: Implement `step2_5.py` — Chunk Screener

## Overview

This step sits between `step2.py` (event extraction) and the timeline assembly.

It reads `*_pass1.json` files, screens each chunk against the patient profile,
adds a `decision` field (`KEEP` or `REJECT`) to each chunk, and removes chunks
where `events_by_date` is empty. Output is written as `*_pass2.json`.

---

## Input

- Files matching pattern: `*_pass1.json`
- Each file is a JSON array of chunk objects with this schema:

```json
[
  {
    "chunk": "hzce4y-0",
    "chunk_number": 1,
    "pageNumbers": [1],
    "events_by_date": [
      {
        "date": "2022-06-01",
        "events": [
          {
            "event_time": "13:37",
            "event_tag": "imaging",
            "event_summary": "MRI cervical spine without contrast performed for Erik Barrezueta..."
          }
        ]
      }
    ]
  },
  {
    "chunk": "e9vzmx-2",
    "chunk_number": 2,
    "pageNumbers": [1],
    "events_by_date": []
  }
]
```

- Patient profile: a `.txt` file (e.g., `patient_profile.txt`) loaded once at runtime
  and injected into every screening prompt.

---

## Output

- Files written as `*_pass2.json` alongside the input files (same directory)
- Same schema as input BUT:
  1. **Add** `"decision": "KEEP"` or `"decision": "REJECT"` to each chunk object
  2. **Remove** all chunks where `events_by_date` is `[]` (empty) — do this BEFORE
     sending to LLM, no need to screen empty chunks

Output chunk schema:

```json
{
  "chunk": "hzce4y-0",
  "chunk_number": 1,
  "pageNumbers": [1],
  "decision": "KEEP",
  "events_by_date": [...]
}
```

---

## What the LLM Does

For each batch of k chunks (non-empty only), send a single API call.
LLM receives:
- The patient profile (injected once at top of prompt)
- A JSON map of chunks to screen, keyed by positional index `"1"`, `"2"` ... `"k"`
- Each chunk in the map contains only the fields needed for screening judgment:

```json
{
  "1": {
    "date": "2022-06-01",
    "event_tag": "imaging",
    "event_summary": "MRI cervical spine without contrast performed for Erik Barrezueta, revealing straightening of cervical lordosis and broad-based central disc herniation at C5-6."
  },
  "2": {
    "date": "2019-03-15",
    "event_tag": "injury",
    "event_summary": "Slip and fall injury sustained by John Doe at a warehouse facility in Brooklyn."
  }
}
```

LLM returns ONLY this — nothing else:

```json
{
  "1": "KEEP",
  "2": "REJECT"
}
```

---

## Screening Prompt

```
You are a medical-legal document screener for an insurance company.

You will receive a PATIENT PROFILE and a batch of extracted medical events.
Your job is to decide whether each event belongs to this patient's case or not.

PATIENT PROFILE:
{patient_profile}

---

SCREENING RULES — apply in this order:

1. If the event date is before the patient's date of birth → REJECT

2. If the event date is before the patient's legal working age (16 years old):
   - No patient name in summary → REJECT
   - Patient name present but event is not medical → REJECT
   - Patient name present and event is medical → KEEP

3. If the event date is before the incident date:
   - No name present → REJECT
   - Different person's name → REJECT
   - Patient name present and medical → KEEP
   - Patient name present and not medical → REJECT
   - Patient name present, references a prior claim or injury → KEEP

4. If the event date is on or after the incident date:
   - Different person's name explicitly present → REJECT
   - Patient name present and medical → KEEP
   - Patient name present and not medical → REJECT
   - No name, but provider or facility matches patient's known providers → KEEP
   - No name, body part matches patient's claimed injuries → KEEP
   - No name, event looks unrelated to patient's accident and injuries → REJECT
   - References a cited legal case or third-party incident → REJECT

5. Always KEEP regardless of other conditions:
   - Chunk contains the exact incident date
   - Chunk is an IME report (even if it contradicts the claim)
   - Chunk references a prior claim or settlement involving the patient

6. Always REJECT regardless of other conditions:
   - Pure billing or administrative content with no clinical relevance
   - Legal boilerplate with no patient-specific content

IMPORTANT:
- When in doubt between KEEP and REJECT, always choose KEEP
- Never REJECT based on low confidence alone — only REJECT when a rule clearly applies

---

BATCH INPUT:
{batch_json}

---

Return ONLY a JSON object mapping each key to either "KEEP" or "REJECT".
No explanation, no preamble, no markdown. Example:
{"1": "KEEP", "2": "REJECT", "3": "KEEP"}
```

---

## Parallelism — Mirror Step 2 Exactly

```
BATCH_SIZE = 4              # chunks per LLM call (non-empty only)
MAX_CONCURRENT_FILES = 4    # files processed in parallel
MAX_CONCURRENT_BATCH_CALLS = 15  # global cap on in-flight LLM calls across all files
```

Use the same two-level parallelism as `step2.py`:
- `asyncio.Semaphore(MAX_CONCURRENT_FILES)` to cap file-level concurrency
- `asyncio.Semaphore(MAX_CONCURRENT_BATCH_CALLS)` to cap batch call concurrency globally
- Use `asyncio.gather()` for both file-level and batch-level parallelism

---

## Resilience — Mirror Step 2 Exactly

- Parse LLM response per-key, not all-or-nothing
- If a key is missing from LLM response → default to `"KEEP"` (safe fallback, never lose data)
- If LLM returns an invalid value for a key (not "KEEP"/"REJECT") → default to `"KEEP"`
- If entire LLM response is unparseable JSON → default ALL keys in that batch to `"KEEP"`
- If LLM call fails after retries → default ALL keys in that batch to `"KEEP"`
- Never crash the pipeline — always write output even if screening had errors

---

## Processing Flow Per File

```
1. Load *_pass1.json
2. Load patient_profile.txt (loaded once globally, passed into all calls)
3. Split chunks into two lists:
   - empty_chunks:    where events_by_date == []  → these are DROPPED from output entirely
   - non_empty_chunks: where events_by_date != []  → these go to LLM for screening
4. Split non_empty_chunks into batches of BATCH_SIZE
5. For each batch:
   a. Build batch_json with only: date, event_tag, event_summary per chunk
      (take first event of first date if multiple exist — enough for screening judgment)
   b. Call LLM with screening prompt
   c. Parse response → map positional key back to chunk
   d. Attach decision field to each chunk object
6. Assemble final list: only chunks with decision attached (KEEP or REJECT both included)
7. Write to *_pass2.json
```

---

## Logging

Log the following (mirror step2.py style):
- File processing start/end
- Batch start with chunk numbers included in batch
- Any parse errors or fallbacks to default KEEP
- Count of empty chunks dropped
- Count of KEEP vs REJECT decisions per file
- Final output path
- Total step duration

---

## Model

Use the same model as step2.py (`gpt-4.1-nano` or whatever is configured).
Screening is a simple classification task — no need for a larger model.

---

## Notes for Cursor

- Reference `step2.py` directly for the async structure, semaphore pattern,
  batch assembly, per-key parsing, and file discovery logic — replicate it closely
- The only new logic is: build slim batch_json, call screening prompt, parse KEEP/REJECT,
  attach to chunk, drop empty chunks from output
- Patient profile is loaded once at startup and passed as a string into every prompt call
- Do not modify or re-extract `events_by_date` — treat it as read-only passthrough