# Task: Refactor Step 2 — Batch Chunk Processing

## Context

`step2.py` is the event extraction step in a LangGraph pipeline (`pass0 → pass1`).

It reads cleaned chunks from `*_pass0.json`, sends each chunk's `suggestedText` to
`gpt-4.1-nano` for medical event extraction, and writes results to `*_pass1.json`.

**Current problem:** It makes **1 API call per chunk**. A document with 120 chunks = 120
API calls. We want to batch multiple chunks into a single API call.

---

## What We Are Changing

Instead of sending 1 chunk per API call, we will send **k chunks together** (k = `BATCH_SIZE`,
configurable e.g. 5, 10) and receive all k results back in one structured response.

### Batch Input to LLM (what we send)

We will send chunks as a JSON object where each key is a **positional batch index**
(1-based integer as string: `"1"`, `"2"`, ... `"k"`) and each value is that chunk's
`suggestedText`:

```json
{
  "1": "MRI of the cervical spine was performed on June 1, 2022 at 13:37...",
  "2": "30-80 31st Street, Astoria, NY 11102. TECHNIQUE: multiplanar MRI...",
  "3": "Digitally Signed By: Naiyer Imam. Digitally Signed Date: June 2, 2022..."
}
```

These `"1"`, `"2"`, `"3"` keys are **temporary positional indices for the LLM only**.
They have no relationship to the chunk's actual `chunk_number` or `chunk` ID.

### Batch Output from LLM (what we receive)

The LLM returns a JSON object with the **same positional keys**, each containing a
`ChunkEvents`-shaped value (identical to what it returns today for a single chunk):

```json
{
  "1": {
    "events_by_date": [
      {
        "date": "2022-06-01",
        "events": [
          {
            "event_time": "13:37",
            "event_tag": "imaging",
            "event_summary": "MRI of cervical spine performed without contrast."
          }
        ]
      }
    ]
  },
  "2": {
    "events_by_date": []
  },
  "3": {
    "events_by_date": []
  }
}
```

### Final Output (pass1.json) — UNCHANGED

After receiving the batch response, we **map results back** using the positional index
to find the original chunk metadata (`chunk`, `chunk_number`, `pageNumbers`) and
reconstruct `ChunkTimeline` objects — exactly as today.

The `pass1.json` output format must remain **100% identical**:

```json
[
  {
    "chunk": "hzce4y-0",
    "chunk_number": 1,
    "pageNumbers": [1],
    "events_by_date": [...]
  },
  {
    "chunk": "e9vzmx-2",
    "chunk_number": 2,
    "pageNumbers": [1],
    "events_by_date": []
  }
]
```

The positional batch keys (`"1"`, `"2"`, `"3"`) are **internal to the batch call only**
and never appear in the final output.

---

## Changes Required

### 1. New Config Constant

Add `BATCH_SIZE = 10` (or whatever default we settle on) alongside the existing
`MAX_CONCURRENT_CHUNK_CALLS`. This controls how many chunks are packed into one API call.

```
# Before
MAX_CONCURRENT_CHUNK_CALLS = 25

# After
MAX_CONCURRENT_CHUNK_CALLS = 10   # now limits concurrent BATCH calls, not chunk calls
BATCH_SIZE = 10                   # chunks per API call
```

---

### 2. New Pydantic Schema — `BatchChunkEvents`

Add one new schema. Everything else (`Event`, `EventsByDate`, `ChunkEvents`,
`ChunkTimeline`) stays **completely unchanged**.

```
# New — wraps ChunkEvents keyed by positional batch index string
class BatchChunkEvents(BaseModel):
    results: Dict[str, ChunkEvents]
    # keys are "1", "2", ... "k" matching what was sent
```

---

### 3. `build_chain` — Swap Structured Output Schema

Change `.with_structured_output` from `ChunkEvents` to `BatchChunkEvents`.
Everything else in `build_chain` stays the same.

```
# Before
llm.with_structured_output(ChunkEvents)

# After
llm.with_structured_output(BatchChunkEvents)
```

---

### 4. Prompt Template Variable — `{batch_json}` replaces `{chunk_text}`

The `ChatPromptTemplate` currently uses `{chunk_text}` as its single input variable.
Change it to `{batch_json}`.

```
# Before
ChatPromptTemplate.from_template("... {chunk_text}")

# After
ChatPromptTemplate.from_template("... {batch_json}")
```

The variable `{batch_json}` will receive a serialized JSON string of the batch input
(the `{"1": "...", "2": "..."}` dict described above).

---

### 5. Replace `process_chunk` with `process_batch`

Remove `process_chunk`. Add `process_batch` which:

1. Takes a **list of chunk dicts** (the slice of size k)
2. Builds the positional index map:
   - key `"1"` → `chunks[0]["suggestedText"]`
   - key `"2"` → `chunks[1]["suggestedText"]`
   - ...
   - key `"k"` → `chunks[k-1]["suggestedText"]`
3. Serializes it to a JSON string → passes as `{"batch_json": ...}` to `chain.ainvoke`
4. Receives `BatchChunkEvents` back
5. **Reconciles**: checks that every sent key `"1"..."k"` exists in `response.results`
   - Missing keys → log a warning, fall back to individual retry for that chunk using
     current single-chunk logic (reuse the old `process_chunk` or an equivalent helper)
6. Maps each result back to the original chunk metadata using positional index:
   - result `"1"` → `chunks[0]` → reconstruct `ChunkTimeline`
   - result `"2"` → `chunks[1]` → reconstruct `ChunkTimeline`
7. Returns `List[ChunkTimeline]`

---

### 6. Replace `process_chunk_with_limit` with `process_batch_with_limit`

Same semaphore pattern, but now wraps `process_batch` instead of `process_chunk`.
Logs batch boundaries instead of individual chunk numbers:

```
# Before: logs chunk_number=5
# After:  logs batch chunk_numbers=[5,6,7,8,9,10]
```

---

### 7. `process_document` — Slice chunks into batches

Replace the per-chunk task list with a per-batch task list:

```
# Before
tasks = [process_chunk_with_limit(chain, chunk, sem) for chunk in chunks]

# After
batches = [chunks[i : i + BATCH_SIZE] for i in range(0, len(chunks), BATCH_SIZE)]
tasks = [process_batch_with_limit(chain, batch, sem) for batch in batches]
```

`asyncio.gather` stays the same. Results from each task are now `List[ChunkTimeline]`
instead of a single `ChunkTimeline`, so flatten them when building `clean_results`.

---

### 8. Update `extract_dates.md` Prompt

This is the most important change. The prompt must:

**a) Describe the new input format:**
```
You will receive a JSON object where each key is a positional index ("1", "2", ... "k")
and each value is the text content of one document chunk.
```

**b) Describe the required output format:**
```
Return a JSON object with the SAME keys. Each value must follow the ChunkEvents schema:
{
  "events_by_date": [
    {
      "date": "YYYY-MM-DD",
      "events": [
        {
          "event_time": "HH:MM or null",
          "event_tag": "<one of the allowed tags>",
          "event_summary": "..."
        }
      ]
    }
  ]
}
```

**c) Strict rules the model must follow:**
```
- EVERY key present in the input must appear in your response — never skip a key
- If a chunk contains no medical events, return "events_by_date": [] for that key
- Treat each chunk as completely independent — never merge or share events across chunks
- Keys in your response must exactly match the input keys as strings ("1", "2", etc.)
- Do not add any keys that were not in the input
```

**d) Keep all existing extraction instructions** (date formats, allowed event_tag values,
event_time format, etc.) exactly as they are today — only the framing changes from
single-chunk to multi-chunk.

---

## Data Flow Summary

```
pass0.json (N chunks)
    │
    ▼
slice into batches of BATCH_SIZE
    │
    ├─ batch 1: chunks [1..10]  ──► one API call ──► BatchChunkEvents {"1":...,"10":...}
    ├─ batch 2: chunks [11..20] ──► one API call ──► BatchChunkEvents {"1":...,"10":...}
    └─ batch M: chunks [N-9..N] ──► one API call ──► BatchChunkEvents {"1":...}
    │
    ▼
reconcile (retry any missing keys individually)
    │
    ▼
map positional keys back to original chunk metadata
    │
    ▼
List[ChunkTimeline]  (same as today)
    │
    ▼
pass1.json  (identical format to today)
```

---

## What Does NOT Change

- `Event`, `EventsByDate`, `ChunkEvents`, `ChunkTimeline` schemas — untouched
- `load_chunks`, `save_json`, `pass1_output_path_from_pass0` — untouched
- `run_step2` — untouched
- `step2_extract_dates_node` — untouched
- `pass1.json` output format — untouched

---

## Files to Modify

| File | Change |
|---|---|
| `step2.py` | Add `BATCH_SIZE`, add `BatchChunkEvents`, update `build_chain`, replace `process_chunk` + `process_chunk_with_limit`, update `process_document` |
| `extract_dates.md` | Rewrite input/output framing for batch JSON; keep all extraction rules |