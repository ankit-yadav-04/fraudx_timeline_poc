# Pass 4 — Contradiction Detection Pipeline

## Overview
Build a Python pipeline that:
1. Takes `pass3_output.json` as input
2. Compresses it into a pipe-delimited text format (no legend)
3. Sends the compressed text to Claude LLM
4. Returns a structured JSON with contradictions found across the timeline

---

## Step 1 — compressor.py

### Input
`pass3_output.json` — structured medical timeline JSON

### Compression Rules

**Format per line:**
```
DATE|TAGS|DETAILS|FLAGS|||TAGS|DETAILS|FLAGS
```

- One line per date
- `|` separates columns within an event: `TAGS`, `DETAILS`, `FLAGS`
- `|||` separates multiple events on the same date
- `;` separates multiple detail strings within one event
- `+` separates multiple tags within one event (e.g., `surgery+admission`)
- `-` for empty flags
- For flags: `FLAG_TYPE[severity]:detail` — multiple flags separated by ` || `


### Output
Plain `.txt` string — no file saved, passed directly to LLM in memory.

**Example output:**
```
2020-09-30|surgery|C5-C6 ACDF; cerv decomp; biomech+ant instr; allo graft; gen anesthesia; fluoro+evoked monitoring; Dr.T@Surgicare|-
2021-02-22|procedure|cerv hern/myelo/radiculo C5-C6; Dr.T@Surgicare|-|||surgery|L-shoulder arthro; ant capsulorrhaphy; SLAP+rotator cuff debridement; subac decomp|-
```

---

## Step 2 — pass4.py

### Flow
```
pass3_output.json
      ↓
compressor.py → compressed_text (string, not saved)
      ↓
Build prompt from prompt.md (inject compressed_text into user prompt)
      ↓
Call llm
      ↓
Parse JSON from response
      ↓
Save to pass4_output.json
```

### Output JSON Schema
```json
{
  "contradictions": [
    {
      "contradiction_id": "C001",
      "dates_involved": ["2021-07-07", "2021-07-21"],
      "contradiction_type": "duplicate_procedure",
      "detail_a": {
        "date": "2021-07-07",
        "text": "C5-C6 ACDF performed at NYP"
      },
      "detail_b": {
        "date": "2021-07-21",
        "text": "C5-C6 ACDF performed at NYP by Dr.M"
      },
      "explanation": "Same procedure appears performed twice within 14 days",
      "severity": "high"
    }
  ]
}
```

## contradiction_type

After merging, scan the final event list for the following flags.
Apply every flag that matches. If none match, return empty array [].

### HIGH SEVERITY

**DUPLICATE_PROCEDURE**
Trigger: same procedure appears in two separate events in the output
         with no clinical reason for repetition
Example: Two surgery events both performing C5-C6 arthrodesis on same date
Relevance: Possible duplicate billing

**PROVIDER_CONFLICT**
Trigger: same procedure in same event attributed to two different providers
         or two different facilities
Example: "[A]: Dr. Touliopoulos" and "[B]: Dr. Merola" for same surgery
Relevance: Duplicate billing or record mixing

**ANATOMY_SIDE_MISMATCH**
Trigger: same event references both left and right for the same body part
         in a contradictory way
Example: Left shoulder surgery details mention right shoulder finding
Relevance: Copy-paste error, wrong patient, or fabricated report

**TIMELINE_IMPOSSIBILITY**
Trigger: discharge event exists but no admission event on same date,
         or clinical events appear after a discharge with no re-admission
Example: Discharge at top of event list, new procedure entry below it
Relevance: Falsified timestamps or documentation error

**INJURY_MECHANISM_CONFLICT**
Trigger: two events describe the same injury with different causes,
         locations, or body parts
Example: One says beam hit right arm, another says left arm
Relevance: Conflicting injury descriptions weaken claim validity

### MEDIUM SEVERITY

**DOSAGE_DISCREPANCY**
Trigger: same drug appears in two events with different doses
Example: Acetaminophen 650mg in one event, 500mg in another
Relevance: Separate prescriptions being billed, or documentation error

**DIAGNOSIS_PROCEDURE_MISMATCH**
Trigger: a diagnosis event references one body part or side;
         a procedure event on same date references another
Example: Diagnosis says C5-C6; procedure performed at L5-S1
Relevance: Mismatch raises questions about medical necessity

**VAGUE_PROCEDURE_CODE**
Trigger: procedure or imaging entry has only a code, no description,
         no result
Example: "Procedure 2460R — no details available"
Relevance: Unverifiable claim item

### LOW SEVERITY

**MISSING_PROVIDER**
Trigger: clinical event (procedure, surgery, medication, imaging) has
         no provider name anywhere in event_details
Relevance: Cannot confirm who performed or ordered the action

**WITHIN_DOC_DUPLICATE**
Trigger: same event documented multiple times in input with same tag
         and same content
Relevance: Redundant documentation; merge but flag

**METADATA_EVENT**
Trigger: event is administrative or system-generated, not clinical
Example: "Document updated by non-clinical staff"
Relevance: Not a clinical event

---

## Notes
- Do NOT save the compressed text to disk — it's only an in-memory intermediate step
- Do NOT include the abbreviation legend in the compressed text sent to LLM — the prompt handles context
- The compressor should be reusable (importable function, not just a script)
- Add a `__main__` block to `pass4.py` so it can be run directly: `python pass4.py`