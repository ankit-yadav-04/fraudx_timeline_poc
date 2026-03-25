```
{
  "events_by_date": [
    {
      "event_date": "2021-10-29",
      "events": [
        {
          "event_tags": ["admission", "checkup"],
          "event_details": [
            "Patient Alex Tipantaxi admitted to St. Barnabas Hospital ED at 13:17 for right elbow pain from workplace metal beam accident",
            "Provider: Paul Beyer DO, documented by Mulham Alom MD/Resident",
            "Vitals stable: BP 107/68, HR 67, SpO2 97%, Temp 97.7°F",
            "Right elbow tenderness on radial head, no swelling, neurovascular intact"
          ],
          "event_flags": []
        },
        {
          "event_tags": ["imaging"],
          "event_details": [
            "X-ray right elbow (3 views) + right humerus (2 views) — no fracture, dislocation, or joint effusion",
            "Interpreted by Dr. David Braunstein",
            "Incidental finding: 9mm ossific density lateral to left humeral head in soft tissue"
          ],
          "event_flags": [
            {
              "flag_type": "ANATOMY_SIDE_MISMATCH",
              "severity": "medium",
              "detail": "Right elbow X-ray report references left humeral head — possible transcription error or extraction artifact",
              "insurance_relevance": "Could indicate report was copy-pasted or wrongly attributed"
            }
          ]
        },
        {
          "event_tags": ["medication"],
          "event_details": [
            "Acetaminophen 650mg oral given in ED at 14:40 by Nurse Nyasha Beepot",
            "Acetaminophen 500mg capsules prescribed: 2 caps every 6hr for 7 days (Oct 29 – Nov 4)",
            "Ibuprofen 400mg tablets prescribed: 1 tab every 6hr for 7 days (Oct 29 – Nov 4)"
          ],
          "event_flags": [
            {
              "flag_type": "DOSAGE_DISCREPANCY",
              "severity": "low",
              "detail": "In-ED acetaminophen dose was 650mg; discharge prescription was 500mg — different strengths for same drug same day",
              "insurance_relevance": "Minor but worth noting if prescription cost is being claimed"
            }
          ]
        },
        {
          "event_tags": ["discharge"],
          "event_details": [
            "Patient discharged from ED at 16:24 in stable condition",
            "Instructions: activity as tolerated, follow up with PCP within one week"
          ],
          "event_flags": []
        }
      ]
    },
    {
      "event_date": "2021-11-01",
      "events": [
        {
          "event_tags": ["other"],
          "event_details": [
            "Administrative document update by non-clinical staff (Services S)"
          ],
          "event_flags": [
            {
              "flag_type": "METADATA_EVENT",
              "severity": "low",
              "detail": "Not a clinical event — system/document update entry",
              "insurance_relevance": "No clinical relevance; filter from final timeline"
            }
          ]
        }
      ]
    }
  ]
}
```

---

## Full Flag Taxonomy

Grouping by what they mean for insurance:

### 🔴 High Severity — Direct Fraud Indicators
| Flag | What It Catches |
|---|---|
| `ANATOMY_SIDE_MISMATCH` | Left/right inconsistency across reports for same procedure |
| `TIMELINE_IMPOSSIBILITY` | Discharge before admission, procedure after death, overlapping inpatient stays |
| `PROVIDER_CONFLICT` | Same procedure, same date, two different providers or facilities claimed |
| `DUPLICATE_CLAIM_EVENT` | Same procedure appears to be billed twice across documents |
| `PRE_EXISTING_CONDITION_OVERLAP` | Condition claimed as new injury existed in earlier records |

### 🟡 Medium Severity — Inconsistency / Needs Review
| Flag | What It Catches |
|---|---|
| `DOSAGE_DISCREPANCY` | Same drug, same date, different dose across records |
| `DIAGNOSIS_PROCEDURE_MISMATCH` | Diagnosis says left knee, procedure done on right knee |
| `INJURY_MECHANISM_CONFLICT` | One doc says workplace accident, another says home injury |
| `DATE_INCONSISTENCY` | Same event reported on different dates across documents |
| `SYMPTOM_ESCALATION_GAP` | Sudden severity jump with no supporting intermediate records |

### 🔵 Low Severity — Data Quality / Informational
| Flag | What It Catches |
|---|---|
| `METADATA_EVENT` | Admin/system entry, not a clinical event |
| `WITHIN_DOC_DUPLICATE` | Same event documented multiple times in one document |
| `VAGUE_EVENT` | Summary too generic to verify — "procedure performed", "test done" |
| `MISSING_PROVIDER` | Event has no associated provider name |
| `AMBIGUOUS_DATE` | "Early March", "next week" — no exact date resolvable |

---

## Subtle Suggestions

**1. Add `source_doc_id` to every event now**
You'll need it in Pass 3 when cross-document contradictions appear. Without it you can't say *"Doc A says X, Doc B says Y."* Add it at Pass 2.5 while structure is still clean.

**2. Keep `METADATA_EVENT` entries but mark them separately**
Don't drop them at Pass 2.5. Pass 3 might need context from them. Just filter them out of the final output at the very end.

**3. Flag severity should drive Pass 3 behavior**
```
low flags    → Pass 3 can auto-resolve during merge
medium flags → Pass 3 surfaces them in contradiction report
high flags   → Pass 3 escalates, never auto-merges these events
```
This means Pass 3 prompt can be told explicitly: *"Never merge events that carry high severity flags — always keep both and escalate."*

**4. `insurance_relevance` field is valuable, keep it**
This is the field an investigator actually reads. Keep it human-readable and specific. Don't let it become generic like *"requires review"* — that's useless. Force it to say *why* it matters for the specific claim.

---

## Will It Work for Pass 3?

Yes — and here's exactly what Pass 3 receives per event:
```
✅ Crisp bullet details instead of paragraphs    → fewer tokens
✅ Tags as list                                   → easy cross-doc grouping
✅ Flags pre-attached with severity               → no re-detection needed
✅ High severity flags block auto-merge           → no data loss
✅ insurance_relevance already written            → human-readable output ready