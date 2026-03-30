
**Zone 1: Before Date of Birth**
- Event date is before DOB → REJECT (impossible)

---

**Zone 2: Between DOB and Working Age (16)**
- No patient name present → REJECT
- Patient name present but event is not medical → REJECT
- Patient name present and event is medical → KEEP (childhood medical history can matter other than birth)

---

**Zone 3: Between Working Age and Incident Date (pre-accident)**
- No name present → REJECT
- Different person's name → REJECT
- Patient name present and medical → KEEP (establishes pre-existing condition baseline, important for defense)
- Patient name present and not medical → REJECT
- References a prior claim or injury for patient → KEEP

---

**Zone 4: On and After Incident Date (post-accident)**
- Your rules already cover this well, I'd add:
- Patient name present, medical, but injury/body part has zero overlap with claimed injuries → KEEP
- No name, body part mentioned matches claimed injuries → KEEP
- No name, body part mentioned has no match to claimed injuries → REJECT
- Different person's name → REJECT
- References a cited legal case or third-party incident → REJECT
- Chunk contains patient's DOB explicitly → always KEEP (identity anchor other than birth itself, we don't want to keep patients birth as event)

---

**General rules across all zones:**
- Pure billing/invoice content with no clinical detail → REJECT
- Legal boilerplate with no patient-specific content → REJECT
- Document is an IME report — KEEP
- Document references a settlement or prior claim payout for the patient → KEEP
- Chunk contains the incident date explicitly → always KEEP regardless of other conditions (high anchor value)
- Totally different ( more than 70 percent dissimilarity with patient case): REJECT


---

**The "never auto-reject, always Keep" cases:**
- Confidence in name matching is low (misspelling, partial name, nickname)
- Event date is ambiguous or relative ("two weeks after the accident")
- Body part is adjacent to claimed injury (e.g., left shoulder when right shoulder is claimed — could be compensatory injury)
- Provider name is partially matching but not exact
- Chunk is very short (under 3-4 lines) — not enough context to judge
