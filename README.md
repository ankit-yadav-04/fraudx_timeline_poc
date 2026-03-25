## Full Pipeline Flow Now

Your system now becomes:

PDF
  ↓
chunking
  ↓
load_chunks()
  ↓
process_document()
  ↓
semaphore (5 workers)
  ↓
process_chunk()
  ↓
LLM extraction
  ↓
ChunkTimeline outputs

## Example Execution Timeline

If you have 20 chunks:

Batch 1 → chunk 1-5
Batch 2 → chunk 6-10
Batch 3 → chunk 11-15
Batch 4 → chunk 16-20

Each batch runs in parallel.

## Example Output

Your final result will look like:

[
  ChunkTimeline(...),
  ChunkTimeline(...),
  ChunkTimeline(...),
]

Each containing:

    boundingBoxes
    chunk
    pageNumbers
    events_by_date