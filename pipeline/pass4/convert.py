import json


def format_flags(flags: list) -> str:
    """Convert list of flag dicts into compact string. Empty = '-'"""
    if not flags:
        return "-"
    parts = []
    for f in flags:
        flag_type = f.get("flag_type", "")
        severity = f.get("severity", "")
        detail = f.get("detail", "")
        parts.append(f"{flag_type}[{severity}]:{detail}")
    return " || ".join(parts)


def format_event(event: dict) -> str:
    """Convert one event dict into TAGS|DETAILS|FLAGS string."""
    tags = "+".join(event.get("event_tags", []))
    details = "; ".join(event.get("event_details", []))
    flags = format_flags(event.get("event_flags", []))
    return f"{tags}|{details}|{flags}"


def compress(input_path: str, output_path: str):
    # ── Load JSON ──────────────────────────────────────────────────────────────
    with open(input_path, "r") as f:
        data = json.load(f)

    lines = []

    # ── Column header ──────────────────────────────────────────────────────────
    lines.append("DATE|TAGS|DETAILS|FLAGS[|||TAGS|DETAILS|FLAGS ...]")
    lines.append("")

    # ── Data rows ──────────────────────────────────────────────────────────────
    for date_entry in data.get("events_by_date", []):
        date = date_entry.get("event_date", "")
        events = date_entry.get("events", [])

        event_strings = [format_event(e) for e in events]
        events_combined = "|||".join(event_strings)

        lines.append(f"{date}|{events_combined}")

    # ── Write output ───────────────────────────────────────────────────────────
    output = "\n".join(lines)
    with open(output_path, "w") as f:
        f.write(output)

    # ── Stats ──────────────────────────────────────────────────────────────────
    original_size = len(json.dumps(data))
    compressed_size = len(output)
    reduction = round((1 - compressed_size / original_size) * 100, 1)

    print(f"✅ Done!")
    print(f"   Input  : {input_path}")
    print(f"   Output : {output_path}")
    print(f"   Original size  : {original_size:,} chars")
    print(f"   Compressed size: {compressed_size:,} chars")
    print(f"   Reduction      : {reduction}%")
    print(f"\n── Preview (first 3 data lines) ──")
    data_lines = [l for l in lines if l and not l.startswith("DATE")]
    for l in data_lines[:3]:
        print(f"  {l[:140]}{'...' if len(l) > 140 else ''}")


if __name__ == "__main__":
    compress(
        input_path="/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/pass3_output.json",
        output_path="/home/ankit/smartsense_code/fraudx_timeline_poc/rough_jsons/15308/pass4_compressed.txt",
    )
