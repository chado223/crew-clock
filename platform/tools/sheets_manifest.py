#!/usr/bin/env python3
"""Fingerprint a downloaded copy of the Crew Clock Google Sheet (read-only).

    python3 sheets_manifest.py "Crew Clock.xlsx" > sheets-manifest.json

Opens the file read-only and never writes to it. Prints, as JSON:
  * the file's SHA-256 (proves later that the archived copy wasn't altered)
  * every tab with its row count
  * for each weekly tab ("Week YYYY-WW"): shift rows, crews, and total hours
    as the sheet itself recorded them (the "hours" column, not recalculated)
This manifest is what a future import into time_entries is checked against.
"""
import hashlib, json, re, sys
from collections import defaultdict

try:
    import openpyxl
except ImportError:
    sys.exit("pip install openpyxl")


def main(path: str) -> None:
    sha = hashlib.sha256(open(path, "rb").read()).hexdigest()
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    tabs, weeks = [], []
    for ws in wb.worksheets:
        rows = [r for r in ws.iter_rows(values_only=True) if any(c not in (None, "") for c in r)]
        tabs.append({"tab": ws.title, "rows": max(len(rows) - 1, 0)})
        if not re.fullmatch(r"Week \d{4}-\d{2}", ws.title) or not rows:
            continue
        head = [str(c or "").strip().lower() for c in rows[0]]
        col = {name: head.index(name) for name in ("date", "crew", "action", "hours") if name in head}
        hours, shifts, debug = defaultdict(float), 0, 0
        for r in rows[1:]:
            crew = str(r[col["crew"]] or "").strip() if "crew" in col else ""
            if crew.upper() == "DEBUG":
                debug += 1
                continue
            h = r[col["hours"]] if "hours" in col else None
            try:
                h = float(h)
            except (TypeError, ValueError):
                continue  # clock-in rows have no hours yet
            shifts += 1
            hours[crew] += h
        weeks.append({"tab": ws.title, "shifts_with_hours": shifts, "debug_rows": debug,
                      "hours_by_crew": {k: round(v, 2) for k, v in sorted(hours.items())},
                      "total_hours": round(sum(hours.values()), 2)})
    print(json.dumps({"file": path, "sha256": sha, "tabs": tabs, "weeks": weeks,
                      "all_weeks_total_hours": round(sum(w["total_hours"] for w in weeks), 2)}, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
