#!/usr/bin/env python3
"""Fingerprint a downloaded copy of the Crew Clock Google Sheet (read-only).

    python3 sheets_manifest.py "Crew Clock.xlsx" > sheets-manifest.json

Opens the file read-only and never writes to it. Prints, as JSON:
  * the file's SHA-256 (proves later that the archived copy wasn't altered)
  * every tab with its row count
  * for each weekly tab ("Week YYYY-WW"): hours per crew by the old app's own
    rule (the "hours" column on OUT rows, not recalculated), and every OUT row
    with no hours ("unpaired_out": its clock-in was lost) for review
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
        missing = [n for n in ("crew", "action", "hours") if n not in head]
        if missing:
            # The old app refuses to total such a tab too; never report it as 0 hours.
            sys.exit(f"{ws.title}: missing column(s) {missing}; header is {head}")
        col = {n: head.index(n) for n in ("date", "crew", "action", "ts_in", "ts_out", "hours") if n in head}
        get = lambda r, n: (r[col[n]] if n in col and col[n] < len(r) else None)
        hours, outs, ins, unpaired, debug = defaultdict(float), 0, 0, [], 0
        for i, r in enumerate(rows[1:], start=2):
            crew = str(get(r, "crew") or "").strip()
            action = str(get(r, "action") or "").strip().upper()
            if crew.upper() == "DEBUG":
                debug += 1  # counted separately; the old app's totals only ever add OUT rows
            if action == "IN":
                ins += 1
                continue
            if action != "OUT":
                continue
            h = get(r, "hours")
            try:
                h = float(str(h).strip())
            except (TypeError, ValueError):
                # Clock-out with no hours: its clock-in was lost (the old server forgot it when it slept).
                unpaired.append({"row": i, "crew": crew, "ts_out": str(get(r, "ts_out") or get(r, "date") or "")})
                continue
            outs += 1
            hours[crew] += h
        weeks.append({
            "tab": ws.title,
            # Same rule as App.py update_weekly_totals_for_week: sum "hours" on OUT rows, by crew name.
            # Rows sit in the week of the clock-OUT; the new app buckets shifts by clock-IN (decide per shift on import).
            "out_rows_with_hours": outs, "in_rows": ins, "debug_rows": debug,
            "unpaired_out": unpaired,
            "hours_by_crew": {k: round(v, 2) for k, v in sorted(hours.items())},
            "total_hours": round(sum(hours.values()), 2),
        })
    print(json.dumps({"file": path, "sha256": sha, "tabs": tabs, "weeks": weeks,
                      "all_weeks_total_hours": round(sum(w["total_hours"] for w in weeks), 2)}, indent=2))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
