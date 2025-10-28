from datetime import datetime
from zoneinfo import ZoneInfo
from collections import defaultdict

def to_local(dt_utc, tzname="America/New_York"):
    if dt_utc is None:
        return None
    return dt_utc.astimezone(ZoneInfo(tzname))

def weekly_buckets(rows, tzname="America/New_York"):
    """Pairs clock_in/out entries and totals hours per week per person."""
    by_person = defaultdict(list)
    for r in rows:
        by_person[r.crew_name].append(r)

    report = {}
    for name, logs in by_person.items():
        logs.sort(key=lambda r: r.ts)
        total = defaultdict(float)
        i = 0
        while i < len(logs):
            if logs[i].action != "clock_in":
                i += 1
                continue
            start = logs[i].ts
            end = None
            j = i + 1
            while j < len(logs):
                if logs[j].action == "clock_out":
                    end = logs[j].ts
                    break
                j += 1
            if end:
                wk = f"{start.isocalendar().year}-W{start.isocalendar().week:02d}"
                total[wk] += (end - start).total_seconds() / 3600
                i = j + 1
            else:
                i += 1
        report[name] = {wk: round(h, 2) for wk, h in total.items()}
    return report
