# App.py
import os
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
from zoneinfo import ZoneInfo
from collections import defaultdict, deque

app = Flask(__name__)

# ------------------------------------------------------------------
# Database path now uses an environment variable for cloud storage
# ------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "clock.db")

# ---- Timezone (with fallback) ----
try:
    TZ = ZoneInfo(os.getenv("TZ", "America/New_York"))
except Exception:
    TZ = datetime.now().astimezone().tzinfo

# ---- Initialize Database ----
def init_db():
    """Create the entries table if missing."""
    # Ensure parent directory exists (important for Render disk mount)
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                crew TEXT NOT NULL,
                action TEXT NOT NULL,
                ts TEXT NOT NULL
            )
        """)
        conn.commit()

init_db()

# ---- Helpers ----
def get_recent_entries(limit=50):
    """Return recent punches (id, crew, action, timestamp)."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, crew, action, ts FROM entries ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return rows

def insert_entry(crew, action, timestamp):
    """Add a clock entry."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO entries (crew, action, ts) VALUES (?, ?, ?)",
            (crew, action, timestamp)
        )
        conn.commit()

def calculate_daily_hours():
    """Return {crew: {YYYY-MM-DD: total_hours}} for all completed IN/OUT pairs."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT crew, action, ts FROM entries ORDER BY crew, ts"
        ).fetchall()

    open_in = defaultdict(deque)
    totals = defaultdict(lambda: defaultdict(float))

    for crew, action, ts in rows:
        t = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
        if action == "IN":
            open_in[crew].append(t)
        elif action == "OUT" and open_in[crew]:
            t_in = open_in[crew].popleft()
            # Count hours toward the IN day
            key = t_in.date().strftime("%Y-%m-%d")
            totals[crew][key] += (t - t_in).total_seconds() / 3600.0

    # Convert nested defaultdicts to plain dicts
    return {crew: dict(days) for crew, days in totals.items()}

# ---- Routes ----
@app.route("/", methods=["GET"])
def clock_page():
    rows = get_recent_entries()
    daily_hours = calculate_daily_hours()
    return render_template("clock.html", punches=rows, daily_hours_str=daily_hours)

@app.route("/clock", methods=["POST"])
def clock_submit():
    crew = (request.form.get("crew") or "").strip()
    action = (request.form.get("action") or "").strip().upper()
    if not crew or action not in {"IN", "OUT"}:
        return redirect(url_for("clock_page"))

    now_local = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    insert_entry(crew, action, now_local)
    return redirect(url_for("clock_page"))

# ---- Simple health check for Render ----
@app.route("/health")
def health():
    try:
        _ = get_recent_entries(1)
        return {"ok": True}, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

# ---- Run ----
if __name__ == "__main__":
    # Bind to all interfaces (important for Render / Docker / Pi)
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)

