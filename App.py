# App.py
import os
import sqlite3
from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
from zoneinfo import ZoneInfo
from collections import defaultdict, deque

# ---- GOOGLE SHEETS IMPORTS ----
import json, traceback
import gspread
from gspread import exceptions as gsex
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# ------------------------------------------------------------------
# Database path (Render-ready; local default is clock.db)
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

# ---- Local Helpers ----
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
            key = t_in.date().strftime("%Y-%m-%d")
            totals[crew][key] += (t - t_in).total_seconds() / 3600.0

    return {crew: dict(days) for crew, days in totals.items()}

# ------------------------------------------------------------------
# GOOGLE SHEETS HELPERS
# ------------------------------------------------------------------
def _get_gs_client():
    """Return an authenticated gspread client from the Render Secret File."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    secret_path = "/etc/secrets/service_account.json"
    if os.path.exists(secret_path):
        creds = Credentials.from_service_account_file(secret_path, scopes=scopes)
        return gspread.authorize(creds)
    raise RuntimeError("No Google credentials found. Add Secret File on Render.")

def _week_title(dt: datetime) -> str:
    iso = dt.isocalendar()  # (year, week, weekday)
    return f"Week {iso.year}-{iso.week:02d}"

def _get_or_create_week_ws(sh, week_title: str):
    """Find or create worksheet for that week."""
    for ws in sh.worksheets():
        if ws.title == week_title:
            return ws
    ws = sh.add_worksheet(title=week_title, rows=1000, cols=8)
    ws.update("A1:G1", [[
        "date", "crew", "action", "ts_in", "ts_out", "hours", "source"
    ]])
    return ws

def _find_open_in_ts(crew: str):
    """Return most recent unmatched IN timestamp for a crew, or None."""
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT action, ts FROM entries WHERE crew=? ORDER BY ts ASC, id ASC",
            (crew,)
        ).fetchall()
    stack = []
    for action, ts in rows:
        if action == "IN":
            stack.append(ts)
        elif action == "OUT" and stack:
            stack.pop()
    return stack[-1] if stack else None

def log_week_row(date_str, crew, action, ts_in, ts_out, hours):
    """Append a row to the correct weekly sheet."""
    try:
        sheet_id = os.getenv("SHEET_ID", "").strip()
        if not sheet_id:
            return False, "Missing SHEET_ID"

        gc = _get_gs_client()
        sh = gc.open_by_key(sheet_id)
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        week_title = _week_title(dt)
        ws = _get_or_create_week_ws(sh, week_title)

        ws.append_row(
            [date_str, crew, action, ts_in or "", ts_out or "", hours or "", "crew-clock"],
            value_input_option="RAW",
            insert_data_option="INSERT_ROWS",
        )
        return True, None

    except gsex.APIError as e:
        try:
            err_json = e.response.json()
        except Exception:
            err_json = str(e)
        traceback.print_exc()
        return False, f"APIError: {err_json}"
    except Exception as e:
        traceback.print_exc()
        return False, f"{type(e).__name__}: {str(e)}"

# ------------------------------------------------------------------
# ROUTES
# ------------------------------------------------------------------
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

    now_dt = datetime.now(TZ)
    date_str = now_dt.strftime("%Y-%m-%d")
    ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Calculate hours if OUT
    ts_in_val = None
    ts_out_val = None
    hours_val = None
    if action == "OUT":
        open_in_ts = _find_open_in_ts(crew)
        if open_in_ts:
            ts_in_val = open_in_ts
            ts_out_val = ts_str
            t_in = datetime.strptime(open_in_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
            t_out = now_dt
            hours_val = round((t_out - t_in).total_seconds() / 3600.0, 2)

    # Write to DB
    insert_entry(crew, action, ts_str)

    # Log to Sheet
    if action == "IN":
        ok, err = log_week_row(date_str, crew, action, ts_in=ts_str, ts_out=None, hours=None)
    else:
        ok, err = log_week_row(date_str, crew, action, ts_in=ts_in_val, ts_out=ts_out_val, hours=hours_val)

    if not ok:
        print("Sheets logging failed:", err)

    return redirect(url_for("clock_page"))

# ------------------------------------------------------------------
# HEALTH / TEST
# ------------------------------------------------------------------
@app.route("/health")
def health():
    try:
        _ = get_recent_entries(1)
        return {"ok": True}, 200
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

@app.route("/healthz")
def healthz():
    return health()

@app.route("/gs-test", methods=["GET"])
def gs_test():
    now_dt = datetime.now(TZ)
    date_str = now_dt.strftime("%Y-%m-%d")
    ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
    ok, err = log_week_row(date_str, "TEST", "PING", ts_in=ts_str, ts_out=None, hours=None)
    if ok:
        return {"ok": True, "wrote": [date_str, "TEST", "PING"]}, 200
    else:
        return {"ok": False, "error": err}, 500

@app.route("/gs-debug", methods=["GET"])
def gs_debug():
    out = {}
    try:
        sheet_id = os.getenv("SHEET_ID", "").strip()
        out["SHEET_ID_present"] = bool(sheet_id)
        gc = _get_gs_client()
        out["auth"] = "ok"
        sh = gc.open_by_key(sheet_id)
        out["spreadsheet_title"] = sh.title
        out["worksheets"] = [ws.title for ws in sh.worksheets()]
        now_dt = datetime.now(TZ)
        date_str = now_dt.strftime("%Y-%m-%d")
        ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")
        week_title = _week_title(now_dt)
        ws = _get_or_create_week_ws(sh, week_title)
        ws.append_row([date_str, "DEBUG", "PING", ts_str, "", "", "crew-clock"],
                      value_input_option="RAW",
                      insert_data_option="INSERT_ROWS")
        out["append"] = "ok"
        out["week_title"] = week_title
        out["row_written"] = [date_str, "DEBUG", "PING", ts_str]
        return out, 200
    except gsex.APIError as e:
        try:
            return {"ok": False, "where": "API", "error": e.response.json()}, 500
        except Exception:
            return {"ok": False, "where": "API", "error": str(e)}, 500
    except Exception as e:
        return {"ok": False, "where": type(e).__name__, "error": str(e)}, 500

# ------------------------------------------------------------------
# RUN
# ------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
