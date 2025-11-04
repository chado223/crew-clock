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
# GOOGLE SHEETS HELPERS (with Drive scope + better error reporting)
# ------------------------------------------------------------------
def _get_gs_client():
    """Return an authenticated gspread client from the Render Secret File."""
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",  # helps with shared-drive access
    ]
    secret_path = "/etc/secrets/service_account.json"
    if os.path.exists(secret_path):
        creds = Credentials.from_service_account_file(secret_path, scopes=scopes)
        return gspread.authorize(creds)
    raise RuntimeError("No Google credentials found. Add Secret File on Render.")

def log_to_google_sheets(date_str, ts_str, crew, action):
    """Append a new row and return detailed errors if any."""
    try:
        sheet_id = os.getenv("SHEET_ID", "").strip()
        if not sheet_id:
            return False, "Missing SHEET_ID"

        gc = _get_gs_client()
        sh = gc.open_by_key(sheet_id)           # Raises if not shared / bad ID
        ws = sh.sheet1                          # First worksheet

        ws.append_row(
            [date_str, ts_str, crew, action, "crew-clock"],
            value_input_option="RAW",
            insert_data_option="INSERT_ROWS",
            table_range=None,
        )
        return True, None

    except gspread.exceptions.APIError as e:
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

    # ---- TIMESTAMP & SHEETS LOGGING ----
    now_dt = datetime.now(TZ)
    date_str = now_dt.strftime("%Y-%m-%d")
    ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    insert_entry(crew, action, ts_str)

    ok, err = log_to_google_sheets(date_str, ts_str, crew, action)
    if not ok:
        print("Sheets logging failed:", err)

    return redirect(url_for("clock_page"))

# ------------------------------------------------------------------
# HEALTH ENDPOINTS (for Render)
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
    """Secondary health endpoint for Render compatibility."""
    return health()

# ------------------------------------------------------------------
# TEST GOOGLE SHEETS ENDPOINT
# ------------------------------------------------------------------
@app.route("/gs-test", methods=["GET"])
def gs_test():
    now_dt  = datetime.now(TZ)
    date_str = now_dt.strftime("%Y-%m-%d")
    ts_str   = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    ok, err = log_to_google_sheets(date_str, ts_str, "TEST", "PING")
    if ok:
        return {"ok": True, "wrote": [date_str, ts_str, "TEST", "PING"]}, 200
    else:
        return {"ok": False, "error": err}, 500

# ------------------------------------------------------------------
# DEBUG GOOGLE SHEETS ENDPOINT
# ------------------------------------------------------------------
@app.route("/gs-debug", methods=["GET"])
def gs_debug():
    out = {}
    try:
        sheet_id = os.getenv("SHEET_ID", "").strip()
        out["SHEET_ID_present"] = bool(sheet_id)
        out["SHEET_ID_length"] = len(sheet_id)

        gc = _get_gs_client()
        out["auth"] = "ok"

        sh = gc.open_by_key(sheet_id)
        out["spreadsheet_title"] = sh.title
        out["worksheets"] = [ws.title for ws in sh.worksheets()]

        now_dt  = datetime.now(TZ)
        date_str = now_dt.strftime("%Y-%m-%d")
        ts_str   = now_dt.strftime("%Y-%m-%d %H:%M:%S")

        ws = sh.sheet1
        ws.append_row([date_str, ts_str, "DEBUG", "PING", "crew-clock"],
                      value_input_option="RAW",
                      insert_data_option="INSERT_ROWS",
                      table_range=None)
        out["append"] = "ok"
        out["row_written"] = [date_str, ts_str, "DEBUG", "PING", "crew-clock"]
        return out, 200

    except gspread.exceptions.APIError as e:
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
