# App.py
import os
import sqlite3
from datetime import datetime
from flask import (
    Flask, render_template, request, redirect, url_for,
    request as flask_request, session, flash, jsonify
)
from zoneinfo import ZoneInfo
from collections import defaultdict, deque, OrderedDict

# ---- GOOGLE SHEETS IMPORTS ----
import traceback
import gspread
from gspread import exceptions as gsex
from google.oauth2.service_account import Credentials

app = Flask(__name__)

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
DB_PATH = os.getenv("DB_PATH", "clock.db")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "").strip()
app.secret_key = os.getenv("SECRET_KEY", "dev-insecure").encode()

try:
    TZ = ZoneInfo(os.getenv("TZ", "America/New_York"))
except Exception:
    TZ = datetime.now().astimezone().tzinfo

# ------------------------------------------------------------------
# DB Init
# ------------------------------------------------------------------
def init_db():
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

# ------------------------------------------------------------------
# DB Helpers
# ------------------------------------------------------------------
def get_recent_entries(limit=50):
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT id, crew, action, ts FROM entries ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
    return rows

def insert_entry(crew, action, timestamp):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT INTO entries (crew, action, ts) VALUES (?, ?, ?)",
            (crew, action, timestamp)
        )
        conn.commit()

def calculate_daily_hours():
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
# Google Sheets Helpers (same as before)
# ------------------------------------------------------------------
def _get_gs_client():
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
    iso = dt.isocalendar()
    return f"Week {iso.year}-{iso.week:02d}"

def _get_or_create_week_ws(sh, week_title: str):
    for ws in sh.worksheets():
        if ws.title == week_title:
            return ws
    ws = sh.add_worksheet(title=week_title, rows=1000, cols=8)
    ws.update("A1:G1", [["date","crew","action","ts_in","ts_out","hours","source"]])
    return ws

def _get_or_create_totals_ws(sh, week_title: str):
    title = f"Totals {week_title}"
    for ws in sh.worksheets():
        if ws.title == title:
            return ws
    ws = sh.add_worksheet(title=title, rows=200, cols=4)
    ws.update("A1:C1", [["crew","total_hours","updated_at"]])
    return ws

def _get_or_create_all_weeks_ws(sh):
    title = "All Weeks Summary"
    for ws in sh.worksheets():
        if ws.title == title:
            return ws
    ws = sh.add_worksheet(title=title, rows=5000, cols=4)
    ws.update("A1:D1", [["week_title","crew","total_hours","updated_at"]])
    return ws

def _find_open_in_ts(crew: str):
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
            [date_str, crew, action, ts_in or "", ts_out or "", hours if hours is not None else "", "crew-clock"],
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

from collections import OrderedDict as _OD
def update_all_weeks_summary(sh, week_title: str, ordered_totals: _OD, updated_at: str):
    ws = _get_or_create_all_weeks_ws(sh)
    values = ws.get_all_values()
    rows = [["week_title","crew","total_hours","updated_at"]]
    if values and len(values) > 1:
        header = values[0]
        idx = {name: i for i, name in enumerate(header)}
        for row in values[1:]:
            if len(row) < 2: continue
            if row[0] != week_title:
                rows.append([
                    row[idx.get("week_title",0)],
                    row[idx.get("crew",1)],
                    row[idx.get("total_hours",2)],
                    row[idx.get("updated_at",3)],
                ])
    for crew, hrs in ordered_totals.items():
        rows.append([week_title, crew, round(hrs,2), updated_at])
    if ordered_totals:
        grand = round(sum(ordered_totals.values()),2)
        rows.append([week_title, "__WEEK_TOTAL__", grand, updated_at])
    ws.clear()
    ws.update(f"A1:D{len(rows)}", rows)

def update_weekly_totals_for_week(week_title: str):
    try:
        sheet_id = os.getenv("SHEET_ID", "").strip()
        if not sheet_id:
            return False, "Missing SHEET_ID"
        gc = _get_gs_client()
        sh = gc.open_by_key(sheet_id)
        week_ws = _get_or_create_week_ws(sh, week_title)
        values = week_ws.get_all_values()
        totals_ws = _get_or_create_totals_ws(sh, week_title)

        if not values or len(values) < 2:
            totals_ws.clear()
            totals_ws.update("A1:C1", [["crew","total_hours","updated_at"]])
            update_all_weeks_summary(sh, week_title, _OD(), datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S"))
            return True, None

        header = values[0]
        idx = {name: i for i, name in enumerate(header)}
        required = ["crew","action","hours"]
        if not all(col in idx for col in required):
            return False, f"Missing columns in {week_title}: need {required}, have {header}"

        totals = defaultdict(float)
        for row in values[1:]:
            try:
                action = row[idx["action"]].strip().upper()
                if action != "OUT": continue
                crew = row[idx["crew"]].strip()
                hrs_str = row[idx["hours"]].strip()
                if not hrs_str: continue
                hrs = float(hrs_str)
                totals[crew] += hrs
            except Exception:
                continue

        ordered = _OD(sorted(totals.items(), key=lambda x: x[0].lower()))
        now_str = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
        totals_ws.clear()
        rows = [["crew","total_hours","updated_at"]]
        for crew, hrs in ordered.items():
            rows.append([crew, round(hrs,2), now_str])
        if len(rows) > 1:
            grand = round(sum(v for v in totals.values()),2)
            rows.append(["__WEEK_TOTAL__", grand, now_str])
        totals_ws.update(f"A1:C{len(rows)}", rows)
        update_all_weeks_summary(sh, week_title, ordered, now_str)
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
# Public routes (unchanged)
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
    if not crew or action not in {"IN","OUT"}:
        return redirect(url_for("clock_page"))
    now_dt = datetime.now(TZ)
    date_str = now_dt.strftime("%Y-%m-%d")
    ts_str = now_dt.strftime("%Y-%m-%d %H:%M:%S")

    ts_in_val = ts_out_val = None
    hours_val = None
    if action == "OUT":
        open_in_ts = _find_open_in_ts(crew)
        if open_in_ts:
            ts_in_val = open_in_ts
            ts_out_val = ts_str
            t_in = datetime.strptime(open_in_ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=TZ)
            t_out = now_dt
            hours_val = round((t_out - t_in).total_seconds() / 3600.0, 2)

    insert_entry(crew, action, ts_str)

    if action == "IN":
        ok, err = log_week_row(date_str, crew, action, ts_in=ts_str, ts_out=None, hours=None)
    else:
        ok, err = log_week_row(date_str, crew, action, ts_in=ts_in_val, ts_out=ts_out_val, hours=hours_val)
        week_title = _week_title(now_dt)
        ok2, err2 = update_weekly_totals_for_week(week_title)
        if not ok2:
            print("Totals update failed:", err2)
    if not ok:
        print("Sheets logging failed:", err)
    return redirect(url_for("clock_page"))

# ------------------------------------------------------------------
# Health / Debug (kept)
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

@app.route("/rebuild-totals", methods=["POST", "GET"])
def rebuild_totals():
    try:
        q = flask_request.args.get("week", "").strip()
        if q:
            year, week = q.split("-")
            week_title = f"Week {int(year)}-{int(week):02d}"
        else:
            week_title = _week_title(datetime.now(TZ))
        ok, err = update_weekly_totals_for_week(week_title)
        if ok:
            return {"ok": True, "week_title": week_title}, 200
        return {"ok": False, "week_title": week_title, "error": err}, 500
    except Exception as e:
        return {"ok": False, "error": str(e)}, 500

# ------------------------------------------------------------------
# Auth + Admin
# ------------------------------------------------------------------
def require_login(f):
    def wrapper(*args, **kwargs):
        if not session.get("admin"):
            return redirect(url_for("login", next=flask_request.path))
        return f(*args, **kwargs)
    wrapper.__name__ = f.__name__
    return wrapper

@app.route("/login", methods=["GET", "POST"])
def login():
    if flask_request.method == "POST":
        pwd = flask_request.form.get("password", "")
        if ADMIN_PASSWORD and pwd == ADMIN_PASSWORD:
            session["admin"] = True
            flash("Logged in.", "ok")
            return redirect(flask_request.args.get("next") or url_for("admin"))
        flash("Bad password.", "err")
    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "ok")
    return redirect(url_for("login"))

@app.route("/admin", methods=["GET", "POST"])
@require_login
def admin():
    # Handle actions
    msg = None
    if flask_request.method == "POST":
        if flask_request.form.get("action") == "rebuild":
            ok, err = update_weekly_totals_for_week(_week_title(datetime.now(TZ)))
            msg = "Rebuilt totals." if ok else f"Rebuild failed: {err}"

    # Gather data for dashboard
    # 1) This Week Totals
    sheet_id = os.getenv("SHEET_ID", "").strip()
    totals = []
    updated_at = ""
    week_title = _week_title(datetime.now(TZ))
    try:
        gc = _get_gs_client()
        sh = gc.open_by_key(sheet_id)
        totals_ws = _get_or_create_totals_ws(sh, week_title)
        vals = totals_ws.get_all_values()
        if vals and len(vals) > 1:
            header = vals[0]
            for row in vals[1:]:
                if not row: continue
                if row[0] == "__WEEK_TOTAL__":
                    updated_at = row[2] if len(row) > 2 else ""
                    continue
                crew = row[0]
                hours = row[1] if len(row) > 1 else ""
                ua = row[2] if len(row) > 2 else ""
                totals.append((crew, hours))
                updated_at = ua or updated_at
    except Exception as e:
        msg = msg or f"Sheets read failed: {e}"

    # 2) Summary tail
    summary_tail = []
    try:
        sh = gc.open_by_key(sheet_id)
        all_ws = _get_or_create_all_weeks_ws(sh)
        vals = all_ws.get_all_values()
        if vals and len(vals) > 1:
            tail = vals[-10:]
            for r in tail[1:] if tail[0][0].lower() == "week_title" else tail:
                if len(r) >= 4:
                    summary_tail.append((r[0], r[1], r[2], r[3]))
    except Exception:
        pass

    # 3) Recent punches
    punches = get_recent_entries(limit=50)

    return render_template(
        "admin.html",
        msg=msg,
        week_title=week_title,
        totals=totals,
        updated_at=updated_at,
        summary_tail=summary_tail,
        punches=punches
    )

# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)
