import os
import re
import json
import traceback
import sys                   # ← 新增
from datetime import date, datetime, time, timedelta
from functools import wraps
from calendar import monthrange


def resource_path(relative_path: str) -> str:
    """取得打包後或開發環境下都能用的檔案路徑"""
    if hasattr(sys, "_MEIPASS"):        # PyInstaller 解壓後的暫存資料夾
        base_path = sys._MEIPASS
    else:
        base_path = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(base_path, relative_path)
    KEY_PATH = resource_path(os.path.join("key", "yv-bq-key.json"))

from flask import (
    Flask, request, redirect, url_for, session, render_template
)
from google.cloud import bigquery

import gspread
from google.oauth2.service_account import Credentials

# ================== 1. BigQuery + Google Sheet 憑證設定 ==================

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

SHEET_SPREADSHEET_ID = "1uGA6GBkhItPp730Fbj7anMSQ1LS_eK7T0GQcb5iVt3w"
SHEET_GID = 0

SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# 優先從環境變數讀 JSON（金鑰不放到 GitHub）
SERVICE_ACCOUNT_JSON = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")

if SERVICE_ACCOUNT_JSON:
    # 雲端（Render）用：環境變數裡直接放整包 JSON
    key_info = json.loads(SERVICE_ACCOUNT_JSON)

    creds = Credentials.from_service_account_info(
        key_info,
        scopes=SHEETS_SCOPES,
    )

    bq_client = bigquery.Client(
        credentials=creds,
        project=key_info.get("project_id"),  # 從 JSON 裡抓 project_id
    )
else:
    # 本機：沿用原本 key/yv-bq-key.json
    KEY_PATH = resource_path(os.path.join("key", "yv-bq-key.json"))
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_PATH

    creds = Credentials.from_service_account_file(
        KEY_PATH,
        scopes=SHEETS_SCOPES,
    )

    bq_client = bigquery.Client()

# Sheets client 共用同一組 creds
sheets_client = gspread.authorize(creds)

# ================== 2. Flask App 基本設定 ==================

app = Flask(
    __name__,
    template_folder=resource_path("templates"),
    static_folder=resource_path("static"),
)
# session 用的 secret key，正式環境請換成更長的亂數
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-change-me")

def login_required(f):
    """簡單的登入檢查 decorator"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        if "serial_id" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return wrapper


# ================== 3. BigQuery / Sheets 輔助函式 ==================


def get_resident_by_login(serial_id: str, agency_id: int):
    """
    用 SERIAL_ID + AGENCY_ID 當帳密，從 resdient_agency_device 找住民：
      - note 必須為 'C'
      - serial_id 不得為空
    找到就回傳 dict，找不到回傳 None
    """
    query = f"""
    SELECT
      resident_id,
      resident_name,
      agency_id,
      agency_name,
      codename,
      bed_number,
      note,
      serial_id
    FROM {RESIDENT_TABLE}
    WHERE
      serial_id = @serial_id
      AND agency_id = @agency_id
      AND note = 'C'
      AND serial_id IS NOT NULL
      AND serial_id != ''
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("serial_id", "STRING", serial_id),
            bigquery.ScalarQueryParameter("agency_id", "INT64", agency_id),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())
    if not rows:
        return None
    return dict(rows[0])


def get_latest_created_date(resident_id: int) -> date:
    """
    回傳這個 resident 在 DAILY_TABLE 中最後一筆 created_at 的「日期」。
    若沒有任何資料，回傳今天。
    """
    query = f"""
    SELECT
      DATE(MAX(created_at)) AS last_date
    FROM {DAILY_TABLE}
    WHERE
      resident_id = @resident_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())
    if rows and rows[0].get("last_date") is not None:
        last_date = rows[0]["last_date"]
        if isinstance(last_date, datetime):
            return last_date.date()
        return last_date
    return date.today()


def get_daily_for_resident_by_range(resident_id: int, start_date: date, end_date: date):
    """
    撈某個 resident 在指定日期區間的所有紀錄
    提供 /daily 列表用
    """
    query = f"""
    SELECT
      d.*
    FROM {DAILY_TABLE} AS d
    WHERE
      d.resident_id = @resident_id
      AND DATE(d.created_at) BETWEEN @start_date AND @end_date
    ORDER BY d.created_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())
    return [dict(r) for r in rows]


def get_30min_slots_for_date(resident_id: int, day_date: date):
    """
    從 N_bq_toC_fig 撈出某一天的作息資料：
      - 時間範圍：day_date 12:00 ~ (day_date + 1 天) 12:00
      - 切成 30 分鐘一格，共 48 格
      - 同一格若有多筆，優先順序：08 > 07 > 00
      - 回傳長度 48 的 list，每格可能是 '08' / '07' / '00' / 'none'
    """
    start_dt = datetime.combine(day_date, time(12, 0, 0))
    end_dt = start_dt + timedelta(days=1)

    query = f"""
    SELECT
      slot_index,
      MAX(priority) AS max_p
    FROM (
      SELECT
        CAST(TIMESTAMP_DIFF(detect_at, @start_ts, MINUTE) / 30 AS INT64) AS slot_index,
        CASE value
          WHEN '08' THEN 3
          WHEN '07' THEN 2
          WHEN '00' THEN 1
          ELSE 0
        END AS priority
      FROM {TOC_FIG_TABLE}
      WHERE
        resident_id = @resident_id
        AND detect_at >= @start_ts
        AND detect_at < @end_ts
        AND value IN ('00', '07', '08')
    )
    GROUP BY slot_index
    HAVING slot_index BETWEEN 0 AND 47
    ORDER BY slot_index
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", start_dt),
            bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", end_dt),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())

    # 預設 48 格都沒有資料
    slots = ["none"] * 48
    priority_to_value = {3: "08", 2: "07", 1: "00"}

    for r in rows:
        idx = r["slot_index"]
        max_p = r["max_p"]
        val = priority_to_value.get(max_p)
        if val is None:
            continue
        if 0 <= idx < 48:
            slots[idx] = val

    return slots


def convert_asleep_start_to_hour(value):
    """
    將 asleep_start 轉成「數值小時」，方便畫圖：
      - 先取時間 (HH:MM)
      - 如果時間 < 12:00，視為「隔天凌晨」=> +24
      - 這樣 y 軸大約是 20 ~ 32 (晚上8點~隔天早上8點)
    """
    if value is None:
        return None

    t = None

    if isinstance(value, datetime):
        t = value.time()
    elif isinstance(value, time):
        t = value
    else:
        s = str(value)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                    "%H:%M:%S", "%H:%M"):
            try:
                t = datetime.strptime(s, fmt).time()
                break
            except ValueError:
                continue
        if t is None:
            return None

    h = t.hour + t.minute / 60.0 + t.second / 3600.0

    # 凌晨 (例如 01:30) 視為「隔日」，加 24 小時
    if h < 12:
        h += 24.0

    return h


def build_night_interval_for_day(day_date, asleep_value, night_sleep_hours):
    """
    建立某天 d 的「夜間區間」：
      night_start = d + asleep_start
      night_end   = night_start + night_sleep_hours(小時)
    可能跨日（例如 21:00 ~ 翌日 08:00）
    """
    if asleep_value is None or night_sleep_hours is None:
        return None, None

    # 把 asleep_value 轉成 time
    if isinstance(asleep_value, datetime):
        t = asleep_value.time()
    elif isinstance(asleep_value, time):
        t = asleep_value
    else:
        s = str(asleep_value)
        t = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
                    "%H:%M:%S", "%H:%M"):
            try:
                t = datetime.strptime(s, fmt).time()
                break
            except ValueError:
                continue
        if t is None:
            return None, None

    try:
        nh = float(night_sleep_hours)
    except (TypeError, ValueError):
        return None, None

    night_start = datetime.combine(day_date, t)
    night_end = night_start + timedelta(hours=nh)
    return night_start, night_end


def compute_day_night_avg_intervals(day_date, night_start_dt, night_end_dt, flip_datetimes):
    """
    計算某天 day_date 的：
      - 夜間翻身平均間隔（分鐘）
      - 日間翻身平均間隔（分鐘）

    規則：
      - 只考慮 flip_dt.date() 在 { day_date, day_date+1 } 的翻身
      - 若 night_start_dt <= flip_dt < night_end_dt → 夜間翻身
        其他 → 日間翻身
    """
    day0 = day_date
    day1 = day_date + timedelta(days=1)
    night_flips = []
    day_flips = []

    for flip_dt in flip_datetimes:
        fdate = flip_dt.date()
        if fdate < day0 or fdate > day1:
            continue
        if (night_start_dt is not None and night_end_dt is not None
                and night_start_dt <= flip_dt < night_end_dt):
            night_flips.append(flip_dt)
        else:
            day_flips.append(flip_dt)

    def avg_interval(flips):
        if len(flips) < 2:
            return None
        flips_sorted = sorted(flips)
        diffs = []
        for i in range(1, len(flips_sorted)):
            delta_min = (flips_sorted[i] - flips_sorted[i - 1]).total_seconds() / 60.0
            if delta_min > 0:
                diffs.append(delta_min)
        if not diffs:
            return None
        return sum(diffs) / len(diffs)

    return avg_interval(night_flips), avg_interval(day_flips)


def get_date_key(v) -> str:
    """把 created_at 轉成 'YYYY-MM-DD' 字串，方便跟月份每一天對齊。"""
    if v is None:
        return ""
    if hasattr(v, "strftime"):
        try:
            if isinstance(v, datetime):
                return v.date().strftime("%Y-%m-%d")
            return v.strftime("%Y-%m-%d")
        except Exception:
            pass
    s = str(v)
    return s[:10]  # '2025-10-03 00:00:00' -> '2025-10-03'


def parse_month_cell(text):
    """
    從「月份」欄位文字裡抓出 (year, month)，例如：
      2025/10、2025-10、2025年10月 -> (2025, 10)
    找不到就回傳 (None, None)
    """
    s = str(text or "").strip()
    m = re.search(r"(\d{4}).*?(\d{1,2})", s)
    if not m:
        return None, None
    try:
        y = int(m.group(1))
        mth = int(m.group(2))
        return y, mth
    except ValueError:
        return None, None


def to_int_or_none(x):
    if x is None or x == "":
        return None
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


# ---------- 評語比對用的輔助函式 ----------

def _norm_text(s):
    """
    將文字標準化：
      - None -> ""
      - 轉成 str
      - 移除全形空白、所有空白
      - 轉小寫
    """
    if s is None:
        return ""
    s = str(s).replace("\u3000", " ")
    s = re.sub(r"\s+", "", s)
    return s.lower()


def _norm_digits(x):
    """
    只保留數字，例如：
      '  001-23 ' -> '00123'
      112 -> '112'
    """
    if x is None:
        return ""
    s = str(x)
    digits = re.findall(r"\d+", s)
    return "".join(digits)


def get_month_comments_from_sheet(serial_id: str, agency_id: int, year: int, month: int):
    """
    讀取 Google Sheet 評語；對 serial/agency 進行強力正規化比對。
    """
    result = {
        "active_summary": "", "active_resp": "", "active_sleep_range": "",
        "active_asleep_start": "", "active_night_leave": "",
        "active_trend": "", "active_trend_summary": "",
        "bed_summary": "", "bed_resp": "", "bed_night_bed": "",
        "bed_leave_total": "", "bed_turn": "", "bed_trend": "",
        "bed_trend_summary": "",
    }

    # 先打開試算表
    try:
        sh = sheets_client.open_by_key(SHEET_SPREADSHEET_ID)
        try:
            ws = sh.get_worksheet_by_id(SHEET_GID)
        except Exception:
            ws = sh.get_worksheet(0)
        print(f"[DEBUG] Sheet='{sh.title}', WS='{ws.title}', gid={ws.id}")
    except Exception as e:
        print("[WARN] 開啟試算表失敗（repr）：", repr(e))
        traceback.print_exc()
        return result

    # 讀資料
    try:
        values = ws.get_all_values()
    except Exception as e:
        print(f"[WARN] 讀取試算表失敗：{e}")
        return result

    if not values or len(values) < 2:
        print("[WARN] 試算表沒有資料/只有表頭")
        return result

    # 表頭正規化
    raw_headers = values[0]
    headers, seen = [], set()
    for i, h in enumerate(raw_headers):
        name = (h or "").strip().replace("\u3000", " ")
        if not name:
            name = f"__col{i}__"
        if name in seen:
            name = f"{name}_{i}"
        seen.add(name)
        headers.append(name)
    print("[DEBUG] Headers:", headers)

    # 目標（正規化）
    target_serial_norm = _norm_text(serial_id)
    target_agency_norm = _norm_digits(agency_id)
    print(f"[DEBUG] Target serial='{target_serial_norm}', agency='{target_agency_norm}', ym={year}-{month:02d}")

    # 先收集同月候選列
    month_candidates = []

    hit_row = None
    for row_vals in values[1:]:
        row = {headers[i]: (row_vals[i] if i < len(row_vals) else "") for i in range(len(headers))}
        y, mth = parse_month_cell(row.get("月份"))
        if y != year or mth != month:
            continue

        sheet_serial_raw = row.get("serial_id")
        sheet_agency_raw = row.get("agency_id")

        sheet_serial_norm = _norm_text(sheet_serial_raw)
        sheet_agency_norm = _norm_digits(sheet_agency_raw)

        month_candidates.append((row.get("月份"), sheet_serial_raw, sheet_agency_raw,
                                 sheet_serial_norm, sheet_agency_norm))

        # serial 必須相等（正規化後）
        if sheet_serial_norm != target_serial_norm:
            continue

        # agency 規則：
        #   - 試算表該欄空白 -> 視為通配（任何 agency 都可命中）
        #   - 否則用純數字比對
        if sheet_agency_norm and (sheet_agency_norm != target_agency_norm):
            continue

        hit_row = row
        break

    print("[DEBUG] 當月候選列（前 5 筆）：")
    for item in month_candidates[:5]:
        print("  月份/serial/agency/raw_norm =", item)

    if not hit_row:
        print("[WARN] 仍未命中評語列（請檢查上面 raw_norm 值是否與 Target 一致）")
        return result

    mapping = {
        "active_summary": "本月總結_離床",
        "active_resp": "月呼吸率紀錄_離床",
        "active_sleep_range": "每日休息時段_離床",
        "active_asleep_start": "每日上床休息時間_離床",
        "active_night_leave": "夜間休息離床次數_離床",
        "active_trend": "每月趨勢狀態_離床",
        "active_trend_summary": "月趨勢狀態總結_離床",
        "bed_summary": "本月總結_臥床",
        "bed_resp": "月呼吸率紀錄_臥床",
        "bed_night_bed": ["夜間最床臥床時段_臥床", "夜間最長臥床時長_臥床"],
        "bed_leave_total": ["全日離床總時長_臥床", "日間離床總時長_臥床"],
        "bed_turn": "日夜翻身間隔_臥床",
        "bed_trend": "每月趨勢狀態_臥床",
        "bed_trend_summary": "月趨勢狀態總結_臥床",
    }
    for key, col in mapping.items():
        if isinstance(col, list):
            for c in col:
                val = (hit_row.get(c) or "").strip()
                if val:
                    result[key] = val
                    break
        else:
            result[key] = (hit_row.get(col) or "").strip()

    print("[DEBUG] 命中評語鍵：", [k for k, v in result.items() if v])
    return result


# ================== 4. Routes ==================


@app.route("/")
def index():
    # 已登入就先到報表選擇頁，否則去 login
    if "serial_id" in session:
        return redirect(url_for("report_menu"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "GET":
        session.clear()
    """
    登入頁面：
      帳號：serial_id（設備序號）
      密碼：agency_id（客戶編號）
    只有 note='C' 且 serial_id 不為空的住民可以登入
    """
    error = None

    if request.method == "POST":
        serial_id = request.form.get("serial_id", "").strip()
        agency_id_str = request.form.get("agency_id", "").strip()

        if not serial_id or not agency_id_str:
            error = "請輸入設備序號與客戶編號。"
        else:
            try:
                agency_id = int(agency_id_str)
            except ValueError:
                error = "客戶編號（AGENCY_ID）必須是數字。"
            else:
                resident = get_resident_by_login(serial_id, agency_id)
                if resident is None:
                    error = "帳號或密碼錯誤，或該帳號未開啟（note 不是 C / serial_id 為空）。"
                else:
                    # 登入成功，把必要資訊寫進 session
                    session["serial_id"] = resident["serial_id"]
                    session["agency_id"] = resident["agency_id"]
                    session["resident_id"] = resident["resident_id"]
                    session["resident_name"] = resident.get("resident_name") or ""
                    session["agency_name"] = resident.get("agency_name") or ""
                    session["codename"] = resident.get("codename") or ""
                    session["bed_number"] = resident.get("bed_number") or ""
                    return redirect(url_for("report_menu"))

    return render_template("login.html", error=error)


@app.route("/logout")
@login_required
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/report_menu")
@login_required
def report_menu():
    """
    報表選單頁：
      - 顯示住民資訊
      - 提供選擇年份 / 月份
      - 可以連到 月報 / 30日作息
    """
    resident_id = session["resident_id"]

    resident_info = {
        "resident_id": resident_id,
        "resident_name": session.get("resident_name", ""),
        "serial_id": session.get("serial_id", ""),
        "agency_id": session.get("agency_id", ""),
        "agency_name": session.get("agency_name", ""),
        "codename": session.get("codename", ""),
        "bed_number": session.get("bed_number", ""),
    }

    last_date = get_latest_created_date(resident_id)
    default_year = last_date.year
    default_month = last_date.month

    return render_template(
        "report_menu.html",
        resident=resident_info,
        default_year=default_year,
        default_month=default_month,
    )


@app.route("/daily")
@login_required
def daily():
    """
    已登入使用者的頁面：
      1. 上方可以輸入 年份 + 月份，切換該月份資料
      2. 若沒給 year/month，就自動抓「這位住民最後一筆 created_at 的年月」
      3. 顯示 resdient_agency_device 的基本資料
      4. 顯示該月份在 DAILY_TABLE 的資料（列表）
      5. 顯示 Google Sheet「本月評語摘要」列表
    """

    resident_id = session["resident_id"]

    # 讀取 query string 的 year / month，例如 /daily?year=2025&month=10
    arg_year = request.args.get("year", type=int)
    arg_month = request.args.get("month", type=int)

    if arg_year and arg_month and 1 <= arg_month <= 12:
        year, month = arg_year, arg_month
    else:
        # 沒指定或有誤 → 用這個 resident 的最新一筆 created_at 當預設年月
        last_date = get_latest_created_date(resident_id)
        year, month = last_date.year, last_date.month

    # 該月份的起訖日期
    start_date = date(year, month, 1)
    if month == 12:
        end_date = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)

    resident_info = {
        "resident_id": resident_id,
        "resident_name": session.get("resident_name", ""),
        "serial_id": session.get("serial_id", ""),
        "agency_id": session.get("agency_id", ""),
        "agency_name": session.get("agency_name", ""),
        "codename": session.get("codename", ""),
        "bed_number": session.get("bed_number", ""),
    }

    daily_list = get_daily_for_resident_by_range(resident_id, start_date, end_date)

    print(f"[DEBUG] /daily resident_id={resident_id} range={start_date}~{end_date} rows={len(daily_list)}")

    # ---------- 組 HTML ----------

    html = []

    # 月份選擇表單
    html.append("<h2>月份選擇</h2>")
    html.append('<form method="get" action="/daily">')
    html.append(
        '年份：<input type="number" name="year" value="{}" style="width:80px;">'.format(
            year
        )
    )
    html.append(
        '　月份：<input type="number" name="month" min="1" max="12" value="{}" style="width:60px;">'.format(
            month
        )
    )
    html.append('　<button type="submit">切換月份</button>')
    html.append("</form>")

    # 同月份月報連結
    report_url = url_for("report", year=year, month=month)
    html.append(f'<p><a href="{report_url}">查看 {year}-{month:02d} 月月報</a></p>')

    # 住民基本資料
    html.append("<h2>住民基本資料</h2>")
    html.append('<table border="1" cellspacing="0" cellpadding="4">')
    html.append("<tr><th>欄位</th><th>值</th></tr>")
    for k, v in resident_info.items():
        cell = "" if v is None else str(v)
        html.append(f"<tr><td>{k}</td><td>{cell}</td></tr>")
    html.append("</table>")

    # ======= 讀取當月評語（來自 Google Sheet），直接列為摘要清單 =======
    month_comments = get_month_comments_from_sheet(
        serial_id=session["serial_id"],
        agency_id=session["agency_id"],
        year=year,
        month=month
    )

    # 顯示評語（為了易讀，做個中文標題對照）
    label_map = [
        ("active_summary",       "（離床）本月總結"),
        ("active_resp",          "（離床）月呼吸率紀錄"),
        ("active_sleep_range",   "（離床）每日休息時段"),
        ("active_asleep_start",  "（離床）每日上床休息時間"),
        ("active_night_leave",   "（離床）夜間休息離床次數"),
        ("active_trend",         "（離床）每月趨勢狀態"),
        ("active_trend_summary", "（離床）月趨勢狀態總結"),
        ("bed_summary",          "（臥床）本月總結"),
        ("bed_resp",             "（臥床）月呼吸率紀錄"),
        ("bed_night_bed",        "（臥床）夜間最長臥床時長"),
        ("bed_leave_total",      "（臥床）日間離床總時長"),
        ("bed_turn",             "（臥床）日夜翻身間隔"),
        ("bed_trend",            "（臥床）每月趨勢狀態"),
        ("bed_trend_summary",    "（臥床）月趨勢狀態總結"),
    ]

    html.append("<h2>本月評語摘要</h2>")
    any_comment = False
    html.append('<ul style="line-height:1.6">')
    for key, title in label_map:
        text = (month_comments.get(key) or "").strip()
        if text:
            any_comment = True
            text_html = text.replace("\n", "<br>")
            html.append(f"<li><b>{title}</b>：{text_html}</li>")
    html.append("</ul>")
    if not any_comment:
        html.append("<p>（本月無對應評語或 Google Sheet 未填寫。）</p>")

    # 該月份 daily
    html.append(f"<h2>{year}-{month:02d} DAILY 資料（{start_date} ~ {end_date}）</h2>")

    if not daily_list:
        html.append("<p>這個月份沒有任何紀錄。（DEBUG: rows=0）</p>")
    else:
        fields = list(daily_list[0].keys())
        html.append('<table border="1" cellspacing="0" cellpadding="4">')
        html.append("<tr>")
        for name in fields:
            html.append(f"<th>{name}</th>")
        html.append("</tr>")
        for row in daily_list:
            html.append("<tr>")
            for name in fields:
                value = row.get(name)
                cell = "" if value is None else str(value)
                html.append(f"<td>{cell}</td>")
            html.append("</tr>")
        html.append("</table>")

    html.append('<p><a href="/logout">登出</a></p>')

    return "".join(html)


@app.route("/report")
@login_required
def report():
    """
    月報頁：
      - 依照 query string year/month 顯示該月份資料
      - 若沒給 year/month，就用這個 resident 最新一筆 created_at 的年月
      - 判斷本月是「臥床報表」或「離床報表」
      - 只使用「有效天」計算平均與畫圖
        (night_on_bed / night_sleep / sleep_respiration 任何一個為 0 的天會被排除)
      - 夜間最長臥床時長：從 N_bq_Duration24 找出 bed_state='09'、
        每天 duration 最大值（秒），並轉成「小時」
      - 全日離床總時長：24 - day_on_bed - night_on_bed
      - 日夜平均翻身間隔：從 N_bq_Duration24 的翻身時間（time_start）計算
      - 評語：從 Google Sheet 讀取
    """

    resident_id = session["resident_id"]
    serial_id = session.get("serial_id", "")
    agency_id = session.get("agency_id", None)

    arg_year = request.args.get("year", type=int)
    arg_month = request.args.get("month", type=int)

    if arg_year and arg_month and 1 <= arg_month <= 12:
        report_year, report_month = arg_year, arg_month
    else:
        last_date = get_latest_created_date(resident_id)
        report_year, report_month = last_date.year, last_date.month

    # 查詢範圍：該月份 1 號 ~ 最後一天
    start_date = date(report_year, report_month, 1)
    if report_month == 12:
        end_date = date(report_year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = date(report_year, report_month + 1, 1) - timedelta(days=1)
    end_date_plus1 = end_date + timedelta(days=1)

    # 住民資訊（直接從 session 拿）
    resident_info = {
        "resident_id": resident_id,
        "resident_name": session.get("resident_name", ""),
        "serial_id": serial_id,
        "agency_id": agency_id,
        "agency_name": session.get("agency_name", ""),
        "codename": session.get("codename", ""),
        "bed_number": session.get("bed_number", ""),
    }

    # ========== 讀 Google Sheet 評語 ==========
    month_comments = get_month_comments_from_sheet(serial_id, agency_id, report_year, report_month)

    # 撈這個住民該月份的 daily 資料
    query = f"""
    SELECT
      d.*
    FROM {DAILY_TABLE} AS d
    WHERE
      d.resident_id = @resident_id
      AND DATE(d.created_at) BETWEEN @start_date AND @end_date
    ORDER BY d.created_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    rows = list(bq_client.query(query, job_config=job_config).result())
    daily_list = [dict(r) for r in rows]

    print(f"[DEBUG] /report resident_id={resident_id} range={start_date}~{end_date} rows={len(daily_list)}")

    # ====== 撈 N_bq_Duration24：bed_state='09'，每天 duration 最大值（秒→小時） ======
    duration_query = f"""
    SELECT
      DATE(created_at) AS d,
      MAX(SAFE_CAST(duration AS FLOAT64)) / 3600.0 AS max_duration_hours
    FROM {DURATION_TABLE}
    WHERE
      resident_id = @resident_id
      AND bed_state = '09'
      AND DATE(created_at) BETWEEN @start_date AND @end_date
    GROUP BY d
    """

    duration_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    duration_rows = list(bq_client.query(duration_query, job_config=duration_job_config).result())
    duration_map = {}
    for r in duration_rows:
        d_val = r["d"]
        if isinstance(d_val, datetime):
            d_val = d_val.date()
        key = d_val.strftime("%Y-%m-%d")
        dur = r["max_duration_hours"]
        try:
            dur = float(dur) if dur is not None else None
        except (TypeError, ValueError):
            dur = None
        duration_map[key] = dur

    # ====== 撈 N_bq_Duration24：bed_state='09' 的翻身時間，用來算日夜翻身間隔 ======
    turn_query = f"""
    SELECT
      DATE(created_at) AS d,
      time_start
    FROM {DURATION_TABLE}
    WHERE
      resident_id = @resident_id
      AND bed_state = '09'
      AND DATE(created_at) BETWEEN @start_date AND @end_date_plus1
    ORDER BY d, time_start
    """

    turn_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date_plus1", "DATE", end_date_plus1),
        ]
    )

    turn_rows = list(bq_client.query(turn_query, job_config=turn_job_config).result())
    flip_datetimes = []
    for r in turn_rows:
        d_val = r["d"]
        if isinstance(d_val, datetime):
            d_val = d_val.date()
        date_part = d_val
        t_val = r["time_start"]

        # 轉成 time
        if isinstance(t_val, datetime):
            t_obj = t_val.time()
        elif isinstance(t_val, time):
            t_obj = t_val
        else:
            s = str(t_val)
            t_obj = None
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    t_obj = datetime.strptime(s, fmt).time()
                    break
                except ValueError:
                    continue
            if t_obj is None:
                continue

        flip_dt = datetime.combine(date_part, t_obj)
        flip_datetimes.append(flip_dt)

    # ====== 把 daily 資料先對齊到「這個月的每一天」 ======
    date_to_row = {}
    for row in daily_list:
        key = get_date_key(row.get("created_at"))
        if key:
            date_to_row[key] = row  # 假設一天最多一筆

    # 準備圖表用 array
    labels              = []
    resp_rate           = []  # 每日呼吸紀錄
    night_sleep_range   = []  # 每日夜間休息時段 [start_hour, end_hour]
    asleep_start_hours  = []  # 每日上床時間
    leave_bed_total     = []  # 全日離床總時長
    night_turn_interval = []  # 夜間翻身平均間隔（分鐘）
    day_turn_interval   = []  # 日間翻身平均間隔（分鐘）
    night_bed_hours     = []  # 夜間最長臥床時長 (from N_bq_Duration24)
    night_leave_count   = []  # 夜間離床次數（使用 night_leave）

    # 呼吸評分用：每一天的 std_dev
    rr_std_list         = []

    # 作息品質評分用
    night_sleep_for_score = []  # 每天的 night_sleep（有效天）
    night_leave_for_score = []  # 每天的 night_leave（有效天）
    sleep_eff_for_score   = []  # 每天的 night_sleep / night_on_bed（有效天）

    # ====== 用來判斷「臥床 / 離床」的累計 ======
    sum_day_leave   = 0.0
    cnt_day_leave   = 0
    sum_onbed_total = 0.0  # (night_on_bed + day_on_bed)
    cnt_onbed_total = 0

    # 小工具：安全轉 float
    def to_float(x):
        if x is None:
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    # 逐日處理
    d = start_date
    while d <= end_date:
        key = d.strftime("%Y-%m-%d")
        labels.append(key)
        row = date_to_row.get(key)

        if row is None:
            # 完全沒 daily 資料的日子 → 全部 None
            resp_rate.append(None)
            night_sleep_range.append(None)
            asleep_start_hours.append(None)
            leave_bed_total.append(None)
            night_turn_interval.append(None)
            day_turn_interval.append(None)
            night_bed_hours.append(None)
            night_leave_count.append(None)
            d += timedelta(days=1)
            continue

        # 先拿出會影響「有效 / 無效」判斷的欄位
        v_night_on_bed = to_float(row.get("night_on_bed"))
        v_night_sleep  = to_float(row.get("night_sleep"))
        v_resp         = to_float(row.get("sleep_respiration"))
        v_day_on_bed   = to_float(row.get("day_on_bed"))

        # 判斷這天是不是要整天丟掉
        invalid_day = False

        # 規則 1：night_on_bed / night_sleep / resp 任一為 0 → 不列入
        for v in (v_night_on_bed, v_night_sleep, v_resp):
            if v is not None and v == 0:
                invalid_day = True
                break

        # 規則 2：night_on_bed < 2 小時 → 也不列入
        if not invalid_day and v_night_on_bed is not None and v_night_on_bed < 2:
            invalid_day = True

        if invalid_day:
            # 這一天不要顯示也不算平均
            resp_rate.append(None)
            night_sleep_range.append(None)
            asleep_start_hours.append(None)
            leave_bed_total.append(None)
            night_turn_interval.append(None)
            day_turn_interval.append(None)
            night_bed_hours.append(None)
            night_leave_count.append(None)
            d += timedelta(days=1)
            continue

        # -------- 這裡開始是「有效天」 --------

        # 作息品質評分用：夜間休息時間
        if v_night_sleep is not None:
            night_sleep_for_score.append(v_night_sleep)

        # 0. 解析呼吸變異 std_dev（respiration_analy JSON 欄位）
        std_val = None
        try:
            resp_analy_raw = row.get("respiration_analy")
            if resp_analy_raw:
                if isinstance(resp_analy_raw, str):
                    analy_obj = json.loads(resp_analy_raw)
                else:
                    analy_obj = resp_analy_raw  # BigQuery STRUCT 可能直接是 dict-like
                std_val = analy_obj.get("std_dev")
                if std_val is not None:
                    std_val = float(std_val)
        except Exception:
            std_val = None
        if std_val is not None:
            rr_std_list.append(std_val)

        # 1. 每日呼吸紀錄
        resp_rate.append(v_resp if v_resp is not None else None)

        # 2. 上床時間（轉成 20~32 小時）
        start_h = convert_asleep_start_to_hour(row.get("asleep_start"))
        asleep_start_hours.append(start_h)

        # 2b. 夜間休息時段（起點 = asleep_start, 終點 = asleep_start + night_sleep）
        if start_h is None or v_night_sleep is None:
            night_sleep_range.append(None)
        else:
            night_sleep_range.append([start_h, start_h + v_night_sleep])

        # 3. 全日離床總時長 = 24 - day_on_bed - night_on_bed
        if v_day_on_bed is not None and v_night_on_bed is not None:
            total_on_bed = v_day_on_bed + v_night_on_bed
            total_leave = 24.0 - total_on_bed
            if total_leave < 0:
                total_leave = 0.0
            leave_bed_total.append(total_leave)
        else:
            leave_bed_total.append(None)

        # 作息品質評分用：休息效率 night_sleep / night_on_bed
        if (
            v_night_sleep is not None
            and v_night_on_bed is not None
            and v_night_on_bed > 0
        ):
            sleep_eff_for_score.append(v_night_sleep / v_night_on_bed)

        # 4. 日夜翻身平均間隔（分鐘）──來自 N_bq_Duration24
        night_start_dt, night_end_dt = build_night_interval_for_day(
            d, row.get("asleep_start"), v_night_sleep
        )
        night_avg_min, day_avg_min = compute_day_night_avg_intervals(
            d, night_start_dt, night_end_dt, flip_datetimes
        )

        # ★ 新增：若夜間 / 日間翻身平均間隔 > 720 分鐘，視為異常，不列入顯示與評分
        if night_avg_min is not None and night_avg_min > 720:
            night_avg_min = None
        if day_avg_min is not None and day_avg_min > 720:
            day_avg_min = None

        night_turn_interval.append(night_avg_min)
        day_turn_interval.append(day_avg_min)

        # 5. 夜間最長臥床時長 (from N_bq_Duration24 每天 duration 最大值，小時)
        v_dur = duration_map.get(key)
        night_bed_hours.append(v_dur if v_dur is not None else 0)

        # 6. 夜間離床次數：改用 asleep_leave（睡眠期間離床次數）
        v_asleep_leave = to_float(row.get("asleep_leave"))
        if v_asleep_leave is not None:
            # 這個 list 會同時用在：
            # 1) 判斷 report_type（bed / active）
            # 2) Sleep Score 的「夜離狀況」評分
            night_leave_for_score.append(v_asleep_leave)
        night_leave_count.append(v_asleep_leave if v_asleep_leave is not None else 0)

        # ===== 累計用於判斷「臥床 / 離床」 =====

        # day_leave 平均
        v_day_leave = to_float(row.get("day_leave"))
        if v_day_leave is not None:
            sum_day_leave += v_day_leave
            cnt_day_leave += 1

        # night_on_bed + day_on_bed 平均
        if v_night_on_bed is not None and v_day_on_bed is not None:
            sum_onbed_total += (v_night_on_bed + v_day_on_bed)
            cnt_onbed_total += 1

        d += timedelta(days=1)

    # ===== 月平均計算 =====
    avg_day_leave = (sum_day_leave / cnt_day_leave) if cnt_day_leave > 0 else 0.0
    avg_onbed_total = (sum_onbed_total / cnt_onbed_total) if cnt_onbed_total > 0 else 0.0

    # ===== 供報表類型判斷用的 night_leave 平均 =====
    # night_leave_for_score 是前面逐日累積的「有效天 night_leave」
    if night_leave_for_score:
        avg_night_leave_for_mode = sum(night_leave_for_score) / len(night_leave_for_score)
    else:
        avg_night_leave_for_mode = None

    # ===== 判斷報表類型 =====
    # 1) 先看 URL 是否有強制指定 ?mode=bed / ?mode=active
    force_mode = request.args.get("mode")
    if force_mode in ("bed", "active"):
        report_type = force_mode
    else:
        # 2) 不再使用 codename，只看兩個月平均條件：
        #    (night_on_bed + day_on_bed) 平均 >= 10 小時 且 night_leave 平均 <= 1
        if (
            avg_onbed_total is not None
            and avg_onbed_total >= 10
            and avg_night_leave_for_mode is not None
            and avg_night_leave_for_mode <= 1
        ):
            report_type = "bed"
        else:
            report_type = "active"

    print(
        f"[DEBUG] report_type={report_type}, "
        f"avg_day_leave={avg_day_leave:.2f}, "
        f"avg_onbed_total={avg_onbed_total:.2f}, "
        f"avg_night_leave_for_mode={avg_night_leave_for_mode}, "
        f"codename={resident_info.get('codename')}"
    )

    # ===== 呼吸狀況評分（RR Score） =====
    # 1. sleep_respiration 平均值
    valid_rr_values = [v for v in resp_rate if v is not None]
    avg_rr = sum(valid_rr_values) / len(valid_rr_values) if valid_rr_values else None
    if avg_rr is None:
        score1 = 2  # 無資料時給中間分
    else:
        # 規則：
        #   12–26 → 3 分
        #   9–12 或 26–30 → 2 分
        #   其餘 (<9 或 >30) → 1 分
        if 12 <= avg_rr <= 26:
            score1 = 3
        elif (9 <= avg_rr < 12) or (26 < avg_rr <= 30):
            score1 = 2
        else:
            score1 = 1

    # 2. respiration_analy.std_dev 平均值
    avg_std = sum(rr_std_list) / len(rr_std_list) if rr_std_list else None
    if avg_std is None:
        score2 = 2  # 無資料時給中間分
    else:
        # 規則：
        #   0–4 → 3 分
        #   4–5 → 2 分
        #   5–6 → 1 分
        #   >6 → 1 分
        if avg_std <= 4:
            score2 = 3
        elif avg_std <= 5:
            score2 = 2
        elif avg_std <= 6:
            score2 = 1
        else:
            score2 = 1

    # 3. sleep_respiration 趨勢斜率（線性回歸，取絕對值）
    xy = [(i, v) for i, v in enumerate(resp_rate) if v is not None]
    if len(xy) >= 2:
        xs = [p[0] for p in xy]
        ys = [p[1] for p in xy]
        mean_x = sum(xs) / len(xs)
        mean_y = sum(ys) / len(ys)
        num = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        den = sum((x - mean_x) ** 2 for x in xs)
        slope = (num / den) if den != 0 else 0.0
    else:
        slope = 0.0  # 資料太少就當作趨勢平緩

    slope_abs = abs(slope)
    # 規則：
    #   0–0.02 → 3 分
    #   0.02–0.05 → 2 分
    #   >0.05 → 1 分
    if slope_abs <= 0.05:
        score3 = 3
    elif slope_abs <= 0.1:
        score3 = 2
    else:
        score3 = 1

    rr_score = min(score1, score2, score3)

    print(
        f"[DEBUG] RR scoring: avg_rr={avg_rr}, avg_std={avg_std}, "
        f"slope={slope}, score1={score1}, score2={score2}, "
        f"score3={score3}, rr_score={rr_score}"
    )

    # ===== 作息狀況 / 臥床照顧 評分（Daily Score） =====
    # A. 離床（active）：上床/起床時間規則
    # B. 臥床（bed）：翻身 + 日間離床規則

    # ---------- A. 離床：作息狀況評分（上床 / 起床時間） ----------

    # 使用前面已經算好的：
    #   - asleep_start_hours：每一天的上床時間（convert_asleep_start_to_hour 後，無效日為 None）
    #   - night_sleep_range：每一天的 [start_h, end_h]，無效日為 None
    asleep_start_valid = [h for h in asleep_start_hours if h is not None]

    asleep_end_valid = []
    for rng in night_sleep_range:
        if (
            isinstance(rng, list)
            and len(rng) == 2
            and rng[0] is not None
            and rng[1] is not None
        ):
            # rng[1] 就是 asleep_end（= asleep_start + night_sleep）
            asleep_end_valid.append(float(rng[1]))

    # 方便 debug
    avg_start = None
    avg_start_mod = None
    diff_start = None
    diff_end = None

    # ---- 1. 上床平均時間 ----
    # asleep_start 的平均上床時間：
    #   20:00~01:00 → 3 分
    #   17:00~20:00 或 01:00~02:00 → 2 分
    #   早於 17:00 或 晚於 02:00 → 1 分
    if asleep_start_valid:
        avg_start = sum(asleep_start_valid) / len(asleep_start_valid)
        # convert_asleep_start_to_hour 會把凌晨加 24，所以用 mod 24 回到 0~24 小時
        avg_start_mod = avg_start % 24

        # 20:00~24:00 或 00:00~01:00
        if (20 <= avg_start_mod < 24) or (0 <= avg_start_mod < 1):
            active_s1 = 3
        # 17:00~20:00 或 01:00~02:00
        elif (17 <= avg_start_mod < 20) or (1 <= avg_start_mod < 2):
            active_s1 = 2
        else:
            active_s1 = 1
    else:
        # 沒資料給中間分
        active_s1 = 2

    # ---- 2. 上床時間差 ----
    # 最晚上床時間與最早上床時間的差：
    #   差 ≤ 3 小時 → 3 分
    #   差 3~4 小時 → 2 分
    #   差 > 4 小時 → 1 分
    if len(asleep_start_valid) >= 2:
        min_start = min(asleep_start_valid)
        max_start = max(asleep_start_valid)
        diff_start = max_start - min_start
        if diff_start <= 3:
            active_s2 = 3
        elif diff_start <= 4:
            active_s2 = 2
        else:
            active_s2 = 1
    else:
        active_s2 = 2

    # ---- 3. 起床時間差 ----
    # asleep_end 最晚起床時間與最早起床時間的差：
    #   差 ≤ 3 小時 → 3 分
    #   差 3~4 小時 → 2 分
    #   差 > 4 小時 → 1 分
    if len(asleep_end_valid) >= 2:
        min_end = min(asleep_end_valid)
        max_end = max(asleep_end_valid)
        diff_end = max_end - min_end
        if diff_end <= 3:
            active_s3 = 3
        elif diff_end <= 4:
            active_s3 = 2
        else:
            active_s3 = 1
    else:
        active_s3 = 2

    active_daily_score = min(active_s1, active_s2, active_s3)

    print(
        f"[DEBUG] Active daily scoring: "
        f"avg_start={avg_start}, avg_start_mod={avg_start_mod}, "
        f"diff_start={diff_start}, diff_end={diff_end}, "
        f"s1={active_s1}, s2={active_s2}, s3={active_s3}, "
        f"active_daily_score={active_daily_score}"
    )

    # ---------- B. 臥床：臥床照顧評分（翻身 + 日間離床） ----------

    # night_turn_interval / day_turn_interval 單位：分鐘（已過濾 >720 的異常值）
    night_turn_valid = [v for v in night_turn_interval if v is not None]
    day_turn_valid   = [v for v in day_turn_interval if v is not None]

    avg_night_turn = (
        sum(night_turn_valid) / len(night_turn_valid) if night_turn_valid else None
    )
    avg_day_turn = (
        sum(day_turn_valid) / len(day_turn_valid) if day_turn_valid else None
    )

    # 日間離床總時長：使用前面算好的 avg_day_leave（來源是 daily.day_leave）
    avg_day_leave_for_score = avg_day_leave if cnt_day_leave > 0 else None

    # 1. 夜間翻身平均時間
    #   平均 < 370 分鐘 → 3 分
    #   平均 370~430 分鐘 → 2 分
    #   平均 > 430 分鐘 → 1 分
    if avg_night_turn is None:
        bed_s1 = 2
    else:
        if avg_night_turn < 370:
            bed_s1 = 3
        elif avg_night_turn <= 430:
            bed_s1 = 2
        else:
            bed_s1 = 1

    # 2. 日間翻身平均時間
    #   平均 < 190 分鐘 → 3 分
    #   平均 190~250 分鐘 → 2 分
    #   平均 > 250 分鐘 → 1 分
    if avg_day_turn is None:
        bed_s2 = 2
    else:
        if avg_day_turn < 190:
            bed_s2 = 3
        elif avg_day_turn <= 250:
            bed_s2 = 2
        else:
            bed_s2 = 1

    # 3. 日間離床總時長（平均）
    #   平均 > 4 小時 → 3 分
    #   平均 2~4 小時 → 2 分
    #   平均 < 2 小時 → 1 分
    if avg_day_leave_for_score is None:
        bed_s3 = 2
    else:
        if avg_day_leave_for_score > 4:
            bed_s3 = 3
        elif avg_day_leave_for_score >= 2:
            bed_s3 = 2
        else:
            bed_s3 = 1

    bed_daily_score = min(bed_s1, bed_s2, bed_s3)

    print(
        f"[DEBUG] Bed-care scoring: "
        f"avg_night_turn={avg_night_turn}, avg_day_turn={avg_day_turn}, "
        f"avg_day_leave={avg_day_leave_for_score}, "
        f"s1={bed_s1}, s2={bed_s2}, s3={bed_s3}, "
        f"bed_daily_score={bed_daily_score}"
    )

    # ---------- C. 根據 report_type 選擇要顯示的 daily_score ----------
    if report_type == "bed":
        daily_score = bed_daily_score
    else:
        daily_score = active_daily_score

    print(f"[DEBUG] Daily score final: report_type={report_type}, daily_score={daily_score}")

    # ===== 作息品質評分（Sleep Score） =====
    # 1. 平均休息時間 night_sleep
    if night_sleep_for_score:
        avg_night_sleep = sum(night_sleep_for_score) / len(night_sleep_for_score)
        # 規則：
        #   night_sleep 平均 6~10 → 3 分
        #   4~6 或 10~12 → 2 分
        #   <4 或 >12 → 1 分
        if 6 <= avg_night_sleep <= 10:
            sleep_s1 = 3
        elif (4 <= avg_night_sleep < 6) or (10 < avg_night_sleep <= 12):
            sleep_s1 = 2
        else:
            sleep_s1 = 1
    else:
        avg_night_sleep = None
        sleep_s1 = 2  # 沒資料給中間分

    # 2. 夜離狀況 night_leave
    if night_leave_for_score:
        avg_night_leave = sum(night_leave_for_score) / len(night_leave_for_score)
        # 規則：
        #   night_leave 平均 0~5 → 3 分
        #   5~15 → 2 分
        #   >15 → 1 分
        if avg_night_leave <= 5:
            sleep_s2 = 3
        elif avg_night_leave <= 15:
            sleep_s2 = 2
        else:
            sleep_s2 = 1
    else:
        avg_night_leave = None
        sleep_s2 = 2  # 沒資料給中間分

    # 3. 休息效率 night_sleep / night_on_bed
    if sleep_eff_for_score:
        avg_eff = sum(sleep_eff_for_score) / len(sleep_eff_for_score)
        # 規則：
        #   平均 >0.8 → 3 分
        #   0.8~0.6 → 2 分
        #   <0.6 → 1 分
        if avg_eff > 0.8:
            sleep_s3 = 3
        elif avg_eff >= 0.6:
            sleep_s3 = 2
        else:
            sleep_s3 = 1
    else:
        avg_eff = None
        sleep_s3 = 2  # 沒資料給中間分

    # 三個指標取最小值當作「作息品質」評分
    sleep_score = min(sleep_s1, sleep_s2, sleep_s3)

    print(
        f"[DEBUG] Sleep scoring: "
        f"avg_night_sleep={avg_night_sleep}, "
        f"avg_night_leave={avg_night_leave}, "
        f"avg_eff={avg_eff}, "
        f"s1={sleep_s1}, s2={sleep_s2}, s3={sleep_s3}, "
        f"sleep_score={sleep_score}"
    )

    return render_template(
        "month_report.html",
        resident=resident_info,
        report_year=report_year,
        report_month=report_month,
        start_date=start_date,
        end_date=end_date,
        labels=labels,
        resp_rate=resp_rate,
        night_sleep_range=night_sleep_range,
        asleep_start_hours=asleep_start_hours,
        leave_bed_total=leave_bed_total,
        night_turn_interval=night_turn_interval,
        day_turn_interval=day_turn_interval,
        night_bed_hours=night_bed_hours,
        night_leave_count=night_leave_count,
        report_type=report_type,
        avg_day_leave=avg_day_leave,
        avg_onbed_total=avg_onbed_total,
        month_comments=month_comments,
        rr_score=rr_score,
        daily_score=daily_score,
        sleep_score=sleep_score,
    )

@app.route("/half_report", methods=["GET"])
@login_required
def half_report():
    """
    半年追蹤報表：

      - 以 URL year/month 為「當月」，回溯包含當月在內共 6 個月。
      - 報表類型判斷：
          * 若 URL 有 ?mode=bed / ?mode=active → 直接沿用
          * 否則，使用與 /report 相同的規則，
            以「當月有效天的 (night_on_bed + day_on_bed) 平均」及「asleep_leave 平均」判斷：
               (night_on_bed + day_on_bed) 平均 >= 10 且 asleep_leave 平均 <= 1 → 臥床模板 (bed)
               否則 → 離床模板 (active)
      - 離床模板 (report_type='active')：
          圖1：每月平均夜間在床 / 休息時長
      - 臥床模板 (report_type='bed')：
          圖1：每月平均日夜翻身間隔
    """
    resident_id = session["resident_id"]
    resident = {
        "resident_id": resident_id,
        "resident_name": session.get("resident_name", ""),
        "serial_id": session.get("serial_id", ""),
        "agency_id": session.get("agency_id", ""),
        "agency_name": session.get("agency_name", ""),
        "codename": session.get("codename", ""),
        "bed_number": session.get("bed_number", ""),
    }

    # 取得基準年月：URL > 最新 daily
    arg_year = request.args.get("year", type=int)
    arg_month = request.args.get("month", type=int)
    if arg_year and arg_month and 1 <= arg_month <= 12:
        report_year, report_month = arg_year, arg_month
    else:
        last_date = get_latest_created_date(resident_id)
        report_year, report_month = last_date.year, last_date.month

    # 先記住 URL 的 mode，實際 report_type 之後再決定
    force_mode = request.args.get("mode")

    # ===== 半年月份清單（含當月，共 6 個） =====
    def shift_month(year: int, month: int, offset: int):
        base = year * 12 + (month - 1) + offset
        y = base // 12
        m = base % 12 + 1
        return y, m

    month_keys = []      # [(y,m), ...]
    labels_months = []   # ["2025/05", ...]
    for i in range(-5, 1):  # -5, -4, -3, -2, -1, 0
        y, m = shift_month(report_year, report_month, i)
        month_keys.append((y, m))
        labels_months.append(f"{y}/{m:02d}")

    # 查詢範圍：最早月份 1 號 ~ 當月最後一天
    first_year, first_month = month_keys[0]
    start_date = date(first_year, first_month, 1)
    last_year, last_month = month_keys[-1]
    end_date = date(last_year, last_month, monthrange(last_year, last_month)[1])
    end_date_plus1 = end_date + timedelta(days=1)

    # ===== 從 DAILY_TABLE 撈出半年內所有 daily 資料 =====
    query = f"""
    SELECT
      DATE(created_at) AS d,
      night_on_bed,
      night_sleep,
      sleep_respiration,
      day_on_bed,
      day_leave,
      night_leave,
      asleep_leave,
      asleep_leave_minute,
      asleep_start
    FROM {DAILY_TABLE}
    WHERE
      resident_id = @resident_id
      AND DATE(created_at) BETWEEN @start_date AND @end_date
    ORDER BY d
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    rows = [dict(r) for r in bq_client.query(query, job_config=job_config).result()]

    # ===== 逐日分配到對應月份，並套用「有效天」規則（跟 /report 一致） =====
    def to_float(x):
        if x is None or x == "":
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    # 每月統計：畫圖用
    month_stats = {
        key: {
            "night_on_bed": [],
            "night_sleep": [],
            "sleep_resp": [],
            "day_on_bed": [],  # 新增：紀錄 day_on_bed
            "day_leave": [],  # 保留原本欄位（若之後還要用）
            "day_leave_total": [],  # ★ 新增：24 - day_on_bed - night_on_bed
            "night_leave": [],
            "asleep_leave_min": [],
            "asleep_start_hour": [],
        }
        for key in month_keys
    }

    # 每月統計：臥床 / 離床 判斷用
    month_mode_stats = {
        key: {
            "sum_onbed_total": 0.0,
            "cnt_onbed_total": 0,
            "sum_asleep_leave": 0.0,
            "cnt_asleep_leave": 0,
        }
        for key in month_keys
    }

    # 給「翻身間隔」用：記錄「有效天」的 night_sleep / asleep_start
    daily_for_turn = {}

    for row in rows:
        d_val = row.get("d")
        if isinstance(d_val, datetime):
            d_val = d_val.date()
        if not isinstance(d_val, date):
            continue

        key = (d_val.year, d_val.month)
        if key not in month_stats:
            continue

        v_night_on_bed = to_float(row.get("night_on_bed"))
        v_night_sleep   = to_float(row.get("night_sleep"))
        v_resp          = to_float(row.get("sleep_respiration"))
        v_day_on_bed    = to_float(row.get("day_on_bed"))
        v_asleep_leave  = to_float(row.get("asleep_leave"))

        # 「有效天」判斷：night_on_bed / night_sleep / resp 有 0 或 night_on_bed < 2 小時 → 略過
        invalid = False
        for v in (v_night_on_bed, v_night_sleep, v_resp):
            if v is not None and v == 0:
                invalid = True
                break
        if (not invalid) and v_night_on_bed is not None and v_night_on_bed < 2:
            invalid = True
        if invalid:
            continue

        ms = month_stats[key]

        if v_night_on_bed is not None:
            ms["night_on_bed"].append(v_night_on_bed)
        if v_night_sleep is not None:
            ms["night_sleep"].append(v_night_sleep)
        if v_resp is not None:
            ms["sleep_resp"].append(v_resp)

        # 讀 day_on_bed
        v_day_on_bed = to_float(row.get("day_on_bed"))
        if v_day_on_bed is not None:
            ms["day_on_bed"].append(v_day_on_bed)

        # 原本的 day_leave 欄位照存
        v_day_leave = to_float(row.get("day_leave"))
        if v_day_leave is not None:
            ms["day_leave"].append(v_day_leave)

        # ★ 新增：用 day_on_bed + night_on_bed 算「全日離床時長」
        if (v_day_on_bed is not None) and (v_night_on_bed is not None):
            leave_total = 24.0 - v_day_on_bed - v_night_on_bed
            if leave_total < 0:
                leave_total = 0.0
            ms["day_leave_total"].append(leave_total)

        # 夜間休息離床次數：一律改用 asleep_leave（睡眠期間離床次數）
        v_asleep_leave = to_float(row.get("asleep_leave"))
        if v_asleep_leave is not None:
            # 為了不改前端變數名稱，仍然塞到 night_leave 陣列，但內容其實是 asleep_leave
            ms["night_leave"].append(v_asleep_leave)

        v_asleep_leave_min = to_float(row.get("asleep_leave_minute"))
        if v_asleep_leave_min is not None:
            ms["asleep_leave_min"].append(v_asleep_leave_min)

        # 給「臥床 / 離床」判斷用的累計（跟 /report 規則一致）
        mode_ms = month_mode_stats[key]
        if (v_night_on_bed is not None) and (v_day_on_bed is not None):
            mode_ms["sum_onbed_total"] += (v_night_on_bed + v_day_on_bed)
            mode_ms["cnt_onbed_total"] += 1
        if v_asleep_leave is not None:
            mode_ms["sum_asleep_leave"] += v_asleep_leave
            mode_ms["cnt_asleep_leave"] += 1

        # 上床時間...
        h = convert_asleep_start_to_hour(row.get("asleep_start"))
        if h is not None:
            ms["asleep_start_hour"].append(h)

        # 給「翻身間隔」用：記錄這個有效天的 night_sleep / asleep_start
        if v_night_sleep is not None:
            daily_for_turn[d_val] = {
                "night_sleep": v_night_sleep,
                "asleep_start": row.get("asleep_start"),
            }

    def avg_or_none(values):
        return sum(values) / len(values) if values else None

    # ===== 日夜翻身間隔（半年度）：來自 N_bq_Duration24 =====
    turn_query = f"""
    SELECT
      DATE(created_at) AS d,
      time_start
    FROM {DURATION_TABLE}
    WHERE
      resident_id = @resident_id
      AND bed_state = '09'
      AND DATE(created_at) BETWEEN @start_date AND @end_date_plus1
    ORDER BY d, time_start
    """

    turn_job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date_plus1", "DATE", end_date_plus1),
        ]
    )

    turn_rows = list(bq_client.query(turn_query, job_config=turn_job_config).result())
    flip_datetimes = []
    for r in turn_rows:
        d_val = r["d"]
        if isinstance(d_val, datetime):
            d_val = d_val.date()
        date_part = d_val

        t_val = r["time_start"]
        if isinstance(t_val, datetime):
            t_obj = t_val.time()
        elif isinstance(t_val, time):
            t_obj = t_val
        else:
            s = str(t_val)
            t_obj = None
            for fmt in ("%H:%M:%S", "%H:%M"):
                try:
                    t_obj = datetime.strptime(s, fmt).time()
                    break
                except ValueError:
                    continue
            if t_obj is None:
                continue

        flip_datetimes.append(datetime.combine(date_part, t_obj))

    # 依「每天」計算日 / 夜翻身平均間隔，然後再依月份取平均
    month_turn_stats = {
        key: {"night": [], "day": []}
        for key in month_keys
    }

    for d_val, info in daily_for_turn.items():
        night_sleep_hours = info["night_sleep"]
        asleep_start_val = info["asleep_start"]

        night_start_dt, night_end_dt = build_night_interval_for_day(
            d_val, asleep_start_val, night_sleep_hours
        )
        if night_start_dt is None or night_end_dt is None:
            continue

        night_avg_min, day_avg_min = compute_day_night_avg_intervals(
            d_val, night_start_dt, night_end_dt, flip_datetimes
        )

        # 過濾 > 12 小時的異常值（720 分鐘）
        if night_avg_min is not None and night_avg_min > 720:
            night_avg_min = None
        if day_avg_min is not None and day_avg_min > 720:
            day_avg_min = None

        key = (d_val.year, d_val.month)
        if key not in month_turn_stats:
            continue
        if night_avg_min is not None:
            month_turn_stats[key]["night"].append(night_avg_min)
        if day_avg_min is not None:
            month_turn_stats[key]["day"].append(day_avg_min)

    # ===== 依月份計算各圖表需要的數列 =====
    chart1_night_on_bed = []
    chart1_night_sleep = []
    chart1_night_turn = []  # 夜間翻身平均間隔（分鐘）
    chart1_day_turn = []    # 日間翻身平均間隔（分鐘）

    chart2_eff_percent = []
    chart2_leave_count = []

    chart3_resp = []
    chart4_leave_min = []
    chart4_day_on_bed = []     # ★ 每月平均日間在床
    chart4_night_on_bed = []   # ★ 每月平均夜間在床
    chart5_asleep_hour = []
    chart5_day_leave_hours = []  # 臥床模板用（日間離床小時）

    for key in month_keys:
        ms = month_stats[key]

        # 圖1（離床模板用）：夜間在床 / 夜間休息
        avg_on_bed = avg_or_none(ms["night_on_bed"])
        avg_sleep = avg_or_none(ms["night_sleep"])
        chart1_night_on_bed.append(avg_on_bed)
        chart1_night_sleep.append(avg_sleep)

        # 圖1（臥床模板用）：日 / 夜翻身平均間隔
        mts = month_turn_stats.get(key, {"night": [], "day": []})
        chart1_night_turn.append(avg_or_none(mts["night"]))
        chart1_day_turn.append(avg_or_none(mts["day"]))

        # 圖2：夜間休息效率 & 離床次數
        sum_on_bed = sum(ms["night_on_bed"]) if ms["night_on_bed"] else 0.0
        sum_sleep = sum(ms["night_sleep"]) if ms["night_sleep"] else 0.0
        if sum_on_bed > 0 and sum_sleep > 0:
            eff = (sum_sleep / sum_on_bed) * 100.0
        else:
            eff = None
        chart2_eff_percent.append(eff)
        chart2_leave_count.append(avg_or_none(ms["night_leave"]))

        # 圖3：每月平均呼吸
        chart3_resp.append(avg_or_none(ms["sleep_resp"]))

        # 圖4：每月平均夜間離床狀況（分鐘，用 asleep_leave_minute）
        chart4_leave_min.append(avg_or_none(ms["asleep_leave_min"]))

        # ★ 圖4（臥床模板用）：每月平均日間 / 夜間在床時間（小時）
        chart4_day_on_bed.append(avg_or_none(ms["day_on_bed"]))
        chart4_night_on_bed.append(avg_or_none(ms["night_on_bed"]))

        # 圖5：上床時間 / 日間離床
        chart5_asleep_hour.append(avg_or_none(ms["asleep_start_hour"]))

        avg_day_leave_total_m = avg_or_none(ms["day_leave_total"])
        chart5_day_leave_hours.append(
            avg_day_leave_total_m if avg_day_leave_total_m is not None else None
        )

    chart1_data = {
        "night_on_bed": chart1_night_on_bed,
        "night_sleep": chart1_night_sleep,
        "night_turn": chart1_night_turn,
        "day_turn": chart1_day_turn,
    }
    chart2_data = {
        "efficiency_percent": chart2_eff_percent,
        "leave_count": chart2_leave_count,
    }
    chart3_data = chart3_resp
    chart4_data = {
        "leave_min": chart4_leave_min,
        "day_on_bed": chart4_day_on_bed,
        "night_on_bed": chart4_night_on_bed,
    }
    chart5_data = {
        "asleep_start_hour": chart5_asleep_hour,
        "day_leave_hours": chart5_day_leave_hours,
    }

    # ===== 根據「當月」資料決定 report_type（若沒強制 mode） =====
    avg_onbed_total = None
    avg_night_leave_for_mode = None

    if force_mode in ("bed", "active"):
        report_type = force_mode
    else:
        base_key = (report_year, report_month)
        ms_mode = month_mode_stats.get(base_key)

        if ms_mode:
            if ms_mode["cnt_onbed_total"] > 0:
                avg_onbed_total = (
                    ms_mode["sum_onbed_total"] / ms_mode["cnt_onbed_total"]
                )
            if ms_mode["cnt_asleep_leave"] > 0:
                avg_night_leave_for_mode = (
                    ms_mode["sum_asleep_leave"] / ms_mode["cnt_asleep_leave"]
                )

        if (
            avg_onbed_total is not None
            and avg_onbed_total >= 10
            and avg_night_leave_for_mode is not None
            and avg_night_leave_for_mode <= 1
        ):
            report_type = "bed"
        else:
            report_type = "active"

    print(
        f"[DEBUG] /half_report type={report_type}, "
        f"avg_onbed_total={avg_onbed_total}, "
        f"avg_night_leave_for_mode={avg_night_leave_for_mode}"
    )

    # Google Sheet 評語：讀取「當月」的評語（例如 2025/10）
    month_comments = get_month_comments_from_sheet(
        serial_id=resident["serial_id"],
        agency_id=resident["agency_id"],
        year=report_year,
        month=report_month,
    )

    # 半年摘要：依模板選擇對應欄位
    if report_type == "bed":
        half_summary = month_comments.get("bed_trend_summary", "")
    else:
        half_summary = month_comments.get("active_trend_summary", "")

    if not half_summary:
        half_summary = "半年追蹤摘要尚未設定，之後可依照需求由後端帶入文字。"

    return render_template(
        "halfreport_report.html",
        resident=resident,
        report_type=report_type,
        report_year=report_year,
        report_month=report_month,
        labels_months=labels_months,
        chart1_data=chart1_data,
        chart2_data=chart2_data,
        chart3_data=chart3_data,
        chart4_data=chart4_data,
        chart5_data=chart5_data,
        month_comments=month_comments,
        half_summary=half_summary,
    )

@app.route("/report_30days")
@login_required
def report_30days():
    """
    30日作息頁：

      - 指定 year/month（或用最後一筆 Daily 的年月）
      - 例如看 11 月：
          10/31 12:00 ~ 11/01 12:00
          11/01 12:00 ~ 11/02 12:00
          ...
          11/30 12:00 ~ 12/01 12:00
        看 10 月：
          09/30 12:00 ~ 10/01 12:00
          ...
          10/31 12:00 ~ 11/01 12:00
      - 每條切成 30 分鐘 48 格，顏色依 08 > 07 > 00 > none
    """
    resident_id = session["resident_id"]

    # 住民資訊
    resident_info = {
        "resident_id": resident_id,
        "resident_name": session.get("resident_name", ""),
        "serial_id": session.get("serial_id", ""),
        "agency_id": session.get("agency_id", ""),
        "agency_name": session.get("agency_name", ""),
        "codename": session.get("codename", ""),
        "bed_number": session.get("bed_number", ""),
    }

    # 取得要看的年月：優先用 URL ?year=&month=，否則用最後一筆 daily 的年月
    arg_year = request.args.get("year", type=int)
    arg_month = request.args.get("month", type=int)
    if arg_year and arg_month and 1 <= arg_month <= 12:
        year, month = arg_year, arg_month
    else:
        last_date = get_latest_created_date(resident_id)
        year, month = last_date.year, last_date.month

    # 該月有幾天
    days_in_month = monthrange(year, month)[1]

    # 前一個月的年月
    if month == 1:
        prev_year = year - 1
        prev_month = 12
    else:
        prev_year = year
        prev_month = month - 1

    # 前一個月最後一天
    prev_month_last_day = monthrange(prev_year, prev_month)[1]
    first_window_date = date(prev_year, prev_month, prev_month_last_day)

    # 要畫的所有「起始日」（每個起始日代表：起始日 12:00 ~ 起始日+1 天 12:00）
    # 一共 days_in_month + 1 條
    day_dates = [
        first_window_date + timedelta(days=i)
        for i in range(days_in_month + 1)
    ]

    # 對每一條去撈 48 個 30 分鐘 slot
    windows = []
    for d in day_dates:
        slots = get_30min_slots_for_date(resident_id, d)
        start = d
        end = d + timedelta(days=1)
        label = f"{start.month}/{start.day} 12:00 - {end.month}/{end.day} 12:00"
        windows.append({
            "date": d,
            "label": label,
            "slots": slots,
        })

    # 時間軸標籤：12,13,...,23,00,01,...,11,12（畫在下面）
    hour_labels = []
    h = 12
    for i in range(25):  # 25 個刻度
        hour_labels.append((h + i) % 24)

    return render_template(
        "30days_report.html",
        resident=resident_info,
        year=year,
        month=month,
        windows=windows,
        hour_labels=hour_labels,
    )


# ================== 5. Debug：resident_id=112 的簡易列表 ==================

@app.route("/debug_res112_oct")
def debug_res112_oct():
    """
    列出 resident_id = 112 在 2025-10-01 ~ 2025-10-31 的所有 DAILY 資料
    不用登入，純粹 debug 看資料。
    """
    resident_id = 112
    start_date = date(2025, 10, 1)
    end_date = date(2025, 10, 31)

    query = f"""
    SELECT
      d.*
    FROM {DAILY_TABLE} AS d
    WHERE
      d.resident_id = @resident_id
      AND DATE(d.created_at) BETWEEN @start_date AND @end_date
    ORDER BY d.created_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("resident_id", "INT64", resident_id),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    rows = [dict(r) for r in bq_client.query(query, job_config=job_config).result()]

    html = []
    html.append("<h2>Debug：resident_id = 112 的 2025/10 DAILY 資料</h2>")
    html.append(f"<p>期間：{start_date} ~ {end_date}，共 {len(rows)} 筆</p>")

    if not rows:
        html.append("<p>這段期間沒有任何紀錄。</p>")
        return "".join(html)

    # 表格
    fields = list(rows[0].keys())
    html.append('<table border="1" cellspacing="0" cellpadding="4">')
    html.append("<tr>")
    for name in fields:
        html.append(f"<th>{name}</th>")
    html.append("</tr>")

    for row in rows:
        html.append("<tr>")
        for name in fields:
            val = row.get(name)
            cell = "" if val is None else str(val)
            html.append(f"<td>{cell}</td>")
        html.append("</tr>")

    html.append("</table>")
    return "".join(html)


if __name__ == "__main__":
    import webbrowser
    webbrowser.open("http://127.0.0.1:5000/")
    app.run(host="0.0.0.0", port=5000, debug=False)
