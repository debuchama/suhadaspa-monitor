"""
すはだSPA 千葉店 — ダッシュボード用データエクスポート
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GitHub Actions: weekly_collect / daily_monitor 後に実行
- DB → JSON (data/dashboard_data.json)
- GitHub Pages のダッシュボードが読み込む
"""

import json
import os
import sys
import logging
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db_setup import get_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "dashboard_data.json")


def export():
    now_jst = datetime.now(JST)
    today = now_jst.date()
    today_str = today.isoformat()

    conn = get_connection()
    data = {}

    # ── Meta ──
    data["generated_at"] = now_jst.isoformat(timespec="seconds")
    data["today"] = today_str

    # ── 今週の日付リスト (7日分) ──
    week_dates = [(today + timedelta(days=i)).isoformat() for i in range(7)]
    data["week_dates"] = week_dates

    # ── セラピストマスタ ──
    rows = conn.execute(
        """SELECT therapist_id, name, age, height, bust, waist, hip, cup,
                  description, first_seen, last_seen, is_active
           FROM therapists ORDER BY last_seen DESC, name"""
    ).fetchall()
    therapists = []
    for r in rows:
        therapists.append({
            "therapist_id": r[0], "name": r[1], "age": r[2], "height": r[3],
            "bust": r[4], "waist": r[5], "hip": r[6], "cup": r[7],
            "description": (r[8] or "")[:200], "first_seen": r[9],
            "last_seen": r[10], "is_active": r[11],
        })
    data["therapists"] = therapists

    # ── 今日のスケジュール ──
    today_rows = conn.execute(
        """SELECT ds.therapist_id, t.name, ds.start_time, ds.end_time,
                  ds.raw_start, ds.raw_end
           FROM daily_schedules ds
           JOIN therapists t ON t.therapist_id = ds.therapist_id
           WHERE ds.schedule_date = ?
           ORDER BY ds.start_time""",
        (today_str,),
    ).fetchall()
    data["today_schedule"] = [
        {"therapist_id": r[0], "name": r[1], "start_time": r[2], "end_time": r[3],
         "raw_start": r[4], "raw_end": r[5]}
        for r in today_rows
    ]

    # ── 週間スケジュール (全日分) ──
    week_sched = {}
    for d in week_dates:
        rows = conn.execute(
            """SELECT ds.therapist_id, t.name, ds.start_time, ds.end_time,
                      ds.raw_start, ds.raw_end
               FROM daily_schedules ds
               JOIN therapists t ON t.therapist_id = ds.therapist_id
               WHERE ds.schedule_date = ?
               ORDER BY ds.start_time""",
            (d,),
        ).fetchall()
        week_sched[d] = [
            {"therapist_id": r[0], "name": r[1], "start_time": r[2], "end_time": r[3],
             "raw_start": r[4], "raw_end": r[5]}
            for r in rows
        ]
    data["week_schedule"] = week_sched

    # ── 日別サマリー (直近30日) ──
    daily_summary = conn.execute(
        """SELECT schedule_date, COUNT(*) as staff_count
           FROM daily_schedules
           WHERE schedule_date >= date(?, '-30 days')
           GROUP BY schedule_date
           ORDER BY schedule_date""",
        (today_str,),
    ).fetchall()
    data["daily_summary"] = [
        {"date": r[0], "count": r[1]} for r in daily_summary
    ]

    # ── 曜日別出勤パターン ──
    dow_rows = conn.execute(
        """SELECT
             t.therapist_id, t.name,
             CASE CAST(strftime('%w', ds.schedule_date) AS INTEGER)
               WHEN 0 THEN '日' WHEN 1 THEN '月' WHEN 2 THEN '火'
               WHEN 3 THEN '水' WHEN 4 THEN '木' WHEN 5 THEN '金'
               WHEN 6 THEN '土'
             END AS dow,
             COUNT(*) AS cnt
           FROM daily_schedules ds
           JOIN therapists t ON t.therapist_id = ds.therapist_id
           WHERE ds.schedule_date >= date(?, '-30 days')
           GROUP BY t.therapist_id, dow
           ORDER BY t.name, dow""",
        (today_str,),
    ).fetchall()
    dow_data = {}
    for r in dow_rows:
        tid = r[0]
        if tid not in dow_data:
            dow_data[tid] = {"therapist_id": tid, "name": r[1], "days": {}}
        dow_data[tid]["days"][r[2]] = r[3]
    data["dow_patterns"] = list(dow_data.values())

    # ── 出勤頻度ランキング (直近30日) ──
    freq_rows = conn.execute(
        """SELECT t.therapist_id, t.name, t.age, t.cup,
                  COUNT(DISTINCT ds.schedule_date) AS shift_count
           FROM daily_schedules ds
           JOIN therapists t ON t.therapist_id = ds.therapist_id
           WHERE ds.schedule_date >= date(?, '-30 days')
           GROUP BY t.therapist_id
           ORDER BY shift_count DESC""",
        (today_str,),
    ).fetchall()
    data["frequency_ranking"] = [
        {"therapist_id": r[0], "name": r[1], "age": r[2], "cup": r[3], "shift_count": r[4]}
        for r in freq_rows
    ]

    # ── 時間帯別カバー (直近7日の集計) ──
    time_coverage = {}
    for h in range(10, 28):
        time_coverage[h] = 0
    for d in week_dates:
        for t in week_sched.get(d, []):
            st = _time_to_float(t["start_time"])
            et = _time_to_float(t["end_time"])
            if st is None or et is None:
                continue
            for h in range(int(st), min(int(et) + 1, 28)):
                if h in time_coverage:
                    time_coverage[h] += 1
    data["time_coverage"] = [{"hour": h, "count": c} for h, c in sorted(time_coverage.items())]

    # ── スナップショット統計 (直近7日) ──
    snap_rows = conn.execute(
        """SELECT schedule_date, checked_at, COUNT(*) AS therapist_count,
                  SUM(CASE WHEN status='available' THEN 1 ELSE 0 END) AS available_count
           FROM availability_snapshots
           WHERE schedule_date >= date(?, '-7 days')
           GROUP BY schedule_date, checked_at
           ORDER BY checked_at DESC
           LIMIT 200""",
        (today_str,),
    ).fetchall()
    data["snapshots"] = [
        {"date": r[0], "checked_at": r[1], "total": r[2], "available": r[3]}
        for r in snap_rows
    ]

    # ── 新人/復帰アラート ──
    newcomers = conn.execute(
        """SELECT therapist_id, name, first_seen
           FROM therapists
           WHERE first_seen >= date(?, '-7 days')
           ORDER BY first_seen DESC""",
        (today_str,),
    ).fetchall()
    data["newcomers"] = [
        {"therapist_id": r[0], "name": r[1], "first_seen": r[2]}
        for r in newcomers
    ]

    # ── 書き出し ──
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

    logger.info("✅ ダッシュボードデータ出力: %s (%d bytes)", OUTPUT_PATH, os.path.getsize(OUTPUT_PATH))
    conn.close()


def _time_to_float(t):
    if not t:
        return None
    import re
    m = re.match(r"^(\d+):(\d{2})$", t)
    if not m:
        return None
    return int(m.group(1)) + int(m.group(2)) / 60


if __name__ == "__main__":
    export()
