"""
すはだSPA 千葉店 — 30分毎モニタリング
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GitHub Actions: JST 10:00〜翌5:00 の間、30分毎に実行
- 今日のスケジュールページをスクレイプ
- 前回との差分検出 (出勤追加/削除/時間変更)
- availability_snapshots テーブルにスナップショット記録
- daily_schedules をリアルタイム更新
"""

import os
import sys
import logging
from datetime import date, datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SLEEP_BETWEEN_REQUESTS
from db_setup import get_connection, setup as db_setup
from scraper import make_client, scrape_schedule_day

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def main():
    now_jst = datetime.now(JST)
    today_jst = now_jst.date()
    today_str = today_jst.isoformat()
    now_str = now_jst.strftime("%Y-%m-%d %H:%M:%S")

    logger.info("━━━ 30分モニタリング ━━━ JST: %s", now_jst.strftime("%H:%M"))

    db_setup()
    conn = get_connection()
    client = make_client()

    try:
        # ── 1. 今日のスケジュール取得 ──
        current = scrape_schedule_day(client, today_jst)
        current_map = {t["therapist_id"]: t for t in current}
        current_ids = set(current_map.keys())

        # ── 2. 前回スナップとの比較 ──
        prev_rows = conn.execute(
            """SELECT DISTINCT therapist_id, status, start_time, end_time
               FROM availability_snapshots
               WHERE schedule_date = ?
                 AND checked_at = (
                     SELECT MAX(checked_at) FROM availability_snapshots
                     WHERE schedule_date = ?
                 )""",
            (today_str, today_str),
        ).fetchall()

        prev_map = {
            row[0]: {"status": row[1], "start": row[2], "end": row[3]}
            for row in prev_rows
        }
        prev_ids = {tid for tid, info in prev_map.items() if info["status"] == "available"}

        # ── 3. 差分検出 ──
        added = current_ids - prev_ids
        removed = prev_ids - current_ids
        continued = current_ids & prev_ids

        for tid in added:
            t = current_map[tid]
            logger.info("🟢 出勤追加: %s (%s～%s)", t["name"], t["raw_start"], t["raw_end"])

        for tid in removed:
            name = prev_map.get(tid, {}).get("name", f"ID:{tid}")
            # DBからセラピスト名取得
            row = conn.execute(
                "SELECT name FROM therapists WHERE therapist_id=?", (tid,)
            ).fetchone()
            if row:
                name = row[0]
            logger.info("🔴 シフト終了/削除: %s", name)

        for tid in continued:
            t = current_map[tid]
            p = prev_map.get(tid, {})
            if t["start_time"] != p.get("start") or t["end_time"] != p.get("end"):
                logger.info(
                    "🟡 時間変更: %s (%s～%s → %s～%s)",
                    t["name"], p.get("start"), p.get("end"),
                    t["start_time"], t["end_time"],
                )

        # ── 4. スナップショット記録 ──
        for tid in current_ids:
            t = current_map[tid]
            conn.execute(
                """INSERT INTO availability_snapshots
                   (therapist_id, schedule_date, status, start_time, end_time, checked_at)
                   VALUES (?, ?, 'available', ?, ?, ?)""",
                (tid, today_str, t["start_time"], t["end_time"], now_str),
            )

        # サイトから消えた人 → shift_ended
        for tid in removed:
            prev_info = prev_map.get(tid, {})
            conn.execute(
                """INSERT INTO availability_snapshots
                   (therapist_id, schedule_date, status, start_time, end_time, checked_at)
                   VALUES (?, ?, 'shift_ended', ?, ?, ?)""",
                (tid, today_str, prev_info.get("start"), prev_info.get("end"), now_str),
            )

        # ── 5. daily_schedules をリアルタイム更新 ──
        for tid, t in current_map.items():
            # セラピストマスタに存在確認 → なければ追加
            exists = conn.execute(
                "SELECT 1 FROM therapists WHERE therapist_id=?", (tid,)
            ).fetchone()
            if not exists:
                conn.execute(
                    """INSERT INTO therapists (therapist_id, name, first_seen, last_seen)
                       VALUES (?, ?, ?, ?)""",
                    (tid, t["name"], today_str, today_str),
                )
                logger.info("🆕 新規検出: %s (ID:%d)", t["name"], tid)
            else:
                conn.execute(
                    "UPDATE therapists SET last_seen=?, is_active=1 WHERE therapist_id=?",
                    (today_str, tid),
                )

            conn.execute(
                """INSERT INTO daily_schedules
                   (therapist_id, schedule_date, start_time, end_time, raw_start, raw_end)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(therapist_id, schedule_date) DO UPDATE SET
                     start_time=excluded.start_time,
                     end_time=excluded.end_time,
                     raw_start=excluded.raw_start,
                     raw_end=excluded.raw_end,
                     collected_at=datetime('now')
                """,
                (tid, today_str, t["start_time"], t["end_time"], t["raw_start"], t["raw_end"]),
            )

        conn.commit()
        logger.info(
            "✅ スナップ完了: 出勤%d名 (追加%d / 終了%d / 継続%d)",
            len(current_ids), len(added), len(removed), len(continued),
        )

    except Exception as e:
        logger.exception("❌ エラー: %s", e)
        raise
    finally:
        client.close()
        conn.close()


if __name__ == "__main__":
    main()
