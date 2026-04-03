"""
すはだSPA 千葉店 — 週間スケジュール収集
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
GitHub Actions: 毎日 JST 10:00 に実行
- 7日分のスケジュールをスクレイプ
- セラピスト一覧から基本プロフィールを更新
- active/inactive ステータスを更新
"""

import os
import sys
import time
import logging
from datetime import date, datetime, timezone, timedelta

# スクリプトディレクトリをパスに追加
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import SLEEP_BETWEEN_REQUESTS, SCHEDULE_DAYS
from db_setup import get_connection, setup as db_setup
from scraper import make_client, scrape_schedule_week, scrape_therapist_list, scrape_therapist_profile

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

JST = timezone(timedelta(hours=9))


def upsert_therapist(conn, tid: int, name: str, **kwargs):
    """セラピストマスタを UPSERT"""
    today = date.today().isoformat()
    existing = conn.execute(
        "SELECT therapist_id FROM therapists WHERE therapist_id=?", (tid,)
    ).fetchone()

    if existing:
        updates = ["last_seen=?", "is_active=1"]
        params = [today]
        for col in ("age", "height", "bust", "waist", "hip", "cup", "description"):
            val = kwargs.get(col)
            if val is not None and str(val).strip() not in ("", "-"):
                updates.append(f"{col}=?")
                params.append(val)
        updates.append("name=?")
        params.append(name)
        params.append(tid)
        conn.execute(
            f"UPDATE therapists SET {', '.join(updates)} WHERE therapist_id=?",
            params,
        )
    else:
        cols = ["therapist_id", "name", "first_seen", "last_seen", "is_active"]
        vals = [tid, name, today, today, 1]
        for col in ("age", "height", "bust", "waist", "hip", "cup", "description"):
            val = kwargs.get(col)
            if val is not None and str(val).strip() not in ("", "-"):
                cols.append(col)
                vals.append(val)
        placeholders = ", ".join("?" * len(cols))
        conn.execute(
            f"INSERT INTO therapists ({', '.join(cols)}) VALUES ({placeholders})",
            vals,
        )
        logger.info("🆕 新規セラピスト: %s (ID:%d)", name, tid)


def upsert_schedule(conn, rec: dict):
    """日別スケジュールを UPSERT"""
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
        (
            rec["therapist_id"],
            rec["schedule_date"],
            rec["start_time"],
            rec["end_time"],
            rec["raw_start"],
            rec["raw_end"],
        ),
    )


def main():
    now_jst = datetime.now(JST)
    today_jst = now_jst.date()
    logger.info("━━━ 週間スケジュール収集開始 ━━━ JST: %s", now_jst.strftime("%Y-%m-%d %H:%M"))

    db_setup()
    conn = get_connection()
    client = make_client()

    try:
        # ── 1. スケジュールページから7日分取得 ──
        logger.info("📅 スケジュール取得 (%d日分)...", SCHEDULE_DAYS)
        week_data = scrape_schedule_week(client, today_jst, SCHEDULE_DAYS)

        schedule_count = 0
        active_ids = set()
        for date_str, therapists in week_data.items():
            for rec in therapists:
                upsert_therapist(conn, rec["therapist_id"], rec["name"])
                upsert_schedule(conn, rec)
                active_ids.add(rec["therapist_id"])
                schedule_count += 1

        logger.info("📅 スケジュール: %d件保存 / %d名検出", schedule_count, len(active_ids))

        # ── 2. セラピスト一覧ページから全員取得 ──
        logger.info("👥 セラピスト一覧取得...")
        time.sleep(SLEEP_BETWEEN_REQUESTS)
        all_therapists = scrape_therapist_list(client)

        for t in all_therapists:
            upsert_therapist(
                conn, t["therapist_id"], t["name"],
                bust=t.get("bust"), waist=t.get("waist"),
                hip=t.get("hip"), cup=t.get("cup"),
            )
        logger.info("👥 セラピスト一覧: %d名更新", len(all_therapists))

        # ── 3. 今週出勤者のプロフィール詳細を取得 ──
        logger.info("📝 プロフィール詳細取得 (%d名)...", len(active_ids))
        for tid in active_ids:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            profile = scrape_therapist_profile(client, tid)
            if profile:
                upsert_therapist(
                    conn, tid, profile["name"],
                    age=profile["age"], height=profile["height"],
                    bust=profile["bust"], waist=profile["waist"],
                    hip=profile["hip"], cup=profile["cup"],
                    description=profile["description"],
                )
                # プロフィールページのスケジュールも保存
                for ps in profile.get("schedule", []):
                    conn.execute(
                        """INSERT OR IGNORE INTO profile_schedules
                           (therapist_id, schedule_date, start_time, end_time)
                           VALUES (?, ?, ?, ?)""",
                        (tid, ps["schedule_date"], ps["start_time"], ps["end_time"]),
                    )

        # ── 4. 長期不在者を inactive に ──
        cutoff = (today_jst - timedelta(days=30)).isoformat()
        conn.execute(
            "UPDATE therapists SET is_active=0 WHERE last_seen < ? AND is_active=1",
            (cutoff,),
        )

        conn.commit()
        logger.info("✅ 週間スケジュール収集完了")

        # サマリー出力
        stats = conn.execute(
            "SELECT COUNT(*), SUM(is_active) FROM therapists"
        ).fetchone()
        logger.info("📊 DB: 全%d名 / アクティブ%d名", stats[0], stats[1])

    except Exception as e:
        logger.exception("❌ エラー: %s", e)
        raise
    finally:
        client.close()
        conn.close()


if __name__ == "__main__":
    main()
