"""
すはだSPA 千葉店 — DB セットアップ
WordPress (VSW系) CMS 対応
"""

import sqlite3
import os
import sys

# scripts/ から実行されても data/ を見つけられるようにする
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "suhadaspa.db")


def get_connection(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


SCHEMA_SQL = """
-- ─── セラピストマスタ ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS therapists (
    therapist_id  INTEGER PRIMARY KEY,
    name          TEXT NOT NULL,
    age           INTEGER,
    height        INTEGER,
    bust          TEXT,
    waist         TEXT,
    hip           TEXT,
    cup           TEXT,
    description   TEXT,
    first_seen    TEXT NOT NULL DEFAULT (date('now')),
    last_seen     TEXT NOT NULL DEFAULT (date('now')),
    is_active     INTEGER NOT NULL DEFAULT 1
);

-- ─── 日別スケジュール ───────────────────────────────────────
-- 毎日のスクレイプで UPSERT
CREATE TABLE IF NOT EXISTS daily_schedules (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    therapist_id   INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date  TEXT NOT NULL,     -- YYYY-MM-DD
    start_time     TEXT,              -- HH:MM (正規化済み: 深夜帯=25:00~)
    end_time       TEXT,              -- HH:MM (正規化済み)
    raw_start      TEXT,              -- 元データ
    raw_end        TEXT,              -- 元データ
    collected_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(therapist_id, schedule_date)
);
CREATE INDEX IF NOT EXISTS idx_ds_date ON daily_schedules(schedule_date);
CREATE INDEX IF NOT EXISTS idx_ds_tid  ON daily_schedules(therapist_id);

-- ─── 30分毎スナップショット ─────────────────────────────────
-- スケジュールページに「出勤中」として表示されているかの記録
CREATE TABLE IF NOT EXISTS availability_snapshots (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    therapist_id   INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date  TEXT NOT NULL,     -- YYYY-MM-DD
    status         TEXT NOT NULL,     -- 'available' | 'shift_ended' | 'removed'
    start_time     TEXT,
    end_time       TEXT,
    checked_at     TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_snap_date ON availability_snapshots(schedule_date);
CREATE INDEX IF NOT EXISTS idx_snap_tid  ON availability_snapshots(therapist_id);
CREATE INDEX IF NOT EXISTS idx_snap_checked ON availability_snapshots(checked_at);

-- ─── セラピスト個別スケジュール (プロフィールページから) ─────
-- 各セラピストのプロフィールページに表示される1週間のスケジュール
CREATE TABLE IF NOT EXISTS profile_schedules (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    therapist_id   INTEGER NOT NULL REFERENCES therapists(therapist_id),
    schedule_date  TEXT NOT NULL,
    start_time     TEXT,
    end_time       TEXT,
    collected_at   TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(therapist_id, schedule_date, collected_at)
);

-- ─── 統計用ビュー ───────────────────────────────────────────

-- 曜日別出勤パターン
CREATE VIEW IF NOT EXISTS v_dow_pattern AS
SELECT
    t.therapist_id,
    t.name,
    CASE CAST(strftime('%w', ds.schedule_date) AS INTEGER)
        WHEN 0 THEN '日' WHEN 1 THEN '月' WHEN 2 THEN '火'
        WHEN 3 THEN '水' WHEN 4 THEN '木' WHEN 5 THEN '金'
        WHEN 6 THEN '土'
    END AS dow,
    COUNT(*) AS shift_count
FROM daily_schedules ds
JOIN therapists t ON t.therapist_id = ds.therapist_id
GROUP BY t.therapist_id, t.name, dow;

-- 出勤頻度ランキング (直近30日)
CREATE VIEW IF NOT EXISTS v_frequency_ranking AS
SELECT
    t.therapist_id,
    t.name,
    t.age,
    t.cup,
    COUNT(DISTINCT ds.schedule_date) AS shift_count,
    MIN(ds.schedule_date) AS earliest,
    MAX(ds.schedule_date) AS latest
FROM daily_schedules ds
JOIN therapists t ON t.therapist_id = ds.therapist_id
WHERE ds.schedule_date >= date('now', '-30 days')
GROUP BY t.therapist_id
ORDER BY shift_count DESC;
"""


def setup(db_path: str | None = None):
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    print(f"✅ DB セットアップ完了: {db_path or DB_PATH}")
    
    # テーブル確認
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    print(f"   テーブル: {', '.join(t[0] for t in tables)}")
    conn.close()


if __name__ == "__main__":
    setup()
