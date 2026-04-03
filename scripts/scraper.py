"""
すはだSPA 千葉店 スクレイパー
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WordPress (VSW系) CMS 対応
- スケジュール取得: POST form (scheduleDay=YYYYMMDD)
- プロフィール取得: /therapist/{id}
- 1店舗のみ (千葉店)
- 予約状況は非公開 → 出勤有無のみ追跡
"""

import re
import time
import logging
from datetime import date, timedelta
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from config import (
    BASE_URL, SCHEDULE_URL, THERAPIST_URL, THERAPIST_LIST_URL,
    REQUEST_HEADERS, SLEEP_BETWEEN_REQUESTS,
)

logger = logging.getLogger(__name__)


# ─── 時刻正規化 ─────────────────────────────────────────────

def normalize_time(t: str) -> str:
    """深夜帯 (0:00~5:59) を 24:00~29:59 に正規化"""
    if not t:
        return t
    m = re.match(r'^(\d{1,2}):(\d{2})$', t.strip())
    if not m:
        return t
    h, mn = int(m.group(1)), int(m.group(2))
    if 0 <= h <= 5:
        h += 24
    return f"{h:02d}:{mn:02d}"


# ─── HTTP クライアント ───────────────────────────────────────

def make_client() -> httpx.Client:
    return httpx.Client(
        follow_redirects=True,
        headers=REQUEST_HEADERS,
        timeout=30,
    )


# ─── スケジュールページ スクレイピング ───────────────────────

def scrape_schedule_day(client: httpx.Client, target_date: date) -> list[dict]:
    """
    指定日のスケジュールをスクレイプ。
    POST form で scheduleDay=YYYYMMDD を送信。
    """
    date_val = target_date.strftime("%Y%m%d")
    date_str = target_date.isoformat()

    try:
        resp = client.post(
            SCHEDULE_URL,
            data={"scheduleDay": date_val},
        )
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Schedule fetch failed for %s: %s", date_str, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    # "todays girl" セクション内のカードを取得
    # 構造: div.inview > a[href*=/therapist/] > div.therapistInfo > span群
    results = []
    seen_ids = set()

    for link_el in soup.find_all("a", href=re.compile(r"/therapist/\d+")):
        href = link_el.get("href", "")
        tid_match = re.search(r"/therapist/(\d+)", href)
        if not tid_match:
            continue
        tid = int(tid_match.group(1))

        # 重複スキップ (girlsリストにも同じ人が出る)
        if tid in seen_ids:
            continue

        info_div = link_el.find("div", class_=re.compile(r"therapistInfo"))
        if not info_div:
            continue

        spans = info_div.find_all("span", recursive=True)
        if not spans:
            continue

        # 名前 (最初の size-11 span)
        name_span = info_div.find("span", class_=re.compile(r"size-11"))
        name = name_span.get_text(strip=True) if name_span else "?"

        # 時間帯を探す
        time_match = None
        for sp in spans:
            txt = sp.get_text(strip=True)
            tm = re.search(r"(\d{1,2}:\d{2})[～~〜](\d{1,2}:\d{2})", txt)
            if tm:
                time_match = tm
                break

        if not time_match:
            # 時間帯がない = girlsリスト（出勤スケジュールではない）
            continue

        raw_start = time_match.group(1)
        raw_end = time_match.group(2)

        seen_ids.add(tid)
        results.append({
            "therapist_id": tid,
            "name": name,
            "schedule_date": date_str,
            "start_time": normalize_time(raw_start),
            "end_time": normalize_time(raw_end),
            "raw_start": raw_start,
            "raw_end": raw_end,
        })

    logger.info("Schedule %s: %d therapists found", date_str, len(results))
    return results


def scrape_schedule_week(client: httpx.Client, start_date: date | None = None,
                         days: int = 7) -> dict[str, list[dict]]:
    """複数日分のスケジュールを取得"""
    if start_date is None:
        start_date = date.today()

    all_data = {}
    for i in range(days):
        d = start_date + timedelta(days=i)
        data = scrape_schedule_day(client, d)
        all_data[d.isoformat()] = data
        if i < days - 1:
            time.sleep(SLEEP_BETWEEN_REQUESTS)

    return all_data


# ─── セラピスト一覧ページ スクレイピング ─────────────────────

def scrape_therapist_list(client: httpx.Client) -> list[dict]:
    """
    /therapist/ ページから全セラピストの基本情報を取得。
    BWH/Cup はこのページに表示される。
    """
    try:
        resp = client.get(THERAPIST_LIST_URL)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Therapist list fetch failed: %s", e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    results = []

    for link_el in soup.find_all("a", href=re.compile(r"/therapist/\d+")):
        href = link_el.get("href", "")
        tid_match = re.search(r"/therapist/(\d+)", href)
        if not tid_match:
            continue
        tid = int(tid_match.group(1))

        info_div = link_el.find("div", class_=re.compile(r"therapistInfo"))
        if not info_div:
            continue

        name_span = info_div.find("span", class_=re.compile(r"size-11"))
        name = name_span.get_text(strip=True) if name_span else "?"

        # BWH / Cup
        stats = {}
        stats_span = info_div.find("span", class_=re.compile(r"size-08"))
        if stats_span:
            txt = stats_span.get_text(strip=True)
            bm = re.search(r"B:([^\s/]+)", txt)
            wm = re.search(r"W:([^\s/]+)", txt)
            hm = re.search(r"H:([^\s/]+)", txt)
            cm = re.search(r"Cup:(\w+)", txt)
            stats = {
                "bust": bm.group(1) if bm else None,
                "waist": wm.group(1) if wm else None,
                "hip": hm.group(1) if hm else None,
                "cup": cm.group(1) if cm else None,
            }

        # 既存結果に同一IDがあればスキップ
        if any(r["therapist_id"] == tid for r in results):
            continue

        results.append({
            "therapist_id": tid,
            "name": name,
            **stats,
        })

    logger.info("Therapist list: %d therapists found", len(results))
    return results


# ─── 個別プロフィールページ スクレイピング ────────────────────

def scrape_therapist_profile(client: httpx.Client, tid: int) -> dict | None:
    """
    /therapist/{id} から詳細プロフィールを取得:
    - Age, Height, BWH, Cup
    - 紹介文
    - 個別スケジュール (1週間分)
    """
    url = f"{THERAPIST_URL}/{tid}"
    try:
        resp = client.get(url)
        resp.raise_for_status()
    except httpx.HTTPError as e:
        logger.error("Profile fetch failed for %d: %s", tid, e)
        return None

    html = resp.text

    # 名前
    name_m = re.search(r'<h2[^>]*>([^<]{2,20})</h2>', html)
    name = name_m.group(1).strip() if name_m else f"ID:{tid}"

    # Age / Height / BWH / Cup (インライン表示)
    stats_m = re.search(
        r"Age\s*:\s*(\d+|-)\s*/\s*Height\s*:\s*(\d+|-|[^/]*)\s*/\s*"
        r"B\s*:\s*(\d+|-|[^/]*)\s*/\s*W\s*:\s*(\d+|-|[^/]*)\s*/\s*"
        r"H\s*:\s*(\d+|-|[^/]*)\s*/\s*Cup\s*:\s*(\w+|-)",
        html
    )
    age = _parse_int(stats_m.group(1)) if stats_m else None
    height = _parse_int(stats_m.group(2)) if stats_m else None
    bust = stats_m.group(3).strip() if stats_m else None
    waist = stats_m.group(4).strip() if stats_m else None
    hip = stats_m.group(5).strip() if stats_m else None
    cup = stats_m.group(6).strip() if stats_m else None

    # 紹介文
    desc_m = re.search(r'<div class="size-09 ms-2 mt-3">(.*?)</div>', html, re.S)
    description = re.sub(r"<[^>]+>", " ", desc_m.group(1)).strip()[:500] if desc_m else None

    # 個別スケジュール
    schedule = []
    sched_blocks = re.findall(
        r"(\d{2}/\d{2})<span>\(([^)]+)\)</span>.*?schedulePplO[^\"]*\">(.*?)</div>",
        html, re.S
    )
    current_year = date.today().year
    for date_str, dow, content in sched_blocks:
        month, day_num = map(int, date_str.split("/"))
        try:
            sched_date = date(current_year, month, day_num).isoformat()
        except ValueError:
            continue
        time_m = re.search(r"(\d{1,2}:\d{2})[～~〜](\d{1,2}:\d{2})", content)
        if time_m:
            schedule.append({
                "schedule_date": sched_date,
                "start_time": normalize_time(time_m.group(1)),
                "end_time": normalize_time(time_m.group(2)),
            })

    return {
        "therapist_id": tid,
        "name": name,
        "age": age,
        "height": height,
        "bust": bust,
        "waist": waist,
        "hip": hip,
        "cup": cup,
        "description": description,
        "schedule": schedule,
    }


def _parse_int(s: str) -> Optional[int]:
    if not s or s.strip() == "-":
        return None
    m = re.search(r"\d+", s)
    return int(m.group()) if m else None
