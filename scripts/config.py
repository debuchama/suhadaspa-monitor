"""
すはだSPA 千葉店 モニター設定
"""

BASE_URL = "https://suhadaspa.vsw.jp/chiba"
SCHEDULE_URL = f"{BASE_URL}/schedule/"
THERAPIST_URL = f"{BASE_URL}/therapist"
THERAPIST_LIST_URL = f"{BASE_URL}/therapist/"

DB_PATH = "data/suhadaspa.db"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9",
}

SLEEP_BETWEEN_REQUESTS = 1.5  # サーバー負荷軽減

# お気に入りセラピスト (ID)
FAVORITES = []

# スケジュール取得日数 (サイトは最大7日先まで)
SCHEDULE_DAYS = 7
