# すはだSPA 千葉店 スケジュールモニター

セラピスト出勤スケジュールを自動収集・分析するダッシュボード。

## アーキテクチャ

```
GitHub Actions (cron)
  ├── weekly_collect.yml  ← 毎日 JST 10:00 (7日分スケジュール + プロフィール更新)
  ├── availability_monitor.yml ← 30分毎 JST 10:00~翌5:00 (スナップショット)
  └── export → dashboard_data.json → GitHub Pages
```

## セットアップ

```bash
pip install -r requirements.txt
cd scripts
python db_setup.py         # DB初期化
python weekly_collect.py   # スケジュール収集
python daily_monitor.py    # 30分スナップショット
python export_data.py      # ダッシュボードJSON出力
```

## ファイル構成

```
scripts/
  config.py           - サイト固有設定
  db_setup.py         - SQLiteスキーマ
  scraper.py          - スクレイピング (POST form対応)
  weekly_collect.py   - 日次収集
  daily_monitor.py    - 30分毎モニタリング
  export_data.py      - JSON出力
data/
  suhadaspa.db        - SQLite DB
  dashboard_data.json - ダッシュボード用JSON
dashboard/
  index.html          - ダッシュボードUI
.github/workflows/
  weekly_collect.yml
  availability_monitor.yml
```

## サイト仕様メモ

- CMS: WordPress (VSW系)
- スケジュール取得: POST `scheduleDay=YYYYMMDD`
- 表示範囲: 当日から7日先まで
- 予約状況: **非公開** (出勤有無のみ追跡可能)
- 店舗: 千葉店のみ (1店舗)
