# LOLRanksSummary

**LOLRanksSummary** は、Discord 上で動作する League of Legends (LoL) のランク情報追跡・分析ボットです。
OP.GG からデータを取得し、ユーザーのランク推移を記録、定期的にグラフやレポート形式で Discord チャンネルに通知します。

## 主な機能

- **ランク自動追跡**: 登録済みユーザーのランク情報を毎日 23:55 に自動収集・保存します。
- **並行更新処理**: 全ユーザーの OP.GG 更新リクエストを同時に送信し、高速にデータを取得します。
- **マルチサーバー対応**: 複数の Discord サーバーで独立して動作し、サーバーごとにユーザーや通知設定を管理できます。
- **定期レポート**: 指定したタイミング・チャンネルに、日次/週次/月次のランク変動レポートを自動送信します（表形式・グラフ形式に対応）。
  - **日次 (Daily)**: 毎日指定時間に通知。
  - **週次 (Weekly)**: 毎週金曜日の指定時間に通知。
  - **月次 (Monthly)**: 毎月1日の指定時間に通知。
- **グラフ生成**: `/report` コマンドで、過去のランク推移を視覚的に確認できます。
- **手動更新**: `/fetch` コマンドで、リアルタイムのランク情報を即座に取得・反映できます。
- **自動エラー通知**: Riot IDの変更やアカウント削除によりデータが取得できなくなったユーザーを自動検知し、定期通知時等にチャンネルへ警告と削除手順を案内します。

## 必要条件

- Python 3.10+
- PostgreSQL Database
- Discord Bot Token

### 依存ライブラリ

| ライブラリ | 用途 |
| :--- | :--- |
| `discord.py >= 2.3.2` | Discord API ラッパー |
| `asyncpg >= 0.29.0` | 非同期 PostgreSQL クライアント |
| `apscheduler >= 3.10.4` | 定期実行スケジューラー |
| `matplotlib >= 3.8.0` | グラフ・画像生成（Agg バックエンド使用） |
| `pandas >= 2.2.0` | データ処理 |
| `tabulate >= 0.9.0` | テキスト表形式出力 |
| `opgg.py >= 3.1.0` | OP.GG データ取得用 |
| `python-dotenv >= 1.0.0` | 環境変数管理 |

## セットアップ

### 1. リポジトリのクローン
```bash
git clone https://github.com/371038-tooth/LOLRanksSummary.git
cd LOLRanksSummary
```

### 2. ライブラリのインストール
```bash
pip install -r requirements.txt
```
※ 日本語フォント（`assets/fonts/JapaneseFont.otf`）が `src/utils/graph_generator.py` で利用されます。環境に合わせて配置してください。

### 3. 環境変数の設定
以下の環境変数を設定してください（`.env` ファイル等）。

| 変数名 | 説明 | 例 |
| :--- | :--- | :--- |
| `DISCORD_BOT_TOKEN` | Discord Developer Portal で取得した Bot トークン | `MTE...` |
| `DATABASE_URL` | PostgreSQL 接続 URL | `postgresql://user:pass@localhost:5432/dbname` |
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` | （`DATABASE_URL` の代わりに個別の接続情報も使用可能） | |
| `TZ` | (任意) タイムゾーン。Railway 等の Linux 環境では `Asia/Tokyo` を設定推奨。 | `Asia/Tokyo` |

※ 本ボットは内部で `Asia/Tokyo` タイムゾーンを強制する設定（`os.environ['TZ']`）を含んでおり、JST でのスケジュール実行およびレポート生成に最適化されています。

### 4. 起動
```bash
python src/main.py
```
初回起動時に `schema.sql` が読み込まれ、必要なテーブルが自動的に作成されます。

## コマンド一覧

すべてのコマンドはスラッシュコマンド（`/`）として実装されています。

### ユーザー管理 (`/user`)
- `/user add` : ユーザーを登録します。
  - 引数: `riot_id` (Name#Tag または OPGGのURL), `discord_user` (任意: 表示名、ユーザー名、またはID)
  - ※ 自分の場合は `riot_id` のみ入力、他人の場合は `discord_user` オプションを使用します。
- `/user show` : 現在のサーバーに登録されているユーザー一覧を表示します。
- `/user del` : 指定した ID の登録を解除します。

### スケジュール管理 (`/schedule`)
定期レポートの送信設定を管理します。

- `/schedule show` : 現在のサーバーのスケジュール一覧を表示します。
- `/schedule add` : 新しい通知スケジュールを作成します。
  - 引数: `output_type` (table/graph), `time` (HH:MM), `channel` (here/ID), `period` (daily/weekly/monthly), `split` (True/False ※graphのみ)
- `/schedule edit` : 既存スケジュールの設定を変更します。
  - 引数: `output_type` (対象テーブルの指定), `schedule_id` (ID), `time`, `channel`, `period`, `split`
- `/schedule del` : 指定した形式と ID のスケジュールを削除します。
- `/schedule enable` / `/schedule disable` : スケジュールの有効/無効を切り替えます。

### 分析・レポート
- `/report` : 指定期間の集計レポート（表形式またはグラフ形式）を保存済みのDBデータから即座に生成・表示します（OP.GGへの新規取得は行いません）。
  - 引数: `period`（daily/weekly/monthly）, `output_type`（table/graph）, `split`（True/False）, `riot_id`（特定ユーザーのみ表示する場合）
  - ※ レポート生成時にデータ取得エラーが発生したユーザー（Riot ID変更など）がいれば、併せて通知されます。
- `/fetch <riot_id>` : 指定ユーザー（または `all`）の最新ランク情報を OP.GG から取得し、DB を更新します。
  - `all` を指定すると全ユーザーを並行更新します。失敗したユーザーは結果に表示されます。

### メンテナンス・その他 (`!`)
- `!ping` : Bot の応答確認を行います。
- `!sync` : スラッシュコマンドを現在のサーバーに強制同期します（管理者専用）。
- `!unsync` : サーバー固有のコマンド設定を削除します（管理者専用）。

## 仕様詳細

### データベース構造
- **users**: 登録ユーザー情報（サーバーID, Discord ID, Riot ID, PUUID）
- **rank_history**: ランク履歴（サーバーID, Discord ID, Riot ID, Tier, Rank, LP, Wins, Losses, 取得日時）
- **end_rank_history**: 日次最終ランク履歴（23:55時点のデータを保存。構造は `rank_history` と同等）
- **schedules_table**: 表形式の通知設定（サーバーID, 時間, チャンネル, 期間, 有効/無効）
- **schedules_graph**: グラフ形式の通知設定（サーバーID, 時間, チャンネル, 期間, 分割設定, 有効/無効）

※ すべてのテーブルには `server_id` が含まれ、サーバーごとにデータが隔離されています。

### データ取得フロー
1. **毎日 23:55**: 全サーバーの全ユーザーの OP.GG 更新リクエストを**並行送信**し、ランク情報を **`end_rank_history`** に保存します。
2. **定期レポート実行時**: レポート送信の直前に最新ランク情報を取得し、**`rank_history`** に保存します。レポート（表形式）では、前回取得時との比較が表示され、ヘッダーに取得時間が反映されます。また、日次レポートの場合は比較対象の2日間のみが表示されます。
3. **手動更新 (`/fetch`)**: 実行時に **`rank_history`** として保存されます（同日に複数回実行した場合は最新のみ保持）。
