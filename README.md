# LOLRanksSummary

**LOLRanksSummary** は、Discord 上で動作する League of Legends (LoL) のランク情報追跡・分析ボットです。
OP.GG からデータを取得し、ユーザーのランク推移を記録、定期的にグラフやレポート形式で Discord チャンネルに通知します。

## 主な機能

- **ランク自動追跡**: 登録済みユーザーのランク情報を毎日自動収集・保存します。
- **マルチサーバー対応**: 複数の Discord サーバーで独立して動作し、サーバーごとにユーザーや通知設定を管理できます。
- **定期レポート**: 指定したタイミング・チャンネルに、日次/週次/月次のランク変動レポートを自動送信します（表形式・グラフ形式に対応）。
  - **日次 (Daily)**: 毎日指定時間に通知。
  - **週次 (Weekly)**: 毎週金曜日の指定時間に通知。
  - **月次 (Monthly)**: 毎月1日の指定時間に通知。
- **グラフ生成**: `/graph` コマンドで、過去のランク推移を視覚的に確認できます。
- **手動更新**: `/fetch` コマンドで、リアルタイムのランク情報を即座に取得・反映できます。

## 必要条件

- Python 3.10+
- PostgreSQL Database
- Discord Bot Token

### 依存ライブラリ
- `discord.py`: Discord API ラッパー
- `asyncpg`: 非同期 PostgreSQL クライアント
- `apscheduler`: 定期実行スケジューラー
- `matplotlib`: グラフ・画像生成
- `pandas`: データ処理（内部利用）
- `opgg.py`: OP.GG データ取得用（カスタム `opgg_client.py` ラッパーを使用）

## セットアップ

### 1. リポジトリのクローン
```bash
git clone https://github.com/Start-Takahashi/LOLAnalyzer.git
cd LOLAnalyzer
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
| `PGHOST`, `PGUSER`, ... | （`DATABASE_URL` の代わりに個別の接続情報も使用可能） | |

### 4. 起動
```bash
python src/main.py
```
初回起動時に `schema.sql` が読み込まれ、必要なテーブルが自動的に作成されます。

## コマンド一覧

すべてのコマンドはスラッシュコマンド（`/`）として実装されています。

### ユーザー管理 (`/user`)
- `/user add` : ユーザーを登録します（対話形式）。
  - 入力例: `me Name#Tag`, `@User Name#Tag`
- `/user show` : 現在のサーバーに登録されているユーザー一覧を表示します。
- `/user del` : 指定した Riot ID の登録を解除します。

### スケジュール管理 (`/schedule`)
定期レポートの送信設定を管理します。

- `/schedule add` : 新しい通知スケジュールを作成します（対話形式）。
  - 入力形式: `時間(HH:MM) チャンネル 期間(daily/weekly/monthly) 出力形式(table/graph)`
  - 例: `21:00 here daily table`
- `/schedule show` : 現在のサーバーのスケジュール一覧を表示します。
- `/schedule edit` : 既存スケジュールの設定を変更します。
- `/schedule del` : 指定 ID のスケジュールを削除します。
- `/schedule enable` / `/disable` : スケジュールの有効/無効を切り替えます。

### 分析・レポート
- `/report` : 指定期間の集計レポート（表形式またはグラフ形式）を表示します。
  - 引数: `period`（daily/weekly/monthly）, `output_type`（table/graph）, `riot_id`（特定ユーザーのみ表示する場合）
- `/fetch` : 指定ユーザー（または 'all'）の最新ランク情報を OPGG から取得し、DBを更新します。

### メンテナンス・その他 (`!`)
- `!ping` : Botの応答確認を行います。
- `!sync` : スラッシュコマンドを現在のサーバーに強制同期します（管理者専用）。
- `!unsync` : サーバー固有のコマンド設定を削除します（管理者専用）。

## 仕様詳細

### データベース構造
- **users**: 登録ユーザー情報（サーバーID, Discord ID, Riot ID, PUUID）
- **rank_history**: ランク履歴（サーバーID, Discord ID, Riot ID, Tier, Rank, LP, Wins, Losses, 取得日）
- **schedules**: 通知設定（サーバーID, 時間, チャンネル, 期間, 形式）

※ すべてのテーブルには `server_id` が含まれ、サーバーごとにデータが隔離されています。

### データ取得
- 毎日 23:55 に全サーバーの全ユーザーのランク情報を自動取得・保存します。
- 手動で `/fetch` を実行した場合も履歴として保存されます（同日に複数回実行した場合は最新のみ保持）。
