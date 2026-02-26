FROM python:3.12-slim

# システムの依存関係をインストール（グラフ生成などに必要）
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依存ライブラリをインストール
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ソースコードをコピー
COPY . .

# ログをリアルタイムで表示するための設定
ENV PYTHONUNBUFFERED=1

# Botを起動
CMD ["python", "-m", "src.main"]
