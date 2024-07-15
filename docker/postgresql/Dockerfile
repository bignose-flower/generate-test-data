FROM postgres:latest

ENV POSTGRES_DB=testdb
ENV POSTGRES_USER=testuser
ENV POSTGRES_PASSWORD=testpassword

# 初期化スクリプトをコピー
COPY init.sql /docker-entrypoint-initdb.d/

# ポートを公開
EXPOSE 5432