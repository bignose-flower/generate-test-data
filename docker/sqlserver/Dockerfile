# SQL Server用のDockerfile
FROM mcr.microsoft.com/mssql/server:latest

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD=YourStrong@Passw0rd

# 初期化スクリプトをコピー
COPY init.sql /usr/src/app/init.sql

# 初期化スクリプトを実行するスクリプトをコピー
COPY entrypoint.sh /usr/src/app/entrypoint.sh

# ポートを公開
EXPOSE 1433

# コンテナ起動時に初期化スクリプトを実行
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]