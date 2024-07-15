#!/bin/bash

# installが必要なライブラリ
sudo apt-get update
sudo apt-get install unixodbc unixodbc-dev

# ODBC Driver 17 for sqlserver
curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
sudo su
curl https://packages.microsoft.com/config/ubuntu/$(lsb_release -rs)/prod.list > /etc/apt/sources.list.d/mssql-release.list
exit
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install msodbcsql17