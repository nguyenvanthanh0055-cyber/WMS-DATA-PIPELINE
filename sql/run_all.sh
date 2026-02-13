#!/usr/bin/env bash
set -euo pipefail

psql_base=(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB")

run_sql_file() {
  local file="$1"
  echo "[initdb] running ${file}"
  "${psql_base[@]}" -f "$file"
}

shopt -s nullglob

for file in /docker-entrypoint-initdb.d/sql/*.sql; do
  run_sql_file "$file"
done