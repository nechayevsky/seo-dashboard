#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/antonnechayevsky/Documents/SEO Dashboard/seo-dashboard"
LOG_DIR="$PROJECT_DIR/logs"
SERVER_LOG="$LOG_DIR/http_server.log"
PORT="8000"
URL="http://localhost:$PORT/output/seo-dashboard.html"

mkdir -p "$LOG_DIR"

cd "$PROJECT_DIR"

EXISTING_PID="$(lsof -ti tcp:$PORT || true)"

if [ -n "$EXISTING_PID" ]; then
  echo "Port $PORT is already in use by PID $EXISTING_PID. Reusing existing server."
else
  nohup python3 -m http.server "$PORT" > "$SERVER_LOG" 2>&1 &
  sleep 2
fi

open "$URL"

