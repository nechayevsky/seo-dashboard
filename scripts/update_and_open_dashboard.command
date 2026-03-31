#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/antonnechayevsky/Documents/SEO Dashboard/seo-dashboard"
UPDATE_SCRIPT="$PROJECT_DIR/scripts/update_dashboard.sh"
OPEN_SCRIPT="$PROJECT_DIR/scripts/open_dashboard.command"

if [ ! -x "$UPDATE_SCRIPT" ]; then
  echo "ERROR: Update script not executable: $UPDATE_SCRIPT"
  exit 1
fi

if [ ! -x "$OPEN_SCRIPT" ]; then
  echo "ERROR: Open script not executable: $OPEN_SCRIPT"
  exit 1
fi

"$UPDATE_SCRIPT"
"$OPEN_SCRIPT"
