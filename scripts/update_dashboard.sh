#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/antonnechayevsky/Documents/SEO Dashboard/seo-dashboard"
VENV_ACTIVATE="$PROJECT_DIR/.venv/bin/activate"
PYTHON_BIN="$PROJECT_DIR/.venv/bin/python"
LOG_DIR="$PROJECT_DIR/logs"
TIMESTAMP="$(date '+%Y-%m-%d_%H-%M-%S')"
LOG_FILE="$LOG_DIR/update_dashboard_$TIMESTAMP.log"
LATEST_LOG="$LOG_DIR/update_dashboard_latest.log"

mkdir -p "$LOG_DIR"

exec > >(tee -a "$LOG_FILE") 2>&1

echo "=================================================="
echo "SEO Dashboard update started: $(date)"
echo "Project dir: $PROJECT_DIR"
echo "Log file: $LOG_FILE"
echo "=================================================="

if [ ! -d "$PROJECT_DIR" ]; then
  echo "ERROR: Project directory not found: $PROJECT_DIR"
  osascript -e 'display notification "Project folder not found" with title "SEO Dashboard Update Failed"'
  exit 1
fi

if [ ! -f "$VENV_ACTIVATE" ]; then
  echo "ERROR: Virtual environment activate script not found: $VENV_ACTIVATE"
  osascript -e 'display notification ".venv not found" with title "SEO Dashboard Update Failed"'
  exit 1
fi

if [ ! -x "$PYTHON_BIN" ]; then
  echo "ERROR: Python binary not found or not executable: $PYTHON_BIN"
  osascript -e 'display notification "Python in .venv not found" with title "SEO Dashboard Update Failed"'
  exit 1
fi

cd "$PROJECT_DIR"
source "$VENV_ACTIVATE"

run_step() {
  local step_name="$1"
  local command_name="$2"

  echo ""
  echo ">>> Running: $step_name"
  "$PYTHON_BIN" -m src.main --config settings.json --command "$command_name"
}

run_step "Validate config" "validate-config"
run_step "Fetch GSC" "fetch-gsc"
run_step "Fetch GA4" "fetch-ga4"
run_step "Merge pages" "merge-pages"
run_step "Score pages" "score-pages"
run_step "Inspect top pages" "inspect-top-pages"
run_step "Generate dashboard" "generate-dashboard"

ln -sf "$LOG_FILE" "$LATEST_LOG"

echo ""
echo "=================================================="
echo "SEO Dashboard update completed successfully: $(date)"
echo "Latest log symlink: $LATEST_LOG"
echo "=================================================="

osascript -e 'display notification "Dashboard updated successfully" with title "SEO Dashboard"'
