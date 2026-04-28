#!/bin/bash
# RE Deal Finder — shell wrapper
# Activates the virtual env (if present) and runs the pipeline.
# Designed to be called by launchd or cron.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Activate virtualenv if it exists
if [ -f "$SCRIPT_DIR/.venv/bin/activate" ]; then
    source "$SCRIPT_DIR/.venv/bin/activate"
elif [ -f "$SCRIPT_DIR/venv/bin/activate" ]; then
    source "$SCRIPT_DIR/venv/bin/activate"
fi

# Pass all arguments through to Python
exec python3 "$SCRIPT_DIR/run_search.py" "$@"
