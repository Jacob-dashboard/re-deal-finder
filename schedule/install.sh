#!/bin/bash
# RE Deal Finder — launchd install script
# Copies the plist to ~/Library/LaunchAgents/ and loads it.
# Run once: bash schedule/install.sh

set -euo pipefail

PLIST_NAME="com.jacob.deal-finder.plist"
PLIST_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$PLIST_NAME"
PLIST_DST="$HOME/Library/LaunchAgents/$PLIST_NAME"
LAUNCH_AGENTS="$HOME/Library/LaunchAgents"

echo "=== RE Deal Finder — launchd installer ==="
echo ""

# Verify plist exists
if [ ! -f "$PLIST_SRC" ]; then
    echo "ERROR: plist not found at $PLIST_SRC"
    exit 1
fi

# Ensure LaunchAgents dir exists
mkdir -p "$LAUNCH_AGENTS"

# Unload existing job if present (idempotent reinstall)
if launchctl list | grep -q "com.jacob.deal-finder" 2>/dev/null; then
    echo "Unloading existing job..."
    launchctl unload "$PLIST_DST" 2>/dev/null || true
fi

# Copy plist
cp "$PLIST_SRC" "$PLIST_DST"
echo "Installed plist → $PLIST_DST"

# Set correct permissions (launchd requires 644)
chmod 644 "$PLIST_DST"

# Make run_search.sh executable
chmod +x "$(dirname "$PLIST_SRC")/../run_search.sh"

# Create output directory
mkdir -p "$(dirname "$PLIST_SRC")/../output"

# Load the job
launchctl load "$PLIST_DST"
echo "Job loaded."
echo ""

# Confirm it's registered
if launchctl list | grep -q "com.jacob.deal-finder"; then
    echo "✓ com.jacob.deal-finder is registered with launchd"
    echo ""
    echo "Schedule: 10:00 AM and 10:00 PM (local time / America/Chicago)"
    echo "Logs:     ~/Projects/re-deal-finder/output/launchd_stdout.log"
    echo ""
    echo "To run immediately:  bash run_search.sh"
    echo "To uninstall:        launchctl unload $PLIST_DST && rm $PLIST_DST"
else
    echo "WARNING: job may not have loaded correctly. Check Console.app for errors."
    exit 1
fi
