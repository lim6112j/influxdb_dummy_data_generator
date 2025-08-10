#!/bin/bash

# Script to view app logs
LOG_DIR="logs"

if [ ! -d "$LOG_DIR" ]; then
    echo "No logs directory found"
    exit 1
fi

# Get the latest log file
LATEST_LOG=$(ls -t "$LOG_DIR"/app_*.log 2>/dev/null | head -1)

if [ -z "$LATEST_LOG" ]; then
    echo "No log files found in $LOG_DIR"
    exit 1
fi

echo "Latest log file: $LATEST_LOG"
echo "Choose an option:"
echo "1) View entire log"
echo "2) Tail log (follow in real-time)"
echo "3) View last 50 lines"
echo "4) View last 100 lines"
read -p "Enter choice (1-4): " choice

case $choice in
    1)
        less "$LATEST_LOG"
        ;;
    2)
        echo "Following log file (Ctrl+C to stop):"
        tail -f "$LATEST_LOG"
        ;;
    3)
        tail -50 "$LATEST_LOG"
        ;;
    4)
        tail -100 "$LATEST_LOG"
        ;;
    *)
        echo "Invalid choice"
        exit 1
        ;;
esac
