#!/bin/bash

# Script to check if the app is running
PID_FILE="app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "App is not running (no PID file found)"
    exit 1
fi

PID=$(cat "$PID_FILE")

if kill -0 "$PID" 2>/dev/null; then
    echo "App is running with PID: $PID"
    echo "Memory usage: $(ps -o pid,ppid,cmd,%mem,%cpu --sort=-%mem -p $PID)"
    
    # Show latest log entries
    LATEST_LOG=$(ls -t logs/app_*.log 2>/dev/null | head -1)
    if [ -n "$LATEST_LOG" ]; then
        echo ""
        echo "Latest log file: $LATEST_LOG"
        echo "Last 5 log entries:"
        tail -5 "$LATEST_LOG"
    fi
else
    echo "App is not running (PID $PID not found)"
    rm -f "$PID_FILE"
    exit 1
fi
