#!/bin/bash

# Script to run app.py with nohup and logging
# Usage: ./run_app.sh

APP_NAME="app"
LOG_DIR="logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/${APP_NAME}_${TIMESTAMP}.log"
PID_FILE="${APP_NAME}.pid"

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Check if app is already running
if [ -f "$PID_FILE" ] && kill -0 "$(cat $PID_FILE)" 2>/dev/null; then
    echo "App is already running with PID $(cat $PID_FILE)"
    echo "Use './stop_app.sh' to stop it first"
    exit 1
fi

echo "Starting app with nohup..."
echo "Logs will be written to: $LOG_FILE"

# Start the application with nohup
nohup uv run app.py > "$LOG_FILE" 2>&1 &
APP_PID=$!

# Save PID to file
echo $APP_PID > "$PID_FILE"

echo "App started with PID: $APP_PID"
echo "Log file: $LOG_FILE"
echo ""
echo "To view logs in real-time: tail -f $LOG_FILE"
echo "To stop the app: ./stop_app.sh"
echo "To check if app is running: ./check_app.sh"

# Wait a moment to see if the process starts successfully
sleep 2

# Check if the process is still running
if kill -0 "$APP_PID" 2>/dev/null; then
    echo "App is running successfully!"
else
    echo "Warning: App may have failed to start. Check the logs:"
    echo "tail $LOG_FILE"
    # Clean up PID file if process failed
    rm -f "$PID_FILE"
    exit 1
fi
