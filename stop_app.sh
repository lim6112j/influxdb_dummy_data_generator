#!/bin/bash

# Script to stop the running app
PID_FILE="app.pid"

if [ ! -f "$PID_FILE" ]; then
    echo "PID file not found. App may not be running."
    
    # Check if there are any uv processes running app.py
    UV_PIDS=$(pgrep -f "uv run app.py")
    if [ -n "$UV_PIDS" ]; then
        echo "Found running uv processes for app.py:"
        ps -f -p $UV_PIDS
        read -p "Do you want to kill these processes? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            kill $UV_PIDS
            echo "Processes killed"
        fi
    fi
    exit 1
fi

PID=$(cat "$PID_FILE")

if kill -0 "$PID" 2>/dev/null; then
    echo "Stopping app with PID: $PID"
    kill "$PID"
    
    # Wait for process to stop
    sleep 2
    
    if kill -0 "$PID" 2>/dev/null; then
        echo "Process didn't stop gracefully, force killing..."
        kill -9 "$PID"
        sleep 1
    fi
    
    # Also kill any child processes (uv might spawn python processes)
    pkill -P "$PID" 2>/dev/null
    
    rm -f "$PID_FILE"
    echo "App stopped successfully"
else
    echo "Process with PID $PID is not running"
    rm -f "$PID_FILE"
fi
