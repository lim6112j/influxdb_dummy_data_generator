# App Management Scripts

This directory contains scripts to manage your `app.py` application with nohup and logging.

## Scripts

### `run_app.sh`
Starts the application in the background with nohup and logging.
- Creates timestamped log files in `logs/` directory
- Saves process PID for management
- Prevents multiple instances from running

**Usage:**
```bash
./run_app.sh
```

### `stop_app.sh`
Stops the running application gracefully.
- Uses saved PID to stop the process
- Attempts graceful shutdown, then force kills if needed

**Usage:**
```bash
./stop_app.sh
```

### `check_app.sh`
Checks if the application is running and shows status.
- Shows PID and memory usage
- Displays last 5 log entries

**Usage:**
```bash
./check_app.sh
```

### `view_logs.sh`
Interactive script to view application logs.
- Shows latest log file
- Options to view entire log, tail in real-time, or view last N lines

**Usage:**
```bash
./view_logs.sh
```

## Quick Commands

```bash
# Start the app
./run_app.sh

# Check if running
./check_app.sh

# View logs in real-time
tail -f logs/app_*.log

# Stop the app
./stop_app.sh
```

## Log Files

- Logs are stored in `logs/` directory
- Format: `app_YYYYMMDD_HHMMSS.log`
- Each run creates a new timestamped log file
