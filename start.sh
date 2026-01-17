#!/bin/bash

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

# Set PATH for cron (cron has minimal PATH)
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

# Set up logging directory
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"

# Generate log filename with timestamp
LOG_FILE="$LOG_DIR/alert_processing_$(date +%Y%m%d_%H%M%S).log"
ERROR_LOG="$LOG_DIR/alert_processing_errors.log"

# Write initial log entry before redirecting
{
    echo "=========================================="
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting alert processing script"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Working directory: $SCRIPT_DIR"
    echo "=========================================="
} >> "$LOG_FILE"

# Redirect stdout and stderr to log file
exec 1>>"$LOG_FILE" 2>&1

# Check if virtual environment exists and activate it
if [ -d "$SCRIPT_DIR/venv" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Activating virtual environment..."
    source "$SCRIPT_DIR/venv/bin/activate" || {
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Failed to activate virtual environment" | tee -a "$ERROR_LOG"
        exit 1
    }
fi

# Check if python3 is available
if ! command -v python3 &> /dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: python3 not found in PATH" | tee -a "$ERROR_LOG"
    exit 1
fi

# Check if main.py exists
if [ ! -f "$SCRIPT_DIR/main.py" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: main.py not found in $SCRIPT_DIR" | tee -a "$ERROR_LOG"
    exit 1
fi

# Run the main script
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Executing main.py..."
python3 "$SCRIPT_DIR/main.py"
EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: main.py exited with code $EXIT_CODE" | tee -a "$ERROR_LOG"
    exit $EXIT_CODE
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Alert processing completed successfully"
echo "=========================================="