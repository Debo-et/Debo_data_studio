#!/bin/bash

echo "=== Starting Debo Data Studio Debug ==="
echo "Timestamp: $(date)"
echo "Current directory: $(pwd)"
echo "User: $(whoami)"

# Create log directory
LOG_DIR="$HOME/.debo-studio-debug"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/electron-debug-$(date +%Y%m%d-%H%M%S).log"

echo "Log file: $LOG_FILE"
echo "=== Environment ===" > "$LOG_FILE"
env | sort >> "$LOG_FILE"

echo "=== Starting Electron App ===" >> "$LOG_FILE"
echo "Starting Electron app..."

# Run the AppImage with all possible logging
./"Debo Data Studio-1.0.0.AppImage" \
  --no-sandbox \
  --disable-gpu \
  --enable-logging \
  --log-level=0 \
  --v=1 \
  --enable-features=LogJsConsoleMessages \
  --disable-features=VizDisplayCompositor \
  --js-flags="--logfile=$LOG_DIR/v8.log --logging --log-all" \
  2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=$?
echo "=== Electron exited with code: $EXIT_CODE ===" >> "$LOG_FILE"
echo "Debug session completed. Check: $LOG_FILE"
