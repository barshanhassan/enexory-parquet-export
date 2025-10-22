#!/bin/bash

# Example run: ./parse_binlogs.sh --binlog-folder=/var/log/mysql --days-back=1

BINLOG_FOLDER=""
DAYS_BACK=""

RSYNC_SOURCE_DIR="/root/data/"
RSYNC_DEST_HOST="root@195.201.204.21"
RSYNC_DEST_DIR="/root/data/"
RSYNC_SSH_KEY="/root/.ssh/backup_parquet"

LOG_DIR="/root/parquet_sync_logs"

mkdir -p "$LOG_DIR" || { echo "CRITICAL: Could not create log directory '$LOG_DIR'. Exiting." >&2; exit 1; }
LOG_FILE="${LOG_DIR}/$(date +'%Y-%m-%d_%H-%M-%S').log"

echo "Logging all output to: ${LOG_FILE}"

exec > >(tee -a "$LOG_FILE") 2>&1

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --binlog-folder=*)
            BINLOG_FOLDER="${1#*=}"
            shift
            ;;
        --binlog-folder)
            if [[ -n $2 ]]; then
                BINLOG_FOLDER="$2"
                shift 2
            else
                echo "Error: --binlog-folder requires a value" >&2
                exit 1
            fi
            ;;
        --days-back=*)
            DAYS_BACK="${1#*=}"
            shift
            ;;
        --days-back)
            if [[ -n $2 ]]; then
                DAYS_BACK="$2"
                shift 2
            else
                echo "Error: --days-back requires a value" >&2
                exit 1
            fi
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 --binlog-folder=/path/to/folder --days-back=N" >&2
            exit 1
            ;;
    esac
done

# Validate required args
if [[ -z "$BINLOG_FOLDER" ]]; then
    echo "Error: --binlog-folder is required" >&2
    exit 1
fi
if [[ -z "$DAYS_BACK" || ! "$DAYS_BACK" =~ ^[0-9]+$ ]]; then
    echo "Error: --days-back must be a non-negative integer" >&2
    exit 1
fi

# Set index file
BINLOG_INDEX="${BINLOG_FOLDER}/mysql-bin.index"

if [[ ! -f "$BINLOG_INDEX" ]]; then
    echo "Error: Index file not found at '$BINLOG_INDEX'" >&2
    exit 1
fi

# Calculate start and stop datetimes in UTC+2
# Get current date in UTC+2
CURRENT_DATE=$(TZ=UTC-2 date +%F)
START_DATE=$(TZ=UTC-2 date -d "$CURRENT_DATE - $DAYS_BACK days" +%F)
STOP_DATE=$(TZ=UTC-2 date -d "$CURRENT_DATE - $DAYS_BACK days + 1 day" +%F)
START_DATETIME="${START_DATE} 00:00:00"
STOP_DATETIME="${STOP_DATE} 00:00:00"

# Validate datetimes
if ! date -d "$START_DATETIME" >/dev/null 2>&1; then
    echo "Error: Invalid start datetime '$START_DATETIME'" >&2
    exit 1
fi
if ! date -d "$STOP_DATETIME" >/dev/null 2>&1; then
    echo "Error: Invalid stop datetime '$STOP_DATETIME'" >&2
    exit 1
fi

# Helper to parse index: full paths, stripped
parse_index() {
    grep -v '^[[:space:]]*$' "$1" | sed 's/[[:space:]]*$//'
}

echo "Processing all files in the index from $START_DATETIME to $STOP_DATETIME (days back: $DAYS_BACK)"

# Generate file list from mysql-bin.index
files=()
while IFS= read -r file; do
    if [[ -f "$file" ]]; then
        files+=("$file")
    fi
done < <(parse_index "$BINLOG_INDEX")

if [[ ${#files[@]} -eq 0 ]]; then
    echo "Error: No valid files found in the index." >&2
    exit 1
fi

# Process files one at a time with progress
total_files=${#files[@]}
current_file=0
for file in "${files[@]}"; do
    ((current_file++))
    echo "Processing file $current_file/$total_files: $file" >&2
    mysqlbinlog --verbose \
      --start-datetime="$START_DATETIME" \
      --stop-datetime="$STOP_DATETIME" \
      "$file" \
      | awk '
        /^### (INSERT INTO|UPDATE|DELETE FROM) `enexory`.`api_data_timeseries`/ {
            in_stmt=1;
            gsub(/^### /,"");
            print;
            next
        }
        in_stmt {
            if ($0 ~ /^###/) {
                gsub(/^### /,"");
                print
            } else {
                in_stmt=0
            }
        }
      ' | ./consolidate || {
        echo "Error: Failed processing $file" >&2
        exit 1
    }
done

echo "Binlog processing complete. Starting rsync to destination..."

rsync -avz --checksum --delete -e "ssh -i ${RSYNC_SSH_KEY}" "${RSYNC_SOURCE_DIR}" "${RSYNC_DEST_HOST}:${RSYNC_DEST_DIR}" || {
    echo "Error: rsync failed to sync data to the destination." >&2
    exit 1
}

echo "Binlog processing and rsync to destination complete."