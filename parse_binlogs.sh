#!/bin/bash

# Example run: ./parse_binlogs.sh --binlog-folder=/var/log/mysql --days-back=1

BINLOG_FOLDER=""
DAYS_BACK=""

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

# Get epoch ts from datetime (in UTC+2)
get_ts() {
    TZ=UTC-2 date -d "$1" +%s 2>/dev/null || {
        echo "Error: Invalid datetime '$1'" >&2
        exit 1
    }
}

# Find latest file <= ts (last in ordered list with mtime <= ts)
get_start_file() {
    local ts="$1"
    local last_leq=""
    while IFS= read -r file; do
        [[ -f "$file" ]] || continue  # Skip if file missing
        local mtime=$(stat -c %Y "$file" 2>/dev/null || echo 0)
        if (( mtime <= ts )); then
            last_leq="$file"
        fi
    done < <(parse_index "$BINLOG_INDEX")
    [[ -n "$last_leq" ]] || {
        echo "Error: No binlog <= $START_DATETIME" >&2
        exit 1
    }
    echo "$last_leq"
}

# Find earliest file > ts (first in ordered list with mtime > ts), fallback to last
get_end_file() {
    local ts="$1"
    local first_gt=""
    local found_after=0
    local last_file=""
    while IFS= read -r file; do
        [[ -f "$file" ]] || continue
        last_file="$file"
        local mtime=$(stat -c %Y "$file" 2>/dev/null || echo 0)
        if (( mtime > ts )); then
            if (( found_after == 0 )); then
                first_gt="$file"
                found_after=1
            fi
        fi
    done < <(parse_index "$BINLOG_INDEX")
    if (( found_after == 1 )); then
        echo "$first_gt"
    else
        echo "$last_file"
    fi
}

# Main
start_ts=$(get_ts "$START_DATETIME")
stop_ts=$(get_ts "$STOP_DATETIME")

start_file=$(get_start_file "$start_ts")
end_file=$(get_end_file "$stop_ts")

# Extract just the number (e.g., 237 from mysql-bin.000237)
start_num=$(basename "$start_file" | sed -E 's/.*\.([0-9]{6})$/\1/' | sed 's/^0*//')
end_num=$(basename "$end_file" | sed -E 's/.*\.([0-9]{6})$/\1/' | sed 's/^0*//')

echo "Processing $START_DATETIME to $STOP_DATETIME (days back: $DAYS_BACK)"
echo "Start file: $start_file (num: $start_num)"
echo "End file: $end_file (num: $end_num)"

# Generate file list from mysql-bin.index within start_num to end_num
files=()
while IFS= read -r file; do
    num=$(basename "$file" | sed -E 's/.*\.([0-9]{6})$/\1/' | sed 's/^0*//')
    if [[ -n "$num" && "$num" -ge "$start_num" && "$num" -le "$end_num" && -f "$file" ]]; then
        files+=("$file")
    fi
done < <(parse_index "$BINLOG_INDEX")

if [[ ${#files[@]} -eq 0 ]]; then
    echo "Error: No valid files in range" >&2
    exit 1
fi

if [[ ${#files[@]} -eq 0 ]]; then
    echo "Error: No valid files in range" >&2
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

echo "Sync fully complete."