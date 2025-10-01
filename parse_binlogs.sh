#!/bin/bash

# No defaults; all args required

# Example run: ./parse_binlogs.sh --binlog-folder /var/log/mysql --start-datetime '2025-09-29 00:00:00' --stop-datetime '2025-09-30 00:00:00' --output-file events.csv

START_DATETIME=""
STOP_DATETIME=""
BINLOG_FOLDER=""
OUTPUT_FILE=""

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
        --start-datetime=*)
            START_DATETIME="${1#*=}"
            shift
            ;;
        --stop-datetime=*)
            STOP_DATETIME="${1#*=}"
            shift
            ;;
        --start-datetime)
            if [[ -n $2 ]]; then
                START_DATETIME="$2"
                shift 2
            else
                echo "Error: --start-datetime requires a value" >&2
                exit 1
            fi
            ;;
        --stop-datetime)
            if [[ -n $2 ]]; then
                STOP_DATETIME="$2"
                shift 2
            else
                echo "Error: --stop-datetime requires a value" >&2
                exit 1
            fi
            ;;
        --output-file=*)
            OUTPUT_FILE="${1#*=}"
            shift
            ;;
        --output-file)
            if [[ -n $2 ]]; then
                OUTPUT_FILE="$2"
                shift 2
            else
                echo "Error: --output-file requires a value" >&2
                exit 1
            fi
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 --binlog-folder=/path/to/folder --start-datetime=YYYY-MM-DD HH:MM:SS --stop-datetime=YYYY-MM-DD HH:MM:SS --output-file=filename.txt" >&2
            exit 1
            ;;
    esac
done

# Validate required args
if [[ -z "$BINLOG_FOLDER" ]]; then
    echo "Error: --binlog-folder is required" >&2
    exit 1
fi
if [[ -z "$START_DATETIME" ]]; then
    echo "Error: --start-datetime is required" >&2
    exit 1
fi
if [[ -z "$STOP_DATETIME" ]]; then
    echo "Error: --stop-datetime is required" >&2
    exit 1
fi
if [[ -z "$OUTPUT_FILE" ]]; then
    echo "Error: --output-file is required" >&2
    exit 1
fi

# Set index file
BINLOG_INDEX="${BINLOG_FOLDER}/mysql-bin.index"

if [[ ! -f "$BINLOG_INDEX" ]]; then
    echo "Error: Index file not found at '$BINLOG_INDEX'" >&2
    exit 1
fi

# Helper to parse index: full paths, stripped
parse_index() {
    grep -v '^[[:space:]]*$' "$1" | sed 's/[[:space:]]*$//'
}

# Get epoch ts from datetime
get_ts() {
    date -d "$1" +%s 2>/dev/null || {
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

echo "Start file: $start_file (num: $start_num)"
echo "End file: $end_file (num: $end_num)"

# Generate full path list from start_num to end_num (assuming consecutive) as an array
files=()
for i in $(seq "$start_num" "$end_num"); do
    file_name=$(printf 'mysql-bin.%06d' "$i")
    full_path="${BINLOG_FOLDER}/${file_name}"
    if [[ -f "$full_path" ]]; then
        files+=("$full_path")
    else
        echo "Warning: Skipping missing file $full_path" >&2
    fi
done

if [[ ${#files[@]} -eq 0 ]]; then
    echo "Error: No valid files in range" >&2
    exit 1
fi

# Run mysqlbinlog pipeline and pipe to C++ consolidator
mysqlbinlog --verbose --database=enexory \
  --start-datetime="$START_DATETIME" \
  --stop-datetime="$STOP_DATETIME" \
  "${files[@]}" \
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
  ' | ./consolidate > "$OUTPUT_FILE"

echo "Consolidated output written to $OUTPUT_FILE"