#!/bin/bash

# This script is specifically tailored for MySQL 5.7 environments.

# Rigorous shell script mode
set -e
set -u
set -o pipefail

# --- Default values ---
PRIMARY_HOST=""
PRIMARY_USER=""
PRIMARY_PASS=""
SOURCE_REPLICA_HOST=""
SOURCE_REPLICA_USER=""
SOURCE_REPLICA_PASS=""
TARGET_USER=""
TARGET_PASS=""
DUMP_FILE="db_target_dump_$(date +%F-%H%M%S).sql"
CATCHUP_TIMEOUT_SECONDS=3600 # 1 hour timeout for replica catch-up

# --- Style variables ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Help/Usage function ---
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "This script initializes a new target server as a replica using a pure GTID workflow."
    echo "It dumps from a source, catches up using GTID auto-positioning, and includes an"
    echo "algorithm to automatically resolve duplicate key errors (1062) by deleting"
    echo "conflicting rows on the target."
    echo ""
    echo -e "${RED}WARNING: The auto-fix mechanism is destructive. It assumes the source/primary is always correct.${NC}"
    echo ""
    echo "Options:"
    echo "  --primary-host <host>          -H <host>      (Required) The hostname of the final Primary server."
    echo "  --primary-user <user>          -U <user>      (Required) The replication username for the Primary server."
    echo "  --primary-pass <password>      -P <password>  (Required) The password for the Primary's replication user."
    echo ""
    echo "  --target-user <user>           -t <user>      (Required) The username for the Target (this new) database server."
    echo "  --target-pass <password>       -k <password>  (Required) The password for the Target user."
    echo ""
    echo "  --source-replica-host <host>   -S <host>      (Optional) A Source replica to dump from. If omitted, will dump from the Primary."
    echo "  --source-replica-user <user>   -s <user>      (Optional) The username for the Source replica. Defaults to --primary-user if not set."
    echo "  --source-replica-pass <password> -w <password>    (Optional) The password for the Source replica user. Defaults to --primary-pass if not set."
    echo ""
    echo "  --help                           -h           Display this help message."
    echo -e "${YELLOW}Very important: Use single quotation marks to enclose your arguments to avoid problems with special characters.${NC}"
    exit 1
}

# --- Parse Command-Line Arguments ---
# (Parsing logic remains the same as previous scripts)
PARSED_ARGS=$(getopt -o H:U:P:t:k:S:s:w:h --long primary-host:,primary-user:,primary-pass:,target-user:,target-pass:,source-replica-host:,source-replica-user:,source-replica-pass:,help -n "$0" -- "$@")
if [ $? -ne 0 ]; then
    usage
fi
eval set -- "$PARSED_ARGS"
while true; do
    case "$1" in
        -H|--primary-host) PRIMARY_HOST="$2"; shift 2 ;;
        -U|--primary-user) PRIMARY_USER="$2"; shift 2 ;;
        -P|--primary-pass) PRIMARY_PASS="$2"; shift 2 ;;
        -t|--target-user)  TARGET_USER="$2"; shift 2 ;;
        -k|--target-pass)  TARGET_PASS="$2"; shift 2 ;;
        -S|--source-replica-host) SOURCE_REPLICA_HOST="$2"; shift 2 ;;
        -s|--source-replica-user) SOURCE_REPLICA_USER="$2"; shift 2 ;;
        -w|--source-replica-pass) SOURCE_REPLICA_PASS="$2"; shift 2 ;;
        -h|--help)         usage ;;
        --)                shift; break ;;
        *)                 echo "Internal error!"; exit 1 ;;
    esac
done

# --- Argument Validation ---
if [[ -z "${PRIMARY_HOST}" || -z "${PRIMARY_USER}" || -z "${PRIMARY_PASS}" || -z "${TARGET_USER}" || -z "${TARGET_PASS}" ]]; then
    echo -e "${RED}Error: Missing required arguments.${NC}\n"; usage;
fi

# --- ADVANCED Function to wait for slave and auto-fix duplicate key errors ---
wait_and_fix_slave_catchup() {
    echo -e "\n${BLUE}>>> Waiting for target to catch up... (Timeout: ${CATCHUP_TIMEOUT_SECONDS}s)${NC}"
    echo -e "${YELLOW}>>> Auto-fix for duplicate key errors (1062) is ENABLED.${NC}"
    SECONDS=0
    
    while true; do
        # Fetch slave status once per loop iteration
        SLAVE_STATUS=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G" 2>/dev/null || true)

        if [[ -z "${SLAVE_STATUS}" ]]; then
            echo -e "${YELLOW}Warning: Could not get slave status. Retrying in 10s...${NC}"
            sleep 10
            continue
        fi

        # Parse all necessary fields from the status
        IO_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_IO_Running:' | awk '{print $2}')
        SQL_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_SQL_Running:' | awk '{print $2}')
        LAG=$(echo "${SLAVE_STATUS}" | grep 'Seconds_Behind_Master:' | awk '{print $2}')
        LAST_ERRNO=$(echo "${SLAVE_STATUS}" | grep 'Last_Errno:' | awk '{print $2}')
        
        # Check for success condition first
        if [[ "${IO_RUNNING}" == "Yes" && "${SQL_RUNNING}" == "Yes" && ("${LAG}" == "0" || "${LAG}" == "NULL") ]]; then
            echo -e "${GREEN}Target has successfully caught up with source! Lag is 0.${NC}"
            break
        fi
        
        # Check for IO thread failure
        if [[ "${IO_RUNNING}" != "Yes" ]]; then
            echo -e "${RED}Error: Slave IO thread has stopped. Cannot continue.${NC}"; echo "${SLAVE_STATUS}"; exit 1;
        fi

        # Handle SQL thread errors
        if [[ "${SQL_RUNNING}" == "No" ]]; then
            if [[ "${LAST_ERRNO}" == "1062" ]]; then
                LAST_ERROR=$(echo "${SLAVE_STATUS}" | grep 'Last_SQL_Error:' | sed 's/Last_SQL_Error: //')
                echo -e "${YELLOW}Warning: Detected duplicate key error (1062). Attempting to auto-fix...${NC}"
                echo "  - Error: ${LAST_ERROR}"

                # --- BEGIN ROBUST PARSING ---
                TABLE_NAME=$(echo "$LAST_ERROR" | sed -n "s/.*for table '\\([^']*\\)'.*/\1/p")
                DB_NAME=$(echo "$TABLE_NAME" | cut -d'.' -f1 | tr -d '`')
                TBL_ONLY_NAME=$(echo "$TABLE_NAME" | cut -d'.' -f2 | tr -d '`')

                # Get the raw string of values (e.g., "123,'abc'")
                PK_VALUES_STRING=$(echo "$LAST_ERROR" | sed -n "s/.*Duplicate entry '\([^']*\)' for key.*/\1/p")
                
                if [[ -z "$TBL_ONLY_NAME" || -z "$PK_VALUES_STRING" ]]; then
                    echo -e "${RED}Error: Could not parse table name or PK values from error message. Cannot auto-fix.${NC}"; exit 1;
                fi

                # Discover the primary key columns and their types from the database itself
                PK_INFO_QUERY="SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '${DB_NAME}' AND TABLE_NAME = '${TBL_ONLY_NAME}' AND COLUMN_KEY = 'PRI' ORDER BY ORDINAL_POSITION;"
                
                # Store the discovered columns and types
                PK_COLUMNS=()
                PK_TYPES=()
                while read -r col type; do
                    PK_COLUMNS+=("$col")
                    PK_TYPES+=("$type")
                done < <(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -N -e "${PK_INFO_QUERY}")
                
                # Split the raw values string into an array, using '-' as the delimiter
                IFS='-' read -r -a PK_VALUES <<< "$PK_VALUES_STRING"

                # Dynamically build the WHERE clause
                WHERE_CLAUSE=""
                for i in "${!PK_COLUMNS[@]}"; do
                    col_name="${PK_COLUMNS[$i]}"
                    col_type="${PK_TYPES[$i]}"
                    col_val="${PK_VALUES[$i]}"

                    # Add AND if this is not the first condition
                    if [[ -n "$WHERE_CLAUSE" ]]; then
                        WHERE_CLAUSE+=" AND "
                    fi

                    # Quote strings, but not numbers
                    if [[ "$col_type" =~ (char|varchar|text|date|time|enum|set) ]]; then
                        WHERE_CLAUSE+="\`${col_name}\` = '${col_val}'"
                    else
                        WHERE_CLAUSE+="\`${col_name}\` = ${col_val}"
                    fi
                done
                # --- END ROBUST PARSING ---

                DELETE_CMD="DELETE FROM \`${DB_NAME}\`.\`${TBL_ONLY_NAME}\` WHERE ${WHERE_CLAUSE};"

                echo "  - Attempting to execute: ${DELETE_CMD}"
                mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${DELETE_CMD}"
                echo "  - Conflicting row deleted. Restarting slave..."
                mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"
                sleep 5 # Give it a moment to reconnect
            else
                echo -e "${RED}Error: Replication SQL thread has stopped with an unhandled error (${LAST_ERRNO}).${NC}"; echo "${SLAVE_STATUS}"; exit 1;
            fi
        fi

        echo "Current lag: ${LAG}. Waiting..."
        sleep 15

        if (( SECONDS > CATCHUP_TIMEOUT_SECONDS )); then
            echo -e "${RED}Error: Timed out waiting for target to catch up.${NC}"; exit 1;
        fi
    done
}

# --- Set dump source variables ---
DUMP_HOST=${SOURCE_REPLICA_HOST:-${PRIMARY_HOST}}
DUMP_USER=${SOURCE_REPLICA_USER:-${PRIMARY_USER}}
DUMP_PASS=${SOURCE_REPLICA_PASS:-${PRIMARY_PASS}}

# --- SAFETY WARNING AND CONFIRMATION ---
# (Warning remains the same)
echo -e "${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo -e "${RED}This script will PERMANENTLY DELETE all user databases on the TARGET MySQL server (localhost)."
echo -e "${YELLOW}You are about to wipe the target database and re-sync it according to this plan:${NC}"
echo -e "  - DATA SOURCE HOST:      ${GREEN}${DUMP_HOST}${NC}"
echo -e "  - FINAL PRIMARY HOST:    ${GREEN}${PRIMARY_HOST}${NC}"
echo -e "  - TARGET SERVER USER:    ${GREEN}${TARGET_USER}${NC}"
echo -e "${YELLOW}Ensure you have a backup and are running this on the correct TARGET server.${NC}"
echo -e "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!${NC}"
read -p "Type 'YES' to proceed: " confirmation
if [ "$confirmation" != "YES" ]; then
    echo "Operation cancelled by user."; exit 0;
fi

# --- Main Execution ---
echo -e "\n${GREEN}>>> Step 1: Wiping replication state and databases...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "STOP SLAVE; RESET SLAVE ALL;"

DB_LIST=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -ANe "SELECT GROUP_CONCAT('DROP DATABASE IF EXISTS \`', schema_name, '\`') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');")
if [ -n "$DB_LIST" ] && [ "$DB_LIST" != "NULL" ]; then
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${DB_LIST}"; echo "Target databases dropped.";
else
    echo "No user databases to drop on target.";
fi

echo -e "\n${GREEN}>>> Step 2: Dumping databases and GTID history from source (${DUMP_HOST})...${NC}"
mysqldump --all-databases \
    -h "${DUMP_HOST}" \
    -u "${DUMP_USER}" \
    -p"${DUMP_PASS}" \
    --single-transaction \
    --master-data=2 \
    --routines \
    --triggers \
    --flush-privileges \
    --hex-blob \
    --default-character-set=utf8 \
    --set-gtid-purged=OFF > "${DUMP_FILE}"
echo "Dump complete. File created: ${DUMP_FILE}"

echo -e "\n${GREEN}>>> Step 3: Importing data and GTID history...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" < "${DUMP_FILE}"
echo "Import complete."

# --- Conditional Catch-Up Logic ---
if [[ -n "${SOURCE_REPLICA_HOST}" ]]; then
    echo -e "\n${BLUE}>>> Step 4: Catch-up phase initiated (GTID Auto-Positioning).${NC}"
    
    CHANGE_TO_SOURCE_CMD="CHANGE MASTER TO \
        MASTER_HOST='${DUMP_HOST}', \
        MASTER_USER='${DUMP_USER}', \
        MASTER_PASSWORD='${DUMP_PASS}', \
        MASTER_AUTO_POSITION=1;"
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${CHANGE_TO_SOURCE_CMD}"
    
    echo "Starting replication from source (${DUMP_HOST})..."
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"
    
    wait_and_fix_slave_catchup

    echo "Stopping slave temporarily before switching to the primary."
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "STOP SLAVE;"
fi

echo -e "\n${GREEN}>>> Step 5: Configuring final replication to the primary (${PRIMARY_HOST}) with GTID...${NC}"
CHANGE_TO_PRIMARY_CMD="CHANGE MASTER TO \
    MASTER_HOST='${PRIMARY_HOST}', \
    MASTER_USER='${PRIMARY_USER}', \
    MASTER_PASSWORD='${PRIMARY_PASS}', \
    MASTER_AUTO_POSITION=1;"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${CHANGE_TO_PRIMARY_CMD}"

echo "Starting replication from primary..."
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"

echo -e "\n${GREEN}>>> Step 6: Final catch-up from primary (${PRIMARY_HOST}) with auto-fixing...${NC}"
wait_and_fix_slave_catchup

echo -e "\n${GREEN}>>> Step 7: Checking final slave status on target...${NC}"
sleep 5
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G"

echo -e "\n${GREEN}--- Process Complete ---${NC}"
echo "Please review the status output above to confirm replication is running."
echo -e "${YELLOW}The temporary dump file has NOT been deleted. Please confirm everything is working"
echo -e "and then manually delete the file at:${NC} ${GREEN}${DUMP_FILE}${NC}"