#!/bin/bash

# This script is specifically tailored for MySQL 5.7 environments.

# Rigorous shell script mode
# Exit immediately if a command exits with a non-zero status.
set -e
# Treat unset variables as an error when substituting.
set -u
# Prevent errors in a pipeline from being masked.
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
    echo "This script initializes a new target database server as a replica using a GTID-based workflow."
    echo "It can dump from an existing source replica (to offload work from the primary), have the target"
    echo "catch up to the source, and then switch it to follow the primary using GTID auto-positioning."
    echo ""
    echo "Terminology:"
    echo "  Primary: The main database server that will be replicated from in the final state."
    echo "  Source:  An existing replica used as the source for the initial data dump."
    echo "  Target:  The new server this script is running on, which will become a replica."
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
    echo -e "${RED}Error: Missing required arguments (--primary-host, --primary-user, --primary-pass, --target-user, --target-pass).${NC}\n"
    usage
fi

# --- Function to wait for slave to catch up ---
wait_for_slave_catchup() {
    echo -e "\n${BLUE}>>> Waiting for target to catch up with its source... (Timeout: ${CATCHUP_TIMEOUT_SECONDS}s)${NC}"
    SECONDS=0
    while true; do
        SLAVE_STATUS=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G" 2>/dev/null || true)

        if [[ -z "${SLAVE_STATUS}" ]]; then
            echo -e "${YELLOW}Warning: Could not get slave status. This is expected if the slave is stopped. Retrying in 10s...${NC}"
            sleep 10
            continue
        fi

        IO_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_IO_Running:' | awk '{print $2}')
        SQL_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_SQL_Running:' | awk '{print $2}')
        LAG=$(echo "${SLAVE_STATUS}" | grep 'Seconds_Behind_Master:' | awk '{print $2}')

        if [[ "${IO_RUNNING}" != "Yes" || "${SQL_RUNNING}" != "Yes" ]]; then
            echo -e "${RED}Error: Replication has stopped running unexpectedly.${NC}"
            echo "${SLAVE_STATUS}"
            exit 1
        fi

        if [[ "${LAG}" == "0" ]]; then
            echo -e "${GREEN}Target has successfully caught up with source! Lag is 0.${NC}"
            break
        fi

        echo "Current lag: ${LAG} seconds. Waiting..."
        sleep 15

        if (( SECONDS > CATCHUP_TIMEOUT_SECONDS )); then
            echo -e "${RED}Error: Timed out waiting for target to catch up.${NC}"
            exit 1
        fi
    done
}

# --- Set dump source variables ---
DUMP_HOST=${SOURCE_REPLICA_HOST:-${PRIMARY_HOST}}
DUMP_USER=${SOURCE_REPLICA_USER:-${PRIMARY_USER}}
DUMP_PASS=${SOURCE_REPLICA_PASS:-${PRIMARY_PASS}}

# --- SAFETY WARNING AND CONFIRMATION ---
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
    echo "Operation cancelled by user."
    exit 0
fi

# --- Main Execution ---
echo -e "\n${GREEN}>>> Step 1: Stopping and resetting target server's slave state...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "STOP SLAVE; RESET SLAVE ALL;"

echo -e "\n${GREEN}>>> Step 2: Dropping all user databases on the target server...${NC}"
DB_LIST=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -ANe "SELECT GROUP_CONCAT('DROP DATABASE IF EXISTS \`', schema_name, '\`') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');")

if [ -n "$DB_LIST" ] && [ "$DB_LIST" != "NULL" ]; then
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${DB_LIST}"
    echo "Target databases dropped."
else
    echo "No user databases to drop on target."
fi

echo -e "\n${GREEN}>>> Step 3: Dumping all databases from source (${DUMP_HOST}) using --master-data=1...${NC}"
mysqldump --all-databases \
    -h "${DUMP_HOST}" \
    -u "${DUMP_USER}" \
    -p"${DUMP_PASS}" \
    --single-transaction \
    --master-data=1 \
    --routines \
    --triggers \
    --flush-privileges \
    --hex-blob \
    --default-character-set=utf8 \
    --set-gtid-purged=OFF \
    --insert-ignore > "${DUMP_FILE}"
echo "Dump complete. File created: ${DUMP_FILE}"

echo -e "\n${GREEN}>>> Step 4: Importing the dump file to the target server...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" < "${DUMP_FILE}"
echo "Import complete."

# --- Conditional Catch-Up Logic ---
if [[ -n "${SOURCE_REPLICA_HOST}" ]]; then
    echo -e "\n${BLUE}>>> Step 5a: Catch-up phase initiated.${NC}"
    echo "Starting replication from source (${SOURCE_REPLICA_HOST}) to get the target in sync."
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"
    
    wait_for_slave_catchup

    echo "Stopping slave temporarily before switching to the primary."
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "STOP SLAVE;"
fi

echo -e "\n${GREEN}>>> Step 6: Configuring final replication to the primary (${PRIMARY_HOST}) with GTID...${NC}"
CHANGE_TO_PRIMARY_CMD="CHANGE MASTER TO \
    MASTER_HOST='${PRIMARY_HOST}', \
    MASTER_USER='${PRIMARY_USER}', \
    MASTER_PASSWORD='${PRIMARY_PASS}', \
    MASTER_AUTO_POSITION=1;"

echo "Executing final CHANGE MASTER command to point target to primary..."
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${CHANGE_TO_PRIMARY_CMD}"

echo "Starting replication from primary..."
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"

echo -e "\n${GREEN}>>> Step 7: Checking final slave status on target...${NC}"
sleep 5 # Give slave a moment to connect
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G"

echo -e "\n${GREEN}>>> Step 8: Deleting temporary SQL file (${DUMP_FILE})...${NC}"
rm "${DUMP_FILE}"
echo "Temporary file deleted."

echo -e "\n${GREEN}--- Process Complete ---${NC}"
echo "Please review the status output above. Check that 'Slave_IO_Running' and 'Slave_SQL_Running' are both 'Yes'."