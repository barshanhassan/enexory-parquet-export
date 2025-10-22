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
    echo "This script initializes a new target server as a replica. It will prompt you to choose"
    echo "between a GTID or a classic Binary Log Position workflow during execution."
    echo ""
    echo "Primary and Target Server (Required):"
    echo "  --primary-host <host>          -H <host>      The hostname of the final Primary server to replicate from."
    echo "  --primary-user <user>          -U <user>      The replication username for the Primary server."
    echo "  --primary-pass <password>      -P <password>  The password for the Primary's replication user."
    echo "  --target-user <user>           -t <user>      The username for the Target (this new) database server."
    echo "  --target-pass <password>       -k <password>  The password for the Target user."
    echo ""
    echo "Data Source for Dump (Optional):"
    echo "  To dump from a source replica, you must provide all three of the following arguments."
    echo "  If all are omitted, the script will dump directly from the Primary."
    echo "  --source-replica-host <host>   -S <host>      The hostname of a Source Replica to dump data from."
    echo "  --source-replica-user <user>   -s <user>      The username for the Source Replica."
    echo "  --source-replica-pass <password> -w <password>    The password for the Source Replica's user."
    echo ""
    echo "Other Options:"
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
    echo -e "${RED}Error: Missing required arguments for the primary and/or target server.${NC}\n"; usage;
fi

# Check for partially-provided source replica arguments. This triggers if any are set, but not all.
if [[ (-n "${SOURCE_REPLICA_HOST}" || -n "${SOURCE_REPLICA_USER}" || -n "${SOURCE_REPLICA_PASS}") && \
      ! (-n "${SOURCE_REPLICA_HOST}" && -n "${SOURCE_REPLICA_USER}" && -n "${SOURCE_REPLICA_PASS}") ]]; then
    echo -e "${RED}Error: Incomplete Source Replica details provided.${NC}"
    echo "To dump from a source replica, all three of the following arguments are required together:"
    echo "  --source-replica-host"
    echo "  --source-replica-user"
    echo "  --source-replica-pass"
    echo -e "\nTo dump from the primary instead, please omit all three arguments."
    exit 1
fi

if [[ -n "${SOURCE_REPLICA_HOST}" && ("${SOURCE_REPLICA_HOST}" == "${PRIMARY_HOST}") ]]; then
    echo -e "${RED}Error: The Source Replica host cannot be the same as the Primary host.${NC}"
    echo "If you intend to dump from the primary, simply omit all '--source-replica-*' arguments."
    exit 1
fi

check_mysql_credentials() {
    local host="$1"
    local user="$2"
    local pass="$3"
    local server_type="$4" # A descriptive name like "Primary" or "Target"

    echo -e "${BLUE}>>> Verifying credentials for ${server_type} server (${host})...${NC}"

    # Attempt to connect and run a simple query.
    # The output is redirected to /dev/null to keep the script clean.
    # We rely on the exit code of the mysql command to determine success.
    if ! mysql -h "${host}" -u "${user}" -p"${pass}" -e "SELECT 1" >/dev/null 2>&1; then
        echo -e "${RED}Error: Failed to connect to the ${server_type} server (${host}) with the provided credentials.${NC}"
        echo -e "${YELLOW}Please check the host, username, and password for the ${server_type} and try again.${NC}"
        exit 1
    fi

    echo -e "${GREEN}Successfully connected to the ${server_type} server.${NC}"
}

check_mysql_credentials "localhost" "${TARGET_USER}" "${TARGET_PASS}" "Target"
check_mysql_credentials "${PRIMARY_HOST}" "${PRIMARY_USER}" "${PRIMARY_PASS}" "Primary"

# If a source replica is specified, check its credentials as well.
if [[ -n "${SOURCE_REPLICA_HOST}" ]]; then
    check_mysql_credentials "${SOURCE_REPLICA_HOST}" "${SOURCE_REPLICA_USER}" "${SOURCE_REPLICA_PASS}" "Source Replica"
fi

# --- Function to wait for slave catch-up ---
wait_for_catchup() {
    echo -e "\n${BLUE}>>> Waiting for replica to catch up with the primary...${NC}"
    echo -e "${YELLOW}The script will now monitor the replication status. You can safely exit (Ctrl+C) at any time."
    echo -e "Replication will continue in the background. Remember to delete the dump file when done: ${GREEN}${DUMP_FILE}${NC}"

    while true; do
        SLAVE_STATUS=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G" 2>/dev/null || true)

        if [[ -z "${SLAVE_STATUS}" ]]; then
            echo -e "${YELLOW}Warning: Could not get slave status. Replication might not have started. Retrying in 15s...${NC}"
            sleep 15
            continue
        fi

        IO_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_IO_Running:' | awk '{print $2}')
        SQL_RUNNING=$(echo "${SLAVE_STATUS}" | grep 'Slave_SQL_Running:' | awk '{print $2}')
        LAG=$(echo "${SLAVE_STATUS}" | grep 'Seconds_Behind_Master:' | awk '{print $2}')
        
        if [[ "${IO_RUNNING}" == "Yes" && "${SQL_RUNNING}" == "Yes" && "${LAG}" == "0" ]]; then
            echo -e "${GREEN}Replica has successfully caught up with the primary! Lag is 0.${NC}"
            break
        fi
        
        if [[ "${IO_RUNNING}" != "Yes" || "${SQL_RUNNING}" != "Yes" ]]; then
            echo -e "${RED}Error: Replication has stopped or failed. Please check the status below. Retrying in 15s...${NC}"
            echo "${SLAVE_STATUS}"
            sleep 15
        fi

        echo "Current lag: ${LAG}. Rechecking in 15s..."
        sleep 15
    done
}

# --- Set data source variables ---
if [[ -n "${SOURCE_REPLICA_HOST}" ]]; then
    DATA_SOURCE_HOST=${SOURCE_REPLICA_HOST}
    DATA_SOURCE_USER=${SOURCE_REPLICA_USER}
    DATA_SOURCE_PASS=${SOURCE_REPLICA_PASS}
else
    DATA_SOURCE_HOST=${PRIMARY_HOST}
    DATA_SOURCE_USER=${PRIMARY_USER}
    DATA_SOURCE_PASS=${PRIMARY_PASS}
fi

# --- Interactive Workflow Selection ---
WORKFLOW_MODE=""
while true; do
    read -p "Use Binary Log or GTID workflow? Type BINLOG or GTID to continue: " choice
    CHOICE_UPPER=$(echo "$choice" | tr '[:lower:]' '[:upper:]')
    
    if [[ "$CHOICE_UPPER" == "BINLOG" || "$CHOICE_UPPER" == "GTID" ]]; then
        WORKFLOW_MODE=$CHOICE_UPPER
        break
    else
        echo -e "${YELLOW}Invalid input. Please type 'BINLOG' or 'GTID'.${NC}"
    fi
done

# --- Workflow-specific confirmations ---
if [[ "$WORKFLOW_MODE" == "BINLOG" && -n "$SOURCE_REPLICA_HOST" ]]; then
    echo -e "${YELLOW}You have specified a source replica with the BINLOG workflow.${NC}"
    echo -e "${RED}Please confirm that this source replica (${SOURCE_REPLICA_HOST}) is a direct replica of the final primary (${PRIMARY_HOST}).${NC}"
    read -p "Type 'YES' to confirm: " binlog_confirmation
    if [ "$binlog_confirmation" != "YES" ]; then
        echo "Operation cancelled by user."; exit 1;
    fi
elif [[ "$WORKFLOW_MODE" == "GTID" ]]; then
    GTID_PROMPT_HOSTS="the Primary (${PRIMARY_HOST})"
    if [[ -n "$SOURCE_REPLICA_HOST" ]]; then
        GTID_PROMPT_HOSTS="both the Primary (${PRIMARY_HOST}) and the Source Replica (${SOURCE_REPLICA_HOST})"
    fi
    echo -e "${YELLOW}You have chosen the GTID workflow.${NC}"
    echo -e "${RED}Please confirm that GTID is enabled and enforced on ${GTID_PROMPT_HOSTS}.${NC}"
    read -p "Type 'YES' to confirm this is correctly configured: " gtid_confirmation
    if [ "$gtid_confirmation" != "YES" ]; then
        echo "Operation cancelled by user."; exit 1;
    fi

    if [[ -n "$SOURCE_REPLICA_HOST" ]]; then
        echo -e "${RED}For this to work, the source replica (${SOURCE_REPLICA_HOST}) MUST have a complete GTID set from the primary and ideally be using MASTER_AUTO_POSITION=1 itself.${NC}"
        read -p "Type 'YES' to confirm this is correctly configured: " gtid_confirmation2
        if [ "$gtid_confirmation2" != "YES" ]; then
            echo "Operation cancelled by user. Cannot proceed without confirmation."; exit 1;
        fi
    fi
fi

# --- FINAL SAFETY WARNING AND CONFIRMATION ---
echo -e "\n${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! FINAL WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo -e "${RED}This script will PERMANENTLY DELETE all user databases on the TARGET MySQL server (localhost)."
echo -e "${YELLOW}Please review the final execution plan:${NC}"
echo -e "  - DATA SOURCE HOST (for dump): ${GREEN}${DATA_SOURCE_HOST}${NC}"
echo -e "  - FINAL PRIMARY HOST:          ${GREEN}${PRIMARY_HOST}${NC}"
echo -e "  - TARGET SERVER USER:          ${GREEN}${TARGET_USER}${NC}"
echo -e "  - REPLICATION MODE:          ${BLUE}${WORKFLOW_MODE}${NC}"
echo -e "${YELLOW}Ensure you have a backup and are running this on the correct TARGET server.${NC}"
echo -e "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!${NC}"
read -p "Type 'YES' to proceed with the wipe and initialization: " confirmation
if [ "$confirmation" != "YES" ]; then
    echo "Operation cancelled by user."; exit 0;
fi

# --- Main Execution ---
echo -e "\n${GREEN}>>> Step 1: Wiping replication state and databases...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "STOP SLAVE; RESET SLAVE ALL;"

DB_LIST=$(mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -ANe "SELECT GROUP_CONCAT('DROP DATABASE IF EXISTS \`', schema_name, '\`' SEPARATOR ';') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');")
if [ -n "$DB_LIST" ] && [ "$DB_LIST" != "NULL" ]; then
    mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${DB_LIST}"; echo "Target databases dropped.";
else
    echo "No user databases to drop on target.";
fi

mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "RESET MASTER;"

# --- Set mysqldump options based on selected workflow ---
MYSQLDUMP_OPTIONS=""
if [ "$WORKFLOW_MODE" == "BINLOG" ]; then
    MYSQLDUMP_OPTIONS="--set-gtid-purged=OFF "
    if [[ -z "${SOURCE_REPLICA_HOST}" ]]; then
        MYSQLDUMP_OPTIONS+="--master-data=2"
    else
        MYSQLDUMP_OPTIONS+="--dump-slave=2"
    fi
else # GTID Workflow
    MYSQLDUMP_OPTIONS="--master-data=2 --set-gtid-purged=ON"
fi

echo -e "\n${GREEN}>>> Step 2: Dumping databases from data source (${DATA_SOURCE_HOST})...${NC}"
mysqldump --all-databases \
    -h "${DATA_SOURCE_HOST}" \
    -u "${DATA_SOURCE_USER}" \
    -p"${DATA_SOURCE_PASS}" \
    --single-transaction \
    --insert-ignore \
    ${MYSQLDUMP_OPTIONS} \
    --routines \
    --triggers \
    --flush-privileges \
    --hex-blob \
    --default-character-set=utf8 > "${DUMP_FILE}"
echo "Dump complete. File created: ${DUMP_FILE}"

echo -e "\n${GREEN}>>> Step 3: Importing data...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" < "${DUMP_FILE}"
echo "Import complete."

# --- Step 4: Configure Replication and Start Slave ---
echo -e "\n${GREEN}>>> Step 4: Configuring replication to point to the final primary (${PRIMARY_HOST})...${NC}"
CHANGE_MASTER_CMD=""
if [ "$WORKFLOW_MODE" == "BINLOG" ]; then
    MASTER_LOG_FILE=$(grep "CHANGE MASTER TO" "${DUMP_FILE}" | head -n1 | sed -n "s/.*MASTER_LOG_FILE='\([^']*\)'.*/\1/p") || true
    MASTER_LOG_POS=$(grep "CHANGE MASTER TO" "${DUMP_FILE}" | head -n1 | sed -n "s/.*MASTER_LOG_POS=\([0-9]*\).*/\1/p") || true
    
    if [[ -z "$MASTER_LOG_FILE" || -z "$MASTER_LOG_POS" ]]; then
        echo -e "${RED}Error: Could not extract binlog position from dump file. Exiting.${NC}"
        exit 1
    fi
    echo "Extracted binlog coordinates: ${MASTER_LOG_FILE}, Position: ${MASTER_LOG_POS}"
    
    CHANGE_MASTER_CMD="CHANGE MASTER TO \
        MASTER_HOST='${PRIMARY_HOST}', \
        MASTER_USER='${PRIMARY_USER}', \
        MASTER_PASSWORD='${PRIMARY_PASS}', \
        MASTER_LOG_FILE='${MASTER_LOG_FILE}', \
        MASTER_LOG_POS=${MASTER_LOG_POS}, \
        MASTER_AUTO_POSITION=0;"
else # GTID Workflow
    CHANGE_MASTER_CMD="CHANGE MASTER TO \
        MASTER_HOST='${PRIMARY_HOST}', \
        MASTER_USER='${PRIMARY_USER}', \
        MASTER_PASSWORD='${PRIMARY_PASS}', \
        MASTER_AUTO_POSITION=1;"
fi

mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "${CHANGE_MASTER_CMD}"
echo "CHANGE MASTER command sent. Starting slave..."
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "START SLAVE;"

# --- Step 5: Wait for Catch-up ---
wait_for_catchup

# --- Final Step: Show status ---
echo -e "\n${GREEN}>>> Final Step: Checking final slave status on target...${NC}"
mysql -u "${TARGET_USER}" -p"${TARGET_PASS}" -e "SHOW SLAVE STATUS\G"

echo -e "\n${GREEN}--- Process Complete ---${NC}"
echo "Please review the status output above to confirm replication is running correctly from the primary."
echo -e "${YELLOW}The temporary dump file has NOT been deleted. Please confirm everything is working"
echo -e "and then manually delete the file at:${NC} ${GREEN}${DUMP_FILE}${NC}"