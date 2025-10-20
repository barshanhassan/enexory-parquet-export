#!/bin/bash

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
REPLICA_USER="root" # Default to 'root' for local user
REPLICA_PASS=""
DUMP_FILE="db_replica_dump_$(date +%F-%H%M%S).sql"

# --- Style variables ---
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# --- Help/Usage function ---
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "This script completely wipes a replica's user databases and re-initializes it from the primary."
    echo ""
    echo "Options:"
    echo "  --primary-host <host>      -H <host>      (Required) The hostname or IP of the primary database server."
    echo "  --primary-user <user>      -U <user>      (Required) The replication username for the primary server."
    echo "  --primary-pass <password>  -P <password>  (Required) The password for the replication user."
    echo "  --replica-user <user>      -u <user>      (Optional) The username for the local (replica) database server. Defaults to 'root'."
    echo "  --replica-pass <password>  -p <password>  (Required) The password for the local (replica) user."
    echo "  --help                       -h           Display this help message."
    echo "Very important: Use single quotation marks to enclose your arguements to this scripts to avoid problems with special characters.
    exit 1
}

# --- Parse Command-Line Arguments ---
# Uses getopt to handle long and short options
PARSED_ARGS=$(getopt -o H:U:P:u:p:h --long primary-host:,primary-user:,primary-pass:,replica-user:,replica-pass:,help -n "$0" -- "$@")
if [ $? -ne 0 ]; then
    usage
fi

eval set -- "$PARSED_ARGS"

while true; do
    case "$1" in
        -H|--primary-host) PRIMARY_HOST="$2"; shift 2 ;;
        -U|--primary-user) PRIMARY_USER="$2"; shift 2 ;;
        -P|--primary-pass) PRIMARY_PASS="$2"; shift 2 ;;
        -u|--replica-user) REPLICA_USER="$2"; shift 2 ;;
        -p|--replica-pass) REPLICA_PASS="$2"; shift 2 ;;
        -h|--help)         usage ;;
        --)                shift; break ;;
        *)                 echo "Internal error!"; exit 1 ;;
    esac
done

# --- Argument Validation ---
if [[ -z "${PRIMARY_HOST}" || -z "${PRIMARY_USER}" || -z "${PRIMARY_PASS}" || -z "${REPLICA_PASS}" ]]; then
    echo -e "${RED}Error: Missing required arguments.${NC}\n"
    usage
fi

# --- SAFETY WARNING AND CONFIRMATION ---
echo -e "${RED}!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! WARNING !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo -e "${RED}This script will PERMANENTLY DELETE all user databases on the LOCAL MySQL server."
echo -e "${YELLOW}You are about to wipe the local database and re-sync it from:"
echo -e "  - PRIMARY HOST:      ${GREEN}${PRIMARY_HOST}${NC}"
echo -e "  - PRIMARY USER:      ${GREEN}${PRIMARY_USER}${NC}"
echo -e "  - LOCAL (REPLICA) USER: ${GREEN}${REPLICA_USER}${NC}"
echo -e "${YELLOW}Ensure you have a backup and are running this on the correct REPLICA server.${NC}"
echo -e "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!${NC}"
read -p "Type 'YES' to proceed: " confirmation
if [ "$confirmation" != "YES" ]; then
    echo "Operation cancelled by user."
    exit 0
fi

# --- Main Execution ---
echo -e "\n${GREEN}>>> Step 1: Stopping replica and dropping all local user databases...${NC}"
DB_LIST=$(mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" -ANe "SELECT GROUP_CONCAT('DROP DATABASE IF EXISTS \`', schema_name, '\`') FROM information_schema.schemata WHERE schema_name NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys');")

# Note: Use STOP REPLICA / RESET REPLICA for MySQL 8+
mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" -e "STOP SLAVE; RESET SLAVE ALL;"

if [ -n "$DB_LIST" ] && [ "$DB_LIST" != "NULL" ]; then
    mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" -e "${DB_LIST}"
    echo "Local databases dropped."
else
    echo "No user databases to drop."
fi

echo -e "\n${GREEN}>>> Step 2: Dumping all databases from primary server (${PRIMARY_HOST})...${NC}"
mysqldump --all-databases \
    -h "${PRIMARY_HOST}" \
    -u "${PRIMARY_USER}" \
    -p"${PRIMARY_PASS}" \
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

echo -e "\n${GREEN}>>> Step 3: Importing the dump file to the local replica...${NC}"
mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" < "${DUMP_FILE}"
echo "Import complete."

echo -e "\n${GREEN}>>> Step 4: Starting replication...${NC}"
# The dump file includes the CHANGE MASTER TO statement. We just need to start the slave.
# For MySQL 8.0.22+ use START REPLICA;
mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" -e "START SLAVE;"
echo "Replication started."

echo -e "\n${GREEN}>>> Step 5: Checking replica status...${NC}"
sleep 5 # Give replica a moment to connect
# For MySQL 8.0.22+ use SHOW REPLICA STATUS\G
mysql -u "${REPLICA_USER}" -p"${REPLICA_PASS}" -e "SHOW SLAVE STATUS\G"

echo -e "\n${GREEN}>>> Step 6: Deleting temporary SQL file (${DUMP_FILE})...${NC}"
rm "${DUMP_FILE}"
echo "Temporary file deleted."

echo -e "\n${GREEN}--- Process Complete ---${NC}"
echo "Please review the status output above. Check that 'Slave_IO_Running' and 'Slave_SQL_Running' are both 'Yes'."