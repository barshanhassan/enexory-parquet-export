import mysql.connector
import time
import subprocess
from datetime import datetime
import threading

MYSQL_USER = "repl"
MYSQL_PASS = "replpass"
PROXYSQL_ADMIN = "admin2"
PROXYSQL_PASS = "admin2"
WRITE_HG = 10
READ_HG = 20
PROXYSQL_NODE = "proxysql"
NODE_LIST = ["mysql-master", "mysql-replica1", "mysql-replica2"]
SLEEP_INTERVAL = 10
REBUILD_LAG_THRESHOLD_HOURS = 6
REPOINT_RETRIES = 3
REPOINT_RETRY_DELAY = 10 # seconds
QUORUM = (len(NODE_LIST) // 2) + 1
NODE_GTID = {}
NODE_STATUS = {}  # alive / dead / rebuilding
STATE_LOCK = threading.Lock()
PROXYSQL_IN_SYNC = True # Assume ProxySQL is initially in sync

def mysql_connect(host, user, password, port=3306):
    try:
        return mysql.connector.connect(host=host, user=user, password=password, port=port)
    except mysql.connector.Error:
        return None

def is_alive(host):
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if conn:
        conn.close()
        return True
    return False

def get_gtid(host):
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        return ""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT @@GLOBAL.gtid_executed;")
        gtid = cursor.fetchone()[0]
        conn.close()
        return gtid
    except mysql.connector.Error:
        conn.close()
        return ""

def get_node_master(host):
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        return None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SHOW SLAVE STATUS")
    result = cursor.fetchone()
    conn.close()
    if result and result['Master_Host']:
        return result['Master_Host']
    return None

for node in NODE_LIST:
    if is_alive(node):
        print(f"{node} is alive, GTID is {get_gtid(node)}, Master is {get_node_master(node)}.")
    else:
        print(f"{node} is dead.")


def update_proxysql_write(master_host):
    """Set all nodes to read-only, then chosen master to write. Returns True on success."""
    print(f"[INFO] Updating ProxySQL: all nodes read-only, {master_host} write")
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[ERROR] Cannot connect to ProxySQL")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM mysql_servers WHERE hostgroup_id = %s;", (WRITE_HG,))
        cursor.execute(f"""
            INSERT INTO mysql_servers (hostgroup_id, hostname, port)
            VALUES ({WRITE_HG}, '{master_host}', 3306);
        """)
        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        conn.commit()
        return True
    except mysql.connector.Error as e:
        print(f"[ERROR] Failed to execute ProxySQL update: {e}")
        return False
    finally:
        conn.close()

def get_master_from_proxysql():
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[WARN] Cannot connect to ProxySQL to detect master. Using fallback.")
        return None

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT hostname FROM runtime_mysql_servers WHERE hostgroup_id = {WRITE_HG}")
        results = cursor.fetchall()
    finally:
        conn.close()

    if len(results) == 1:
        return results[0]['hostname']
    elif len(results) > 1:
        print(f"[ERROR] ProxySQL reports multiple writers: {[r['hostname'] for r in results]}. Manual intervention required.")
        return None
    else:
        return None

print()
update_proxysql_write(NODE_LIST[1])
print(f"Set {NODE_LIST[1]} to master in proxysql. Master in proxysql is now {get_master_from_proxysql()}")
update_proxysql_write(NODE_LIST[0])
print(f"Set {NODE_LIST[0]} to master in proxysql. Master in proxysql is now {get_master_from_proxysql()}")

def handle_promotion(master_node):
    """Promotes a DB node and attempts to update ProxySQL, setting state flags."""
    global PROXYSQL_IN_SYNC
    print(f"[INFO] Promoting {master_node} to master...")
    conn = mysql_connect(master_node, MYSQL_USER, MYSQL_PASS)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("STOP SLAVE;")
            cursor.execute("RESET SLAVE ALL;")
            conn.commit()
        finally:
            conn.close()

    if update_proxysql_write(master_node):
        PROXYSQL_IN_SYNC = True
    else:
        print("[CRITICAL] Database failover complete, but ProxySQL update failed. Will retry.")
        PROXYSQL_IN_SYNC = False

def point_to_master(node, master):
    print(f"[INFO] Pointing {node} to master {master}...")
    conn = mysql_connect(node, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[WARN] Cannot connect to {node}")
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("STOP SLAVE;")
        cursor.execute("RESET SLAVE ALL;")
        cursor.execute(f"""
            CHANGE MASTER TO
              MASTER_HOST='{master}',
              MASTER_USER='{MYSQL_USER}',
              MASTER_PASSWORD='{MYSQL_PASS}',
              MASTER_AUTO_POSITION=1;
        """)
        cursor.execute("START SLAVE;")
        conn.commit()
        conn.close()
        return True
    except mysql.connector.Error as e:
        print(f"[ERROR] Node {node} cannot point to master: {e}")
        return False

print()
handle_promotion(NODE_LIST[1])
print(f"Promoted {NODE_LIST[1]} to a Master. It now points to {get_node_master(NODE_LIST[1])}. Master in proxysql is now {get_master_from_proxysql()}")
point_to_master(NODE_LIST[2], NODE_LIST[1])
print(f"Set {NODE_LIST[2]} to point to {get_node_master(NODE_LIST[2])}.")
print("Resetting back now")
handle_promotion(NODE_LIST[0])
point_to_master(NODE_LIST[1], NODE_LIST[0])
point_to_master(NODE_LIST[2], NODE_LIST[0])
print(f"Master is now {get_master_from_proxysql()}. {NODE_LIST[1]} now points to {get_node_master(NODE_LIST[1])}. {NODE_LIST[2]} now points to {get_node_master(NODE_LIST[2])}")

def latest_replica():
    with STATE_LOCK:
        contenders = {n: gtid for n, gtid in NODE_GTID.items() if NODE_STATUS.get(n) == "alive" and gtid}

    if not contenders:
        return None

    check_node = next(iter(contenders.keys()), None)
    if not check_node:
        return None

    conn = mysql_connect(check_node, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[WARN] Cannot connect to {check_node} to compare GTIDs. Falling back to string comparison.")
        return max(contenders, key=contenders.get)

    try:
        cursor = conn.cursor()
        for node1, gtid1 in contenders.items():
            is_most_advanced = True
            for node2, gtid2 in contenders.items():
                if node1 == node2:
                    continue
                cursor.execute(f"SELECT GTID_SUBSET('{gtid2}', '{gtid1}');")
                if cursor.fetchone()[0] == 0:
                    is_most_advanced = False
                    break
            if is_most_advanced:
                return node1
    finally:
        conn.close()

    print("[WARN] Could not determine a single most advanced replica via GTID sets. Data may have diverged.")
    return max(contenders, key=contenders.get)

def detect_master():
    proxysql_master = get_master_from_proxysql()
    if proxysql_master and proxysql_master in NODE_LIST:
        with STATE_LOCK:
            if NODE_STATUS.get(proxysql_master) == "alive":
                return proxysql_master
            else:
                print(f"[WARN] ProxySQL designates {proxysql_master} as master, but it is not alive.")

    print("[INFO] Falling back to topology analysis to detect master.")
    with STATE_LOCK:
        alive_nodes = [n for n in NODE_LIST if NODE_STATUS.get(n) == "alive"]

    candidates = [n for n in alive_nodes if get_node_master(n) is None]

    if not candidates:
        return latest_replica()
    elif len(candidates) == 1:
        return candidates[0]
    else:
        print(f"[WARN] Multiple master candidates found: {candidates}. Selecting most advanced.")
        candidate_gtids = {n: g for n, g in NODE_GTID.items() if n in candidates}
        return max(candidate_gtids, key=candidate_gtids.get)

def get_lag_hours(target_node):
    try:
        conn = mysql_connect(target_node, MYSQL_USER, MYSQL_PASS)
        if not conn:
            return None
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW SLAVE STATUS")
        result = cursor.fetchone()
        conn.close()
        if result and result['Seconds_Behind_Master'] is not None:
            return result['Seconds_Behind_Master'] / 3600
    except Exception:
        pass
    return None

def select_dump_source(node):
    master = detect_master()
    with STATE_LOCK:
        for n in NODE_LIST:
            if n != node and NODE_STATUS.get(n) == "alive":
                lag = get_lag_hours(n)
                if lag is not None and lag <= REBUILD_LAG_THRESHOLD_HOURS:
                    return n
    return master

print()
print("[INFO] Initializing node status...")
with STATE_LOCK:
    for node in NODE_LIST:
        if is_alive(node):
            NODE_STATUS[node] = "alive"
            NODE_GTID[node] = get_gtid(node)
        else:
            NODE_STATUS[node] = "dead"
            NODE_GTID[node] = ""
print(f"The best node for dump for {NODE_LIST[1]} is {select_dump_source(NODE_LIST[1])}. {NODE_LIST[2]} has a replication lag (in hours) of {get_lag_hours(NODE_LIST[2])}.")

def rebuild_node(node):
    with STATE_LOCK:
        if NODE_STATUS.get(node) == "rebuilding":
            return
        NODE_STATUS[node] = "rebuilding"

    source_node = select_dump_source(node)
    if not source_node:
        print(f"[ERROR] No valid source node found to rebuild {node}. Aborting rebuild.")
        with STATE_LOCK:
            NODE_STATUS[node] = "dead"
        return

    def job():
        try:
            print(f"[INFO] Rebuilding {node} from {source_node}...")

            conn = mysql_connect(node, MYSQL_USER, MYSQL_PASS)
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute("STOP SLAVE;")
                    cursor.execute("RESET SLAVE ALL;")
                    conn.commit()
                finally:
                    conn.close()

            # 1. Drop only user (non-system) databases on target
            wipe_cmd = (
                f"ssh root@{node} "
                f"\"mysql -u{MYSQL_USER} -p{MYSQL_PASS} -N -e "
                f"'SHOW DATABASES' | grep -Ev '^(mysql|sys|performance_schema|information_schema)$' "
                f"| xargs -I{{}} mysql -u{MYSQL_USER} -p{MYSQL_PASS} -e 'DROP DATABASE IF EXISTS {{}};'\""
            )
            subprocess.run(wipe_cmd, shell=True, check=True)
            print(f"[INFO] User databases wiped on {node}")

            # 2. Stream dump directly from source to target (no temp file)
            dump_stream_cmd = (
                f"ssh root@{source_node} "
                f"\"mysqldump --all-databases -h {source_node} -u{MYSQL_USER} -p{MYSQL_PASS} "
                f"--single-transaction --routines --triggers "
                f"--flush-privileges --hex-blob --default-character-set=utf8 "
                f"--set-gtid-purged=OFF --insert-ignore\" "
                f"| ssh root@{node} "
                f"\"mysql -u{MYSQL_USER} -p{MYSQL_PASS}\""
            )
            subprocess.run(dump_stream_cmd, shell=True, check=True)
            print(f"[INFO] Dump streamed and imported directly to {node}")

            current_master = detect_master()
            if current_master:
                point_to_master(node, current_master)
                print(f"[INFO] {node} now pointing to master {current_master}")

        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Rebuild failed for {node}: {e}")

        finally:
            with STATE_LOCK:
                NODE_STATUS[node] = "alive" if is_alive(node) else "dead"

    threading.Thread(target=job, daemon=True).start()

print()
print(f"Testing node rebuild of {NODE_LIST[1]}")
rebuild_node(NODE_LIST[1])