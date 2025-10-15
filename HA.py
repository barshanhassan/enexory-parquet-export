#!/usr/bin/env python3
import mysql.connector
import time
import subprocess
from datetime import datetime
import threading

# ---------------- CONFIG ----------------
MYSQL_USER = "repl"
MYSQL_PASS = "replpass"
PROXYSQL_ADMIN = "admin2"
PROXYSQL_PASS = "admin2"
WRITE_HG = 10
READ_HG = 20
PROXYSQL_NODE = "proxysql"
NODE_LIST = ["mysql-master", "mysql-replica1", "mysql-replica2"]
CONNECTION_TIMEOUT = 5 # seconds
SLEEP_INTERVAL = 4 # seconds
SMALL_INTERVAL = 1 # seconds
REBUILD_LAG_THRESHOLD_HOURS = 6
REPOINT_RETRIES = 2
REPOINT_RETRY_DELAY = 4 # seconds
MASTER_ALIVE_RETIRES = 2
MASTER_ALIVE_RETRY_DELAY = 4 # seconds
QUORUM = (len(NODE_LIST) // 2) + 1
# ---------------------------------------

NODE_GTID = {}
NODE_STATUS = {}  # alive / dead / rebuilding
STATE_LOCK = threading.Lock()
PROXYSQL_LOCK = threading.Lock()

# ---------------- HELPERS ----------------
def mysql_connect(host, user, password, port=3306):
    try:
        return mysql.connector.connect(host=host, user=user, password=password, port=port, connection_timeout=CONNECTION_TIMEOUT)
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

def set_proxysql_node_status(node, node_status):
    if node_status == 'dead' or node_status == 'rebuilding':
        status = 'OFFLINE_HARD'
    elif node_status == "alive":
        status = 'ONLINE'
    else:
        print("[CRITICAL] Wrong node status used in 'set_proxysql_node_status'. Will lead to issues.")
        return
    
    with PROXYSQL_LOCK:
        print(f"[INFO] Updating ProxySQL: {node} is to be set {status}")
        conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
        if not conn:
            print("[ERROR] Cannot connect to ProxySQL")
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE mysql_servers
                SET status=%s
                WHERE hostname=%s
            """, (status, node))
            cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
            cursor.execute("SAVE MYSQL SERVERS TO DISK;")
            conn.commit()
            return True
        except mysql.connector.Error as e:
            print(f"[ERROR] Failed to execute ProxySQL update: {e}")
            return False
        finally:
            conn.close()

def update_proxysql_write(master_host):
    """Set all nodes to read-only, then chosen master to write. Returns True on success."""
    with PROXYSQL_LOCK:
        print(f"[INFO] Updating ProxySQL: all nodes read-only, {master_host} write")
        conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
        if not conn:
            print("[ERROR] Cannot connect to ProxySQL")
            return False
        try:
            cursor = conn.cursor()

            cursor.execute("BEGIN;")
            cursor.execute("DELETE FROM mysql_servers WHERE hostgroup_id = %s;", (WRITE_HG,))
            cursor.execute("""
                INSERT INTO mysql_servers (hostgroup_id, hostname, port)
                VALUES (%s, %s, %s);
            """, (WRITE_HG, master_host, 3306))
            cursor.execute("COMMIT;")
            
            cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
            cursor.execute("SAVE MYSQL SERVERS TO DISK;")

            conn.commit()
            return True
        except mysql.connector.Error as e:
            print(f"[ERROR] Failed to execute ProxySQL update: {e}")
            return False
        finally:
            conn.close()

def handle_promotion(master_node):
    """Promotes a DB node and attempts to update ProxySQL, setting state flags."""
    print(f"[INFO] Promoting {master_node} to master...")
   
    while not update_proxysql_write(master_node):
        print("Could not update ProxySQL to new master node. Retrying.")
        time.sleep(SLEEP_INTERVAL)

    conn = mysql_connect(master_node, MYSQL_USER, MYSQL_PASS)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("STOP SLAVE;")
            cursor.execute("RESET SLAVE ALL;")
            conn.commit()
        finally:
            conn.close()

def _point_to_master(node, master):
    print(f"[INFO] Pointing {node} to master {master}...")
    conn = mysql_connect(node, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[WARN] Cannot connect to {node}")
        return 1
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("STOP SLAVE;")
        cursor.execute(f"""
            CHANGE MASTER TO
              MASTER_HOST='{master}',
              MASTER_USER='{MYSQL_USER}',
              MASTER_PASSWORD='{MYSQL_PASS}',
              MASTER_AUTO_POSITION=1;
        """)
        cursor.execute("START SLAVE;")

        time.sleep(SLEEP_INTERVAL)

        cursor.execute("SHOW SLAVE STATUS")
        row = cursor.fetchone()

        if row.get('Slave_SQL_Running') == 'Yes':
            return 0

        return -1
    except mysql.connector.Error as e:
        print(f"[ERROR] Node {node} has an error: {e}")
        return 1
    finally:
        conn.close()

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

def get_master_from_proxysql():
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[WARN] Cannot connect to ProxySQL to detect master. Using fallback.")
        return None

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT hostname FROM mysql_servers WHERE hostgroup_id = {WRITE_HG}")
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

def choose_master():
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
        with STATE_LOCK:
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
    master = choose_master()
    with STATE_LOCK:
        for n in NODE_LIST:
            if n != node and NODE_STATUS.get(n) == "alive":
                lag = get_lag_hours(n)
                if lag is not None and lag <= REBUILD_LAG_THRESHOLD_HOURS:
                    return n
    return master

def rebuild_node(node):
    with STATE_LOCK:
        if NODE_STATUS.get(node) == "rebuilding":
            return
        NODE_STATUS[node] = "rebuilding"
        while not set_proxysql_node_status(node, NODE_STATUS[node]):
            print(f"Could not update {node} status in ProxySQL. Retrying.")
            time.sleep(SMALL_INTERVAL)
        NODE_GTID[node] = ""

    source_node = select_dump_source(node)
    if not source_node:
        print(f"[ERROR] No valid source node found to rebuild {node}. Aborting rebuild.")
        with STATE_LOCK:
            NODE_STATUS[node] = "dead"
            while not set_proxysql_node_status(node, NODE_STATUS[node]):
                print(f"Could not update {node} status in ProxySQL. Retrying.")
                time.sleep(SMALL_INTERVAL)
            NODE_GTID[node] = ""
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

            while True:
                current_master = get_master_from_proxysql()
                if current_master is None:
                    print("Please check if proxysql is working and configured correctly.")
                    time.sleep(SLEEP_INTERVAL)
                else:
                    res = attempt_repoint(node, current_master)
                    if res == 0:
                        print(f"[INFO] {node} now pointing to master {current_master}")
                    else:
                        print(f"[ERROR] Issue with {node}. Setting to dead.")
                        with STATE_LOCK:
                            NODE_STATUS[node] = "dead"
                            while not set_proxysql_node_status(node, NODE_STATUS[node]):
                                print(f"Could not update {node} status in ProxySQL. Retrying.")
                                time.sleep(SMALL_INTERVAL)
                            NODE_GTID[node] = ""                        
                    break

        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Rebuild failed for {node}: {e}")

        finally:
            with STATE_LOCK:
                if NODE_STATUS.get(node) == "rebuilding":
                    NODE_STATUS[node] = "alive" if is_alive(node) else "dead"
                    while not set_proxysql_node_status(node, NODE_STATUS[node]):
                        print(f"Could not update {node} status in ProxySQL. Retrying.")
                        time.sleep(SMALL_INTERVAL)
                    NODE_GTID[node] = get_gtid(node) if NODE_STATUS.get(node) == "alive" else ""

    threading.Thread(target=job, daemon=True).start()

def attempt_repoint(node, master):
    last_res = 1
    for i in range(REPOINT_RETRIES):
        last_res = _point_to_master(node, master)
        if last_res == 0:
            break
        print(f"[WARN] Failed to point {node} to {master}. Retrying... ({i+1}/{REPOINT_RETRIES})")
        time.sleep(REPOINT_RETRY_DELAY)
    return last_res

# ---------------- MAIN ----------------
print("[INFO] Initializing script...")

with STATE_LOCK:
    for node in NODE_LIST:
        if is_alive(node):
            NODE_STATUS[node] = "alive"
            while not set_proxysql_node_status(node, NODE_STATUS[node]):
                print(f"Could not update {node} status in ProxySQL. Retrying.")
                time.sleep(SMALL_INTERVAL)
            NODE_GTID[node] = get_gtid(node)
        else:
            NODE_STATUS[node] = "dead"
            while not set_proxysql_node_status(node, NODE_STATUS[node]):
                print(f"Could not update {node} status in ProxySQL. Retrying.")
                time.sleep(SMALL_INTERVAL)
            NODE_GTID[node] = ""

# Make sure we are connected to proxysql (must be setup with proper topology, rules and admin user beforehand)
MASTER = get_master_from_proxysql()

if MASTER is None:
    raise SystemError("Please configure ProxySQL with a Master.")

print(f"[INFO] Master from ProxySQL: {MASTER}")
for node in NODE_LIST:
    with STATE_LOCK:
        is_node_alive = NODE_STATUS.get(node) == "alive"
    if node != MASTER and is_node_alive:
        if get_node_master(node) != MASTER:
            print(f"[INFO] Redirecting {node} to point to {MASTER}")
            if attempt_repoint(node, MASTER) == -1:
                rebuild_node(node)

while True:
    print(f"--- {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    # Get master while checking if proxysql is still available.
    while True:
        MASTER = get_master_from_proxysql()
        if MASTER is None:
            print("Please check if proxysql is working and configured correctly.")
            time.sleep(SLEEP_INTERVAL)
        else:
            break
    
    # Set Node status.
    need_rebuild = set()
    with STATE_LOCK:
        for node in NODE_LIST:
            if NODE_STATUS.get(node) == "rebuilding":
                continue
            alive = is_alive(node)
            if alive:
                if NODE_STATUS.get(node) == "dead":
                    print(f"[INFO] Node {node} is back online")
                    NODE_STATUS[node] = "alive"
                    while not set_proxysql_node_status(node, NODE_STATUS[node]):
                        print(f"Could not update {node} status in ProxySQL. Retrying.")
                        time.sleep(SMALL_INTERVAL)
                    NODE_GTID[node] = get_gtid(node)
                    if node != MASTER and is_alive(MASTER) and attempt_repoint(node, MASTER) == -1:
                        need_rebuild.add(node)
            else:
                if NODE_STATUS.get(node) == "alive":
                    print(f"[WARN] Node {node} has gone down")
                    while not set_proxysql_node_status(node, "dead"):
                        print(f"Could not update {node} status in ProxySQL. Retrying.")
                        time.sleep(SMALL_INTERVAL)
                NODE_STATUS[node] = "dead"
                NODE_GTID[node] = ""
    for node in need_rebuild:
        rebuild_node(node)

    master_retry_count = 0
    while(master_retry_count < MASTER_ALIVE_RETIRES):
        master_retry_count += 1
        if is_alive(MASTER):
            break
        time.sleep(MASTER_ALIVE_RETRY_DELAY)
    
    if not is_alive(MASTER):
        print(f"[WARN] Master {MASTER} is down. Initiating failover check...")
        with STATE_LOCK:
            alive_count = sum(1 for status in NODE_STATUS.values() if status == "alive")

        if alive_count < QUORUM:
            print(f"[ERROR] Quorum not met ({alive_count}/{QUORUM}). Partition detected. Aborting failover.")
        else:
            print("[INFO] Quorum met. Promoting new master...")
            new_master = choose_master()
            if new_master:
                MASTER = new_master
                handle_promotion(MASTER)
                for node in NODE_LIST:
                    with STATE_LOCK:
                        is_node_alive = NODE_STATUS.get(node) == "alive"
                    if node != MASTER and is_alive(MASTER) and is_node_alive and attempt_repoint(node, MASTER) == -1:
                        rebuild_node(node)
            else:
                print("[ERROR] Failover failed: Could not detect a new master.")

    time.sleep(SLEEP_INTERVAL)