#!/usr/bin/env python3
# pylint: disable=missing-docstring
# pylint: disable=broad-exception-caught
# pylint: disable=line-too-long
# pylint: disable=invalid-name

import time
from datetime import datetime
from functools import wraps
from typing import Any, Callable, cast, Literal
import mysql.connector
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract

def keeptrying(interval: float):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            while True:
                result = func(*args, **kwargs)
                is_success = False

                if isinstance(result, tuple):
                    if result:
                        is_success = result[0]
                else:
                    is_success = result

                if is_success:
                    return result

                print(f"{func.__name__} failed. Retrying in {interval} seconds.")
                time.sleep(interval)
        return wrapper
    return decorator

# ---------------- CONFIG ----------------
# Must be set via args
MYSQL_USER: str = "repl"
MYSQL_PASS: str = "replpass"
PROXYSQL_ADMIN: str = "admin2"
PROXYSQL_PASS: str = "admin2"
PROXYSQL_NODE: str = "proxysql"
EMAIL_TO_REPORT_TO: str = ""

# May be set via args
CONNECTION_TIMEOUT: float = 5 # seconds
SLEEP_INTERVAL: float = 4 # seconds
SMALL_INTERVAL: float = 1 # seconds
REBUILD_LAG_THRESHOLD_HOURS: float = 6 # hours
REPOINT_RETRIES: int = 2
REPOINT_RETRY_DELAY: float = 4 # seconds
MASTER_ONLINE_RETIRES: int = 2
MASTER_ONLINE_RETRY_DELAY: float = 4 # seconds

# Are not set via args
WRITE_HG: int = 10
READ_HG: int = 20
BROKEN_HG: int = 30
NODE_LIST: list[str] = []
NODE_STATUS: dict[str, str] = {}
QUORUM: int = -1

# ---------------- HELPERS ----------------
def mysql_connect(host: str, user: str, password: str, port: int = 3306) -> PooledMySQLConnection | MySQLConnectionAbstract | None:
    try:
        return mysql.connector.connect(host=host, user=user, password=password, port=port, connection_timeout=CONNECTION_TIMEOUT)
    except mysql.connector.Error:
        return None

def is_online(host: str) -> bool:
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if conn:
        conn.close()
        return True
    return False

def get_gtid(host: str) -> str:
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        return ""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT @@GLOBAL.gtid_executed;")
        gtid = cast(tuple[Any, ...] | None, cursor.fetchone())
        if not gtid or gtid[0] is None:
            return ""
        return str(gtid[0])
    except Exception as e:
        print(f"[ERROR] Failed to get gtid for {host} due to an error: {e}")
        conn.close()
        return ""
    finally:
        conn.close()

def get_node_master(host: str) -> tuple[int, str | None]:
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        return 1, None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SHOW SLAVE STATUS")
    result = cast(dict[str, Any] | None, cursor.fetchone())
    conn.close()
    if result and result['Master_Host']:
        return 0, result['Master_Host']
    return 0, None

@keeptrying(interval=SLEEP_INTERVAL)
def set_proxysql_node_status(selected_node: str, node_status: Literal["alive", "dead", "broken"]) -> bool:
    if node_status == 'offline' or node_status == 'broken':
        status = 'OFFLINE_HARD'
    elif node_status == "online":
        status = 'ONLINE'
    else:
        print("[CRITICAL] Wrong node status used in 'set_proxysql_node_status'. Will lead to issues.")
        exit(-1)
    
    print(f"[INFO] Updating ProxySQL: {selected_node} is to be set {status}")
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
        """, (status, selected_node))
        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")
        conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Failed to execute ProxySQL update: {e}")
        return False
    finally:
        conn.close()

@keeptrying(interval=SLEEP_INTERVAL)
def update_proxysql_write(master_host: str) -> bool:
    """Set all nodes to read-only, then chosen master to write. Returns True on success."""
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
    except Exception as e:
        print(f"[ERROR] Failed to execute ProxySQL update: {e}")
        return False
    finally:
        conn.close()

@keeptrying(interval=SLEEP_INTERVAL)
def update_proxysql_broken(broken_host: str) -> bool:
    """Sets proxysql to only have hostgroup 30 entry when it comes to the broken host. Returns True on success."""
    print(f"[INFO] Updating ProxySQL: {broken_host} broken")
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[ERROR] Cannot connect to ProxySQL")
        return False
    try:
        cursor = conn.cursor()

        cursor.execute("BEGIN;")
        cursor.execute("DELETE FROM mysql_servers WHERE hostname = %s;", (broken_host,))
        cursor.execute("""
            INSERT INTO mysql_servers (hostgroup_id, hostname, port)
            VALUES (%s, %s, %s);
        """, (BROKEN_HG, broken_host, 3306))
        cursor.execute("COMMIT;")
        
        cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
        cursor.execute("SAVE MYSQL SERVERS TO DISK;")

        conn.commit()
        return True
    except Exception as e:
        print(f"[ERROR] Failed to execute ProxySQL update: {e}")
        return False
    finally:
        conn.close()

@keeptrying(interval=SLEEP_INTERVAL)
def get_proxysql_recognized_nodes() -> tuple[bool, list[str]]:
    """
    Gets a unique set of nodes from ProxySQL that belong to hostgroups 10, 20, or 30.
    Returns a tuple (bool, list): (True, nodes) on success, (False, []) on failure.
    """
    print("[INFO] Querying ProxySQL for recognized nodes...")
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[ERROR] Cannot connect to ProxySQL")
        return False, []

    try:
        cursor = conn.cursor()

        # Corrected SQL query with DISTINCT and parameter substitution
        query = """
            SELECT DISTINCT hostname 
            FROM mysql_servers 
            WHERE hostgroup_id IN (%s, %s, %s);
        """
        cursor.execute(query, (WRITE_HG, READ_HG, BROKEN_HG))

        # Fetch all results and flatten the list of tuples into a list of strings
        nodes: list[str] = [cast(str, item[0]) for item in cursor.fetchall()] # type: ignore[index]

        print(f"[INFO] Found nodes in ProxySQL: {nodes}")
        return True, nodes

    except Exception as e:
        print(f"[ERROR] Failed to query nodes from ProxySQL: {e}")
        return False, []

    finally:
        if conn:
            conn.close()

def handle_promotion(master_node: str) -> None:
    """Promotes a DB node and attempts to update ProxySQL, setting state flags."""
    print(f"[INFO] Promoting {master_node} to master...")
    update_proxysql_write(master_node)

    conn = mysql_connect(master_node, MYSQL_USER, MYSQL_PASS)
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute("STOP SLAVE;")
            cursor.execute("RESET SLAVE ALL;")
            conn.commit()
        finally:
            conn.close()

def _point_to_master(selected_node: str, master: str) -> int:
    print(f"[INFO] Pointing {selected_node} to master {master}...")
    conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[WARN] Cannot connect to {selected_node}")
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
        row = cast(dict[str, Any] | None, cursor.fetchone())

        if not row:
            return 1

        if row.get('Slave_SQL_Running') == 'Yes':
            return 0

        return -1
    except Exception as e:
        print(f"[ERROR] Node {selected_node} with master {master} has an error: {e}")
        return 1
    finally:
        conn.close()

def attempt_repoint(selected_node: str, master: str) -> int:
    last_res = 1
    for i in range(REPOINT_RETRIES):
        last_res = _point_to_master(selected_node, master)
        if last_res == 0:
            break
        print(f"[WARN] Failed to point {selected_node} to {master}. Retrying... ({i+1}/{REPOINT_RETRIES})")
        time.sleep(REPOINT_RETRY_DELAY)
    return last_res

@keeptrying(interval=SLEEP_INTERVAL)
def get_master_from_proxysql() -> tuple[bool, str | None]:
    conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
    if not conn:
        print("[WARN] Cannot connect to ProxySQL to detect master. Using fallback.")
        return False, None

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT hostname FROM mysql_servers WHERE hostgroup_id = {WRITE_HG}")
        results = cast(list[dict[str, Any]], cursor.fetchall())
    finally:
        conn.close()

    if len(results) == 1:
        return True, results[0]['hostname']
    elif len(results) > 1:
        print(f"[ERROR] ProxySQL reports multiple writers: {[r['hostname'] for r in results]}. Manual intervention required.")
        return False, None
    else:
        return True, None

def latest_replica() -> str | None:
    NODE_GTIDS: dict[str, str] = {node: get_gtid(node) for node in NODE_LIST if NODE_STATUS.get(node) == "online"}

    contenders: dict[str, str] = {node: gtid for node, gtid in NODE_GTIDS.items() if gtid}

    if not contenders:
        return None

    check_node = next(iter(contenders.keys()), None)
    if not check_node:
        return None

    conn = mysql_connect(check_node, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[WARN] Cannot connect to {check_node} to compare GTIDs. Falling back to string comparison.")
        return max(contenders, key=cast(Callable[[str], str], contenders.get))

    try:
        cursor = conn.cursor()
        for node1, gtid1 in contenders.items():
            is_most_advanced = True
            for node2, gtid2 in contenders.items():
                if node1 == node2:
                    continue
                cursor.execute(f"SELECT GTID_SUBSET('{gtid2}', '{gtid1}');")
                if cast(tuple[int], cursor.fetchone())[0] == 0:
                    is_most_advanced = False
                    break
            if is_most_advanced:
                return node1
    finally:
        conn.close()

    print("[WARN] Could not determine a single most advanced replica via GTID sets. Data may have diverged.")
    return max(contenders, key=cast(Callable[[str], str], contenders.get))

def choose_master():
    proxysql_master = get_master_from_proxysql()
    if proxysql_master and proxysql_master in NODE_LIST:
        if NODE_STATUS.get(proxysql_master) == "online":
            return proxysql_master
        else:
            print(f"[WARN] ProxySQL designates {proxysql_master} as master, but it is not online.")

    print("[INFO] Falling back to topology analysis to detect master.")
    online_nodes = [n for n in NODE_LIST if NODE_STATUS.get(n) == "online"]

    candidates = [n for n in online_nodes if get_node_master(n) is None]

    if not candidates:
        return latest_replica()
    elif len(candidates) == 1:
        return candidates[0]
    else:
        print(f"[WARN] Multiple master candidates found: {candidates}. Selecting most advanced.")
        candidate_gtids = {n: g for n, g in NODE_GTID.items() if n in candidates}
        return max(candidate_gtids, key=candidate_gtids.get)

def get_lag_hours(selected_node) -> float | None:
    try:
        conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
        if not conn:
            return None
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SHOW SLAVE STATUS")
        result = cast(dict[str, Any] | None, cursor.fetchone())
        conn.close()
        if result and result['Seconds_Behind_Master'] is not None:
            return result['Seconds_Behind_Master'] / 3600
    except Exception as e:
        print(f"[ERROR] Failed to get lag hours for {selected_node} due to an error: {e}")
    return None

def select_dump_source(selected_node):
    master = choose_master()
    for n in NODE_LIST:
        if n != selected_node and NODE_STATUS.get(n) == "online":
            lag = get_lag_hours(n)
            if lag is not None and lag <= REBUILD_LAG_THRESHOLD_HOURS:
                return n
    return master

# ---------------- Load Save ----------------
NODE_LIST = ["mysql-master", "mysql-replica1", "mysql-replica2"]
QUORUM = (len(NODE_LIST) // 2) + 1


# ---------------- Email Thread ----------------


# ---------------- MAIN ----------------
print("[INFO] Initializing script...")

with STATE_LOCK:
    for node in NODE_LIST:
        if is_online(node):
            NODE_STATUS[node] = "online"
            while not set_proxysql_node_status(node, NODE_STATUS[node]):
                print(f"Could not update {node} status in ProxySQL. Retrying.")
                time.sleep(SMALL_INTERVAL)
            NODE_GTID[node] = get_gtid(node)
        else:
            NODE_STATUS[node] = "offline"
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
        is_node_online = NODE_STATUS.get(node) == "online"
    if node != MASTER and is_node_online:
        if get_node_master(node) != MASTER:
            print(f"[INFO] Redirecting {node} to point to {MASTER}")
            if attempt_repoint(node, MASTER) == -1:
                mark_as_broken(node)

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
            if NODE_STATUS.get(node) == "broken":
                continue
            online = is_online(node)
            if online:
                if NODE_STATUS.get(node) == "offline":
                    print(f"[INFO] Node {node} is back online")
                    NODE_STATUS[node] = "online"
                    while not set_proxysql_node_status(node, NODE_STATUS[node]):
                        print(f"Could not update {node} status in ProxySQL. Retrying.")
                        time.sleep(SMALL_INTERVAL)
                    NODE_GTID[node] = get_gtid(node)
                    if node != MASTER and is_online(MASTER) and attempt_repoint(node, MASTER) == -1:
                        need_rebuild.add(node)
            else:
                if NODE_STATUS.get(node) == "online":
                    print(f"[WARN] Node {node} has gone down")
                    while not set_proxysql_node_status(node, "offline"):
                        print(f"Could not update {node} status in ProxySQL. Retrying.")
                        time.sleep(SMALL_INTERVAL)
                NODE_STATUS[node] = "offline"
                NODE_GTID[node] = ""
    for node in need_rebuild:
        mark_as_broken(node)

    master_retry_count = 0
    while(master_retry_count < MASTER_ONLINE_RETIRES):
        master_retry_count += 1
        if is_online(MASTER):
            break
        time.sleep(MASTER_ONLINE_RETRY_DELAY)
    
    if not is_online(MASTER):
        print(f"[WARN] Master {MASTER} is down. Initiating failover check...")
        with STATE_LOCK:
            online_count = sum(1 for status in NODE_STATUS.values() if status == "online")

        if online_count < QUORUM:
            print(f"[ERROR] Quorum not met ({online_count}/{QUORUM}). Partition detected. Aborting failover.")
        else:
            print("[INFO] Quorum met. Promoting new master...")
            new_master = choose_master()
            if new_master:
                MASTER = new_master
                handle_promotion(MASTER)
                for node in NODE_LIST:
                    with STATE_LOCK:
                        is_node_online = NODE_STATUS.get(node) == "online"
                    if node != MASTER and is_online(MASTER) and is_node_online and attempt_repoint(node, MASTER) == -1:
                        mark_as_broken(node)
            else:
                print("[ERROR] Failover failed: Could not detect a new master.")


    time.sleep(SLEEP_INTERVAL)