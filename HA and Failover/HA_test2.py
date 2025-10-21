#!/usr/bin/env python3
import mysql.connector
import threading
import time
import random
from datetime import datetime
import sys
import subprocess

# ---------------- CONFIG ----------------
PROXYSQL_NODE = "proxysql"
PROXYSQL_PORT = 3306
APP_USER = "appuser"
APP_PASS = "proxypass"
TEST_DB = "testdb"
TEST_TABLE = "test"
OUTPUT_FILE = "output.log"

NODES = ["mysql-master", "mysql-replica1", "mysql-replica2"]
NODE_DOWN_SHORT = 2    # seconds
NODE_DOWN_LONG = 60   # seconds
POST_RECONNECT_DELAY = 30  # seconds

SLEEP_INTERVAL = 1      # main loop sleep
MAX_ID = 0              # incremented manually

# probabilities
P_INSERT = 0.4
P_UPDATE = 0.4
P_DELETE = 0.2

STATE_LOCK = threading.Lock()
STOP_FLAP = True

# in-memory table tracking: {id: value}
EXPECTED_TABLE = {}

# ---------------- HELPERS ----------------
def log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(OUTPUT_FILE, "a") as f:
        f.write(line + "\n")
        f.flush()

def mysql_connect():
    try:
        return mysql.connector.connect(
            host=PROXYSQL_NODE,
            port=PROXYSQL_PORT,
            user=APP_USER,
            password=APP_PASS,
            database=TEST_DB,
            autocommit=True,
            connection_timeout=5
        )
    except mysql.connector.Error as e:
        log(f"[ERROR] Cannot connect to DB: {e}")
        return None

def check_empty_table():
    conn = mysql_connect()
    if not conn:
        sys.exit(1)
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {TEST_TABLE}")
    count = cursor.fetchone()[0]
    conn.close()
    if count > 0:
        log(f"[ERROR] Table {TEST_DB}.{TEST_TABLE} already has {count} rows. Clear before running.")
        sys.exit(1)
    log(f"[INFO] Table {TEST_DB}.{TEST_TABLE} is empty. Proceeding.")

def insert_row(id_val, num_val):
    conn = mysql_connect()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"INSERT INTO {TEST_TABLE} (id, x) VALUES (%s,%s)", (id_val, num_val))
        return True
    except mysql.connector.Error as e:
        log(f"[WARN] Insert failed for id={id_val}: {e}")
        return False
    finally:
        conn.close()

def update_row(id_val):
    conn = mysql_connect()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"UPDATE {TEST_TABLE} SET x = x + 1 WHERE id = %s", (id_val,))
        return cursor.rowcount > 0
    except mysql.connector.Error as e:
        log(f"[WARN] Update failed for id={id_val}: {e}")
        return False
    finally:
        conn.close()

def delete_row(id_val):
    conn = mysql_connect()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {TEST_TABLE} WHERE id = %s", (id_val,))
        return cursor.rowcount > 0
    except mysql.connector.Error as e:
        log(f"[WARN] Delete failed for id={id_val}: {e}")
        return False
    finally:
        conn.close()

def read_row(id_val):
    conn = mysql_connect()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT x FROM {TEST_TABLE} WHERE id = %s", (id_val,))
        result = cursor.fetchone()
        return result[0] if result else None
    except mysql.connector.Error as e:
        log(f"[WARN] Read failed for id={id_val}: {e}")
        return None
    finally:
        conn.close()

# ---------------- NODE UP/DOWN SIM ----------------
def docker_disconnect(node, network="mysql-net"):
    subprocess.run(["docker", "network", "disconnect", network, node])

def docker_connect(node, network="mysql-net"):
    subprocess.run(["docker", "network", "connect", network, node])

def simulate_node_flapping(node_list):
    while not STOP_FLAP:
        node = random.choice(node_list)
        delay = random.randint(NODE_DOWN_SHORT, NODE_DOWN_LONG)
        time.sleep(delay)
        docker_disconnect(node)
        log(f"[INFO] Node {node} is DOWN (simulated) for {delay}s")
        time.sleep(delay)
        docker_connect(node)
        log(f"[INFO] Node {node} is UP (simulated)")
        time.sleep(POST_RECONNECT_DELAY)

# ---------------- TEST SEQUENCE ----------------
WRITE_THREADS = []
def async_write(func, *args):
    t = threading.Thread(target=func, args=args, daemon=False)
    t.start()
    WRITE_THREADS.append(t)

def test_loop():
    global MAX_ID
    WEIGHTS_TABLE = []  # weights in order corresponding to EXPECTED_TABLE keys
    while True:
        rnd = random.random()
        with STATE_LOCK:
            if rnd < P_INSERT:
                # insert
                MAX_ID += 1
                id_val = MAX_ID
                async_write(insert_row, id_val, 1)
                # always record attempt
                EXPECTED_TABLE[id_val] = EXPECTED_TABLE.get(id_val, 0) + 1
                WEIGHTS_TABLE.append(len(WEIGHTS_TABLE) + 1)
                log(f"[INSERT] id={id_val} val={EXPECTED_TABLE[id_val]}")

            elif rnd < P_INSERT + P_UPDATE:
                if EXPECTED_TABLE:
                    keys = list(EXPECTED_TABLE.keys())
                    id_val = random.choices(keys, weights=WEIGHTS_TABLE, k=1)[0]
                    async_write(update_row, id_val)
                    # always increment expected value
                    EXPECTED_TABLE[id_val] += 1
                    log(f"[UPDATE] id={id_val} val={EXPECTED_TABLE[id_val]}")

            else:  # DELETE
                if EXPECTED_TABLE:
                    keys = list(EXPECTED_TABLE.keys())
                    id_val = random.choices(keys, weights=WEIGHTS_TABLE, k=1)[0]
                    async_write(delete_row, id_val)
                    # remove from expected table anyway
                    del EXPECTED_TABLE[id_val]
                    WEIGHTS_TABLE.pop()
                    log(f"[DELETE] id={id_val}")

        # random read (uniform)
        if EXPECTED_TABLE:
            id_val = random.choice(list(EXPECTED_TABLE.keys()))
            val = read_row(id_val)
            expected_val = EXPECTED_TABLE[id_val]
            if val == expected_val:
                log(f"[READ OK] id={id_val} val={val}")
            else:
                log(f"[READ MISMATCH] id={id_val} expected={expected_val} got={val}")

        time.sleep(SLEEP_INTERVAL)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    log("[INFO] Starting test sequence...")
    check_empty_table()

    t = threading.Thread(target=simulate_node_flapping, args=(NODES,), daemon=False)
    t.start()

    try:
        test_loop()
    except KeyboardInterrupt:
        log("[INFO] Test interrupted. Dumping expected vs actual table...")

        STOP_FLAP = True
        t.join()

        for t in WRITE_THREADS:
            t.join()
            WRITE_THREADS.clear()

        print("Wait 30s for any pending sql commands to be applied.")
        time.sleep(30)

        # Print expected table
        log("[INFO] Expected in-memory table:")
        for k, v in EXPECTED_TABLE.items():
            log(f"id={k} val={v}")

        # Print actual table
        conn = mysql_connect()
        if conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT id, x FROM {TEST_TABLE} ORDER BY id")
            rows = cursor.fetchall()
            conn.close()
            log("[INFO] Actual DB table:")
            for row in rows:
                log(f"id={row[0]} val={row[1]}")

            # Compare
            inconsistencies = []
            expected_keys = set(EXPECTED_TABLE.keys())
            actual_keys = set(row[0] for row in rows)
            for k in expected_keys.union(actual_keys):
                expected_val = EXPECTED_TABLE.get(k)
                actual_val = next((r[1] for r in rows if r[0] == k), None)
                if expected_val != actual_val:
                    inconsistencies.append((k, expected_val, actual_val))

            log(f"[INFO] Inconsistencies: {len(inconsistencies)}")
            for k, e, a in inconsistencies:
                log(f"id={k} expected={e} actual={a}")

        log("[INFO] Test complete.")
