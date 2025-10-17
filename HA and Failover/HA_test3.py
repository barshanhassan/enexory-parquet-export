import mysql.connector
import time
import subprocess

SLEEP_INTERVAL = 4
node = 'mysql-replica1'
source_node = 'mysql-master'
master = 'mysql-master'
MYSQL_USER = "repl"
MYSQL_PASS = "replpass"

def mysql_connect(host, user, password, port=3306):
    try:
        return mysql.connector.connect(host=host, user=user, password=password, port=port, connection_timeout=5)
    except mysql.connector.Error:
        return None

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

        if not row:
            return 1

        if row.get('Slave_SQL_Running') == 'Yes':
            return 0

        return -1
    except Exception as e:
        print(f"[ERROR] Node {node} with master {master} has an error: {e}")
        return 1
    finally:
        conn.close()

conn = mysql_connect(node, MYSQL_USER, MYSQL_PASS)
cursor = conn.cursor()
cursor.execute("STOP SLAVE;")
cursor.execute("RESET SLAVE ALL;")
cursor.execute("RESET MASTER;")
conn.commit()
conn.close()

dump_restore_cmd = (
    f"ssh root@{node} "
    f"\"/usr/bin/mysqldump --all-databases --add-drop-database -h {source_node} -u{MYSQL_USER} -p{MYSQL_PASS} "
    f"--single-transaction --routines --triggers "
    f"--flush-privileges --hex-blob --default-character-set=utf8 "
    f"--set-gtid-purged=OFF "
    f"| /usr/bin/mysql -u{MYSQL_USER} -p{MYSQL_PASS}\""
)
print("[INFO] Dumping directly from source to target (executed on target)...")
subprocess.run(dump_restore_cmd, shell=True, check=True)

print(f"[SUCCESS] Successfuly restored {node}.")

print(_point_to_master(node, master))