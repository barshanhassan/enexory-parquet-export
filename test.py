import mysql.connector
import time
import subprocess

SLEEP_INTERVAL = 4
node = 'mysql-replica2'
source_node = 'mysql-replica1'
master = 'mysql-master'
MYSQL_USER = "repl"
MYSQL_PASS = "replpass"
DATA_DIR = "/var/lib/mysql"

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

subprocess.run(f"ssh root@{node} 'systemctl stop mysql'", shell=True, check=True)
if DATA_DIR.strip() == "":
    raise Exception

wipe_data_cmd = f"ssh root@{node} 'rm -rf {DATA_DIR}/*'"
subprocess.run(wipe_data_cmd, shell=True, check=True)
print(f"[INFO] Data directory wiped on {node}")

xtrabackup_cmd = (
    f"ssh root@{source_node} "
    f"\"xtrabackup --backup --stream=xbstream --user={MYSQL_USER} --password={MYSQL_PASS} --databases='*'\" "
    f"| ssh root@{node} \"xbstream -x -C {DATA_DIR}\""
)

subprocess.run(xtrabackup_cmd, shell=True, check=True)
print(f"[INFO] Backup streamed from {source_node} to {node}")

prepare_cmd = f"ssh root@{node} 'xtrabackup --prepare --target-dir={DATA_DIR}'"
subprocess.run(prepare_cmd, shell=True, check=True)
print(f"[INFO] Backup prepared on {node}")

perm_cmd = f"ssh root@{node} 'chown -R mysql:mysql {DATA_DIR} && chmod -R 750 {DATA_DIR}'"
subprocess.run(perm_cmd, shell=True, check=True)
print(f"[INFO] Permissions fixed on {node}")

subprocess.run(f"ssh root@{node} 'systemctl start mysql'", shell=True, check=True)
print(f"[INFO] MySQL started on {node}")

print(_point_to_master(node, master))