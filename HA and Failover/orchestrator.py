#!/usr/bin/env python3
"""
The `orchestrator.py` script monitors node health, can promote new masters when needed,
and updates ProxySQL hostgroups to manage routing.
By automating failover, replica repointing, and ProxySQL configuration,
it aims to keep the cluster operational and support a consistent system state.
"""

# pylint: disable=broad-exception-caught
# pylint: disable=line-too-long
# pylint: disable=invalid-name
# pylint: disable=bad-indentation
# pylint: disable=global-statement
# pylint: disable=too-many-lines

from datetime import datetime
from functools import wraps
import os
from pathlib import Path
import argparse
import sys
import time
from typing import Any, Literal, cast
import mysql.connector
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract
import keyboard  # type: ignore
import sib_api_v3_sdk # type: ignore
from sib_api_v3_sdk.rest import ApiException # type: ignore

#-------------------------------------------------------------------------------

WRITE_HG: int = 10
READ_HG: int = 20
BROKEN_HG: int = 30
SLEEP_INTERVAL: float = 1
CUSTOM_DB: str = "custom_db"
CUSTOM_TABLE: str = "custom_table"
LOCK_VARIABLE: str = "lock_variable"
CONNECTION_TIMEOUT = 5
ONE_GB: int = 1073741824  # 1024 * 1024 * 1024 bytes
TEN_MB: int = 10485760    # 10 * 1024 * 1024 bytes

LOG_INFO_CODE:int = 0
LOG_WARN_CODE:int = 1
LOG_ERROR_CODE:int = 2

COLOR_BLUE: str = "\033[94m"
COLOR_YELLOW: str = "\033[93m"
COLOR_RED: str = "\033[91m"
COLOR_RESET: str = "\033[0m"

parser = argparse.ArgumentParser(description="HA+Failover configuration")
parser.add_argument("--mysql-user", required=True, help="MySQL replication user")
parser.add_argument("--mysql-pass", required=True, help="MySQL replication password")
parser.add_argument("--proxysql-admin", required=True, help="ProxySQL admin username")
parser.add_argument("--proxysql-pass", required=True, help="ProxySQL admin password")
parser.add_argument("--proxysql-node", required=True, help="ProxySQL node hostname or IP")
parser.add_argument("--email-to", required=True, help="Email address for reporting")
parser.add_argument("--log-file", required=False, help="Log file to save logs to", default=Path("./orchestrator.log"))
parser.add_argument("--ignore-start-warning", action="store_true", help="Automatically ignore script start warning")
parser.add_argument("--email-send-hour", required=False, help="Which hour of the day email report should be sent", type=int, default=12)

input_args = parser.parse_args()

MYSQL_USER = input_args.mysql_user
MYSQL_PASS = input_args.mysql_pass
PROXYSQL_ADMIN = input_args.proxysql_admin
PROXYSQL_PASS = input_args.proxysql_pass
PROXYSQL_NODE = input_args.proxysql_node
email: str = input_args.email_to
log_file: Path = input_args.log_file
IGNORE_START_WARNING: bool = input_args.ignore_start_warning
EMAIL_SEND_HOUR: int = input_args.email_send_hour

last_sent: datetime | None = None
stop: bool = False
user_ignored_start_warning = False

#-------------------------------------------------------------------------------

def keeptrying(interval: float, max_retry_count: int | None):
	"""
	Decorator that retries a function call until it succeeds or reaches a retry limit.

	The wrapped function must return either:
	  - A boolean indicating success (True means success, False means failure), or
	  - A tuple whose first element is a boolean success flag.

	Args:
		interval (float): Seconds to wait between retries.
		max_retry_count (int | None): Maximum number of retries. If None, retries indefinitely.

	Returns:
		The result of the wrapped function on success, or the final result after all retries fail.

	Example:
		@keeptrying(interval=2, max_retry_count=5)
		def unreliable_task():
			success = random.choice([True, False])
			return success

		unreliable_task()
	"""
	assert max_retry_count is None or max_retry_count >= 0
	def decorator(func):
		@wraps(func)
		def wrapper(*args, **kwargs):
			tries = 0
			while max_retry_count is None or tries <= max_retry_count:
				tries += 1
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
			return result
		return wrapper
	return decorator

def log_event(log_code: int, log_text: str) -> None:
	"""
	Logs a formatted event message to both console (with color) and file.

	Args:
		log_code (int): The log level indicator.
			- LOG_INFO_CODE  → Blue "[INFO]"
			- LOG_WARN_CODE  → Yellow "[WARN]"
			- LOG_ERROR_CODE → Red "[ERROR]"
			- Any other value → Default "[LOG]"
		log_text (str): The message text to log.

	Behavior:
		- Before logging, it checks if the log file size has reached 1 GB.
		  If it has, it removes the oldest 10 MB of data from the beginning of the file.
		- Prints a timestamped, color-coded message to the console.
		- Appends the same message (without color) to the log file defined by `log_file`.
	"""
	try:
		if log_file.exists() and log_file.stat().st_size >= ONE_GB:
			# Log to console that truncation is happening
			print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {COLOR_YELLOW}[WARN]{COLOR_RESET} Log file has reached 1GB, truncating the oldest 10MB.")

			with open(log_file, "rb+") as f:
				f.seek(TEN_MB)
				remaining_data = f.read()
				f.seek(0)
				f.write(remaining_data)
				f.truncate()
	except Exception as e:
		print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {COLOR_RED}[ERROR]{COLOR_RESET} Could not truncate log file: {e}")

	current_datetime: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

	if log_code == LOG_INFO_CODE:
		color, label = COLOR_BLUE, "INFO"
	elif log_code == LOG_WARN_CODE:
		color, label = COLOR_YELLOW, "WARN"
	elif log_code == LOG_ERROR_CODE:
		color, label = COLOR_RED, "ERROR"
	else:
		color, label = COLOR_RESET, "LOG"

	print(f"{current_datetime} {color}[{label}]{COLOR_RESET} {log_text}")

	with open(log_file, "a", encoding="utf-8") as f:
		f.write(f"{current_datetime} [{label}] {log_text}" + "\n")

def mysql_connect(host: str, user: str, password: str, port: int = 3306) -> PooledMySQLConnection | MySQLConnectionAbstract | None:
	"""
	Attempts to establish a MySQL connection using the given credentials.

	Args:
		host (str): Hostname or IP address of the MySQL server.
		user (str): Username for authentication.
		password (str): Password for authentication.
		port (int, optional): TCP port number for the MySQL server. Defaults to 3306.

	Returns:
		PooledMySQLConnection | MySQLConnectionAbstract | None:
			A MySQL connection object if successful, or None if the connection fails or times out.
	"""
	try:
		return mysql.connector.connect(host=host, user=user, password=password, port=port, connection_timeout=CONNECTION_TIMEOUT)
	except mysql.connector.Error:
		return None

@keeptrying(interval=3, max_retry_count=3)
def is_online(selected_node: str, checking_proxysql_admin: bool = False) -> bool:
	"""
	Checks if a MySQL host or ProxySQL admin interface is reachable.

	Tries to connect using the appropriate credentials:
	  - Regular MySQL node: MYSQL_USER / MYSQL_PASS (port 3306)
	  - ProxySQL admin: PROXYSQL_ADMIN / PROXYSQL_PASS (port 6032)

	Retries are handled by the @keeptrying decorator.

	Args:
		host (str): Hostname or IP address of the MySQL node (ignored if checking_proxysql_admin is True).
		checking_proxysql_admin (bool): Whether to check ProxySQL admin instead of the given host.

	Returns:
		bool: True if the connection succeeds, False otherwise.
	"""
	if checking_proxysql_admin:
		conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, 6032)
	else:
		conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)

	if conn:
		conn.close()
		return True
	return False

@keeptrying(interval=3, max_retry_count=3)
def get_gtid(selected_node: str) -> tuple[bool, str]:
	"""
	Retrieves the GTID (Global Transaction ID) executed set from a MySQL host.

	Connects to the specified MySQL server and executes 
	'SELECT @@GLOBAL.gtid_executed;' to obtain the GTID set string.
	Returns an empty string if the query fails or no GTID data is available.

	Args:
		host (str): Hostname or IP address of the MySQL node.

	Returns:
		str: GTID executed set as a string, or an empty string on failure.
	"""
	conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
	if not conn:
		print(f"[ERROR] Failed to get gtid for {selected_node}: connection failed")
		return False, ""
	try:
		cursor = conn.cursor()
		cursor.execute("SELECT @@GLOBAL.gtid_executed;")
		gtid = cast(tuple[Any, ...] | None, cursor.fetchone())
		if not gtid or gtid[0] is None:
			return True, ""
		return True, str(gtid[0])
	except Exception as e:
		print(f"[ERROR] Failed to get gtid for {selected_node} due to an error: {e}")
		conn.close()
		return False, ""
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=None)
def get_master_from_proxysql() -> tuple[bool, str | None]:
	"""Gets the current writer from the ProxySQL mysql_servers table."""
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False, None
	try:
		cursor = conn.cursor()
		cursor.execute("SELECT hostname FROM mysql_servers WHERE hostgroup_id = %s;", (WRITE_HG,))
		result = cast(list[tuple[str]], cursor.fetchall())
		if len(result) == 1:
			return True, result[0][0]
		if len(result) > 1:
			log_event(LOG_ERROR_CODE, f"Split-brain detected in ProxySQL! Multiple writers: {result}")
			return False, None
		return True, None # No master configured
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to get master from ProxySQL: {e}")
		return False, None
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=None)
def get_proxysql_state_from_nodes_in_host_groups(host_groups: list[int]) -> tuple[bool, dict[str, Literal["online", "offline", "broken"]]]:
	"""
	Queries ProxySQL for all nodes and their statuses across the specified hostgroups.

	Connects to ProxySQL's admin interface (port 6032) and retrieves each node's hostname,
	hostgroup, and status from the `mysql_servers` table. For each node, determines a final
	unified status using this hierarchy:

		- If the node appears in hostgroup 30 -> "broken"
		- Else if all entries have status = "ONLINE" -> "online"
		- Else -> "offline"

	Returns:
		tuple[bool, dict[str, Literal["online", "offline", "broken"]]]:
			- (True, node_status_dict) on success
			- (False, {}) on failure
	"""
	log_event(LOG_INFO_CODE, "Querying ProxySQL for recognized nodes...")
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		log_event(LOG_ERROR_CODE, "Cannot connect to ProxySQL")
		return False, {}

	try:
		cursor = conn.cursor()
		placeholders = ", ".join(["%s"] * len(host_groups))
		query = f"""
			SELECT hostname, hostgroup_id, status
			FROM mysql_servers
			WHERE hostgroup_id IN ({placeholders});
		"""
		cursor.execute(query, host_groups)

		records = cursor.fetchall()
		node_entries: dict[str, list[tuple[int, str]]] = {}

		for hostname, hostgroup_id, status in records:
			hostname = str(hostname)
			hostgroup_id = cast(int, hostgroup_id)
			status = str(status).upper()
			node_entries.setdefault(hostname, []).append((hostgroup_id, status))

		node_status: dict[str, Literal["online", "offline", "broken"]] = {}

		for hostname, entries in node_entries.items():
			# Hostgroup 30 overrides everything
			if any(hg == 30 for hg, _ in entries):
				node_status[hostname] = "broken"
			# If all statuses are ONLINE → online, else offline
			elif all(status == "ONLINE" for _, status in entries):
				node_status[hostname] = "online"
			else:
				node_status[hostname] = "offline"

		log_event(LOG_INFO_CODE, f"Node statuses: {node_status}")
		return True, node_status

	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to query nodes from ProxySQL: {e}")
		return False, {}

	finally:
		if conn:
			conn.close()

@keeptrying(interval=3, max_retry_count= 3)
def get_proxysql_nodes(host_groups: list[int]) -> tuple[bool, list[str]]:
	"""
	Retrieves all unique node hostnames from ProxySQL for the specified hostgroups.

	Connects to ProxySQL's admin interface (port 6032) and queries the `mysql_servers`
	table to collect all hostnames belonging to the given hostgroups. Duplicate hostnames
	are removed before returning.

	Returns:
		tuple[bool, list[str]]:
			- (True, list_of_nodes) on success
			- (False, []) on failure
	"""
	log_event(LOG_INFO_CODE, "Querying ProxySQL for recognized nodes...")
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		log_event(LOG_ERROR_CODE, "Cannot connect to ProxySQL")
		return False, []

	try:
		cursor = conn.cursor()
		placeholders = ", ".join(["%s"] * len(host_groups))
		query = f"""
			SELECT DISTINCT hostname
			FROM mysql_servers
			WHERE hostgroup_id IN ({placeholders});
		"""
		cursor.execute(query, host_groups)

		nodes: list[str] = [cast(str, item[0]) for item in cursor.fetchall()] # type: ignore[index]
		unique_nodes = sorted(set(nodes))

		log_event(LOG_INFO_CODE, f"Found nodes in ProxySQL: {unique_nodes}")
		return True, unique_nodes

	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to query nodes from ProxySQL: {e}")
		return False, []

	finally:
		if conn:
			conn.close()

def stop_program() -> None:
	"""
	Handles graceful program termination on user request.

	Triggered when the user presses the 'q' hotkey.  
	Logs a message indicating a safe shutdown and sets the global `stop` flag to True,  
	which signals the main loop to exit cleanly after completing any ongoing work.
	"""
	global stop
	log_event(LOG_INFO_CODE, "Safely quitting the program due to user action, this may take a while")
	stop = True

@keeptrying(interval=3, max_retry_count=3)
def stop_replication(selected_node: str) -> bool:
	"""
	Connects to a node, stops replication, and resets its slave status.
	This is critical when promoting a new master.
	"""
	log_event(LOG_INFO_CODE, f"Stopping and resetting slave status on {selected_node} for promotion.")
	conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
	if not conn:
		log_event(LOG_ERROR_CODE, f"Cannot connect to {selected_node} to stop replication.")
		return False
	try:
		cursor = conn.cursor()
		cursor.execute("STOP SLAVE;")
		cursor.execute("RESET SLAVE ALL;")
		conn.commit()
		log_event(LOG_INFO_CODE, f"Successfully reset slave status on {selected_node}.")
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to stop/reset slave on {selected_node}: {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def set_proxysql_master(selected_node: str) -> bool:
	"""
	Updates ProxySQL to set the new master in the writer hostgroup (WRITE_HG).
	It first removes all existing entries from the write group to prevent split-brain.
	"""
	log_event(LOG_INFO_CODE, f"Updating ProxySQL: setting {selected_node} as the new writer.")
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		log_event(LOG_ERROR_CODE, "Cannot connect to ProxySQL to set new master.")
		return False
	try:
		cursor = conn.cursor()
		cursor.execute("DELETE FROM mysql_servers WHERE hostgroup_id = %s;", (WRITE_HG,))
		cursor.execute("""
			INSERT INTO mysql_servers (hostgroup_id, hostname, port)
			VALUES (%s, %s, %s);
		""", (WRITE_HG, selected_node, 3306))
		cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
		cursor.execute("SAVE MYSQL SERVERS TO DISK;")
		conn.commit()
		log_event(LOG_INFO_CODE, f"ProxySQL write hostgroup successfully updated to {selected_node}.")
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to execute ProxySQL update for new master: {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def _move_node_to_broken_hg(selected_node: str) -> bool:
	"""Internal function to move a node exclusively to the broken hostgroup."""
	log_event(LOG_WARN_CODE, f"Moving node {selected_node} to the broken hostgroup ({BROKEN_HG}).")
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False
	try:
		cursor = conn.cursor()
		# Remove all other entries for this host
		cursor.execute("DELETE FROM mysql_servers WHERE hostname = %s;", (selected_node,))
		# Add it to the broken hostgroup
		cursor.execute("""
			INSERT INTO mysql_servers (hostgroup_id, hostname, port)
			VALUES (%s, %s, %s);
		""", (BROKEN_HG, selected_node, 3306))
		cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
		cursor.execute("SAVE MYSQL SERVERS TO DISK;")
		conn.commit()
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to move {selected_node} to broken hostgroup: {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def set_proxysql_status(selected_node: str, status: Literal["online", "offline"]) -> bool:
	"""Sets the status of a node in ProxySQL to ONLINE or OFFLINE_HARD."""
	proxysql_status = 'ONLINE' if status == "online" else 'OFFLINE_HARD'
	log_event(LOG_INFO_CODE, f"Setting ProxySQL status for {selected_node} to {proxysql_status}.")

	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False
	try:
		cursor = conn.cursor()
		cursor.execute("""
			UPDATE mysql_servers SET status=%s WHERE hostname=%s
		""", (proxysql_status, selected_node))
		cursor.execute("LOAD MYSQL SERVERS TO RUNTIME;")
		cursor.execute("SAVE MYSQL SERVERS TO DISK;")
		conn.commit()
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to set ProxySQL status for {selected_node}: {e}")
		return False
	finally:
		conn.close()

def set_proxysql_node(selected_node: str, status: Literal["online", "offline", "broken"]) -> bool:
	"""
	General-purpose function to manage a node's state in ProxySQL.
	- 'broken': Moves the node to the broken hostgroup.
	- 'online'/'offline': Sets the node's status accordingly.
	"""
	if status == 'broken':
		return _move_node_to_broken_hg(selected_node)
	return set_proxysql_status(selected_node, status)

@keeptrying(interval=3, max_retry_count=3)
def set_replication_source(master: str, selected_node: str) -> tuple[bool, int]:
	"""
	Ensures a node is a healthy replica of the given master.

	This function is idempotent. It first checks the replica's status.
	If the replica is already configured correctly and running, it does nothing.
	Otherwise, it intervenes to point the replica to the new master.

	Returns:
		A tuple (success, status_code) where status_code is:
		- 0: Success (either already healthy or successfully re-pointed).
		- 1: Connection error or transient issue.
		- -1: Persistent replication error (SQL thread failed after re-point).
	"""
	log_event(LOG_INFO_CODE, f"Ensuring {selected_node} is replicating correctly from {master}.")
	conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
	if not conn:
		log_event(LOG_WARN_CODE, f"Cannot connect to {selected_node} to set replication source.")
		return False, 1
	try:
		cursor = conn.cursor(dictionary=True)

		cursor.execute("SHOW SLAVE STATUS")
		status = cast(dict[str, Any] | None, cursor.fetchone())

		if (status and
				status.get('Master_Host') == master and
				status.get('Slave_IO_Running') == 'Yes' and
				status.get('Slave_SQL_Running') == 'Yes'):

			log_event(LOG_INFO_CODE, f"Node {selected_node} is already a healthy replica of {master}. No action needed.")
			return True, 0

		log_event(LOG_INFO_CODE, f"Node {selected_node} is not healthy or correct. Intervening to re-point.")

		cursor.execute("STOP SLAVE;")
		change_master_query = """
			CHANGE MASTER TO
			MASTER_HOST=%s,
			MASTER_USER=%s,
			MASTER_PASSWORD=%s,
			MASTER_AUTO_POSITION=1
		"""
		cursor.execute(change_master_query, (master, MYSQL_USER, MYSQL_PASS))
		cursor.execute("START SLAVE;")

		time.sleep(2) # Give replication a moment to start and report status

		cursor.execute("SHOW SLAVE STATUS")
		new_status = cast(dict[str, Any] | None, cursor.fetchone())

		if new_status and new_status.get('Slave_SQL_Running') == 'Yes' and new_status.get('Slave_IO_Running') == 'Yes':
			log_event(LOG_INFO_CODE, f"Successfully pointed {selected_node} to {master}.")
			return True, 0

		if new_status:
			log_event(LOG_ERROR_CODE, f"Replication error on {selected_node} after re-point. SQL Running: {new_status.get('Slave_SQL_Running')}, IO Running: {new_status.get('Slave_IO_Running')}. Last error: {new_status.get('Last_Error')}")
		return True, -1

	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to execute CHANGE MASTER on {selected_node}: {e}")
		return False, 1
	finally:
		conn.close()

def choose_new_master(current_master: str, all_nodes: dict[str, Literal["online", "offline", "broken"]]) -> tuple[bool, str | None]:
	"""
	Chooses the best replica to promote as the new master based on GTID sets.
	It selects the node that contains all transactions from all other replicas.
	"""
	log_event(LOG_INFO_CODE, "Choosing a new master based on GTID comparison.")
	contenders = {node for node, status in all_nodes.items() if status != 'broken' and node != current_master and is_online(node)}

	if not contenders:
		log_event(LOG_ERROR_CODE, "No suitable replicas available to promote.")
		return False, None

	contender_gtids: dict[str, str] = {}
	for _node in contenders:
		_success, gtid = get_gtid(_node)
		if _success and gtid:
			contender_gtids[_node] = gtid

	if not contender_gtids:
		log_event(LOG_ERROR_CODE, "Online replicas found, but could not retrieve GTIDs from any of them.")
		return False, None

	# Use any contender to connect and run GTID comparisons
	check_node = next(iter(contender_gtids.keys()))
	conn = mysql_connect(check_node, MYSQL_USER, MYSQL_PASS)
	if not conn:
		log_event(LOG_WARN_CODE, f"Cannot connect to {check_node} to compare GTIDs. Cannot safely choose a new master.")
		return False, None

	try:
		cursor = conn.cursor()
		for node1, gtid1 in contender_gtids.items():
			is_most_advanced = True
			for node2, gtid2 in contender_gtids.items():
				if node1 == node2:
					continue
				# Check if gtid2 is a SUBSET of gtid1. If not, node1 is not the most advanced.
				cursor.execute("SELECT GTID_SUBSET(%s, %s);", (gtid2, gtid1))
				result = cast(tuple[int] | None, cursor.fetchone())
				if result and result[0] == 0:
					is_most_advanced = False
					break
			if is_most_advanced:
				log_event(LOG_INFO_CODE, f"Selected {node1} as the new master. It has the most advanced GTID set.")
				return True, node1
	finally:
		conn.close()

	log_event(LOG_ERROR_CODE, "Could not determine a single most advanced replica. Data may have diverged. Manual intervention required.")
	return False, None

@keeptrying(interval=3, max_retry_count=3)
def _init_custom_db() -> bool:
	"""Initializes the custom database table in ProxySQL's SQLite DB.

	This function creates the table defined by `CUSTOM_TABLE` if it does not
	already exist. This table is used to store persistent script variables,
	such as the lock to prevent multiple instances from running.

	Returns:
		bool: True if the table was created or already exists, False on failure.
	"""
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		log_event(LOG_ERROR_CODE, "Cannot connect to ProxySQL to initialize custom DB.")
		return False
	try:
		cursor = conn.cursor()
		cursor.execute(f"CREATE TABLE IF NOT EXISTS {CUSTOM_TABLE} (variable TEXT PRIMARY KEY, value TEXT);")
		conn.commit()
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to initialize custom DB in ProxySQL: {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def get_custom_value_from_proxysql_db(variable: str, retry: bool = True) -> tuple[bool, Any]:
	"""Retrieves a persistent variable from the custom table in ProxySQL.

	If the table does not exist, it will attempt to initialize it once and
	retry the query.

	Args:
		variable (str): The name of the variable (key) to retrieve.

	Returns:
		tuple[bool, Any]: A tuple containing a success flag and the retrieved
		value, or None if the variable does not exist or an error occurs.
	"""
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False, None
	try:
		cursor = conn.cursor()
		cursor.execute(f"SELECT value FROM {CUSTOM_TABLE} WHERE variable = %s;", (variable,))
		result = cast(tuple[Any] | None, cursor.fetchone())
		return True, result[0] if result else None
	except Exception:
		# Table might not exist yet, try to create it and retry once.
		if retry and _init_custom_db():
			return get_custom_value_from_proxysql_db(variable, retry=False)
		return False, None
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def upsert_custom_value_in_proxysql_db(variable: str, value: Any) -> bool:
	"""Inserts or updates a persistent variable in the custom table in ProxySQL.

	Uses SQLite's `INSERT OR REPLACE` functionality to ensure the variable
	is set, regardless of whether it existed previously.

	Args:
		variable (str): The name of the variable (key) to set.
		value (Any): The value to store. It will be converted to a string.

	Returns:
		bool: True on success, False on failure.
	"""
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False
	try:
		cursor = conn.cursor()
		# Using INSERT OR REPLACE for simplicity (SQLite syntax used by ProxySQL)
		cursor.execute(f"INSERT OR REPLACE INTO {CUSTOM_TABLE} (variable, value) VALUES (%s, %s);", (variable, str(value)))
		conn.commit()
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to upsert custom value '{variable}': {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def delete_custom_value_from_proxysql_db(variable: str) -> bool:
	"""Deletes a persistent variable from the custom table in ProxySQL.

	Args:
		variable (str): The name of the variable (key) to delete.

	Returns:
		bool: True if the variable was deleted or did not exist, False on failure.
	"""
	conn = mysql_connect(PROXYSQL_NODE, PROXYSQL_ADMIN, PROXYSQL_PASS, port=6032)
	if not conn:
		return False
	try:
		cursor = conn.cursor()
		cursor.execute(f"DELETE FROM {CUSTOM_TABLE} WHERE variable = %s;", (variable,))
		conn.commit()
		return True
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Failed to delete custom value '{variable}': {e}")
		return False
	finally:
		conn.close()

@keeptrying(interval=3, max_retry_count=3)
def get_replication_lag(selected_node: str) -> tuple[bool, str]:
	"""Queries a node for its replication lag. Returns a formatted string."""
	conn = mysql_connect(selected_node, MYSQL_USER, MYSQL_PASS)
	if not conn:
		return False, "Connection Failed"
	try:
		cursor = conn.cursor(dictionary=True)
		cursor.execute("SHOW SLAVE STATUS")
		status = cast(dict[str, Any] | None, cursor.fetchone())
		if not status:
			return True, "Not a replica"
		lag = status.get('Seconds_Behind_Master')
		if lag is None:
			return True, "Lag is NULL (Not running?)"
		return True, f"{lag} seconds"
	except Exception as e:
		return False, f"Error: {e}"
	finally:
		conn.close()

def generate_script_started_email_text(ignored_start_warning: bool) -> str:
	"""Generates the HTML email content for when the script starts.

	Includes the start time and a prominent warning if the script was started
	in a dangerous state (i.e., the lock variable was already set).

	Args:
		ignored_start_warning (bool): If True, a warning message is included.

	Returns:
		str: A fully formatted string containing the email subject and HTML body.
	"""
	current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	subject = "Orchestrator Script Started"
	warning_html = ""
	if ignored_start_warning:
		subject = "WARNING: Orchestrator Script Started Dangerously"
		warning_html = """
		<p style="color: red; border: 1px solid red; padding: 10px;">
			<strong>Warning:</strong> The script was started in a potentially unsafe state (lock was present). 
			This action was manually approved by the user.
		</p>
		"""
	body = f"""
	<html><body>
		<h2>Orchestrator Status: Started</h2>
		<p>The orchestrator script was started at <strong>{current_datetime}</strong>.</p>
		{warning_html}
		<p>It will now monitor the MySQL cluster and ProxySQL configuration.</p>
	</body></html>
	"""
	return f"Subject: {subject}\n\n{body}"

def generate_daily_report_email_text(current_master: str, all_nodes: dict[str, Literal["online", "offline", "broken"]]) -> str:
	"""Generates the daily HTML email report of the cluster's health.

	The report includes the current master, the status of all nodes, and the
	replication lag for any online replicas.

	Args:
		current_master (str): The hostname of the current master node.
		all_nodes (dict[str, Literal["online", "offline", "broken"]]): A
			dictionary mapping node hostnames to their current status.

	Returns:
		str: A fully formatted string containing the email subject and HTML body.
	"""
	current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	subject = "Orchestrator Daily Report"

	rows = ""
	for _node, status in sorted(all_nodes.items()):
		lag_info_str = "N/A"
		if status == 'online' and _node != current_master:
			_success, lag_info_str = get_replication_lag(_node)
			if not _success:
				lag_info_str = f"<i>Failed to get lag: {lag_info_str}</i>" # Italicize errors

		style = ""
		if status == 'broken':
			style = ' style="background-color: #ffdddd;"'
		elif status == 'offline':
			style = ' style="background-color: #ffffcc;"'

		rows += f"""
		<tr{style}>
			<td>{'<strong>' + _node + ' (MASTER)</strong>' if _node == current_master else _node}</td>
			<td>{status.upper()}</td>
			<td>{lag_info_str}</td>
		</tr>
		"""

	body = f"""
	<html><body>
		<h2>Orchestrator Daily Health Report - {current_datetime}</h2>
		<p>Current Master: <strong>{current_master}</strong></p>
		<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
			<thead>
				<tr style="background-color: #f2f2f2;">
					<th>Node</th>
					<th>Status</th>
					<th>Replication Lag</th>
				</tr>
			</thead>
			<tbody>
				{rows}
			</tbody>
		</table>
	</body></html>
	"""
	return f"Subject: {subject}\n\n{body}"

def generate_script_stopped_safely_email_text() -> str:
	"""Generates the HTML email content for a safe script shutdown.

	Includes the time of shutdown and confirms that it was user-initiated.

	Returns:
		str: A fully formatted string containing the email subject and HTML body.
	"""
	current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	subject = "Orchestrator Script Stopped Safely"
	body = f"""
	<html><body>
		<h2>Orchestrator Status: Stopped</h2>
		<p>The orchestrator script was stopped safely at <strong>{current_datetime}</strong>.</p>
		<p>The shutdown was initiated by a user ('q' key) and the cleanup process completed successfully.</p>
	</body></html>
	"""
	return f"Subject: {subject}\n\n{body}"

def generate_topology_change_email_text(old_master: str, new_master: str, old_all_nodes: dict[str, Literal["online", "offline", "broken"]], new_all_nodes: dict[str, Literal["online", "offline", "broken"]]) -> str:
	"""Generates an alert email detailing a change in the cluster topology.

	This email is triggered by a master failover or any change in a node's
	status (e.g., from 'online' to 'offline').

	Args:
		old_master (str): The hostname of the master before the change.
		new_master (str): The hostname of the master after the change.
		old_all_nodes (dict): The dictionary of node statuses before the change.
		new_all_nodes (dict): The dictionary of node statuses after the change.

	Returns:
		str: A fully formatted string containing the email subject and HTML body.
	"""
	current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
	subject = "ALERT: MySQL Topology Change Detected"

	changes = ""
	if old_master != new_master:
		changes += f'<li><strong>Master Failover:</strong> Old master <code>{old_master}</code> is down. New master is now <strong><code>{new_master}</code></strong>.</li>'

	all__nodes = sorted(set(old_all_nodes.keys()) | set(new_all_nodes.keys()))
	for _node in all__nodes:
		old_status = old_all_nodes.get(_node, "N/A")
		new_status = new_all_nodes.get(_node, "N/A")
		if old_status != new_status:
			changes += f'<li><strong>Node Status Change:</strong> <code>{_node}</code> changed from <code>{old_status.upper()}</code> to <code>{new_status.upper()}</code>.</li>'

	body = f"""
	<html><body>
		<h2 style="color: #cc0000;">ALERT: MySQL Topology Change Detected at {current_datetime}</h2>
		<p>The orchestrator has automatically reconfigured the cluster. Details:</p>
		<ul>
			{changes if changes else "<li>No specific changes detected, but state refresh was triggered.</li>"}
		</ul>
		<p>Please review the system status to make sure everything is operating as expected.</p>
	</body></html>
	"""
	return f"Subject: {subject}\n\n{body}"

def send_email(email_text: str) -> None:
	"""
	Sends an email using the Brevo (Sendinblue) API.
	Requires BREVO_API_KEY and SENDER_EMAIL environment variables.
	Falls back to printing to console if not configured.
	"""
	api_key = os.environ.get("BREVO_API_KEY")
	sender_email = os.environ.get("SENDER_EMAIL")

	parts = email_text.split('\n\n', 1)
	if len(parts) != 2:
		log_event(LOG_ERROR_CODE, "Email text format invalid")
		# Fallback to console if format is invalid
		print("--- EMAIL START ---")
		print(email_text)
		print("--- EMAIL END ---")
		return

	subject = parts[0].replace("Subject: ", "").strip()
	html_content = parts[1]

	if not api_key or not sender_email:
		log_event(LOG_WARN_CODE, "BREVO_API_KEY or SENDER_EMAIL not set. Printing email to console.")
		print("--- EMAIL START ---")
		print(email_text)
		print("--- EMAIL END ---")
		return

	# Configure the API client
	configuration = sib_api_v3_sdk.Configuration()
	configuration.api_key['api-key'] = api_key
	api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

	sender = sib_api_v3_sdk.SendSmtpEmailSender(name="Orchestrator Alert", email=sender_email)
	to = [sib_api_v3_sdk.SendSmtpEmailTo(email=email)]

	send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
		to=to,
		sender=sender,
		subject=subject,
		html_content=html_content
	)

	try:
		api_response = api_instance.send_transac_email(send_smtp_email)
		msg_id = getattr(api_response, 'message_id', 'unknown')
		log_event(LOG_INFO_CODE, f"Successfully sent email notification to {email}. Message ID: {msg_id}")
	except ApiException as e:
		log_event(LOG_ERROR_CODE, f"An exception occurred while trying to send email via Brevo: {e.body}")
	except Exception as e:
		log_event(LOG_ERROR_CODE, f"Unexpected error sending email: {e}")

#-------------------------------------------------------------------------------

log_file.parent.mkdir(parents=True, exist_ok=True)

# Log function logs to file and screen
log_event(LOG_INFO_CODE, "Script Started")

_init_custom_db()

# Check for an abrupt exit and warn user about it.
success, lock_value = get_custom_value_from_proxysql_db(LOCK_VARIABLE)

if not success:
	log_event(LOG_ERROR_CODE, "Please configure your environment to support usage of ProxySQL's internal DB to support custom variables.")
	log_event(LOG_INFO_CODE, "Performing an early exit of script.")
	sys.exit(1001)

if lock_value is None:
	log_event(LOG_INFO_CODE, "Script was started.")

	if not upsert_custom_value_in_proxysql_db(LOCK_VARIABLE, 1):
		log_event(LOG_WARN_CODE, "Could not set lock variable from ProxySQL internal DB. Will not be able to detect if the script is running dangerously.")
else:
	log_event(LOG_INFO_CODE, "Script was started dangerously.")

	if IGNORE_START_WARNING:
		log_event(LOG_INFO_CODE, "Script start warning will be ingored and script will continue dangerously.")
	else:
		user_response = input("They script was started while another instance runs or after it was abruptly stopped.\n"
							+ "Only continue after confirming healthy topology, correct ProxySQL configuration and lack of a duplicate process.\n"
							+ "Continue? (y/n)")

		if user_response.lower() == 'y':
			log_event(LOG_INFO_CODE, "User chose to run script anyways.")
			user_ignored_start_warning = True
		else:
			log_event(LOG_INFO_CODE, "User chose to stop script to avoid running it dangerously.")
			sys.exit()

send_email(generate_script_started_email_text(user_ignored_start_warning))

success_master, CURRENT_MASTER = get_master_from_proxysql()
success_nodes, ALL_NODES = get_proxysql_state_from_nodes_in_host_groups([WRITE_HG, READ_HG, BROKEN_HG])

log_event(LOG_INFO_CODE, f"Built initial state using ProxySQL. Master: {CURRENT_MASTER}. Nodes:" + "".join(f"\n{k}: {v}" for k, v in ALL_NODES.items()))

print("[Tip] Press \'q\' to safely quit the program")
keyboard.add_hotkey("q", stop_program)

while not stop:
	now = datetime.now()
	if now.hour == EMAIL_SEND_HOUR and (last_sent is None or last_sent.date() != now.date()):
		send_email(generate_daily_report_email_text(CURRENT_MASTER, ALL_NODES))
		last_sent = now

	OLD_CURRENT_MASTER, OLD_ALL_NODES = CURRENT_MASTER, ALL_NODES.copy()

	success, recognized_nodes = get_proxysql_nodes([WRITE_HG, READ_HG, BROKEN_HG])

	if not success:
		log_event(LOG_WARN_CODE, "Could not get recognized nodes from ProxySQL this cycle. Skipping.")
		time.sleep(SLEEP_INTERVAL)
		continue

	for node in list(ALL_NODES.keys()):
		if node not in recognized_nodes:
			log_event(LOG_INFO_CODE, f"Node {node} no longer in ProxySQL, removing from internal state.")
			del ALL_NODES[node]

	if CURRENT_MASTER not in recognized_nodes or is_online(CURRENT_MASTER) is False:
		if CURRENT_MASTER not in recognized_nodes:
			del ALL_NODES[CURRENT_MASTER]
		success, NEW_MASTER = choose_new_master(CURRENT_MASTER, ALL_NODES)
		if success and NEW_MASTER and set_proxysql_master(NEW_MASTER):
			stop_replication(NEW_MASTER)
			CURRENT_MASTER = NEW_MASTER
			set_proxysql_node(CURRENT_MASTER, "online")
			ALL_NODES[CURRENT_MASTER] = "online"

	for node in recognized_nodes:
		if node != CURRENT_MASTER:
			if ALL_NODES.get(node) != 'broken' and is_online(node):
				success, res_code = set_replication_source(CURRENT_MASTER, node)

				if not success:
					set_proxysql_node(node, "offline")
					ALL_NODES[node] = "offline"
				elif res_code == -1:
					set_proxysql_node(node, "broken")
					ALL_NODES[node] = "broken"
				elif res_code == 0:
					set_proxysql_node(node, "online")
					ALL_NODES[node] = "online"
			else:
				if ALL_NODES.get(node) != "broken":
					set_proxysql_node(node, "offline")
					ALL_NODES[node] = "offline"

	if CURRENT_MASTER != OLD_CURRENT_MASTER or OLD_ALL_NODES != ALL_NODES:
		send_email(generate_topology_change_email_text(OLD_CURRENT_MASTER, CURRENT_MASTER, OLD_ALL_NODES, ALL_NODES))

	time.sleep(SLEEP_INTERVAL)

if not delete_custom_value_from_proxysql_db(LOCK_VARIABLE):
	log_event(LOG_WARN_CODE, "Could not delete lock variable from ProxySQL internal DB. This may cause the script to report a false abrupt-stop on the next run.")

send_email(generate_script_stopped_safely_email_text())

log_event(LOG_INFO_CODE, "Script Stopped")
