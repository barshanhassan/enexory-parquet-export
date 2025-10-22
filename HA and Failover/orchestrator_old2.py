DO NOT RUN, NOT FINALIZED

#!/usr/bin/env python3
"""
The `orchestrator.py` script monitors node health, applies quorum rules,
can promote new masters when needed, and updates ProxySQL hostgroups to manage routing.
By automating failover, replica repointing, and ProxySQL configuration,
it aims to keep the cluster operational, reduce the risk of split-brain scenarios,
and support a consistent system state.
"""

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
import argparse

# ---------------- CONFIG ----------------
def parse_args():
    """
    Parses command-line arguments for the orchestrator.py script.

    Returns:
        argparse.Namespace: Parsed arguments containing MySQL, ProxySQL, Email
        and operational parameters.
    """

    parser = argparse.ArgumentParser(description="HA+Failover configuration")

    # Must be provided
    parser.add_argument("--mysql-user", required=True, help="MySQL replication user")
    parser.add_argument("--mysql-pass", required=True, help="MySQL replication password")
    parser.add_argument("--proxysql-admin", required=True, help="ProxySQL admin username")
    parser.add_argument("--proxysql-pass", required=True, help="ProxySQL admin password")
    parser.add_argument("--proxysql-node", required=True, help="ProxySQL node hostname or IP")
    parser.add_argument("--email-to-report-to", required=True, help="Email address for reporting")

    # Optional with defaults
    parser.add_argument("--connection-timeout", type=float, default=5)
    parser.add_argument("--sleep-interval", type=float, default=4)
    parser.add_argument("--small-interval", type=float, default=1)
    parser.add_argument("--rebuild-lag-threshold-hours", type=float, default=6)
    parser.add_argument("--retries", type=int, default=2)
    parser.add_argument("--retry-delay", type=float, default=4)

    return parser.parse_args()

input_args = parse_args()

MYSQL_USER = input_args.mysql_user
MYSQL_PASS = input_args.mysql_pass
PROXYSQL_ADMIN = input_args.proxysql_admin
PROXYSQL_PASS = input_args.proxysql_pass
PROXYSQL_NODE = input_args.proxysql_node
EMAIL_TO_REPORT_TO = input_args.email_to_report_to

CONNECTION_TIMEOUT = input_args.connection_timeout
SLEEP_INTERVAL = input_args.sleep_interval
SMALL_INTERVAL = input_args.small_interval
REBUILD_LAG_THRESHOLD_HOURS = input_args.rebuild_lag_threshold_hours
RETRIES = input_args.retries
RETRY_DELAY = input_args.retry_delay

# Are not set via args
WRITE_HG: int = 10
READ_HG: int = 20
BROKEN_HG: int = 30
NODE_LIST: list[str] = []
NODE_STATUS: dict[str, str] = {}
QUORUM: int = -1

# ---------------- HELPERS ----------------
def keeptrying(interval: float, max_retry_count: int | None = None):
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

@keeptrying(interval=RETRY_DELAY, max_retry_count=RETRIES)
def is_online(host: str, checking_proxysql_admin: bool = False) -> bool:
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
        conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)

    if conn:
        conn.close()
        return True
    return False

def get_gtid(host: str) -> str:
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
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        print(f"[ERROR] Failed to get gtid for {host}: connection failed")
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

@keeptrying(interval=SLEEP_INTERVAL)
def get_proxysql_recognized_nodes() -> tuple[bool, list[str]]:
    """
    Retrieves all unique nodes from ProxySQL assigned to hostgroups 10, 20, or 30.

    Connects to ProxySQL's admin interface (port 6032), queries the mysql_servers
    table for distinct hostnames in the specified hostgroups, and returns them.

    Returns:
        tuple[bool, list[str]]: 
            - True and list of hostnames on success
            - False and empty list on failure
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

def get_node_master(host: str) -> tuple[int, str | None]:
    """
    Determines if a MySQL node is a replica and returns its master host.

    Attempts to connect to the given MySQL host. If the connection succeeds,
    executes `SHOW SLAVE STATUS` to find the master host. 

    Returns a tuple indicating success and the master host:
      - (False, None) if connection fails
      - (True, master_host) if node is a replica
      - (True, None) if node is not a replica

    Args:
        host (str): Hostname or IP address of the MySQL node.

    Returns:
        tuple[bool, str | None]: Connection success flag and master host or None.
    """
    conn = mysql_connect(host, MYSQL_USER, MYSQL_PASS)
    if not conn:
        return False, None
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SHOW SLAVE STATUS")
    result = cast(dict[str, Any] | None, cursor.fetchone())
    conn.close()
    if result and result['Master_Host']:
        return True, result['Master_Host']
    return True, None

