#!/usr/bin/env python3
"""
Monitors MySQL replication status for a list of nodes defined in the config file.

This script directly queries each node to check its health, replication status, and lag.
It sends daily summary emails and immediate anomaly alerts if the state of any
node changes (e.g., goes offline, exceeds lag threshold, or replication stops).
"""

# pylint: disable=broad-exception-caught,invalid-name

import time
import datetime
from typing import Any, Dict, Optional, List

import mysql.connector
import sib_api_v3_sdk # type: ignore
from sib_api_v3_sdk.rest import ApiException # type: ignore

# All configuration is now loaded from the 'monitor_config.py' file.
import watcher_config as cfg

# --- Color Codes for Console Output ---
COLOR_GREEN: str = "\033[92m"
COLOR_YELLOW: str = "\033[93m"
COLOR_RED: str = "\033[91m"
COLOR_RESET: str = "\033[0m"

# --- State Tracking ---
previous_node_statuses: Dict[str, Dict[str, Any]] = {}
last_daily_report_sent_date: Optional[datetime.date] = None

def get_node_status(node: Dict[str, str]) -> Dict[str, Any]:
    """
    Connects to a single MySQL node and returns its technical status.
    It no longer tries to guess if the node is a master.

    Returns a dictionary containing:
      - is_online (bool): True if a connection could be established.
      - replication_status (dict | None): Detailed status from 'SHOW SLAVE STATUS'.
    """
    status: Dict[str, Any] = {
        "is_online": False,
        "is_master": False,  # This will be set in the main loop based on config.
        "replication_status": None,
    }

    conn = None
    try:
        conn = mysql.connector.connect(
            host=node['ip'],
            user=node['user'],
            password=node['pass'],
            connection_timeout=cfg.CONNECTION_TIMEOUT
        )

        if conn:
            status["is_online"] = True
        cursor = conn.cursor(dictionary=True)

        cursor.execute("SHOW SLAVE STATUS")
        slave_status = cursor.fetchone()
        status["replication_status"] = slave_status

    except mysql.connector.Error as err:
        print(f"{COLOR_YELLOW}[WARN]{COLOR_RESET} Could not connect to {node['ip']}: {err}")
    except Exception as e:
        print(f"{COLOR_RED}[ERROR]{COLOR_RESET} An unexpected error occurred while checking {node['ip']}: {e}")
    finally:
        if conn and conn.is_connected(): # Check that conn exists AND is open
            conn.close()

    return status

def format_status_for_email(node_ip: str, status: Dict[str, Any]) -> str:
    """Formats the status of a single node into an HTML table row."""
    if not status['is_online']:
        return f'<tr style="background-color: #ffdddd;"><td>{node_ip}</td><td colspan="4"><strong>OFFLINE</strong></td></tr>'

    # This check now uses the explicitly set 'is_master' flag.
    if status['is_master']:
        return f'<tr style="background-color: #e6f7ff;"><td><strong>{node_ip} (MASTER)</strong></td><td colspan="4">N/A - Master Node</td></tr>'

    repl_status = status.get("replication_status")
    if not repl_status:
        # This will now correctly catch misconfigured replicas or permission issues.
        return f'<tr style="background-color: #ffdddd;"><td>{node_ip}</td><td colspan="4"><strong>REPLICATION NOT RUNNING OR CONFIGURED</strong></td></tr>'


    lag = repl_status.get('Seconds_Behind_Master')
    io_running = repl_status.get('Slave_IO_Running', 'N/A')
    sql_running = repl_status.get('Slave_SQL_Running', 'N/A')
    last_error = repl_status.get('Last_Error', 'None')

    row_style = ""
    is_healthy = (
        lag is not None and lag <= cfg.LAG_THRESHOLD_SECONDS and
        io_running == 'Yes' and sql_running == 'Yes'
    )
    if not is_healthy:
        row_style = ' style="background-color: #fffacd;"'

    lag_display = "NULL" if lag is None else f"{lag}s"

    return (
        f'<tr{row_style}>'
        f'<td>{node_ip}</td>'
        f'<td>{lag_display}</td>'
        f'<td>{io_running}</td>'
        f'<td>{sql_running}</td>'
        f'<td>{last_error}</td>'
        f'</tr>'
    )

def generate_report_html(all_statuses: Dict[str, Dict[str, Any]], title: str, intro_text: str) -> str:
    """Generates a complete HTML email body from the status of all nodes."""
    rows_html = ""
    for ip, status in sorted(all_statuses.items()):
        rows_html += format_status_for_email(ip, status)

    return f"""
    <html>
    <head>
        <style>
            body {{ font-family: sans-serif; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #dddddd; text-align: left; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h2>{title}</h2>
        <p>{intro_text}</p>
        <table>
            <thead>
                <tr>
                    <th>Node IP</th>
                    <th>Replication Lag</th>
                    <th>IO Thread Running</th>
                    <th>SQL Thread Running</th>
                    <th>Last Error</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
        <p><small>Report generated at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC</small></p>
    </body>
    </html>
    """

def send_email(subject: str, html_content: str, recipient_emails: List[str]) -> None:
    """Sends an email using the Brevo (Sendinblue) API to a list of recipients."""
    # Using cfg to get API key and sender email
    api_key = cfg.BREVO_API_KEY
    sender_email = cfg.SENDER_EMAIL

    # Filter out any empty strings from the recipient list
    valid_recipients = [email for email in recipient_emails if email]

    if not api_key or not sender_email or not valid_recipients:
        print(f"{COLOR_YELLOW}[WARN]{COLOR_RESET} Email settings are incomplete or no recipients are defined. Printing email to console instead.")
        print(f"--- EMAIL: {subject} ---\n{html_content}\n--- END EMAIL ---")
        return

    configuration = sib_api_v3_sdk.Configuration()
    configuration.api_key['api-key'] = api_key
    api_instance = sib_api_v3_sdk.TransactionalEmailsApi(sib_api_v3_sdk.ApiClient(configuration))

    sender = sib_api_v3_sdk.SendSmtpEmailSender(name="MySQL Monitor", email=sender_email)
    
    # Create a list of recipient objects for the API call
    to = [sib_api_v3_sdk.SendSmtpEmailTo(email=recipient) for recipient in valid_recipients]

    send_smtp_email = sib_api_v3_sdk.SendSmtpEmail(
        to=to, sender=sender, subject=subject, html_content=html_content
    )

    try:
        api_instance.send_transac_email(send_smtp_email)
        print(f"{COLOR_GREEN}[INFO]{COLOR_RESET} Successfully sent email notification to {', '.join(valid_recipients)}.")
    except ApiException as e:
        print(f"{COLOR_RED}[ERROR]{COLOR_RESET} Exception when calling Brevo API: {e.body}")

def get_anomaly_summary(current_status: Dict[str, Any], prev_status: Dict[str, Any]) -> Optional[str]:
    """Compares current and previous status to determine if a new anomaly occurred."""
    if current_status['is_online'] != prev_status.get('is_online', True):
        return "is now OFFLINE" if not current_status['is_online'] else "is back ONLINE"

    if not current_status['is_online'] or current_status['is_master']:
        return None

    current_repl = current_status.get("replication_status") or {}
    prev_repl = prev_status.get("replication_status") or {}

    lag = current_repl.get('Seconds_Behind_Master')
    prev_lag = prev_repl.get('Seconds_Behind_Master', 0)
    if lag is not None and lag > cfg.LAG_THRESHOLD_SECONDS and (prev_lag is None or prev_lag <= cfg.LAG_THRESHOLD_SECONDS):
        return f"replication lag exceeded threshold ({lag}s)"

    # Using the more precise logic to detect a stopped thread
    for thread in ['Slave_IO_Running', 'Slave_SQL_Running']:
        current_state = current_repl.get(thread)
        prev_state = prev_repl.get(thread)

        if current_state != prev_state:
            thread_name = thread.replace('Slave_', '').replace('_Running', '')
            return (
                f"replication thread '{thread_name}' changed state from "
                f"'{prev_state or 'N/A'}' to '{current_state or 'N/A'}'"
            )

    last_error = current_repl.get('Last_Error')
    if last_error and last_error != prev_repl.get('Last_Error'):
        return f"a new replication error was reported: {last_error}"

    return None

def main():
    """Main execution loop."""
    global previous_node_statuses, last_daily_report_sent_date
    print("--- Starting MySQL Replication Monitor ---")

    node_configs = cfg.NODES
    if not node_configs:
        print(f"{COLOR_RED}[ERROR]{COLOR_RESET} No nodes are defined in 'monitor_config.py'. Monitor is stopping.")
        return
    
    print(f"{COLOR_GREEN}[INFO]{COLOR_RESET} Monitoring {len(node_configs)} nodes. Master identified as {cfg.MASTER_NODE_IP}.")

    while True:
        try:
            now = datetime.datetime.now()
            current_statuses: Dict[str, Dict[str, Any]] = {}
            anomalies_detected: Dict[str, str] = {}
            
            for node in node_configs:
                ip = node['ip']
                status = get_node_status(node)

                # --- NEW: Explicit Master Identification ---
                # Set the 'is_master' flag based on the config file, not a guess.
                if ip == cfg.MASTER_NODE_IP:
                    status['is_master'] = True
                # -----------------------------------------

                current_statuses[ip] = status

                if ip in previous_node_statuses:
                    anomaly = get_anomaly_summary(status, previous_node_statuses[ip])
                    if anomaly:
                        anomalies_detected[ip] = anomaly

            # --- Print current status to console ---
            print(f"\n--- Status at {now.strftime('%Y-%m-%d %H:%M:%S')} UTC | Master: {cfg.MASTER_NODE_IP} ---")
            for ip, status in sorted(current_statuses.items()):
                if not status['is_online']:
                    print(f"{ip:<15} | {COLOR_RED}OFFLINE{COLOR_RESET}")
                    continue

                if status['is_master']:
                    print(f"{ip:<15} | {COLOR_GREEN}MASTER{COLOR_RESET}")
                    continue

                repl = status.get('replication_status')
                if not repl:
                    print(f"{ip:<15} | {COLOR_RED}REPLICATION NOT RUNNING{COLOR_RESET}")
                    continue
                
                lag_val = repl.get('Seconds_Behind_Master')
                io_val = repl.get('Slave_IO_Running')
                sql_val = repl.get('Slave_SQL_Running')
                
                lag_display = "NULL" if lag_val is None else lag_val
                io_display = "N/A" if io_val is None else io_val
                sql_display = "N/A" if sql_val is None else sql_val

                print(f"{ip:<15} | Lag: {str(lag_display):<4} | IO: {io_display:<3} | SQL: {sql_display:<3}")


            if anomalies_detected:
                print(f"{COLOR_YELLOW}[ALERT]{COLOR_RESET} New anomalies detected: {anomalies_detected}")
                intro = "An anomaly has been detected in the MySQL cluster. The following changes occurred:"
                for ip, reason in anomalies_detected.items():
                    intro += f"<br>- <strong>{ip}</strong>: {reason}"
                html_body = generate_report_html(current_statuses, "MySQL Replication Anomaly Alert", intro)
                send_email("ALERT: MySQL Replication Anomaly Detected", html_body, cfg.EMAIL_TO)

            if now.hour == cfg.EMAIL_SEND_HOUR and now.date() != last_daily_report_sent_date:
                print(f"{COLOR_GREEN}[INFO]{COLOR_RESET} Sending daily health report...")
                html_body = generate_report_html(current_statuses, "Daily MySQL Replication Health Report", "This is the scheduled daily summary of the replication cluster status.")
                send_email("Daily MySQL Replication Report", html_body, cfg.EMAIL_TO)
                last_daily_report_sent_date = now.date()

            previous_node_statuses = current_statuses
            time.sleep(cfg.CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print("\n--- Monitor stopped by user. ---")
            break
        except Exception as e:
            print(f"An error occured: {e}")
            pass

if __name__ == "__main__":
    main()
