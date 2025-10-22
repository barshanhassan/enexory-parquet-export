#!/usr/bin/env python3
"""
Configuration settings for the MySQL Replication Monitor script.
"""
# Brevo API Key to send emails via Brevo
BREVO_API_KEY: str = ""

# Email that Brevo uses to send emails to EMAIL_TO
SENDER_EMAIL: str = ""

# One or more email addresses to send reports and alerts to.
EMAIL_TO = [
    "",
]

# The hour of the day (0-23) to send the daily summary email. THE TIME IS IN UTC.
EMAIL_SEND_HOUR: int = 14

# The maximum replication lag in seconds before an anomaly is triggered.
LAG_THRESHOLD_SECONDS: int = 600

# The interval in seconds between each check of the nodes.
CHECK_INTERVAL_SECONDS: float = 5

# The connection timeout in seconds for connecting to a MySQL node.
CONNECTION_TIMEOUT: int = 15

# This MUST match the 'ip' of one of the nodes in the NODES list below.
MASTER_NODE_IP: str = "172.18.0.2"

# --- MySQL Node Definitions ---
# Each node is a dictionary with 'ip', 'user', and 'pass'.
NODES = [
    {
        "ip": "172.18.0.2",  # Master IP
        "user": "repl",
        "pass": "replpass"
    },
    {
        "ip": "172.18.0.3",  # Replica 1 IP
        "user": "repl",
        "pass": "replpass"
    },
    {
        "ip": "172.18.0.4",  # Replica 2 IP
        "user": "repl",
        "pass": "replpass"
    },
    # Add as many other replica nodes as you need here.
]