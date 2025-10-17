This documentation provides a complete overview of the **High Availability (HA) and Failover solution**, including its architecture, setup, and operational behavior. It explains how the system maintains continuity during node failures, details the dataflow under normal and degraded conditions, and outlines how to integrate or roll out the solution in an existing cluster.

You’ll find in this documentation:
- The high-level dataflow of this solution under different conditions.
- The prerequisites and configuration steps for deployment.
- Guidance for rollout in production clusters.
- Implementation details of the core orchestration logic handled by `orchestrator.py`.
- Additional notes, limitations, and practical tips for stable operation.

This guide is intended for system administrators, DevOps engineers, and developers who need to understand, deploy, or maintain the HA+Failover system.

## Table of Contents
1. Introduction
2. High-level view of dataflow
3. Setting up this solution
4. Tips and things to keep in mind
5. Known Limitations
6. Rollout strategy to an existing cluster
7. Innerworkings of orchestrator.py

## Introduction
This HA+Failover solution is intended to provide resilient MySQL cluster management with minimal downtime. It is designed to direct writes to a single master while allowing reads to be served from any node, including replicas, which can help maintain high read availability. The `orchestrator.py` script monitors node health, applies quorum rules, can promote new masters when needed, and updates ProxySQL hostgroups to manage routing. By automating failover, replica repointing, and ProxySQL configuration, it aims to keep the cluster operational, reduce the risk of split-brain scenarios, and support a consistent system state.

## High-level view of dataflow
The diagrams below illustrate the intended flow of data under different node conditions. This example assumes one master and two replicas in the cluster.

#### Healthy topology
When all nodes are online, users connect via the domain on port 3306 to the middleware node. This node routes writes to the current master and distributes reads across all MySQL nodes. Replicas are expected to continuously replicate changes from the master.
<figure style="text-align: center;">
  <img src="Healthy Setup.png" alt="Healthy Setup">
  <figcaption><strong>Fig 1.1</strong>: Dataflow when topology is healthy</figcaption>
</figure>

#### ProxySQL Node is offline
If the middleware node goes down, manual intervention may be required. Point the domain to the current master to allow writes and reads to continue until the middleware node is restored.
<figure style="text-align: center;">
  <img src="Emergency Setup when ProxySQL is down.png" alt="Emergency Setup when ProxySQL is down">
  <figcaption><strong>Fig 1.2</strong>: Dataflow when Middleware Node is offline</figcaption>
</figure>
#### One replica node is offline
If a single replica goes offline, reads and writes can continue on the remaining nodes. The offline replica will need to restart, and any replication errors must be addressed manually if they occur. Replication should continue normally on the online replicas.
<figure style="text-align: center;">
  <img src="Unhealth Setup (1 Replica Down).png" alt="Unhealthy Setup (1 Replica Down)">
  <figcaption><strong>Fig 1.3</strong>: Dataflow when one replica is offline</figcaption>
</figure>

#### All replica nodes are offline
If both replicas go offline, writes/reads can still be done to the master. Offline replicas will need to be restarted, and any replication errors resolved manually. Replication will be paused until at least one replica comes back online.
<figure style="text-align: center;">
  <img src="Unhealth Setup (2 Replica Down).png" alt="Unhealthy Setup (2 Replica Down)">
  <figcaption><strong>Fig 1.4</strong>: Dataflow when two replicas are offline</figcaption>
</figure>

#### Failover scenario: Master went offline
If the master goes down, `orchestrator.py` attempts to promote a suitable replica to master. The other replica will then replicate from this new master.
<figure style="text-align: center;">
  <img src="Failover Scenario Part 1 (Master goes offline).png" alt="Failover Scenario Part 1 (Master goes offline)">
  <figcaption><strong>Fig 1.5</strong>: Dataflow when master goes offline. Before it is revived</figcaption>
</figure>

When the original master returns and is healthy (no replication errors), it can be set to host group 20, as `orchestrator.py` may have marked it as host group 30 while it was considered broken. It will then resume serving reads and be recognized as healthy by `orchestrator.py`.
<figure style="text-align: center;">
  <img src="Failover Scenario Part 2 (Node is back online).png" alt="Failover Scenario Part 2 (Node is back online)">
  <figcaption><strong>Fig 1.6</strong>: Dataflow after the previous master is revived as a replica</figcaption>
</figure>

## Setting up this solution
This section will detail the steps to setup this solution.
#### Pre-requisites before setup
The following are the pre-requisites before setting up this solution:
1. Odd number of MySQL nodes
2. Each MySQL node must be version 5.7
3. The MySQL topology must be one master and remaining being replicas
4. GTIDs must be turned on, and replication must be done with MASTER_AUTO_POSITION=1.
5. Read only mode must be off on all MySQL nodes.
6. Each node's my.cnf file must be similar to the example below and with unique server ids.
7. All nodes, including the middleware node about to be setup, can communicate to each other.
8. Replica user MUST have ALL PRIVILEGES

Example of `my.cnf` file:
```
[mysqld]
server-id=1
log_bin=mysql-bin
gtid_mode=ON
enforce_gtid_consistency=ON
binlog_format=ROW
log_slave_updates = ON
expire_logs_days = 3
read_only = OFF
bind-address = 0.0.0.0
```

Example of how to setup replication using GTIDs and MASTER_AUTO_POSITION=1:
```sql
STOP SLAVE;
CHANGE MASTER TO
  MASTER_AUTO_POSITION=1,
  MASTER_HOST='<Master Node>',
  MASTER_USER='<Replica Username>',
  MASTER_PASSWORD='<Replica Password>';
START SLAVE;
```

Example of how to grant replication user ALL PRIVILEGES:
```sql
GRANT ALL PRIVILEGES ON *.* TO '<Replica Username>'@'%' WITH GRANT OPTION;FLUSH PRIVILEGES;
```

#### Setup after pre-requisites are met
To setup ProxySQL and `orchestrator.py`, follow these steps:


## Tips and things to keep in mind
Below are some tips and things to keep in mind while implementing and running this solution:
1. Temporarily move down or broken nodes to host group 40 to allow `orchestrator.py` to continue functioning without quorum issues.
2. This prevent quorum issues, try to always maintain an odd total count of unique recognized nodes across host groups 10, 20, and 30 in ProxySQL. Nodes in host group 40 are excluded from this count as they are considered invisible to `orchestrator.py`.
3. Do not place multiple nodes in host group 10. Only one writer is permitted, and `orchestrator.py` is designed to intentionally halt if it detects more than one node in this group.
4. Make sure you are receiving emails from `orchestrator.py` at the provided address. They may also appear in your spam folder.
5. Do not run multiple instances of `orchestrator.py` as that may cause each instance of the script to interfere with each other.
6. Shut down `orchestrator.py` before making changes to ProxySQL or modifying the topology of recognized nodes i.e. nodes in ProxySQL outside host group 40.
7. Do not run ProxySQL on any node that also has a MySQL server if both are configured to allow connections on port 3306. The setup steps in this documentation configure ProxySQL with `mysql-port=3306`.

## Known Limitations
Below are the known limitations of the solution, the do not cover any unknown issues or problems:
1. Reads may occasionally return outdated or inconsistent data, specifically for writes that have not yet replicated, since replicas may slightly lag behind the master under normal conditions.
2. If a majority of nodes (quorum) are down, `orchestrator.py` will halt all topology and ProxySQL modifications to try and prevent inconsistent or unsafe state changes.
3. If replication lag is reported as NULL, it is treated as the maximum allowed lag. This means that even if the actual lag is lower, the system assumes it is higher, which may result in inconsistent routing or promotion decisions.
4. If the master fails before its recent writes are replicated, and a replica is promoted as the new master and accepts new writes, the old master (when reattached as a replica) may encounter GTID conflicts or error 1062, requiring manual intervention to recover.
5. If the `orchestrator.py` script stops unexpectedly or is terminated abruptly, it may leave the nodes and ProxySQL in an incorrectly configured state. Always verify the topology and ProxySQL configuration before starting or restarting the script.

## Rollout strategy to an existing cluster
This example assumes an existing cluster with one master and two replicas.

1. **Prepare existing MySQL nodes**  
    All nodes must match the setup specifications. This include correct users and permissions, GTID-based replication (`MASTER_AUTO_POSITION=1`), and properly configured `my.cnf` on each node.
    
2. **Prepare a fourth node**  
    Use either the new node or an existing empty node to host ProxySQL and `orchestrator.py`. Make sure your registered domain points to this fourth node before proceeding. Wait for DNS propagation. 
    
3. **Verify topology and script arguments**  
    Make sure ProxySQL reflects the actual cluster topology and that all arguments passed to `orchestrator.py` are correct. Check if email's from `orchestrator.py` are being sent to you (they may also appear in your spam folder).
    
4. **Pause client connections**  
    Shut down all client connections to the original master. This can be done by blocking traffic via UFW to the master node.
    
5. **Run `orchestrator.py`**  
    Start the script to manage the cluster.
    
6. **Monitor and test**  
    Check logs and simulate read/write operations to validate readiness. Match the same load as peak client usage where possible.
    
7. **Update client software**  
    Roll out an update that points the master’s read/write IP to the domain and sets the port to 3306. As clients update, requests will go to ProxySQL via the domain instead of the blocked original master. Any client software that does not update will continue failing to connect. Make sure ProxySQL app user credentials match those configured in the client software.

**Note:** If ProxySQL is down reroute the domain to the master node to maintain service continuity. If the current master is still the original master, unblock traffic as well.

## Innerworkings of orchestrator.py

