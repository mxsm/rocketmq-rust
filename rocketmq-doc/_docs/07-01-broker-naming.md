---
title: "Understanding Broker Naming in RocketMQ-Rust"
permalink: /docs/broker-naming/
excerpt: "Understanding Broker Naming in RocketMQ-Rust"
last_modified_at: 2025-09-05T21:43:05-04:00
redirect_from:
  - /theme-setup/
toc: false
classes: wide
---

In **RocketMQ-Rust**, the way you name Brokers is not just a matter of convenience—it forms the foundation for **cluster
management, routing discovery, and high availability**. A well-structured naming convention is critical for operations,
monitoring, and troubleshooting.

This article explains the **hierarchical naming structure** RocketMQ-Rust uses for Brokers, how it influences
Master–Slave roles, and best practices for production environments.

------

## 1. The Core Identifier: `brokerName`

In RocketMQ-Rust, a Broker is identified by its **`brokerName`**, rather than by its IP address or port.

### Recommended Format

Although multiple formats are technically supported, the recommended structure is:

```
{brokerClusterName}-{brokerId}
```

Optionally, an `instanceId` may be included for additional differentiation:

```
{brokerClusterName}-{brokerId}-{instanceId}
```

### Field Definitions

| Field               | Description                                | Example              |
|---------------------|--------------------------------------------|----------------------|
| `brokerClusterName` | Name of the cluster the Broker belongs to  | `RocketMQ-Cluster-A` |
| `brokerId`          | Unique ID of the Broker within the cluster | `0`, `1`, `2`, `100` |
| `instanceId`        | Optional instance marker (rarely used)     |                      |

------

## 2. The Role of `brokerId`

The value of `brokerId` determines the **role** of the Broker within the cluster:

| brokerId   | Role                 | Description                                                 |
|------------|----------------------|-------------------------------------------------------------|
| `0`        | **Master**           | Handles both reads and writes; multiple Masters are allowed |
| `> 0`      | **Slave**            | Read-only, replicates data from a Master                    |
| `> 100000` | **Dledger Follower** | Used in Raft-based Dledger mode                             |

> 🔎 **Important:** All nodes under the same `brokerName` (Master + Slaves) must share that `brokerName`, while their *
*roles are distinguished by `brokerId`**.

### Example

```properties
# Master node
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=0

# Slave node (replicating broker-a)
brokerClusterName=DefaultCluster
brokerName=broker-a
brokerId=1
```

Here, `broker-a` is a master–slave group, where `brokerId=0` is the Master and `brokerId=1` is the Slave.

------

## 3. Configuring Brokers (`broker.conf`)

A typical Broker configuration file (`broker.conf`) might look like this:

```conf
# Cluster name (multiple Brokers may share this cluster)
brokerClusterName=rocketmq-cluster

# Logical name of the Broker
brokerName=broker-a

# Role-defining ID (0 = Master, >0 = Slave)
brokerId=0

# Physical address of the Broker (usually auto-registered)
# brokerIP1=192.168.0.1
```

When deploying multiple Brokers, adjust only `brokerName` and `brokerId`:

| Node     | brokerName | brokerId | Role   |
|----------|------------|----------|--------|
| A-Master | broker-a   | 0        | Master |
| A-Slave  | broker-a   | 1        | Slave  |
| B-Master | broker-b   | 0        | Master |
| B-Slave  | broker-b   | 1        | Slave  |

This results in a **multi-master, multi-slave cluster**.

------

## 4. Best Practices for Naming

To ensure stability and maintainability:

- 🔤 **Use meaningful names**
  Example: `broker-prod-01`, `broker-order`, `broker-log`
- 🔢 **Reserve `brokerId=0` for Masters**
  Slaves should use incremental IDs: `1, 2, …`
- 🌐 **Avoid duplicate `brokerName`s across clusters**
  Otherwise, routing conflicts may occur
- 📁 **Use separate config files**
  Each Broker should maintain its own `broker.conf`
- 🧩 **Include environment or purpose in naming**
  Example: `broker-a-prod`, `broker-a-test`

------

## 5. How to Check Broker Names

You can verify Broker names in two common ways:

### From logs

```log
The broker[broker-a, 192.168.0.1:10911] registered successfully.
```

### From CLI

```bash
mqadmin clusterList -n 127.0.0.1:9876
```

Example output:

```
Broker Name: broker-a
    Addr: 192.168.0.1:10911 (brokerId: 0) -> MASTER
    Addr: 192.168.0.2:10911 (brokerId: 1) -> SLAVE
```

------

## 6. Broker Naming in Dledger Mode

In **Dledger mode** (Raft-based high availability), naming rules differ slightly:

- `brokerId=0` no longer strictly indicates Master.
- All nodes in the same Dledger group share the same `brokerName` and `brokerId=0`.
- Roles (Leader/Follower) are determined dynamically through the Raft election process.

Example configuration:

```conf
brokerName=broker-a
brokerId=0
enableDLegerCommitLog=true
dLegerGroup=broker-a
dLegerPeers=n1-192.168.0.1:40911;n2-192.168.0.2:40911;n3-192.168.0.3:40911
```

Here, `brokerName` represents the Dledger group, and individual nodes are identified as `n1`, `n2`, `n3`, etc.

------

## 7. Key Takeaways

| Key Point                             | Explanation                          |
|---------------------------------------|--------------------------------------|
| 🎯 `brokerName` + `brokerId`          | Together define a Broker’s identity  |
| 🔄 `brokerId=0` = Master              | `>0` = Slave (traditional setup)     |
| 🏗️ Use clear, descriptive names      | Simplifies ops and monitoring        |
| 🌐 Keep `brokerName` unique           | Prevents routing conflicts           |
| 📈 Supports multi-master, multi-slave | Improves throughput and availability |

------

## Conclusion

Broker naming in RocketMQ-Rust is **not arbitrary**. It follows a **structured `brokerName-brokerId` scheme** that
underpins replication, cluster routing, and failover mechanisms.

By adopting a clear and consistent naming strategy, you set the stage for a more **resilient, scalable, and maintainable
RocketMQ-Rust cluster**.