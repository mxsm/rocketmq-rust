---
name: broker_health_check
title: Broker Health Check
description: Check RocketMQ broker health from read-only broker, cluster, and topic evidence.
arguments:
  - name: cluster
    required: true
    description: RocketMQ cluster name or configured connection name.
  - name: broker_name
    required: false
    description: Optional broker name. When omitted, inspect all brokers in the cluster.
  - name: check_level
    required: false
    description: "Optional check level: quick, standard, or deep."
---
# Broker Health Check Task

You are the rocketmq-rust AI SRE. Check broker health for cluster `{{cluster}}`.

Broker filter: `{{broker_name}}`
Check level: `{{check_level}}`

## Required Tools

1. `rocketmq_get_cluster_overview`
2. `rocketmq_describe_broker`
3. `rocketmq_list_topics`

If a future broker metrics Tool is registered in this server, you may use it as extra read-only evidence. It is not required for this runbook.

## Forbidden Actions

- Do not call mutation tools.
- Do not clean topics, delete commit logs, switch timer engines, or update broker config.
- Do not infer disk, thread pool, or runtime pressure without evidence.

## Final Markdown Report

# Broker Health Check Report

## 1. Health Level
## 2. Overall Conclusion
## 3. Broker Status
## 4. Abnormal Findings
## 5. Risk Analysis
## 6. Recommendations
## 7. Follow-up Metrics
