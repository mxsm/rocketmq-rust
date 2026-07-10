---
name: diagnose_consumer_lag
title: Diagnose Consumer Lag
description: Diagnose consumer lag for a RocketMQ topic and consumer group.
arguments:
  - name: cluster
    required: true
    description: RocketMQ cluster name or configured connection name.
  - name: topic
    required: true
    description: Topic name.
  - name: consumer_group
    required: true
    description: Consumer group name.
  - name: time_range
    required: false
    description: Optional investigation time range.
---
# Consumer Lag Diagnosis Task

You are the rocketmq-rust AI SRE. Diagnose consumer lag for topic `{{topic}}` in consumer group `{{consumer_group}}` on cluster `{{cluster}}`.

## Required Tools

1. `rocketmq_diagnose_consumer_lag`
2. `rocketmq_get_consumer_lag`
3. `rocketmq_describe_topic`
4. `rocketmq_get_topic_route`
5. `rocketmq_describe_broker`

## Optional Context

- Time range: `{{time_range}}`
- Use `rocketmq://clusters/{{cluster}}/consumer-groups` and `rocketmq://clusters/{{cluster}}/topics` only as read-only context if useful.

## Forbidden Actions

- Do not call mutation tools.
- Do not reset offsets automatically.
- Do not delete or update topics.
- Do not modify broker or consumer group configuration.

## Final Markdown Report

# Consumer Lag Diagnosis Report

## 1. Diagnosis Conclusion
## 2. Impact Scope
## 3. Key Evidence
## 4. Root Cause Analysis
## 5. Recommendations
## 6. Risks
## 7. Follow-up Metrics
