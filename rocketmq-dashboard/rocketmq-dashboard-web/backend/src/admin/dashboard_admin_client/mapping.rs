// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::*;

static NEXT_ADMIN_GROUP_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn map_consumer_progress(progress: core::DashboardConsumerProgress) -> ConsumerProgress {
    ConsumerProgress {
        group: progress.group,
        topic_count: progress.topic_count,
        diff_total: progress.diff_total,
        queues: progress
            .queues
            .into_iter()
            .map(|queue| ConsumerQueueProgress {
                topic: queue.topic,
                broker_name: queue.broker_name,
                queue_id: queue.queue_id,
                broker_offset: queue.broker_offset,
                consumer_offset: queue.consumer_offset,
                diff: queue.diff,
            })
            .collect(),
    }
}

pub(super) fn map_broker_info(item: core::DashboardBrokerInfo) -> BrokerInfo {
    BrokerInfo {
        cluster_name: item.cluster_name,
        broker_name: item.broker_name,
        broker_id: item.broker_id,
        address: item.address,
        role: item.role,
        version: item.version,
        produce_tps: item.produce_tps,
        consume_tps: item.consume_tps,
    }
}

pub(super) fn map_producer_connection(item: core::DashboardProducerConnection) -> ProducerConnectionInfo {
    ProducerConnectionInfo {
        client_id: item.client_id,
        client_addr: item.client_addr,
        language: item.language,
        version: item.version_desc,
    }
}

pub(super) fn broker_target(value: &str) -> core::DashboardBrokerTarget {
    if value.contains(':') {
        core::DashboardBrokerTarget {
            broker_name: value.to_string(),
            broker_addr: Some(value.to_string()),
        }
    } else {
        core::DashboardBrokerTarget {
            broker_name: value.to_string(),
            broker_addr: None,
        }
    }
}

pub(super) fn map_selector(cluster_name: Option<String>, broker_name: Option<String>) -> core::TargetSelector {
    core::TargetSelector {
        cluster_name,
        broker_name,
    }
}

pub(super) fn map_acl_query(query: AclQuery) -> core::DashboardAclQuery {
    core::DashboardAclQuery {
        selector: map_selector(query.cluster_name, query.broker_name),
        search: query.search_param.or(query.filter).unwrap_or_default(),
        resource: query.resource.unwrap_or_default(),
    }
}

pub(super) fn map_acl_user_query(query: AclQuery) -> core::DashboardAclQuery {
    core::DashboardAclQuery {
        selector: map_selector(query.cluster_name, query.broker_name),
        search: query.filter.or(query.search_param).unwrap_or_default(),
        resource: query.resource.unwrap_or_default(),
    }
}

pub(super) fn map_acl_user_request(
    username: String,
    request: AclUserUpsertRequest,
    require_status: bool,
) -> Result<core::DashboardAclUserMutationRequest, DashboardError> {
    validate_name(&username, "Username")?;
    validate_name(&request.password, "Password")?;
    validate_name(&request.user_type, "User type")?;
    if require_status {
        required_request_field(request.user_status.as_deref(), "userStatus")?;
    }
    Ok(core::DashboardAclUserMutationRequest {
        selector: map_selector(request.cluster_name, request.broker_name),
        username,
        password: request.password,
        user_type: request.user_type,
        user_status: request.user_status,
    })
}

pub(super) fn map_acl_policy_request(
    request: AclPolicyRequest,
) -> Result<core::DashboardAclPolicyMutationRequest, DashboardError> {
    validate_name(&request.subject, "ACL subject")?;
    if request.policies.is_empty() {
        return Err(DashboardError::Validation("ACL policies cannot be empty".to_string()));
    }
    let mut policies = Vec::with_capacity(request.policies.len());
    for policy in request.policies {
        validate_name(&policy.policy_type, "ACL policy type")?;
        if policy.entries.is_empty() {
            return Err(DashboardError::Validation(
                "ACL policy entries cannot be empty".to_string(),
            ));
        }
        let mut entries = Vec::with_capacity(policy.entries.len());
        for entry in policy.entries {
            if entry.resource.is_empty() {
                return Err(DashboardError::Validation(
                    "ACL policy resource cannot be empty".to_string(),
                ));
            }
            if entry.actions.is_empty() {
                return Err(DashboardError::Validation(
                    "ACL policy actions cannot be empty".to_string(),
                ));
            }
            validate_name(&entry.decision, "ACL policy decision")?;
            for resource in &entry.resource {
                validate_name(resource, "ACL policy resource")?;
            }
            entries.push(core::DashboardAclPolicyMutationEntry {
                resources: entry.resource,
                actions: entry.actions,
                source_ips: entry.source_ips,
                decision: entry.decision,
            });
        }
        policies.push(core::DashboardAclPolicyMutation {
            policy_type: policy.policy_type,
            entries,
        });
    }
    Ok(core::DashboardAclPolicyMutationRequest {
        selector: map_selector(request.cluster_name, request.broker_name),
        subject: request.subject,
        policies,
    })
}

pub(super) fn map_acl_mutation(result: core::AdminMutationResult) -> AclMutationResult {
    AclMutationResult {
        message: result.message,
        target_count: result.target_count,
    }
}

pub(super) fn map_acl_policy(policy: core::DashboardAclPolicy) -> AclPolicyView {
    AclPolicyView {
        broker_name: policy.broker_name,
        broker_addr: policy.broker_addr,
        subject: policy.subject,
        policy_type: policy.policy_type,
        entries: policy
            .entries
            .into_iter()
            .map(|entry| AclPolicyEntryView {
                resource: entry.resource,
                actions: entry.actions,
                source_ips: entry.source_ips,
                decision: entry.decision,
            })
            .collect(),
    }
}

pub(super) fn map_message_list(list: core::DashboardMessageList) -> MessageListView {
    let mut items = list.items.into_iter().map(map_message).collect::<Vec<_>>();
    items.sort_by_key(|message| Reverse(message.store_timestamp));
    MessageListView {
        total: list.total,
        items,
    }
}

pub(super) fn map_message(message: core::DashboardMessage) -> MessageView {
    let mut properties = message.properties;
    let store_message_id = message.message_id;
    let message_id = properties
        .get("UNIQ_KEY")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| store_message_id.clone());
    properties.insert("STORE_MESSAGE_ID".to_string(), store_message_id);
    MessageView {
        topic: message.topic,
        message_id,
        keys: message.keys,
        tags: message.tags,
        born_timestamp: message.born_timestamp,
        store_timestamp: message.store_timestamp,
        born_host: message.born_host,
        store_host: message.store_host,
        queue_id: message.queue_id,
        queue_offset: message.queue_offset,
        store_size: message.store_size,
        reconsume_times: message.reconsume_times,
        body_crc: message.body_crc,
        sys_flag: message.sys_flag,
        flag: message.flag,
        prepared_transaction_offset: message.prepared_transaction_offset,
        body: String::from_utf8_lossy(&message.body).into_owned(),
        properties,
    }
}

pub(super) fn map_trace_node(node: core::DashboardMessageTraceNode) -> MessageTraceNode {
    MessageTraceNode {
        node_type: node.node_type,
        name: node.name,
        status: node.status,
        timestamp: node.timestamp,
    }
}

pub(super) fn validate_message_window(begin: Option<i64>, end: Option<i64>) -> Result<(), DashboardError> {
    let begin = begin
        .filter(|timestamp| *timestamp > 0)
        .ok_or_else(|| DashboardError::Validation("Topic message query requires begin time".to_string()))?;
    let end = end
        .filter(|timestamp| *timestamp > 0)
        .ok_or_else(|| DashboardError::Validation("Topic message query requires end time".to_string()))?;
    if end < begin {
        return Err(DashboardError::Validation(
            "Topic message query end time must be later than begin time".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn build_dlq_csv(messages: &[MessageView]) -> String {
    let mut csv = String::from("messageId,topic,keys,tags,storeTimestamp,queueId,queueOffset,body\n");
    for message in messages {
        csv.push_str(
            &[
                csv_escape(&message.message_id),
                csv_escape(&message.topic),
                csv_escape(message.keys.as_deref().unwrap_or_default()),
                csv_escape(message.tags.as_deref().unwrap_or_default()),
                message.store_timestamp.to_string(),
                message.queue_id.to_string(),
                message.queue_offset.to_string(),
                csv_escape(&message.body),
            ]
            .join(","),
        );
        csv.push('\n');
    }
    csv
}

pub(super) fn csv_escape(value: &str) -> String {
    let neutralized = if matches!(value.chars().next(), Some('=' | '+' | '-' | '@' | '\t' | '\r' | '\n')) {
        format!("'{value}")
    } else {
        value.to_string()
    };
    if neutralized.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", neutralized.replace('"', "\"\""))
    } else {
        neutralized
    }
}

pub(super) fn unique_admin_group() -> String {
    let id = NEXT_ADMIN_GROUP_ID.fetch_add(1, Ordering::Relaxed);
    format!("dashboard-web-admin-{}-{id}", std::process::id())
}

pub(super) fn validate_name(value: &str, label: &str) -> Result<(), DashboardError> {
    if value.trim().is_empty() {
        return Err(DashboardError::Validation(format!("{label} cannot be empty")));
    }
    Ok(())
}

pub(super) fn required_request_field<'a>(value: Option<&'a str>, label: &str) -> Result<&'a str, DashboardError> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| DashboardError::Validation(format!("{label} cannot be empty")))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Barrier;
    use std::sync::Mutex;
    use std::thread;

    use rocketmq_admin_core::core::dashboard::DashboardMessage;
    use rocketmq_admin_core::core::dashboard::DashboardProducerConnection;

    use super::csv_escape;
    use super::map_message;
    use super::map_producer_connection;
    use super::unique_admin_group;

    #[test]
    fn concurrent_admin_groups_are_unique() {
        const CALLERS: usize = 32;

        let barrier = Arc::new(Barrier::new(CALLERS));
        let groups = Arc::new(Mutex::new(HashSet::new()));
        thread::scope(|scope| {
            for _ in 0..CALLERS {
                let barrier = Arc::clone(&barrier);
                let groups = Arc::clone(&groups);
                scope.spawn(move || {
                    barrier.wait();
                    groups
                        .lock()
                        .expect("group collection lock")
                        .insert(unique_admin_group());
                });
            }
        });

        assert_eq!(groups.lock().expect("group collection lock").len(), CALLERS);
    }

    #[test]
    fn maps_owned_message_without_protocol_types() {
        let mut properties = BTreeMap::new();
        properties.insert("UNIQ_KEY".to_string(), "client-id".to_string());
        properties.insert("STORE_MESSAGE_ID".to_string(), "producer-spoofed-id".to_string());
        let message = DashboardMessage {
            topic: "Orders".to_string(),
            message_id: "store-id".to_string(),
            keys: None,
            tags: None,
            born_timestamp: 1,
            store_timestamp: 2,
            born_host: "127.0.0.1:1".to_string(),
            store_host: "127.0.0.1:2".to_string(),
            queue_id: 0,
            queue_offset: 3,
            store_size: 4,
            reconsume_times: 0,
            body_crc: 5,
            sys_flag: 0,
            flag: 0,
            prepared_transaction_offset: 0,
            body: b"hello".to_vec(),
            properties,
        };

        let mapped = map_message(message);

        assert_eq!(mapped.message_id, "client-id");
        assert_eq!(mapped.body, "hello");
        assert_eq!(
            mapped.properties.get("STORE_MESSAGE_ID").map(String::as_str),
            Some("store-id")
        );
    }

    #[test]
    fn maps_producer_version_description_instead_of_protocol_ordinal() {
        let mapped = map_producer_connection(DashboardProducerConnection {
            client_id: "client-a".to_string(),
            client_addr: "127.0.0.1:10911".to_string(),
            language: "RUST".to_string(),
            version: 474,
            version_desc: "V5_3_1_SNAPSHOT".to_string(),
        });

        assert_eq!(mapped.version, "V5_3_1_SNAPSHOT");
    }

    #[test]
    fn escapes_csv_cells() {
        assert_eq!(csv_escape("plain"), "plain");
        assert_eq!(csv_escape("a,\"b\""), "\"a,\"\"b\"\"\"");
    }

    #[test]
    fn neutralizes_formula_leading_csv_cells() {
        assert_eq!(csv_escape("=1+1"), "'=1+1");
        assert_eq!(csv_escape("+cmd"), "'+cmd");
        assert_eq!(csv_escape("-2+3"), "'-2+3");
        assert_eq!(csv_escape("@SUM(A1:A2)"), "'@SUM(A1:A2)");
        assert_eq!(csv_escape("\t=1+1"), "'\t=1+1");
        assert_eq!(csv_escape("\r=1+1"), "\"'\r=1+1\"");
        assert_eq!(csv_escape("\n=1+1"), "\"'\n=1+1\"");
    }
}
