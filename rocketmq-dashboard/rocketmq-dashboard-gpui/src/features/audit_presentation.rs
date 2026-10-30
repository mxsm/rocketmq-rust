// Copyright 2026 The RocketMQ Rust Authors
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

//! Exhaustive labels for the existing bounded Audit contract.

use rocketmq_dashboard_common::{
    AuditAction, AuditConsumerField, AuditOutcome, AuditTopicField, OperationActor, OperationResource,
};

pub(super) const ACTIONS: [AuditAction; 24] = [
    AuditAction::ConfigurationSaved,
    AuditAction::MonitorSaved,
    AuditAction::MonitorDeleted,
    AuditAction::TopicCreated,
    AuditAction::TopicUpdated,
    AuditAction::TopicDeleted,
    AuditAction::TopicBrokerDeleted,
    AuditAction::TopicMessageSent,
    AuditAction::ConsumerOffsetReset,
    AuditAction::ConsumerBacklogSkipped,
    AuditAction::ConsumerCreated,
    AuditAction::ConsumerUpdated,
    AuditAction::ConsumerDeleted,
    AuditAction::BrokerUpdated,
    AuditAction::MessageConsumed,
    AuditAction::DlqResent,
    AuditAction::AclUserCreated,
    AuditAction::AclUserUpdated,
    AuditAction::AclUserDeleted,
    AuditAction::AclPolicyCreated,
    AuditAction::AclPolicyUpdated,
    AuditAction::AclPolicyDeleted,
    AuditAction::LocalLogin,
    AuditAction::LocalLogout,
];

pub(super) const OUTCOMES: [AuditOutcome; 5] = [
    AuditOutcome::Succeeded,
    AuditOutcome::Rejected,
    AuditOutcome::Partial,
    AuditOutcome::Failed,
    AuditOutcome::Unknown,
];

pub(super) fn action_label(action: AuditAction) -> &'static str {
    match action {
        AuditAction::ConfigurationSaved => "Settings saved",
        AuditAction::MonitorSaved => "Monitor rule saved",
        AuditAction::MonitorDeleted => "Monitor rule deleted",
        AuditAction::TopicCreated => "Topic created",
        AuditAction::TopicUpdated => "Topic updated",
        AuditAction::TopicDeleted => "Topic deleted",
        AuditAction::TopicBrokerDeleted => "Topic Broker target deleted",
        AuditAction::TopicMessageSent => "Topic message sent",
        AuditAction::ConsumerOffsetReset => "Consumer offsets reset",
        AuditAction::ConsumerBacklogSkipped => "Consumer backlog skipped",
        AuditAction::ConsumerCreated => "Consumer created",
        AuditAction::ConsumerUpdated => "Consumer updated",
        AuditAction::ConsumerDeleted => "Consumer deleted",
        AuditAction::BrokerUpdated => "Broker updated",
        AuditAction::MessageConsumed => "Message consumed",
        AuditAction::DlqResent => "DLQ message resent",
        AuditAction::AclUserCreated => "ACL user created",
        AuditAction::AclUserUpdated => "ACL user updated",
        AuditAction::AclUserDeleted => "ACL user deleted",
        AuditAction::AclPolicyCreated => "ACL policy created",
        AuditAction::AclPolicyUpdated => "ACL policy updated",
        AuditAction::AclPolicyDeleted => "ACL policy deleted",
        AuditAction::LocalLogin => "Local sign in",
        AuditAction::LocalLogout => "Local sign out",
    }
}

pub(super) fn outcome_label(outcome: AuditOutcome) -> &'static str {
    match outcome {
        AuditOutcome::Succeeded => "Succeeded",
        AuditOutcome::Rejected => "Rejected",
        AuditOutcome::Partial => "Partial",
        AuditOutcome::Failed => "Failed",
        AuditOutcome::Unknown => "Unknown",
    }
}

pub(super) fn actor_label(actor: &OperationActor) -> String {
    match actor {
        OperationActor::LocalOperator { username } => format!("Local operator: {username}"),
        OperationActor::LocalDesktop => "Local desktop (authentication disabled)".into(),
        OperationActor::System => "System".into(),
    }
}

pub(super) fn resource_label(resource: &OperationResource) -> String {
    match resource {
        OperationResource::Configuration => "Local settings".into(),
        OperationResource::LocalSession => "Local session".into(),
        OperationResource::Monitor(id) => format!("Monitor rule: {id}"),
        OperationResource::Topic(id) => format!("Topic: {id}"),
        OperationResource::ConsumerGroup(id) => format!("Consumer group: {id}"),
        OperationResource::Broker(id) => format!("Broker: {id}"),
        OperationResource::Message(id) => format!("Message: {id}"),
        OperationResource::AclSubject(id) => format!("ACL subject: {id}"),
    }
}

pub(super) fn topic_field_label(field: AuditTopicField) -> &'static str {
    match field {
        AuditTopicField::ReadQueueCount => "Read queue count",
        AuditTopicField::WriteQueueCount => "Write queue count",
        AuditTopicField::Permission => "Permission",
        AuditTopicField::Ordered => "Ordered",
        AuditTopicField::MessageType => "Message type",
    }
}

pub(super) fn consumer_field_label(field: AuditConsumerField) -> &'static str {
    match field {
        AuditConsumerField::RetryMaxTimes => "Maximum retries",
        AuditConsumerField::RetryQueueNums => "Retry queue count",
        AuditConsumerField::ConsumeTimeoutMinutes => "Consume timeout (minutes)",
        AuditConsumerField::ConsumeEnable => "Consume enabled",
        AuditConsumerField::ConsumeFromMinEnable => "Consume from minimum",
        AuditConsumerField::ConsumeBroadcastEnable => "Broadcast consume",
        AuditConsumerField::ConsumeMessageOrderly => "Orderly consume",
        AuditConsumerField::BrokerId => "Broker ID",
        AuditConsumerField::WhichBrokerWhenConsumeSlowly => "Slow consumer Broker ID",
    }
}

pub(super) fn timestamp(value: u64) -> String {
    i64::try_from(value)
        .ok()
        .and_then(chrono::DateTime::from_timestamp_millis)
        .map(|time| time.format("%Y-%m-%d %H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "Unknown time".into())
}
