use rocketmq_error::RocketMQError;
use rocketmq_error::SerializationError;
use rocketmq_error::ToolsError;

pub(crate) fn cluster_metadata_unavailable(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::Tools(ToolsError::ClusterInvalid { reason: reason.into() })
}

pub(crate) fn cluster_not_found(cluster: impl Into<String>) -> RocketMQError {
    RocketMQError::Tools(ToolsError::cluster_not_found(cluster))
}

pub(crate) fn broker_metadata_unavailable(reason: impl Into<String>) -> RocketMQError {
    RocketMQError::Tools(ToolsError::BrokerNotFound { broker: reason.into() })
}

pub(crate) fn broker_not_found(broker: impl Into<String>) -> RocketMQError {
    RocketMQError::Tools(ToolsError::broker_not_found(broker))
}

pub(crate) fn broker_operation_failed(operation: &'static str, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::broker_operation_failed(operation, 0, reason)
}

pub(crate) fn admin_operation_failed(operation: &'static str, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::response_process_failed(operation, reason)
}

pub(crate) fn admin_validation_failed(field: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::validation_error(field, reason)
}

pub(crate) fn admin_serialization_failed(format: &'static str, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::Serialization(SerializationError::encode_failed(format, reason))
}

pub(crate) fn topic_route_not_found(topic: impl Into<String>) -> RocketMQError {
    RocketMQError::RouteNotFound { topic: topic.into() }
}

pub(crate) fn topic_route_inconsistent(topic: impl Into<String>, reason: impl Into<String>) -> RocketMQError {
    RocketMQError::RouteInconsistent {
        topic: topic.into(),
        reason: reason.into(),
    }
}
