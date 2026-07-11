pub struct TraceConstants;

impl TraceConstants {
    pub const GROUP_NAME_PREFIX: &'static str = "_INNER_TRACE_PRODUCER";
    pub const CONTENT_SPLITOR: char = '\u{0001}';
    pub const FIELD_SPLITOR: char = '\u{0002}';
    pub const TRACE_INSTANCE_NAME: &'static str = "PID_CLIENT_INNER_TRACE_PRODUCER";
    pub const TRACE_TOPIC_PREFIX: &'static str = "TRACE_DATA_";
    pub const TO_PREFIX: &'static str = "To_";
    pub const FROM_PREFIX: &'static str = "From_";
    pub const END_TRANSACTION: &'static str = "EndTransaction";
    pub const ROCKETMQ_SERVICE: &'static str = "rocketmq";
    pub const ROCKETMQ_SUCCESS: &'static str = "rocketmq.success";
    pub const ROCKETMQ_TAGS: &'static str = "rocketmq.tags";
    pub const ROCKETMQ_KEYS: &'static str = "rocketmq.keys";
    pub const ROCKETMQ_STORE_HOST: &'static str = "rocketmq.store_host";
    pub const ROCKETMQ_BODY_LENGTH: &'static str = "rocketmq.body_length";
    pub const ROCKETMQ_MSG_ID: &'static str = "rocketmq.mgs_id";
    pub const ROCKETMQ_MSG_TYPE: &'static str = "rocketmq.mgs_type";
    pub const ROCKETMQ_REGION_ID: &'static str = "rocketmq.region_id";
    pub const ROCKETMQ_TRANSACTION_ID: &'static str = "rocketmq.transaction_id";
    pub const ROCKETMQ_TRANSACTION_STATE: &'static str = "rocketmq.transaction_state";
    pub const ROCKETMQ_IS_FROM_TRANSACTION_CHECK: &'static str = "rocketmq.is_from_transaction_check";
    pub const ROCKETMQ_RETRY_TIMERS: &'static str = "rocketmq.retry_times";
}
