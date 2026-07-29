use std::sync::Arc;

use rocketmq_common::common::message::message_ext::MessageExt;

use crate::consumer::listener::consume_concurrently_context::ConsumeConcurrentlyContext;
use crate::consumer::listener::consume_concurrently_status::ConsumeConcurrentlyStatus;

pub trait MessageListenerConcurrently: Sync + Send {
    fn consume_message(
        &self,
        msgs: &[&MessageExt],
        context: &mut ConsumeConcurrentlyContext,
    ) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>;
}

pub type ArcBoxMessageListenerConcurrently = Arc<Box<dyn MessageListenerConcurrently>>;

pub type MessageListenerConcurrentlyFn = Arc<
    dyn Fn(&[&MessageExt], &mut ConsumeConcurrentlyContext) -> rocketmq_error::RocketMQResult<ConsumeConcurrentlyStatus>
        + Send
        + Sync,
>;
