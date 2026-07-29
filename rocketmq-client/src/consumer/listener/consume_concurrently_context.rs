use rocketmq_common::common::message::message_queue::MessageQueue;

pub struct ConsumeConcurrentlyContext {
    pub(crate) message_queue: MessageQueue,
    pub(crate) delay_level_when_next_consume: i32,
    /// Explicit acknowledgement index set by the listener.
    ///
    /// - `None` means the listener did not set an explicit index; the final
    ///   status determines behaviour (`ConsumeSuccess` ⇒ ack all, `ReconsumeLater` ⇒ ack none).
    /// - `Some(n)` means indices `0..=n` were successfully handled. The value is
    ///   clamped to `[−1, msgs.len() − 1]` before use; `−1` means ack nothing.
    pub(crate) ack_index: Option<i32>,
}

impl ConsumeConcurrentlyContext {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self {
            message_queue,
            delay_level_when_next_consume: 0,
            ack_index: None,
        }
    }

    pub fn get_delay_level_when_next_consume(&self) -> i32 {
        self.delay_level_when_next_consume
    }

    pub fn set_delay_level_when_next_consume(&mut self, delay_level_when_next_consume: i32) {
        self.delay_level_when_next_consume = delay_level_when_next_consume;
    }

    pub fn get_message_queue(&self) -> &MessageQueue {
        &self.message_queue
    }

    /// Returns the explicitly set acknowledgement index, or `None` if the listener
    /// did not call [`set_ack_index`].
    pub fn get_ack_index(&self) -> Option<i32> {
        self.ack_index
    }

    /// Record that indices `0..=ack_index` were successfully processed.
    ///
    /// Call this after each successfully handled message inside a batch listener so
    /// that a subsequent `ReconsumeLater` return can correctly identify the unprocessed
    /// suffix. Negative values are accepted and mean "ack nothing".
    pub fn set_ack_index(&mut self, ack_index: i32) {
        self.ack_index = Some(ack_index);
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;
    use rocketmq_common::common::message::message_queue::MessageQueue;

    use super::ConsumeConcurrentlyContext;

    fn mq() -> MessageQueue {
        MessageQueue::from_parts(
            CheetahString::from_static_str("topic"),
            CheetahString::from_static_str("broker"),
            0,
        )
    }

    #[test]
    fn default_ack_index_is_none() {
        let ctx = ConsumeConcurrentlyContext::new(mq());
        assert_eq!(ctx.get_ack_index(), None);
    }

    #[test]
    fn set_ack_index_stores_value() {
        let mut ctx = ConsumeConcurrentlyContext::new(mq());
        ctx.set_ack_index(4);
        assert_eq!(ctx.get_ack_index(), Some(4));
    }

    #[test]
    fn set_ack_index_negative_is_accepted() {
        let mut ctx = ConsumeConcurrentlyContext::new(mq());
        ctx.set_ack_index(-1);
        assert_eq!(ctx.get_ack_index(), Some(-1));
    }

    #[test]
    fn set_ack_index_overwrite() {
        let mut ctx = ConsumeConcurrentlyContext::new(mq());
        ctx.set_ack_index(3);
        ctx.set_ack_index(7);
        assert_eq!(ctx.get_ack_index(), Some(7));
    }
}
