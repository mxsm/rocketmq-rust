use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;
use rocketmq_error::UnifiedServiceError;
use rocketmq_remoting::code::response_code::ResponseCode;
use tokio::sync::mpsc;
use tokio::time;

pub struct MessageQueueOpContext {
    total_size: AtomicU32,
    last_write_timestamp: AtomicU64,
    context_queue: Arc<mpsc::UnboundedSender<String>>,
    context_receiver: mpsc::UnboundedReceiver<String>,
    queue_capacity: usize,
}

impl MessageQueueOpContext {
    pub fn new(timestamp: u64, queue_length: usize) -> Self {
        let unbounded_channel = mpsc::unbounded_channel::<String>();
        MessageQueueOpContext {
            total_size: AtomicU32::new(0),
            last_write_timestamp: AtomicU64::new(timestamp),
            context_queue: Arc::new(unbounded_channel.0),
            context_receiver: unbounded_channel.1,
            queue_capacity: queue_length,
        }
    }

    pub async fn get_total_size(&self) -> u32 {
        self.total_size.load(Ordering::Relaxed)
    }

    pub async fn total_size_add_and_get(&self, delta: u32) -> u32 {
        self.total_size.fetch_add(delta, Ordering::AcqRel) + delta
    }

    pub async fn get_last_write_timestamp(&self) -> u64 {
        self.last_write_timestamp.load(Ordering::Relaxed)
    }

    pub async fn set_last_write_timestamp(&self, timestamp: u64) {
        self.last_write_timestamp.store(timestamp, Ordering::Release);
    }

    pub async fn push(&self, msg: String) -> RocketMQResult<()> {
        if self.context_receiver.len() > self.queue_capacity {
            return Err(RocketMQError::broker_operation_failed(
                "message_queue_push",
                ResponseCode::SystemBusy as i32,
                "queue is full",
            ));
        }
        self.context_queue
            .send(msg)
            .map_err(|_| RocketMQError::Service(UnifiedServiceError::Interrupted))
    }
    pub async fn offer(&self, item: String, timeout: std::time::Duration) -> RocketMQResult<()> {
        if let Ok(res) = time::timeout(timeout, self.push(item)).await {
            return res;
        }
        Err(RocketMQError::Timeout {
            operation: "message_queue_offer",
            timeout_ms: timeout.as_millis() as u64,
        })
    }
    pub async fn pull(&mut self) -> RocketMQResult<String> {
        if let Some(item) = self.context_receiver.recv().await {
            return Ok(item);
        }
        Err(RocketMQError::Service(UnifiedServiceError::Interrupted))
    }
    pub async fn is_empty(&self) -> bool {
        self.context_receiver.len() == 0
    }
}
