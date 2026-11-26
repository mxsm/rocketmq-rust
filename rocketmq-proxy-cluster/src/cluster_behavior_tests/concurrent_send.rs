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

//! Sends of one client: normal messages overlap, ordered messages keep the lane order.

use tokio::sync::Semaphore;

use super::*;

/// Sends that wait at a shared gate, so a test can see how many overlap.
struct GatedSends {
    events: EventLog,
    entered: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
    gate: Semaphore,
}

impl GatedSends {
    fn new(events: EventLog) -> Arc<Self> {
        Arc::new(Self {
            events,
            entered: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            gate: Semaphore::new(0),
        })
    }

    fn record(&self, event: &'static str) {
        self.events.lock().expect("event log lock poisoned").push(event);
    }

    fn events(&self) -> Vec<&'static str> {
        self.events.lock().expect("event log lock poisoned").clone()
    }
}

struct GatedProducerFactory {
    sends: Arc<GatedSends>,
}

impl ClusterProducerFactory for GatedProducerFactory {
    fn create(
        &self,
        _client_runtime: Arc<ClientRuntime>,
        _domain_id: u64,
        _config: &ClusterConfig,
        producer_group: &str,
        _timeout_millis: u64,
        _rpc_hook: Option<Arc<ClientRpcHook>>,
    ) -> Box<dyn ClusterProducerIo> {
        Box::new(GatedProducer {
            sends: self.sends.clone(),
            group: producer_group.to_owned(),
            topics: Vec::new(),
        })
    }
}

struct GatedProducer {
    sends: Arc<GatedSends>,
    group: String,
    topics: Vec<CheetahString>,
}

#[async_trait]
impl ClusterProducerIo for GatedProducer {
    fn set_topics(&mut self, topics: Vec<CheetahString>) {
        self.topics = topics;
    }

    fn topics(&self) -> Vec<CheetahString> {
        self.topics.clone()
    }

    fn set_send_timeout(&mut self, _timeout_millis: u32) {}

    fn producer_group(&self) -> CheetahString {
        self.group.clone().into()
    }

    fn concurrent_handle(&self) -> Option<Box<dyn ClusterProducerIo>> {
        Some(Box::new(GatedProducer {
            sends: self.sends.clone(),
            group: self.group.clone(),
            topics: self.topics.clone(),
        }))
    }

    async fn start(&mut self) -> Result<(), CanonicalError> {
        Ok(())
    }

    async fn shutdown(&mut self) {
        self.sends.record("producer.shutdown");
    }

    async fn recall_message(
        &mut self,
        _topic: CheetahString,
        _recall_handle: CheetahString,
    ) -> Result<String, CanonicalError> {
        Err(unexpected_client_call("producer.recall_message"))
    }

    async fn fetch_publish_message_queues(&mut self, _topic: &str) -> Result<Vec<MessageQueue>, CanonicalError> {
        Err(unexpected_client_call("producer.fetch_publish_message_queues"))
    }

    async fn send(&mut self, _message: Message, _timeout_millis: u64) -> Result<Option<SendResult>, CanonicalError> {
        self.sends.entered.fetch_add(1, Ordering::AcqRel);
        let active = self.sends.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.sends.max_active.fetch_max(active, Ordering::AcqRel);
        self.sends
            .gate
            .acquire()
            .await
            .map_err(|_| unexpected_client_call("producer.send.gate_closed"))?
            .forget();
        self.sends.active.fetch_sub(1, Ordering::AcqRel);
        self.sends.record("producer.send.done");
        Ok(Some(SendResult::default()))
    }

    async fn send_to_queue(
        &mut self,
        message: Message,
        _queue: MessageQueue,
        timeout_millis: u64,
    ) -> Result<Option<SendResult>, CanonicalError> {
        self.send(message, timeout_millis).await
    }

    async fn send_batch(
        &mut self,
        _messages: Vec<Message>,
        _timeout_millis: u64,
    ) -> Result<SendResult, CanonicalError> {
        Err(unexpected_client_call("producer.send_batch"))
    }

    async fn send_batch_to_queue(
        &mut self,
        _messages: Vec<Message>,
        _queue: MessageQueue,
        _timeout_millis: u64,
    ) -> Result<SendResult, CanonicalError> {
        Err(unexpected_client_call("producer.send_batch_to_queue"))
    }
}

fn send_request(message_group: Option<&str>) -> SendMessageRequest {
    let mut message = ProxyMessage::new("TopicA", b"body".to_vec());
    if let Some(group) = message_group {
        message
            .properties_mut()
            .insert(MessageConst::PROPERTY_SHARDING_KEY.into(), group.into());
    }
    SendMessageRequest {
        messages: vec![SendMessageEntry {
            topic: ResourceIdentity::new("", "TopicA"),
            client_message_id: "client-message".to_owned(),
            message,
            queue_id: None,
            broker_name: None,
        }],
        timeout: None,
        validate_message_type: true,
    }
}

fn gated_worker_parts() -> (Arc<ScriptedClientIo>, Arc<GatedSends>, Arc<GatedProducerFactory>) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let client = Arc::new(ScriptedClientIo::new(events.clone()));
    let sends = GatedSends::new(events);
    let factory = Arc::new(GatedProducerFactory { sends: sends.clone() });
    (client, sends, factory)
}

#[tokio::test]
async fn normal_sends_of_one_client_overlap() {
    let (client, sends, factory) = gated_worker_parts();

    run_test_worker(client, factory, |executor, cancellation| async move {
        let first = executor.send_message(send_request(None), Some("client-a".to_owned()), "r1".to_owned());
        let second = executor.send_message(send_request(None), Some("client-a".to_owned()), "r2".to_owned());
        let observe = async {
            wait_until(|| sends.active.load(Ordering::Acquire) == 2).await;
            assert_eq!(executor.lanes.snapshot().current_inflight, 2);
            sends.gate.add_permits(2);
        };
        let (first, second, ()) = tokio::join!(first, second, observe);
        assert!(first.expect("first send")[0].status.is_ok());
        assert!(second.expect("second send")[0].status.is_ok());
        assert_eq!(sends.max_active.load(Ordering::Acquire), 2);
        assert_eq!(executor.lanes.snapshot().current_inflight, 0);
        cancellation.cancel();
    })
    .await;
}

#[tokio::test]
async fn message_group_sends_of_one_client_keep_the_lane_order() {
    let (client, sends, factory) = gated_worker_parts();

    run_test_worker(client, factory, |executor, cancellation| async move {
        let first = executor.send_message(
            send_request(Some("group-a")),
            Some("client-a".to_owned()),
            "r1".to_owned(),
        );
        let second = executor.send_message(
            send_request(Some("group-a")),
            Some("client-a".to_owned()),
            "r2".to_owned(),
        );
        let observe = async {
            wait_until(|| {
                sends.entered.load(Ordering::Acquire) == 1 && executor.lanes.snapshot().queued_and_active == 2
            })
            .await;
            assert_eq!(
                sends.entered.load(Ordering::Acquire),
                1,
                "the second ordered send must wait in the lane"
            );
            sends.gate.add_permits(1);
            wait_until(|| sends.entered.load(Ordering::Acquire) == 2).await;
            sends.gate.add_permits(1);
        };
        let (first, second, ()) = tokio::join!(first, second, observe);
        assert!(first.expect("first send")[0].status.is_ok());
        assert!(second.expect("second send")[0].status.is_ok());
        assert_eq!(sends.max_active.load(Ordering::Acquire), 1);
        cancellation.cancel();
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn idle_lane_keeps_its_producer_while_a_dispatched_send_runs() {
    let (client, sends, factory) = gated_worker_parts();
    let config = ClusterConfig {
        execution_lane_idle_timeout_ms: 10,
        ..ClusterConfig::default()
    };

    run_test_worker_with_config(config, client, factory, |executor, cancellation| async move {
        let send = executor.send_message(send_request(None), Some("client-a".to_owned()), "r1".to_owned());
        let observe = async {
            wait_until(|| sends.active.load(Ordering::Acquire) == 1).await;
            // Several idle periods pass while the send waits outside the lane.
            tokio::time::advance(Duration::from_millis(50)).await;
            for _ in 0..16 {
                tokio::task::yield_now().await;
            }
            assert!(!sends.events().contains(&"producer.shutdown"), "{:?}", sends.events());
            assert_eq!(executor.lanes.snapshot().active_keys, 1);
            sends.gate.add_permits(1);
        };
        let (send, ()) = tokio::join!(send, observe);
        assert!(send.expect("dispatched send")[0].status.is_ok());

        tokio::time::advance(Duration::from_millis(10)).await;
        wait_until(|| executor.lanes.snapshot().active_lane_tasks == 0).await;
        let events = sends.events();
        let done = events
            .iter()
            .position(|event| *event == "producer.send.done")
            .expect("send finished");
        let shutdown = events
            .iter()
            .position(|event| *event == "producer.shutdown")
            .expect("retired lane shut its producer down");
        assert!(done < shutdown, "events: {events:?}");
        tokio::time::resume();
        cancellation.cancel();
    })
    .await;
}
