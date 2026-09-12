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

use super::*;
use rocketmq_protocol::code::response_code::ResponseCode;
use rocketmq_protocol::protocol::header::message_operation_header::send_message_response_header::SendMessageResponseHeader;
use rocketmq_runtime::RuntimeContext;
use rocketmq_runtime::ShutdownDeadline;
use rocketmq_transport::api::AdmissionController;
use rocketmq_transport::api::AdmissionLimits;
use rocketmq_transport::test_support::SessionTransportServer;
use rocketmq_transport::test_support::SessionTransportServerConfig;

struct TargetedRouteFixture {
    processor: Arc<ProducerRoutePreparationProcessor>,
    producer: DefaultMQProducerImpl,
    instance: Arc<MQClientInstance>,
    runtime: Arc<ClientRuntime>,
    server: Arc<SessionTransportServer>,
    server_runtime: RuntimeContext,
}

impl TargetedRouteFixture {
    async fn start() -> Self {
        let server_runtime = RuntimeContext::from_current("targeted-producer-route-test");
        let processor = Arc::new(ProducerRoutePreparationProcessor::default());
        let server = SessionTransportServer::bind(
            server_runtime.service_context("route-server"),
            SessionTransportServerConfig::loopback(),
            processor.clone(),
            Arc::new(AdmissionController::new(AdmissionLimits::default())),
        )
        .await
        .expect("bind route server");
        let address = CheetahString::from_string(server.local_addr().to_string());
        processor
            .responses
            .lock()
            .unwrap()
            .push_back(producer_default_route_response(8, &address));
        server.start().expect("start route server");

        let runtime = test_runtime();
        let config = ClientConfig {
            vip_channel_enabled: false,
            ..ClientConfig::default()
        };
        let instance = MQClientInstance::new_arc(
            config.clone(),
            0,
            "targeted-route-client",
            None,
            runtime.component("instance"),
            runtime.telemetry_handle().clone(),
            runtime.pool().request_future_holder(),
        );
        let api = instance.get_mq_client_api_impl().unwrap();
        api.update_name_server_address_list_sync(address.as_str());
        api.start().await.unwrap();
        let configured = DefaultMQProducer::builder(runtime.clone())
            .producer_group("targeted-route-group")
            .build();
        let producer = DefaultMQProducerImpl::new(
            runtime.clone(),
            config,
            configured.producer_config_snapshot().as_ref().clone(),
            None,
        );
        producer.store_state(ProducerState::Running, Ordering::SeqCst);
        producer.set_service_state(ServiceState::Running);
        producer.bind_client_instance(&instance).unwrap();
        Self {
            processor,
            producer,
            instance,
            runtime,
            server,
            server_runtime,
        }
    }

    fn accept_send(&self, queue_id: i32) {
        let mut response = RemotingCommand::create_success_response_command_with_header(
            SendMessageResponseHeader::new("targeted-message-id".into(), queue_id, 1, None, None, None),
        );
        response.try_make_custom_header_to_net().unwrap();
        self.processor.primary_responses.lock().unwrap().push_back(response);
    }

    async fn send(&self, broker: &str, timeout: u64) -> crate::ClientResult<Option<SendResult>> {
        self.producer
            .sync_send_with_message_queue_timeout(
                Message::builder()
                    .topic("targeted-topic")
                    .body_slice(b"route fixture")
                    .build_unchecked(),
                MessageQueue::from_parts("targeted-topic", broker, 2),
                timeout,
            )
            .await
    }

    async fn shutdown(self) {
        self.instance.shutdown().await;
        self.runtime.shutdown().await.assert_no_task_leak().unwrap();
        self.server
            .shutdown_until(ShutdownDeadline::after(Duration::from_secs(5)))
            .await
            .assert_no_task_leak()
            .unwrap();
        self.server_runtime
            .shutdown_tasks(Duration::from_secs(5))
            .await
            .assert_no_task_leak()
            .unwrap();
    }
}

#[tokio::test]
async fn targeted_queue_send_loads_a_cold_route_and_reuses_the_cached_address() {
    let fixture = TargetedRouteFixture::start().await;
    fixture.accept_send(2);
    fixture.accept_send(2);
    let first = fixture.send("broker-a", 2_000).await;
    let second = fixture.send("broker-a", 2_000).await;
    let processor = fixture.processor.clone();
    fixture.shutdown().await;

    for result in [first, second] {
        let result = result
            .expect("targeted send succeeds without a prior discovery call")
            .unwrap();
        assert_eq!(result.send_status, SendStatus::SendOk);
        assert_eq!(
            result.message_queue,
            Some(MessageQueue::from_parts("targeted-topic", "broker-a", 2))
        );
    }
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 2);
    assert_eq!(
        *processor.topics.lock().unwrap(),
        [CheetahString::from("targeted-topic")]
    );
}

#[tokio::test]
async fn targeted_queue_send_does_not_switch_to_an_unrequested_broker() {
    let fixture = TargetedRouteFixture::start().await;
    let result = fixture.send("missing-broker", 2_000).await;
    let processor = fixture.processor.clone();
    fixture.shutdown().await;

    assert!(result.is_err());
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        *processor.topics.lock().unwrap(),
        [CheetahString::from("targeted-topic")]
    );
}

#[tokio::test]
async fn targeted_queue_send_does_not_query_or_send_after_its_deadline() {
    let fixture = TargetedRouteFixture::start().await;
    let result = fixture.send("broker-a", 0).await;
    let processor = fixture.processor.clone();
    fixture.shutdown().await;

    assert!(result.is_err());
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 0);
    assert!(processor.topics.lock().unwrap().is_empty());
}

#[tokio::test]
async fn targeted_queue_send_propagates_route_failure_without_retrying() {
    let fixture = TargetedRouteFixture::start().await;
    *fixture.processor.responses.lock().unwrap() = [RemotingCommand::create_response_command_with_code(
        ResponseCode::SystemError,
    )]
    .into_iter()
    .collect();
    let result = fixture.send("broker-a", 2_000).await;
    let processor = fixture.processor.clone();
    fixture.shutdown().await;

    assert!(result.is_err());
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 0);
    assert_eq!(
        *processor.topics.lock().unwrap(),
        [CheetahString::from("targeted-topic")]
    );
}

#[tokio::test]
async fn targeted_queue_send_does_not_add_route_attempts_to_the_automatic_retry_path() {
    let fixture = TargetedRouteFixture::start().await;
    let mut message = Message::builder()
        .topic("targeted-topic")
        .body_slice(b"route fixture")
        .build_unchecked();
    let result = fixture
        .producer
        .send_kernel_impl_with_runtime(
            &mut message,
            &MessageQueue::from_parts("targeted-topic", "broker-a", 2),
            CommunicationMode::Sync,
            None,
            Some(&TopicPublishInfo::new()),
            RequestDeadline::from_timeout_millis(2_000),
            &fixture.producer.runtime_snapshot(),
        )
        .await;
    let processor = fixture.processor.clone();
    fixture.shutdown().await;

    assert!(matches!(result, Err(RetryInput::RouteUnavailable)));
    assert_eq!(processor.primary_sends.load(Ordering::SeqCst), 0);
    assert!(processor.topics.lock().unwrap().is_empty());
}
