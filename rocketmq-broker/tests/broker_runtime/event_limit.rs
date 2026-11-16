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
use rocketmq_protocol::protocol::header::get_broker_config_response_header::GetBrokerConfigResponseHeader;
use rocketmq_protocol::protocol::header::update_broker_config_request_header::UpdateBrokerConfigRequestHeader;

async fn read_limit_snapshot(processor: &DefaultServerProcessor) -> (u64, i32, Option<u32>) {
    let mut request = RemotingCommand::create_remoting_command(RequestCode::GetBrokerConfig);
    let response = dispatch_broker_request(processor, &mut request).await.unwrap().unwrap();
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let generation = response
        .decode_command_custom_header::<GetBrokerConfigResponseHeader>()
        .unwrap()
        .config_generation;
    let properties: HashMap<_, _> = std::str::from_utf8(response.body().unwrap())
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .collect();
    (
        generation,
        properties["maxClientEventCount"].parse().unwrap(),
        properties
            .get("maxClientEventCountRuntimeVersion")
            .map(|value| value.parse().unwrap()),
    )
}

async fn patch_limit(processor: &DefaultServerProcessor, expected: u64, value: &str) -> RemotingCommand {
    let mut request = RemotingCommand::create_request_command(
        RequestCode::UpdateBrokerConfigCas,
        UpdateBrokerConfigRequestHeader {
            expected_generation: expected,
        },
    )
    .set_body(format!("maxClientEventCount={value}"));
    request.make_custom_header_to_net();
    dispatch_broker_request(processor, &mut request).await.unwrap().unwrap()
}

#[tokio::test]
async fn d21_limit_capability_is_read_only_and_bound_to_the_committed_cas_snapshot() {
    let mut runtime = new_lite_test_runtime("d21-limit-cas-wire").await;
    assert_eq!(
        runtime
            .composition
            .state
            .config_state
            .client_event_limit_runtime_version(),
        None
    );
    let (processor, _) = runtime.init_processor_checked().unwrap();
    let before = read_limit_snapshot(&processor).await;
    assert_eq!(before.2, Some(1));
    assert!(!runtime
        .composition
        .state
        .broker_config()
        .get_properties()
        .contains_key("maxClientEventCountRuntimeVersion"));
    for invalid in ["0", "01", "10001", "2147483648"] {
        let result = patch_limit(&processor, before.0, invalid).await;
        assert_eq!(ResponseCode::from(result.code()), ResponseCode::InvalidParameter);
        assert_eq!(read_limit_snapshot(&processor).await, before);
    }
    let (first, second) = tokio::join!(
        patch_limit(&processor, before.0, "101"),
        patch_limit(&processor, before.0, "102")
    );
    let codes = [ResponseCode::from(first.code()), ResponseCode::from(second.code())];
    assert_eq!(codes.iter().filter(|code| **code == ResponseCode::Success).count(), 1);
    assert_eq!(
        codes
            .iter()
            .filter(|code| **code == ResponseCode::InvalidParameter)
            .count(),
        1
    );
    let updated = read_limit_snapshot(&processor).await;
    assert_eq!(updated.0, before.0 + 1);
    assert!([101, 102].contains(&updated.1));
    assert_eq!(updated.2, Some(1));
    let stale = patch_limit(&processor, before.0, "103").await;
    assert_eq!(ResponseCode::from(stale.code()), ResponseCode::InvalidParameter);
    assert_eq!(read_limit_snapshot(&processor).await, updated);
    assert_eq!(
        ResponseCode::from(patch_limit(&processor, updated.0, &before.1.to_string()).await.code()),
        ResponseCode::Success
    );
    assert_eq!(
        read_limit_snapshot(&processor).await,
        (updated.0 + 1, before.1, Some(1))
    );
    runtime.shutdown().await;
}

#[tokio::test]
async fn d21_limit_capability_is_not_advertised_when_arrival_listener_installation_fails() {
    let mut runtime = new_lite_test_runtime("d21-limit-missing-listener").await;
    let retained = Arc::clone(runtime.composition.state.message_store.as_ref().unwrap());
    assert!(runtime.init_processor_checked().is_err());
    let marker = runtime
        .composition
        .state
        .config_state
        .client_event_limit_runtime_version();
    drop(retained);
    runtime.shutdown().await;
    assert_eq!(marker, None);
}

#[tokio::test]
async fn d21_held_lite_manager_dispatches_new_limit_after_publication() {
    let mut runtime = new_lite_test_runtime("d21-held-lite-manager").await;
    seed_lite_query_state(&mut runtime);
    seed_lmq_offsets(&mut runtime, &[("child-a", 8), ("child-b", 12)]);
    let (processor, _) = runtime.init_processor_checked().unwrap();
    let mut next = (*runtime.runtime_state_mut().broker_config()).clone();
    next.max_client_event_count = 1;
    runtime.runtime_state_mut().set_broker_config(next).unwrap();
    let client = CheetahString::from_static_str("client-1");
    let mut request = RemotingCommand::create_request_command(
        RequestCode::TriggerLiteDispatch,
        TriggerLiteDispatchRequestHeader {
            group: "group-a".into(),
            client_id: Some(client.clone()),
        },
    );
    request.make_custom_header_to_net();
    let response = dispatch_broker_request(&processor, &mut request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ResponseCode::from(response.code()), ResponseCode::Success);
    let dispatcher = runtime.composition.state.lite_event_dispatcher().clone();
    let pending = dispatcher.pending_events(&client);
    let all = dispatcher.take_pending_events(&client);
    runtime.shutdown().await;
    assert_eq!(
        pending.len(),
        1,
        "the already constructed real manager must use the published default"
    );
    let expected: HashSet<CheetahString> = ["child-a", "child-b"]
        .into_iter()
        .map(|name| to_lmq_name("parent-topic", name).unwrap().into())
        .collect();
    assert!(pending.iter().all(|event| expected.contains(event)));
    assert_eq!(
        all.into_iter().collect::<HashSet<_>>(),
        expected,
        "deferred events must remain intact"
    );
}
