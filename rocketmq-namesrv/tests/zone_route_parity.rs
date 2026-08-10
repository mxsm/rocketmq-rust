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

use cheetah_string::CheetahString;
use rocketmq_model::common::mix_all;
use rocketmq_model::version::RocketMqVersion;
use rocketmq_protocol::code::request_code::RequestCode;
use rocketmq_protocol::protocol::header::client_request_header::GetRouteInfoRequestHeader;
use rocketmq_protocol::protocol::remoting_command::RemotingCommand;
use rocketmq_protocol::protocol::route::topic_route_data::TopicRouteData;
use rocketmq_protocol::protocol::RemotingSerializable;
use serde::Deserialize;

#[path = "../src/route/zone_filter.rs"]
mod zone_filter;

use zone_filter::filter_route_by_zone;
use zone_filter::ZoneRequest;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Corpus {
    oracle: Oracle,
    route: TopicRouteData,
    cases: Vec<Case>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Oracle {
    release_baseline: String,
    source_commit: String,
    source_file: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Case {
    name: String,
    zone_mode: Option<String>,
    zone_name: Option<String>,
    modern_request: bool,
    accept_standard_json_only: bool,
    expected_brokers: Vec<String>,
    expected_byte_mode: ByteMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum ByteMode {
    Legacy,
    Standard,
}

fn corpus() -> Corpus {
    serde_json::from_str(include_str!("fixtures/zone_route_java_5_5_0.json"))
        .expect("Java zone-route corpus must remain valid JSON")
}

#[test]
fn typed_zone_filter_matches_java_5_5_0_golden_corpus() {
    let corpus = corpus();
    assert_eq!(corpus.oracle.release_baseline, "rocketmq-all-5.5.0");
    assert_eq!(corpus.oracle.source_commit.len(), 40);
    assert!(corpus.oracle.source_file.ends_with("ZoneRouteRPCHook.java"));
    assert_eq!(zone_filter::TYPED_ZONE_ROUTE_MARKER, "__rocketmqRustTypedZoneRoute");
    assert_eq!(zone_filter::TYPED_ZONE_ROUTE_ENABLED, "enabled");
    assert_eq!(zone_filter::TYPED_ZONE_ROUTE_SHADOW, "shadow");
    assert!(corpus.route.broker_datas[0]
        .zone_name()
        .expect("same broker should have a zone")
        .as_str()
        .eq_ignore_ascii_case("zone-a"));

    for case in corpus.cases {
        let version = if case.modern_request {
            RocketMqVersion::V4_9_4 as i32
        } else {
            RocketMqVersion::V4_9_3 as i32
        };
        let mut request = RemotingCommand::create_request_command(
            RequestCode::GetRouteinfoByTopic,
            GetRouteInfoRequestHeader::new(
                CheetahString::from_static_str("zone-corpus-topic"),
                Some(case.accept_standard_json_only),
            ),
        )
        .set_version(version);
        request.make_custom_header_to_net();
        if let Some(zone_mode) = case.zone_mode {
            request.add_ext_field(mix_all::ZONE_MODE, zone_mode);
        }
        if let Some(zone_name) = case.zone_name {
            request.add_ext_field(mix_all::ZONE_NAME, zone_name);
        }

        let zone_request = ZoneRequest::from_command(&request);
        let filtered = filter_route_by_zone(&corpus.route, &zone_request);
        let mut actual_brokers = filtered
            .broker_datas
            .iter()
            .map(|broker| broker.broker_name().to_string())
            .collect::<Vec<_>>();
        actual_brokers.sort();
        assert_eq!(actual_brokers, case.expected_brokers, "case {}", case.name);

        let actual_mode = if zone_request.is_enabled() {
            ByteMode::Legacy
        } else if case.modern_request || case.accept_standard_json_only {
            ByteMode::Standard
        } else {
            ByteMode::Legacy
        };
        assert_eq!(actual_mode, case.expected_byte_mode, "case {}", case.name);
        let encoded = match actual_mode {
            ByteMode::Legacy => filtered.encode(),
            ByteMode::Standard => filtered.encode_standard_json(),
        }
        .expect("golden route should encode");
        assert_eq!(
            TopicRouteData::decode(&encoded).expect("golden route should decode"),
            filtered.as_ref().clone(),
            "case {}",
            case.name
        );
    }
}
