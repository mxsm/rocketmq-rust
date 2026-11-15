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
use serde_json::json;

fn route() -> Value {
    json!({"orderTopicConf":null,"queueDatas":[{"brokerName":"a","readQueueNums":4,"writeQueueNums":4,"perm":6,"topicSysFlag":0}],"brokerDatas":[{"cluster":"physical","brokerName":"a","brokerAddrs":{"0":"PRIVATE.internal:10911"},"zoneName":null,"enableActingMaster":false}],"filterServerTable":{},"topicQueueMappingInfo":null})
}
fn decode(value: &Value) -> NameserverRouteObservation {
    decode_nameserver_route(&serde_json::to_vec(value).unwrap(), "physical", "orders", 500).unwrap()
}

#[test]
fn complete_route_preserves_semantic_fields_and_private_debug_boundary() {
    let original = decode(&route());
    assert!(!format!("{original:?}").contains("PRIVATE"));
    let safe = original.into_pseudonymized(|_| "opaque-endpoint".to_owned());
    assert!(!safe.to_string().contains("internal"));
    for (key, value) in [("zoneName", json!("zone-a")), ("enableActingMaster", json!(true))] {
        let mut changed = route();
        changed["brokerDatas"][0][key] = value;
        assert_ne!(
            decode(&changed).into_pseudonymized(|_| "opaque-endpoint".to_owned()),
            safe
        );
    }
    let mut changed = route();
    changed["brokerDatas"][0]["cluster"] = json!("foreign");
    assert_eq!(
        decode_nameserver_route(&serde_json::to_vec(&changed).unwrap(), "physical", "orders", 500).unwrap_err(),
        NameserverObservationStatus::ScopeMismatch
    );
}

#[test]
fn strict_view_rejects_duplicates_missing_invalid_and_never_exposes_private_config() {
    for bytes in [
        br#"{"clusterTest":"false","clusterTest":"true","orderMessageEnable":"false","supportActingMaster":"false"}"#
            .as_slice(),
        b"clusterTest=false\nclusterTest=true\norderMessageEnable=false\nsupportActingMaster=false",
        b"clusterTest=false\norderMessageEnable=false",
        b"clusterTest=FALSE\norderMessageEnable=false\nsupportActingMaster=false",
    ] {
        assert!(decode_nameserver_view(bytes, 500).is_err());
    }
    let (view, rows) = decode_nameserver_view(
        b"clusterTest=false\norderMessageEnable=false\nsupportActingMaster=true\nsecret=NEVER_ECHO",
        500,
    )
    .unwrap();
    assert!(view.support_acting_master);
    assert_eq!(rows, 4);
    assert!(!format!("{view:?}").contains("NEVER_ECHO"));
    assert_eq!(
        decode_nameserver_view(
            b"clusterTest=false\norderMessageEnable=false\nsupportActingMaster=false",
            2
        )
        .unwrap_err(),
        NameserverObservationStatus::RowLimit
    );
}

#[test]
fn legacy_numeric_keys_presence_duplicates_and_bounds_are_not_silently_lost() {
    let wire = serde_json::to_string(&route()).unwrap();
    let legacy = wire.replace("\"0\":", "0:");
    assert_eq!(
        decode_nameserver_route(legacy.as_bytes(), "physical", "orders", 500)
            .unwrap()
            .normalized,
        decode(&route()).normalized
    );
    let duplicate = wire.replace(
        "\"0\":\"PRIVATE.internal:10911\"",
        "\"0\":\"a:10911\",\"0\":\"b:10911\"",
    );
    assert!(decode_nameserver_route(duplicate.as_bytes(), "physical", "orders", 500).is_err());
    let mut missing = route();
    missing.as_object_mut().unwrap().remove("orderTopicConf");
    assert_eq!(
        decode(&missing).normalized["unknown_dimensions"],
        json!(["order_config"])
    );
    assert_eq!(
        decode_nameserver_route(wire.as_bytes(), "physical", "orders", 1).unwrap_err(),
        NameserverObservationStatus::RowLimit
    );
    let mut orphan = route();
    orphan["queueDatas"][0]["brokerName"] = json!("missing");
    assert!(decode_nameserver_route(&serde_json::to_vec(&orphan).unwrap(), "physical", "orders", 500).is_err());
}

#[test]
fn static_mapping_uses_native_signed_domains_without_restricting_queue_u32_fields() {
    let mut value = route();
    value["topicQueueMappingInfo"] = json!({"a":{"topic":"orders","scope":"__global__","totalQueues":4,"bname":"a","epoch":1,"dirty":false,"currIdMap":{"0":1}}});
    assert!(decode_nameserver_route(&serde_json::to_vec(&value).unwrap(), "physical", "orders", 500).is_ok());
    value["topicQueueMappingInfo"]["a"]["totalQueues"] = json!(2147483648u64);
    assert_eq!(
        decode_nameserver_route(&serde_json::to_vec(&value).unwrap(), "physical", "orders", 500).unwrap_err(),
        NameserverObservationStatus::Invalid
    );
    value["topicQueueMappingInfo"] = Value::Null;
    value["queueDatas"][0]["readQueueNums"] = json!(u32::MAX);
    assert!(decode_nameserver_route(&serde_json::to_vec(&value).unwrap(), "physical", "orders", 500).is_ok());
}

#[test]
fn nameserver_route_request_preserves_a_canonical_private_target_and_validated_bounds() {
    let bounds = NameserverObservationBounds {
        timeout_millis: 100,
        max_body_bytes: 1024,
        max_rows: 100,
    };
    let request = NameserverObservationRequest::route("HOST-A:09876", "physical", "orders", bounds).unwrap();
    assert_eq!(request.endpoint(), "host-a:9876");
    assert!(!format!("{request:?}").contains("host-a"));
    assert!(NameserverObservationRequest::route("host-a:9876", "physical", "", bounds).is_err());
    assert!(NameserverObservationRequest::configuration("http://host-a:9876", bounds).is_err());
    assert!(NameserverObservationRequest::configuration(
        "host-a:9876",
        NameserverObservationBounds {
            max_body_bytes: 0,
            ..bounds
        }
    )
    .is_err());
}
