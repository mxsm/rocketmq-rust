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

use prost::Message;
use rocketmq_proxy_core::proto::v2;

#[test]
fn representative_generated_message_preserves_wire_shape() {
    let resource = v2::Resource {
        resource_namespace: "tenant-a".to_owned(),
        name: "TopicA".to_owned(),
    };

    let encoded = resource.encode_to_vec();
    assert_eq!(encoded, b"\n\x08tenant-a\x12\x06TopicA");
    assert_eq!(
        v2::Resource::decode(encoded.as_slice()).expect("decode resource"),
        resource
    );
}

#[test]
fn generated_client_and_server_api_are_available_from_core() {
    fn accept_client(_: Option<v2::messaging_service_client::MessagingServiceClient<tonic::transport::Channel>>) {}

    accept_client(None);
    assert_eq!(v2::Code::Ok as i32, 20_000);
    assert_eq!(v2::Status::default().code, 0);
}
