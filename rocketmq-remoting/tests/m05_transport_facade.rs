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

use rocketmq_remoting::base::pending_request_table::PendingRequestTable as LegacyPending;
use rocketmq_remoting::codec::remoting_command_codec::RemotingCommandCodec as LegacyCodec;
use rocketmq_remoting::connection::ConnectionState as LegacyConnectionState;
use rocketmq_remoting::tls::TlsConfig as LegacyTlsConfig;
use rocketmq_remoting::transport::client::TransportClient;
use rocketmq_transport::base::pending_request_table::PendingRequestTable as CanonicalPending;
use rocketmq_transport::codec::remoting_command_codec::RemotingCommandCodec as CanonicalCodec;
use rocketmq_transport::config::TlsConfig as CanonicalTlsConfig;
use rocketmq_transport::connection::ConnectionState as CanonicalConnectionState;
use tokio_util::codec::Decoder;

fn canonical_pending(value: LegacyPending) -> CanonicalPending {
    value
}

fn canonical_codec(value: LegacyCodec) -> CanonicalCodec {
    value
}

fn canonical_tls(value: LegacyTlsConfig) -> CanonicalTlsConfig {
    value
}

fn canonical_state(value: LegacyConnectionState) -> CanonicalConnectionState {
    value
}

#[test]
fn legacy_transport_paths_are_exact_reexports_and_canonical_path_is_available() {
    let _ = canonical_pending(LegacyPending::new());
    let _ = canonical_codec(LegacyCodec::new());
    let _ = canonical_tls(LegacyTlsConfig::default());
    let _ = canonical_state(LegacyConnectionState::Healthy);
    let _type_identity = std::any::TypeId::of::<TransportClient>();
}

#[test]
fn remoting_composite_codec_keeps_the_legacy_decoder_item() {
    fn assert_command_decoder<T>()
    where
        T: Decoder<Item = rocketmq_remoting::protocol::remoting_command::RemotingCommand>,
    {
    }

    assert_command_decoder::<rocketmq_remoting::CompositeCodec>();
}
