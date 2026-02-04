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

use std::net::SocketAddr;

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub struct MessageId {
    pub address: SocketAddr,
    pub offset: i64,
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    #[test]
    fn creates_message_id_correctly() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let msg_id = MessageId {
            address: addr,
            offset: 1,
        };

        assert_eq!(msg_id.address, addr);
        assert_eq!(msg_id.offset, 1);
    }

    #[test]
    fn message_id_is_copy() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
        let original = MessageId {
            address: addr,
            offset: 10,
        };

        let copied = original;

        assert_eq!(original, copied);
    }

    #[test]
    fn message_id_equality_works() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000);

        let a = MessageId {
            address: addr,
            offset: 1,
        };

        let b = MessageId {
            address: addr,
            offset: 1,
        };

        let c = MessageId {
            address: addr,
            offset: 2,
        };

        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
