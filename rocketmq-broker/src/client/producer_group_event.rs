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

use std::fmt::Display;

/// Producer group events that occur when producers register or unregister.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerGroupEvent {
    /// The group of producer is unregistered.
    GroupUnregister,
    /// The client of this producer is unregistered.
    ClientUnregister,
}

impl Display for ProducerGroupEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProducerGroupEvent::GroupUnregister => write!(f, "GROUP_UNREGISTER"),
            ProducerGroupEvent::ClientUnregister => write!(f, "CLIENT_UNREGISTER"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn producer_group_event_display_group_unregister() {
        let event = ProducerGroupEvent::GroupUnregister;
        assert_eq!(event.to_string(), "GROUP_UNREGISTER");
    }

    #[test]
    fn producer_group_event_display_client_unregister() {
        let event = ProducerGroupEvent::ClientUnregister;
        assert_eq!(event.to_string(), "CLIENT_UNREGISTER");
    }

    #[test]
    fn producer_group_event_equality_same_variant() {
        let event1 = ProducerGroupEvent::GroupUnregister;
        let event2 = ProducerGroupEvent::GroupUnregister;
        assert_eq!(event1, event2);
    }

    #[test]
    fn producer_group_event_equality_different_variants() {
        let event1 = ProducerGroupEvent::GroupUnregister;
        let event2 = ProducerGroupEvent::ClientUnregister;
        assert_ne!(event1, event2);
    }
}
