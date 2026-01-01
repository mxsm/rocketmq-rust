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

#[derive(Debug, Clone, Copy)]
pub enum ConsumerGroupEvent {
    /// Some consumers in the group are changed.
    Change,
    /// The group of consumer is unregistered.
    Unregister,
    /// The group of consumer is registered.
    Register,
    /// The client of this consumer is new registered.
    ClientRegister,
    /// The client of this consumer is unregistered.
    ClientUnregister,
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumer_group_event_variants() {
        let change = ConsumerGroupEvent::Change;
        let unregister = ConsumerGroupEvent::Unregister;
        let register = ConsumerGroupEvent::Register;
        let client_register = ConsumerGroupEvent::ClientRegister;
        let client_unregister = ConsumerGroupEvent::ClientUnregister;

        assert!(matches!(change, ConsumerGroupEvent::Change));
        assert!(matches!(unregister, ConsumerGroupEvent::Unregister));
        assert!(matches!(register, ConsumerGroupEvent::Register));
        assert!(matches!(client_register, ConsumerGroupEvent::ClientRegister));
        assert!(matches!(client_unregister, ConsumerGroupEvent::ClientUnregister));
    }
}
