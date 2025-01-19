/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::fmt;

use cheetah_string::CheetahString;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaIdentity {
    broker_name: CheetahString,
    broker_id: u64,
    broker_address: CheetahString,
    alive: bool,
}

impl ReplicaIdentity {
    pub fn new(
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        broker_address: impl Into<CheetahString>,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_id,
            broker_address: broker_address.into(),
            alive: false,
        }
    }

    pub fn new_with_alive(
        broker_name: impl Into<CheetahString>,
        broker_id: u64,
        broker_address: impl Into<CheetahString>,
        alive: bool,
    ) -> Self {
        Self {
            broker_name: broker_name.into(),
            broker_id,
            broker_address: broker_address.into(),
            alive,
        }
    }

    pub fn get_broker_name(&self) -> &CheetahString {
        &self.broker_name
    }

    pub fn set_broker_name(&mut self, broker_name: impl Into<CheetahString>) {
        self.broker_name = broker_name.into();
    }

    pub fn get_broker_address(&self) -> &CheetahString {
        &self.broker_address
    }

    pub fn set_broker_address(&mut self, broker_address: impl Into<CheetahString>) {
        self.broker_address = broker_address.into();
    }

    pub fn get_broker_id(&self) -> u64 {
        self.broker_id
    }

    pub fn set_broker_id(&mut self, broker_id: u64) {
        self.broker_id = broker_id;
    }

    pub fn get_alive(&self) -> bool {
        self.alive
    }

    pub fn set_alive(&mut self, alive: bool) {
        self.alive = alive;
    }
}

impl fmt::Display for ReplicaIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReplicaIdentity{{ broker_name: '{}', broker_id: {}, broker_address: '{}', alive: {} \
             }}",
            self.broker_name, self.broker_id, self.broker_address, self.alive
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn new_creates_instance_with_default_alive() {
        let replica = ReplicaIdentity::new("broker1", 1, "address1");
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker1"));
        assert_eq!(replica.get_broker_id(), 1);
        assert_eq!(
            replica.get_broker_address(),
            &CheetahString::from("address1")
        );
        assert!(!replica.get_alive());
    }

    #[test]
    fn new_with_alive_creates_instance_with_specified_alive() {
        let replica = ReplicaIdentity::new_with_alive("broker1", 1, "address1", true);
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker1"));
        assert_eq!(replica.get_broker_id(), 1);
        assert_eq!(
            replica.get_broker_address(),
            &CheetahString::from("address1")
        );
        assert!(replica.get_alive());
    }

    #[test]
    fn set_broker_name_updates_broker_name() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_name("broker2");
        assert_eq!(replica.get_broker_name(), &CheetahString::from("broker2"));
    }

    #[test]
    fn set_broker_address_updates_broker_address() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_address("address2");
        assert_eq!(
            replica.get_broker_address(),
            &CheetahString::from("address2")
        );
    }

    #[test]
    fn set_broker_id_updates_broker_id() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_broker_id(2);
        assert_eq!(replica.get_broker_id(), 2);
    }

    #[test]
    fn set_alive_updates_alive_status() {
        let mut replica = ReplicaIdentity::new("broker1", 1, "address1");
        replica.set_alive(true);
        assert!(replica.get_alive());
    }

    #[test]
    fn display_formats_correctly() {
        let replica = ReplicaIdentity::new_with_alive("broker1", 1, "address1", true);
        let display = format!("{}", replica);
        assert_eq!(
            display,
            "ReplicaIdentity{ broker_name: 'broker1', broker_id: 1, broker_address: 'address1', \
             alive: true }"
        );
    }
}
