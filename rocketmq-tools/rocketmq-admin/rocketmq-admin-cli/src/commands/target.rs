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

use rocketmq_error::RocketMQError;

#[derive(PartialEq, Debug)]
pub(crate) enum Target {
    BrokerAddr(String),
    ClusterName(String),
}

impl Target {
    pub fn new(cluster_name: &Option<String>, broker_addr: &Option<String>) -> Result<Self, RocketMQError> {
        let cluster_name: Option<String> = normalize(cluster_name);
        let broker_addr: Option<String> = normalize(broker_addr);
        match (cluster_name, broker_addr) {
            (Some(cluster_name), None) => Ok(Target::ClusterName(cluster_name)),
            (None, Some(broker_addr)) => Ok(Target::BrokerAddr(broker_addr)),
            _ => Err(RocketMQError::IllegalArgument(
                "Exactly one of broker address or cluster name must be specified".into(),
            )),
        }
    }
}

fn normalize(input: &Option<String>) -> Option<String> {
    input
        .as_ref()
        .map(|input| input.trim())
        .filter(|input| !input.is_empty())
        .map(|input| input.into())
}

#[cfg(test)]
mod tests {
    use crate::commands::target::Target;

    #[test]
    fn create_valid_cluster_name_target() {
        let cluster_name = Target::new(&Some("my-cluster".into()), &None);

        assert_eq!(Target::ClusterName("my-cluster".into()), cluster_name.unwrap());
    }

    #[test]
    fn create_valid_broker_address_target() {
        let broker_addr = Target::new(&None, &Some("127.0.0.1:1111".into()));

        assert_eq!(Target::BrokerAddr("127.0.0.1:1111".into()), broker_addr.unwrap());
    }

    #[test]
    fn create_target_too_many_parameters() {
        let result = Target::new(&Some("my-cluster".into()), &Some("127.0.0.1:1111".into()));

        assert!(result.is_err());
    }

    #[test]
    fn create_target_too_few_parameters() {
        let result = Target::new(&None, &None);

        assert!(result.is_err());
    }
}
