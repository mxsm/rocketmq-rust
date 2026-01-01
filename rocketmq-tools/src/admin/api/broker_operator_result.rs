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

#[derive(Debug, Clone)]
pub struct BrokerOperatorResult {
    success_list: Vec<CheetahString>,
    failure_list: Vec<CheetahString>,
}

#[allow(dead_code)]
impl BrokerOperatorResult {
    pub fn new() -> Self {
        BrokerOperatorResult {
            success_list: Vec::new(),
            failure_list: Vec::new(),
        }
    }

    pub fn get_success_list(&self) -> &Vec<CheetahString> {
        &self.success_list
    }

    pub fn set_success_list(&mut self, success_list: Vec<CheetahString>) {
        self.success_list = success_list;
    }

    pub fn get_failure_list(&self) -> &Vec<CheetahString> {
        &self.failure_list
    }

    pub fn set_failure_list(&mut self, failure_list: Vec<CheetahString>) {
        self.failure_list = failure_list;
    }
}

impl std::fmt::Display for BrokerOperatorResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BrokerOperatorResult{{ success_list={:?}, failure_list={:?} }}",
            self.success_list, self.failure_list
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    #[test]
    fn broker_operator_result_default_values() {
        let result = BrokerOperatorResult::new();
        assert!(result.get_success_list().is_empty());
        assert!(result.get_failure_list().is_empty());
    }

    #[test]
    fn broker_operator_result_set_success_list() {
        let mut result = BrokerOperatorResult::new();
        let success_list = vec![CheetahString::from("success1"), CheetahString::from("success2")];
        result.set_success_list(success_list.clone());
        assert_eq!(result.get_success_list(), &success_list);
    }

    #[test]
    fn broker_operator_result_set_failure_list() {
        let mut result = BrokerOperatorResult::new();
        let failure_list = vec![CheetahString::from("failure1"), CheetahString::from("failure2")];
        result.set_failure_list(failure_list.clone());
        assert_eq!(result.get_failure_list(), &failure_list);
    }

    #[test]
    fn broker_operator_result_display_format() {
        let mut result = BrokerOperatorResult::new();
        let success_list = vec![CheetahString::from("success1")];
        let failure_list = vec![CheetahString::from("failure1")];
        result.set_success_list(success_list);
        result.set_failure_list(failure_list);
        let display = format!("{}", result);
        assert_eq!(
            display,
            "BrokerOperatorResult{ success_list=[\"success1\"], failure_list=[\"failure1\"] }"
        );
    }
}
