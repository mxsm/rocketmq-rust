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

pub struct RemotingHelper;

impl RemotingHelper {
    pub fn parse_host_from_address(address: Option<&str>) -> String {
        match address {
            Some(addr) if !addr.is_empty() => {
                let splits: Vec<&str> = addr.split(':').collect();
                if !splits.is_empty() {
                    splits[0].to_string()
                } else {
                    String::new()
                }
            }
            _ => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_host_from_valid_address() {
        let result = RemotingHelper::parse_host_from_address(Some("127.0.0.1:8080"));
        assert_eq!(result, "127.0.0.1");
    }

    #[test]
    fn parse_host_from_address_without_port() {
        let result = RemotingHelper::parse_host_from_address(Some("localhost"));
        assert_eq!(result, "localhost");
    }

    #[test]
    fn parse_host_from_empty_address() {
        let result = RemotingHelper::parse_host_from_address(Some(""));
        assert_eq!(result, "");
    }

    #[test]
    fn parse_host_from_none_address() {
        let result = RemotingHelper::parse_host_from_address(None);
        assert_eq!(result, "");
    }
}
