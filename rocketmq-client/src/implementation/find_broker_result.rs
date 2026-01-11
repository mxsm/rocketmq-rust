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

use cheetah_string::CheetahString;

#[derive(Debug, Clone, Default)]
pub struct FindBrokerResult {
    pub broker_addr: CheetahString,
    pub slave: bool,
    pub broker_version: i32,
}

impl Display for FindBrokerResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FindBrokerResult [broker_addr={}, slave={}, broker_version={}]",
            self.broker_addr, self.slave, self.broker_version
        )
    }
}

#[cfg(test)]
mod tests {
    use cheetah_string::CheetahString;

    use super::*;

    /// Test default construction of FindBrokerResult
    #[test]
    fn test_find_broker_result_default() {
        let result = FindBrokerResult::default();

        assert_eq!(result.broker_addr, CheetahString::new());
        assert!(!result.slave);
        assert_eq!(result.broker_version, 0);
    }

    /// Test construction with explicit values
    #[test]
    fn test_find_broker_result_construction() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("127.0.0.1:10911"),
            slave: false,
            broker_version: 100,
        };

        assert_eq!(result.broker_addr.as_str(), "127.0.0.1:10911");
        assert!(!result.slave);
        assert_eq!(result.broker_version, 100);
    }

    /// Test construction for master broker
    #[test]
    fn test_find_broker_result_master() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("192.168.1.100:10911"),
            slave: false,
            broker_version: 500,
        };

        assert_eq!(result.broker_addr.as_str(), "192.168.1.100:10911");
        assert!(!result.slave);
        assert_eq!(result.broker_version, 500);
    }

    /// Test construction for slave broker
    #[test]
    fn test_find_broker_result_slave() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("192.168.1.101:10911"),
            slave: true,
            broker_version: 500,
        };

        assert_eq!(result.broker_addr.as_str(), "192.168.1.101:10911");
        assert!(result.slave);
        assert_eq!(result.broker_version, 500);
    }

    /// Test Clone trait implementation
    #[test]
    fn test_find_broker_result_clone() {
        let original = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("10.0.0.1:10911"),
            slave: true,
            broker_version: 200,
        };

        let cloned = original.clone();

        assert_eq!(cloned.broker_addr, original.broker_addr);
        assert_eq!(cloned.slave, original.slave);
        assert_eq!(cloned.broker_version, original.broker_version);
    }

    /// Test Debug trait implementation
    #[test]
    fn test_find_broker_result_debug() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("localhost:10911"),
            slave: false,
            broker_version: 123,
        };

        let debug_output = format!("{:?}", result);

        assert!(debug_output.contains("FindBrokerResult"));
        assert!(debug_output.contains("localhost:10911"));
        assert!(debug_output.contains("false"));
        assert!(debug_output.contains("123"));
    }

    /// Test Display trait implementation
    #[test]
    fn test_find_broker_result_display() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("127.0.0.1:10911"),
            slave: false,
            broker_version: 100,
        };

        let display_output = format!("{}", result);

        assert_eq!(
            display_output,
            "FindBrokerResult [broker_addr=127.0.0.1:10911, slave=false, broker_version=100]"
        );
    }

    /// Test Display trait with slave broker
    #[test]
    fn test_find_broker_result_display_slave() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("192.168.1.50:10911"),
            slave: true,
            broker_version: 456,
        };

        let display_output = format!("{}", result);

        assert_eq!(
            display_output,
            "FindBrokerResult [broker_addr=192.168.1.50:10911, slave=true, broker_version=456]"
        );
    }

    /// Test with empty broker address
    #[test]
    fn test_find_broker_result_empty_address() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::new(),
            slave: false,
            broker_version: 0,
        };

        assert_eq!(result.broker_addr.as_str(), "");
        assert!(!result.slave);
        assert_eq!(result.broker_version, 0);
    }

    /// Test with various broker versions
    #[test]
    fn test_find_broker_result_broker_versions() {
        let versions = [0, 1, 100, 500, 999, i32::MAX];

        for version in versions {
            let result = FindBrokerResult {
                broker_addr: CheetahString::from_static_str("test:10911"),
                slave: false,
                broker_version: version,
            };

            assert_eq!(result.broker_version, version);
        }
    }

    /// Test with negative broker version
    #[test]
    fn test_find_broker_result_negative_version() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("test:10911"),
            slave: false,
            broker_version: -1,
        };

        assert_eq!(result.broker_version, -1);
    }

    /// Test field modification
    #[test]
    fn test_find_broker_result_field_modification() {
        let mut result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("old:10911"),
            slave: false,
            broker_version: 100,
        };

        // Modify fields
        result.broker_addr = CheetahString::from_static_str("new:10911");
        result.slave = true;
        result.broker_version = 200;

        assert_eq!(result.broker_addr.as_str(), "new:10911");
        assert!(result.slave);
        assert_eq!(result.broker_version, 200);
    }

    /// Test with different address formats
    #[test]
    fn test_find_broker_result_address_formats() {
        let addresses = [
            "127.0.0.1:10911",
            "192.168.1.100:10911",
            "broker.example.com:10911",
            "localhost:10911",
            "[::1]:10911",
            "10.0.0.1:9876",
        ];

        for addr in addresses {
            let result = FindBrokerResult {
                broker_addr: CheetahString::from_string(addr.to_string()),
                slave: false,
                broker_version: 100,
            };

            assert_eq!(result.broker_addr.as_str(), addr);
        }
    }

    /// Test Display formatting consistency
    #[test]
    fn test_find_broker_result_display_format_consistency() {
        let result = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("test:10911"),
            slave: true,
            broker_version: 789,
        };

        let display1 = format!("{}", result);
        let display2 = result.to_string();

        assert_eq!(display1, display2);
        assert!(display1.starts_with("FindBrokerResult ["));
        assert!(display1.ends_with("]"));
    }

    /// Test Clone independence
    #[test]
    fn test_find_broker_result_clone_independence() {
        let mut original = FindBrokerResult {
            broker_addr: CheetahString::from_static_str("original:10911"),
            slave: false,
            broker_version: 100,
        };

        let cloned = original.clone();

        // Modify original
        original.broker_addr = CheetahString::from_static_str("modified:10911");
        original.slave = true;
        original.broker_version = 200;

        // Cloned should remain unchanged
        assert_eq!(cloned.broker_addr.as_str(), "original:10911");
        assert!(!cloned.slave);
        assert_eq!(cloned.broker_version, 100);
    }

    /// Test usage in Option context
    #[test]
    fn test_find_broker_result_option() {
        let some_result = Some(FindBrokerResult {
            broker_addr: CheetahString::from_static_str("broker:10911"),
            slave: false,
            broker_version: 100,
        });

        let none_result: Option<FindBrokerResult> = None;

        assert!(some_result.is_some());
        if let Some(result) = some_result {
            assert_eq!(result.broker_addr.as_str(), "broker:10911");
        }
        assert!(none_result.is_none());
    }

    /// Test usage in Result context
    #[test]
    fn test_find_broker_result_result() {
        let ok_result: Result<FindBrokerResult, &str> = Ok(FindBrokerResult {
            broker_addr: CheetahString::from_static_str("success:10911"),
            slave: false,
            broker_version: 100,
        });

        let err_result: Result<FindBrokerResult, &str> = Err("Broker not found");

        assert!(ok_result.is_ok());
        if let Ok(result) = ok_result {
            assert_eq!(result.broker_addr.as_str(), "success:10911");
        }
        assert!(err_result.is_err());
    }

    /// Test in Vec collection
    #[test]
    fn test_find_broker_result_in_vec() {
        let results = [
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("broker1:10911"),
                slave: false,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("broker2:10911"),
                slave: true,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("broker3:10911"),
                slave: true,
                broker_version: 200,
            },
        ];

        assert_eq!(results.len(), 3);
        assert!(!results[0].slave);
        assert!(results[1].slave);
        assert_eq!(results[2].broker_version, 200);
    }

    /// Test filtering master brokers
    #[test]
    fn test_find_broker_result_filter_masters() {
        let results = [
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("master1:10911"),
                slave: false,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("slave1:10911"),
                slave: true,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("master2:10911"),
                slave: false,
                broker_version: 200,
            },
        ];

        let masters: Vec<_> = results.iter().filter(|r| !r.slave).collect();

        assert_eq!(masters.len(), 2);
        assert_eq!(masters[0].broker_addr.as_str(), "master1:10911");
        assert_eq!(masters[1].broker_addr.as_str(), "master2:10911");
    }

    /// Test filtering slave brokers
    #[test]
    fn test_find_broker_result_filter_slaves() {
        let results = [
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("master:10911"),
                slave: false,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("slave1:10911"),
                slave: true,
                broker_version: 100,
            },
            FindBrokerResult {
                broker_addr: CheetahString::from_static_str("slave2:10911"),
                slave: true,
                broker_version: 100,
            },
        ];

        let slaves: Vec<_> = results.iter().filter(|r| r.slave).collect();

        assert_eq!(slaves.len(), 2);
        assert!(slaves.iter().all(|s| s.slave));
    }
}
