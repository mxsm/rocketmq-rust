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

/// Enum representing the different modes of communication.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum CommunicationMode {
    /// Synchronous communication mode.
    #[default]
    Sync,
    /// Asynchronous communication mode.
    Async,
    /// One-way communication mode.
    Oneway,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that CommunicationMode::Sync is the default variant
    #[test]
    fn test_communication_mode_default() {
        let default = CommunicationMode::default();
        assert_eq!(default, CommunicationMode::Sync);
    }

    /// Test equality comparisons between different CommunicationMode variants
    #[test]
    fn test_communication_mode_equality() {
        // Test same variants are equal
        assert_eq!(CommunicationMode::Sync, CommunicationMode::Sync);
        assert_eq!(CommunicationMode::Async, CommunicationMode::Async);
        assert_eq!(CommunicationMode::Oneway, CommunicationMode::Oneway);

        // Test different variants are not equal
        assert_ne!(CommunicationMode::Sync, CommunicationMode::Async);
        assert_ne!(CommunicationMode::Sync, CommunicationMode::Oneway);
        assert_ne!(CommunicationMode::Async, CommunicationMode::Oneway);
    }

    /// Test that CommunicationMode implements Clone correctly
    #[test]
    fn test_communication_mode_clone() {
        let sync_mode = CommunicationMode::Sync;
        let cloned_sync = sync_mode;
        assert_eq!(sync_mode, cloned_sync);

        let async_mode = CommunicationMode::Async;
        let cloned_async = async_mode;
        assert_eq!(async_mode, cloned_async);

        let oneway_mode = CommunicationMode::Oneway;
        let cloned_oneway = oneway_mode;
        assert_eq!(oneway_mode, cloned_oneway);
    }

    /// Test that CommunicationMode implements Copy trait correctly
    #[test]
    fn test_communication_mode_copy() {
        let original = CommunicationMode::Async;
        let copied = original; // Uses Copy trait
        assert_eq!(original, copied);

        // Verify original is still usable (proving Copy semantics)
        assert_eq!(original, CommunicationMode::Async);
    }

    /// Test Debug trait implementation for all variants
    #[test]
    fn test_communication_mode_debug() {
        let sync_debug = format!("{:?}", CommunicationMode::Sync);
        assert_eq!(sync_debug, "Sync");

        let async_debug = format!("{:?}", CommunicationMode::Async);
        assert_eq!(async_debug, "Async");

        let oneway_debug = format!("{:?}", CommunicationMode::Oneway);
        assert_eq!(oneway_debug, "Oneway");
    }

    /// Test pattern matching on CommunicationMode variants
    #[test]
    fn test_communication_mode_pattern_matching() {
        let sync = CommunicationMode::Sync;
        let async_mode = CommunicationMode::Async;
        let oneway = CommunicationMode::Oneway;

        match sync {
            CommunicationMode::Sync => {
                // Expected path
            }
            _ => panic!("Pattern matching failed for Sync"),
        }

        match async_mode {
            CommunicationMode::Async => {
                // Expected path
            }
            _ => panic!("Pattern matching failed for Async"),
        }

        match oneway {
            CommunicationMode::Oneway => {
                // Expected path
            }
            _ => panic!("Pattern matching failed for Oneway"),
        }
    }

    /// Test exhaustive matching to ensure all variants are handled
    #[test]
    fn test_communication_mode_exhaustive_match() {
        fn process_mode(mode: CommunicationMode) -> &'static str {
            match mode {
                CommunicationMode::Sync => "synchronous",
                CommunicationMode::Async => "asynchronous",
                CommunicationMode::Oneway => "oneway",
            }
        }

        assert_eq!(process_mode(CommunicationMode::Sync), "synchronous");
        assert_eq!(process_mode(CommunicationMode::Async), "asynchronous");
        assert_eq!(process_mode(CommunicationMode::Oneway), "oneway");
    }

    /// Test that CommunicationMode can be used in collections
    #[test]
    fn test_communication_mode_in_collections() {
        let modes = [
            CommunicationMode::Sync,
            CommunicationMode::Async,
            CommunicationMode::Oneway,
            CommunicationMode::Sync, // Duplicate
        ];

        // Test in Vec
        assert_eq!(modes.len(), 4);
        assert_eq!(modes[0], CommunicationMode::Sync);
        assert_eq!(modes[1], CommunicationMode::Async);
        assert_eq!(modes[2], CommunicationMode::Oneway);
        assert_eq!(modes[3], CommunicationMode::Sync);

        // Test uniqueness with HashSet (requires Hash trait, so this tests PartialEq)
        let unique_count = modes.iter().fold(0, |acc, mode| {
            if modes.iter().filter(|&m| m == mode).count() == 1 {
                acc + 1
            } else {
                acc
            }
        });
        assert_eq!(unique_count, 2); // Only Async and Oneway appear once
    }

    /// Test usage in Option context
    #[test]
    fn test_communication_mode_option() {
        let some_mode: Option<CommunicationMode> = Some(CommunicationMode::Sync);
        let none_mode: Option<CommunicationMode> = None;

        assert!(some_mode.is_some());
        if let Some(mode) = some_mode {
            assert_eq!(mode, CommunicationMode::Sync);
        }
        assert!(none_mode.is_none());
    }

    /// Test usage in Result context
    #[test]
    fn test_communication_mode_result() {
        fn get_mode(valid: bool) -> Result<CommunicationMode, &'static str> {
            if valid {
                Ok(CommunicationMode::Async)
            } else {
                Err("Invalid mode")
            }
        }

        let valid_result = get_mode(true);
        assert!(valid_result.is_ok());
        assert_eq!(valid_result.unwrap(), CommunicationMode::Async);

        let invalid_result = get_mode(false);
        assert!(invalid_result.is_err());
    }

    /// Test that all three variants are distinct
    #[test]
    fn test_all_variants_distinct() {
        let variants = [
            CommunicationMode::Sync,
            CommunicationMode::Async,
            CommunicationMode::Oneway,
        ];

        // Ensure each variant is different from the others
        for (i, variant1) in variants.iter().enumerate() {
            for (j, variant2) in variants.iter().enumerate() {
                if i == j {
                    assert_eq!(variant1, variant2);
                } else {
                    assert_ne!(variant1, variant2);
                }
            }
        }
    }

    /// Test behavior when used as struct fields
    #[test]
    fn test_communication_mode_in_struct() {
        #[derive(Debug, Clone, PartialEq)]
        struct MessageConfig {
            mode: CommunicationMode,
            timeout_ms: u64,
        }

        let config1 = MessageConfig {
            mode: CommunicationMode::Sync,
            timeout_ms: 3000,
        };

        let config2 = MessageConfig {
            mode: CommunicationMode::Sync,
            timeout_ms: 3000,
        };

        let config3 = MessageConfig {
            mode: CommunicationMode::Async,
            timeout_ms: 3000,
        };

        // Test equality
        assert_eq!(config1, config2);
        assert_ne!(config1, config3);

        // Test clone
        let cloned = config1.clone();
        assert_eq!(config1, cloned);

        // Test field access
        assert_eq!(config1.mode, CommunicationMode::Sync);
    }

    /// Test thread safety (Send + Sync)
    #[test]
    fn test_communication_mode_thread_safety() {
        use std::sync::Arc;
        use std::thread;

        let mode = Arc::new(CommunicationMode::Async);
        let mode_clone = Arc::clone(&mode);

        let handle = thread::spawn(move || {
            assert_eq!(*mode_clone, CommunicationMode::Async);
        });

        handle.join().unwrap();
        assert_eq!(*mode, CommunicationMode::Async);
    }

    /// Test that default variant is Sync (verifies #[default] attribute)
    #[test]
    fn test_default_is_sync() {
        let default_mode: CommunicationMode = Default::default();

        match default_mode {
            CommunicationMode::Sync => {
                // This is expected
            }
            _ => panic!("Default should be Sync variant"),
        }
    }

    /// Test size of enum (should be small, single byte for 3 variants)
    #[test]
    fn test_communication_mode_size() {
        use std::mem;

        let size = mem::size_of::<CommunicationMode>();
        // Should be 1 byte for an enum with 3 variants
        assert_eq!(size, 1, "CommunicationMode should be 1 byte");
    }

    /// Test discriminant values are consistent
    #[test]
    fn test_discriminant_consistency() {
        // Create multiple instances and verify they're consistently equal
        let sync1 = CommunicationMode::Sync;
        let sync2 = CommunicationMode::Sync;
        let async1 = CommunicationMode::Async;
        let async2 = CommunicationMode::Async;

        assert_eq!(sync1, sync2);
        assert_eq!(async1, async2);
        assert_ne!(sync1, async1);
    }
}
