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

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum ConnectionNetEvent {
    CONNECTED(SocketAddr),
    DISCONNECTED,
    EXCEPTION,
    IDLE,
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn create_socket_addr(ip: &str, port: u16) -> SocketAddr {
        format!("{}:{}", ip, port).parse().unwrap()
    }

    #[test]
    fn test_connected_event_creation() {
        let addr = create_socket_addr("127.0.0.1", 9876);
        let event = ConnectionNetEvent::CONNECTED(addr);

        match event {
            ConnectionNetEvent::CONNECTED(a) => assert_eq!(a, addr),
            _ => panic!("Expected CONNECTED event"),
        }
    }

    #[test]
    fn test_disconnected_event_creation() {
        let event = ConnectionNetEvent::DISCONNECTED;
        assert!(matches!(event, ConnectionNetEvent::DISCONNECTED));
    }

    #[test]
    fn test_exception_event_creation() {
        let event = ConnectionNetEvent::EXCEPTION;
        assert!(matches!(event, ConnectionNetEvent::EXCEPTION));
    }

    #[test]
    fn test_idle_event_creation() {
        let event = ConnectionNetEvent::IDLE;
        assert!(matches!(event, ConnectionNetEvent::IDLE));
    }

    #[test]
    fn test_event_equality() {
        let addr1 = create_socket_addr("192.168.1.100", 10911);
        let addr2 = create_socket_addr("192.168.1.100", 10911);
        let addr3 = create_socket_addr("192.168.1.101", 10911);

        // CONNECTED events with same address should be equal
        assert_eq!(
            ConnectionNetEvent::CONNECTED(addr1),
            ConnectionNetEvent::CONNECTED(addr2)
        );

        // CONNECTED events with different addresses should not be equal
        assert_ne!(
            ConnectionNetEvent::CONNECTED(addr1),
            ConnectionNetEvent::CONNECTED(addr3)
        );

        // Same variant events should be equal
        assert_eq!(ConnectionNetEvent::DISCONNECTED, ConnectionNetEvent::DISCONNECTED);
        assert_eq!(ConnectionNetEvent::EXCEPTION, ConnectionNetEvent::EXCEPTION);
        assert_eq!(ConnectionNetEvent::IDLE, ConnectionNetEvent::IDLE);

        // Different variant events should not be equal
        assert_ne!(ConnectionNetEvent::DISCONNECTED, ConnectionNetEvent::EXCEPTION);
        assert_ne!(ConnectionNetEvent::EXCEPTION, ConnectionNetEvent::IDLE);
        assert_ne!(ConnectionNetEvent::IDLE, ConnectionNetEvent::DISCONNECTED);
    }

    #[test]
    fn test_event_clone() {
        let addr = create_socket_addr("10.0.0.1", 8080);
        let event1 = ConnectionNetEvent::CONNECTED(addr);
        let event2 = event1;

        assert_eq!(event1, event2);

        let event3 = ConnectionNetEvent::DISCONNECTED;
        let event4 = event3;
        assert_eq!(event3, event4);
    }

    #[test]
    fn test_event_copy() {
        let addr = create_socket_addr("172.16.0.1", 3000);
        let event1 = ConnectionNetEvent::CONNECTED(addr);
        let event2 = event1; // Copy occurs here

        // Both should be usable after copy
        assert_eq!(event1, event2);
        match (event1, event2) {
            (ConnectionNetEvent::CONNECTED(a1), ConnectionNetEvent::CONNECTED(a2)) => {
                assert_eq!(a1, a2);
            }
            _ => panic!("Expected CONNECTED events"),
        }
    }

    #[test]
    fn test_event_hash() {
        let addr1 = create_socket_addr("192.168.1.1", 9000);
        let addr2 = create_socket_addr("192.168.1.1", 9000);
        let addr3 = create_socket_addr("192.168.1.2", 9000);

        let mut set = HashSet::new();

        // Insert first CONNECTED event
        set.insert(ConnectionNetEvent::CONNECTED(addr1));
        assert_eq!(set.len(), 1);

        // Insert duplicate CONNECTED event (same address)
        set.insert(ConnectionNetEvent::CONNECTED(addr2));
        assert_eq!(set.len(), 1); // Should not increase

        // Insert different CONNECTED event (different address)
        set.insert(ConnectionNetEvent::CONNECTED(addr3));
        assert_eq!(set.len(), 2);

        // Insert other event types
        set.insert(ConnectionNetEvent::DISCONNECTED);
        assert_eq!(set.len(), 3);

        set.insert(ConnectionNetEvent::EXCEPTION);
        assert_eq!(set.len(), 4);

        set.insert(ConnectionNetEvent::IDLE);
        assert_eq!(set.len(), 5);

        // Insert duplicate of variant without data
        set.insert(ConnectionNetEvent::IDLE);
        assert_eq!(set.len(), 5); // Should not increase
    }

    #[test]
    fn test_event_ordering() {
        let addr1 = create_socket_addr("10.0.0.1", 1000);
        let addr2 = create_socket_addr("10.0.0.2", 1000);

        // Test ordering of CONNECTED events by address
        let event1 = ConnectionNetEvent::CONNECTED(addr1);
        let event2 = ConnectionNetEvent::CONNECTED(addr2);
        assert!(event1 < event2);

        // Test ordering between different variants
        // The order is determined by the enum variant order
        assert!(ConnectionNetEvent::CONNECTED(addr1) < ConnectionNetEvent::DISCONNECTED);
        assert!(ConnectionNetEvent::DISCONNECTED < ConnectionNetEvent::EXCEPTION);
        assert!(ConnectionNetEvent::EXCEPTION < ConnectionNetEvent::IDLE);
    }

    #[test]
    fn test_event_debug_format() {
        let addr = create_socket_addr("127.0.0.1", 12345);

        let connected = ConnectionNetEvent::CONNECTED(addr);
        let debug_str = format!("{:?}", connected);
        assert!(debug_str.contains("CONNECTED"));
        assert!(debug_str.contains("127.0.0.1"));
        assert!(debug_str.contains("12345"));

        let disconnected = ConnectionNetEvent::DISCONNECTED;
        assert_eq!(format!("{:?}", disconnected), "DISCONNECTED");

        let exception = ConnectionNetEvent::EXCEPTION;
        assert_eq!(format!("{:?}", exception), "EXCEPTION");

        let idle = ConnectionNetEvent::IDLE;
        assert_eq!(format!("{:?}", idle), "IDLE");
    }

    #[test]
    fn test_connected_with_ipv6() {
        let addr: SocketAddr = "[::1]:8080".parse().unwrap();
        let event = ConnectionNetEvent::CONNECTED(addr);

        match event {
            ConnectionNetEvent::CONNECTED(a) => {
                assert_eq!(a, addr);
                assert!(a.is_ipv6());
            }
            _ => panic!("Expected CONNECTED event"),
        }
    }

    #[test]
    fn test_event_pattern_matching() {
        let addr = create_socket_addr("192.168.0.1", 5000);
        let events = vec![
            ConnectionNetEvent::CONNECTED(addr),
            ConnectionNetEvent::DISCONNECTED,
            ConnectionNetEvent::EXCEPTION,
            ConnectionNetEvent::IDLE,
        ];

        let mut connected_count = 0;
        let mut disconnected_count = 0;
        let mut exception_count = 0;
        let mut idle_count = 0;

        for event in events {
            match event {
                ConnectionNetEvent::CONNECTED(_) => connected_count += 1,
                ConnectionNetEvent::DISCONNECTED => disconnected_count += 1,
                ConnectionNetEvent::EXCEPTION => exception_count += 1,
                ConnectionNetEvent::IDLE => idle_count += 1,
            }
        }

        assert_eq!(connected_count, 1);
        assert_eq!(disconnected_count, 1);
        assert_eq!(exception_count, 1);
        assert_eq!(idle_count, 1);
    }

    #[test]
    fn test_event_in_option() {
        let addr = create_socket_addr("10.10.10.10", 7777);
        let some_event: Option<ConnectionNetEvent> = Some(ConnectionNetEvent::CONNECTED(addr));
        let none_event: Option<ConnectionNetEvent> = None;

        assert!(some_event.is_some());
        assert!(none_event.is_none());

        if let Some(ConnectionNetEvent::CONNECTED(a)) = some_event {
            assert_eq!(a, addr);
        } else {
            panic!("Expected Some(CONNECTED)");
        }
    }

    #[test]
    fn test_event_in_result() {
        let addr = create_socket_addr("172.16.1.1", 6666);
        let ok_event: Result<ConnectionNetEvent, &str> = Ok(ConnectionNetEvent::CONNECTED(addr));
        let err_event: Result<ConnectionNetEvent, &str> = Err("connection failed");

        assert!(ok_event.is_ok());
        assert!(err_event.is_err());

        match ok_event {
            Ok(ConnectionNetEvent::CONNECTED(a)) => assert_eq!(a, addr),
            _ => panic!("Expected Ok(CONNECTED)"),
        }
    }

    #[test]
    fn test_multiple_connected_events_different_ports() {
        let addr1 = create_socket_addr("192.168.1.100", 8000);
        let addr2 = create_socket_addr("192.168.1.100", 8001);
        let addr3 = create_socket_addr("192.168.1.100", 8002);

        let event1 = ConnectionNetEvent::CONNECTED(addr1);
        let event2 = ConnectionNetEvent::CONNECTED(addr2);
        let event3 = ConnectionNetEvent::CONNECTED(addr3);

        // All should be different
        assert_ne!(event1, event2);
        assert_ne!(event2, event3);
        assert_ne!(event1, event3);

        // Test ordering
        assert!(event1 < event2);
        assert!(event2 < event3);
    }

    #[test]
    fn test_event_size() {
        use std::mem::size_of;

        // Verify the enum size is reasonable
        let size = size_of::<ConnectionNetEvent>();

        // SocketAddr is either SocketAddrV4 (16 bytes) or SocketAddrV6 (28 bytes)
        // Plus enum discriminant (typically 1-8 bytes depending on alignment)
        // The size should be reasonable (not too large)
        assert!(size <= 32, "ConnectionNetEvent size is {} bytes", size);
    }
}
