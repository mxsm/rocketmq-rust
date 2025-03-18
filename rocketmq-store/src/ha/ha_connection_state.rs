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

/// Connection states for High Availability service
///
/// Represents the different states a connection can be in during the HA replication process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HAConnectionState {
    /// Ready to start connection
    Ready,

    /// CommitLog consistency checking
    Handshake,

    /// Synchronizing data
    Transfer,

    /// Temporarily stop transferring
    Suspend,

    /// Connection shutdown
    Shutdown,
}

impl HAConnectionState {
    /// Convert to string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ready => "READY",
            Self::Handshake => "HANDSHAKE",
            Self::Transfer => "TRANSFER",
            Self::Suspend => "SUSPEND",
            Self::Shutdown => "SHUTDOWN",
        }
    }

    /// Check if connection is active
    pub fn is_active(&self) -> bool {
        match self {
            Self::Ready | Self::Handshake | Self::Transfer => true,
            Self::Suspend | Self::Shutdown => false,
        }
    }

    /// Check if data transfer is allowed in this state
    pub fn allows_transfer(&self) -> bool {
        *self == Self::Transfer
    }
}

impl std::fmt::Display for HAConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<u8> for HAConnectionState {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Ready,
            1 => Self::Handshake,
            2 => Self::Transfer,
            3 => Self::Suspend,
            _ => Self::Shutdown,
        }
    }
}

impl From<HAConnectionState> for u8 {
    fn from(state: HAConnectionState) -> Self {
        match state {
            HAConnectionState::Ready => 0,
            HAConnectionState::Handshake => 1,
            HAConnectionState::Transfer => 2,
            HAConnectionState::Suspend => 3,
            HAConnectionState::Shutdown => 4,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_conversion_to_string() {
        assert_eq!(HAConnectionState::Ready.as_str(), "READY");
        assert_eq!(HAConnectionState::Handshake.as_str(), "HANDSHAKE");
        assert_eq!(HAConnectionState::Transfer.as_str(), "TRANSFER");
        assert_eq!(HAConnectionState::Suspend.as_str(), "SUSPEND");
        assert_eq!(HAConnectionState::Shutdown.as_str(), "SHUTDOWN");
    }

    #[test]
    fn test_active_states() {
        assert!(HAConnectionState::Ready.is_active());
        assert!(HAConnectionState::Handshake.is_active());
        assert!(HAConnectionState::Transfer.is_active());
        assert!(!HAConnectionState::Suspend.is_active());
        assert!(!HAConnectionState::Shutdown.is_active());
    }

    #[test]
    fn test_allows_transfer() {
        assert!(!HAConnectionState::Ready.allows_transfer());
        assert!(!HAConnectionState::Handshake.allows_transfer());
        assert!(HAConnectionState::Transfer.allows_transfer());
        assert!(!HAConnectionState::Suspend.allows_transfer());
        assert!(!HAConnectionState::Shutdown.allows_transfer());
    }

    #[test]
    fn test_u8_conversion() {
        assert_eq!(u8::from(HAConnectionState::Ready), 0);
        assert_eq!(u8::from(HAConnectionState::Handshake), 1);
        assert_eq!(u8::from(HAConnectionState::Transfer), 2);
        assert_eq!(u8::from(HAConnectionState::Suspend), 3);
        assert_eq!(u8::from(HAConnectionState::Shutdown), 4);

        assert_eq!(HAConnectionState::from(0), HAConnectionState::Ready);
        assert_eq!(HAConnectionState::from(1), HAConnectionState::Handshake);
        assert_eq!(HAConnectionState::from(2), HAConnectionState::Transfer);
        assert_eq!(HAConnectionState::from(3), HAConnectionState::Suspend);
        assert_eq!(HAConnectionState::from(4), HAConnectionState::Shutdown);
        assert_eq!(HAConnectionState::from(5), HAConnectionState::Shutdown); // Default for unknown
                                                                             // values
    }
}
