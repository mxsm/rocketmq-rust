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

use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringBackendStatus {
    Available,
    FeatureDisabled,
    UnsupportedPlatform,
}

impl IoUringBackendStatus {
    #[inline]
    pub fn is_available(self) -> bool {
        matches!(self, IoUringBackendStatus::Available)
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            IoUringBackendStatus::Available => "available",
            IoUringBackendStatus::FeatureDisabled => "feature_disabled",
            IoUringBackendStatus::UnsupportedPlatform => "unsupported_platform",
        }
    }
}

#[inline]
pub fn io_uring_backend_status() -> IoUringBackendStatus {
    if !cfg!(target_os = "linux") {
        IoUringBackendStatus::UnsupportedPlatform
    } else if !cfg!(feature = "io_uring") {
        IoUringBackendStatus::FeatureDisabled
    } else {
        IoUringBackendStatus::Available
    }
}

#[inline]
pub fn ensure_io_uring_backend_available() -> io::Result<()> {
    match io_uring_backend_status() {
        IoUringBackendStatus::Available => Ok(()),
        IoUringBackendStatus::FeatureDisabled => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring mapped-file backend requires the io_uring feature",
        )),
        IoUringBackendStatus::UnsupportedPlatform => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring mapped-file backend requires Linux",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_status_matches_target_and_feature() {
        let status = io_uring_backend_status();

        if cfg!(all(target_os = "linux", feature = "io_uring")) {
            assert_eq!(status, IoUringBackendStatus::Available);
            ensure_io_uring_backend_available().expect("io_uring should be available");
        } else {
            assert!(!status.is_available());
            assert!(ensure_io_uring_backend_available().is_err());
        }
    }

    #[test]
    fn backend_status_string_is_stable() {
        assert_eq!(IoUringBackendStatus::Available.as_str(), "available");
        assert_eq!(IoUringBackendStatus::FeatureDisabled.as_str(), "feature_disabled");
        assert_eq!(
            IoUringBackendStatus::UnsupportedPlatform.as_str(),
            "unsupported_platform"
        );
    }
}
