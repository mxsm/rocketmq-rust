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

pub const MIN_IO_URING_KERNEL_VERSION: LinuxKernelVersion = LinuxKernelVersion {
    major: 5,
    minor: 1,
    patch: 0,
};

/// Whether this build of the store can use io_uring at all.
///
/// This only reflects the compilation target and the `io_uring` Cargo feature.
/// It does not look at the running kernel or create an io_uring instance, so
/// [`IoUringBuildSupport::Available`] does not mean io_uring is ready at runtime
/// or that any I/O path is using it. Use [`probe_io_uring_runtime_capability`]
/// for the runtime view, which combines this value with kernel and opcode checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringBuildSupport {
    Available,
    FeatureDisabled,
    UnsupportedPlatform,
}

impl IoUringBuildSupport {
    /// Returns `true` only for [`IoUringBuildSupport::Available`], meaning the
    /// build satisfies the target and feature conditions. This is not a runtime
    /// readiness check.
    #[inline]
    pub fn is_supported(self) -> bool {
        matches!(self, IoUringBuildSupport::Available)
    }

    #[inline]
    pub fn as_str(self) -> &'static str {
        match self {
            IoUringBuildSupport::Available => "available",
            IoUringBuildSupport::FeatureDisabled => "feature_disabled",
            IoUringBuildSupport::UnsupportedPlatform => "unsupported_platform",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct LinuxKernelVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl LinuxKernelVersion {
    pub fn parse_release(release: &str) -> Option<Self> {
        let mut parts = release.split('.');
        let major = parse_numeric_component(parts.next()?)?;
        let minor = parse_numeric_component(parts.next().unwrap_or("0"))?;
        let patch = parse_numeric_component(parts.next().unwrap_or("0"))?;

        Some(Self { major, minor, patch })
    }

    #[inline]
    pub fn supports_io_uring(self) -> bool {
        self >= MIN_IO_URING_KERNEL_VERSION
    }

    #[inline]
    pub fn supports_send_zc(self) -> bool {
        self >= LinuxKernelVersion {
            major: 6,
            minor: 0,
            patch: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IoUringOpcodeSupport {
    pub read: bool,
    pub write: bool,
    pub fsync: bool,
    pub registered_files: bool,
    pub fixed_buffers: bool,
    pub send_zc: bool,
}

impl IoUringOpcodeSupport {
    pub fn from_kernel_version(version: Option<LinuxKernelVersion>) -> Self {
        let Some(version) = version else {
            return Self::default();
        };

        if !version.supports_io_uring() {
            return Self::default();
        }

        Self {
            read: true,
            write: true,
            fsync: true,
            registered_files: true,
            fixed_buffers: true,
            send_zc: version.supports_send_zc(),
        }
    }

    #[inline]
    pub fn required_supported(self) -> bool {
        self.read && self.write && self.fsync
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoUringFallbackReason {
    FeatureDisabled,
    UnsupportedPlatform,
    KernelTooOld {
        required: LinuxKernelVersion,
        actual: Option<LinuxKernelVersion>,
    },
    MissingRequiredOpcodes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoUringRuntimeCapability {
    pub build_support: IoUringBuildSupport,
    pub kernel_version: Option<LinuxKernelVersion>,
    pub opcode_support: IoUringOpcodeSupport,
    pub fallback_reason: Option<IoUringFallbackReason>,
    pub experimental: bool,
}

impl IoUringRuntimeCapability {
    pub fn classify(
        build_support: IoUringBuildSupport,
        kernel_version: Option<LinuxKernelVersion>,
        opcode_support: IoUringOpcodeSupport,
    ) -> Self {
        let fallback_reason = match build_support {
            IoUringBuildSupport::FeatureDisabled => Some(IoUringFallbackReason::FeatureDisabled),
            IoUringBuildSupport::UnsupportedPlatform => Some(IoUringFallbackReason::UnsupportedPlatform),
            IoUringBuildSupport::Available => match kernel_version {
                Some(version) if version.supports_io_uring() => {
                    (!opcode_support.required_supported()).then_some(IoUringFallbackReason::MissingRequiredOpcodes)
                }
                actual => Some(IoUringFallbackReason::KernelTooOld {
                    required: MIN_IO_URING_KERNEL_VERSION,
                    actual,
                }),
            },
        };

        Self {
            build_support,
            kernel_version,
            opcode_support,
            fallback_reason,
            experimental: true,
        }
    }

    #[inline]
    pub fn basic_path_available(self) -> bool {
        self.fallback_reason.is_none()
    }
}

/// Reports whether this build supports io_uring, based only on the compilation
/// target and the `io_uring` Cargo feature. See [`IoUringBuildSupport`].
#[inline]
pub fn io_uring_build_support() -> IoUringBuildSupport {
    if !cfg!(target_os = "linux") {
        IoUringBuildSupport::UnsupportedPlatform
    } else if !cfg!(feature = "io_uring") {
        IoUringBuildSupport::FeatureDisabled
    } else {
        IoUringBuildSupport::Available
    }
}

pub fn probe_io_uring_runtime_capability() -> IoUringRuntimeCapability {
    let build_support = io_uring_build_support();
    let kernel_version = current_linux_kernel_version();
    let opcode_support = IoUringOpcodeSupport::from_kernel_version(kernel_version);

    IoUringRuntimeCapability::classify(build_support, kernel_version, opcode_support)
}

/// Returns `Ok(())` when this build supports io_uring.
///
/// Otherwise returns an [`io::ErrorKind::Unsupported`] error naming the missing
/// requirement (the `io_uring` feature or Linux). Like [`io_uring_build_support`],
/// this does not check runtime readiness.
#[inline]
pub fn ensure_io_uring_build_support() -> io::Result<()> {
    match io_uring_build_support() {
        IoUringBuildSupport::Available => Ok(()),
        IoUringBuildSupport::FeatureDisabled => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring mapped-file backend requires the io_uring feature",
        )),
        IoUringBuildSupport::UnsupportedPlatform => Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "io_uring mapped-file backend requires Linux",
        )),
    }
}

fn parse_numeric_component(component: &str) -> Option<u16> {
    let digits: String = component.chars().take_while(|ch| ch.is_ascii_digit()).collect();
    (!digits.is_empty()).then(|| digits.parse().ok()).flatten()
}

fn current_linux_kernel_version() -> Option<LinuxKernelVersion> {
    #[cfg(target_os = "linux")]
    {
        std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .ok()
            .and_then(|release| LinuxKernelVersion::parse_release(release.trim()))
    }

    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_support_matches_target_and_feature() {
        let build_support = io_uring_build_support();

        if cfg!(all(target_os = "linux", feature = "io_uring")) {
            assert_eq!(build_support, IoUringBuildSupport::Available);
            ensure_io_uring_build_support().expect("io_uring should be available");
        } else {
            assert!(!build_support.is_supported());
            assert!(ensure_io_uring_build_support().is_err());
        }
    }

    #[test]
    fn build_support_string_is_stable() {
        assert_eq!(IoUringBuildSupport::Available.as_str(), "available");
        assert_eq!(IoUringBuildSupport::FeatureDisabled.as_str(), "feature_disabled");
        assert_eq!(
            IoUringBuildSupport::UnsupportedPlatform.as_str(),
            "unsupported_platform"
        );
    }

    #[test]
    fn linux_kernel_version_parses_common_release_strings() {
        assert_eq!(
            LinuxKernelVersion::parse_release("5.15.0-105-generic"),
            Some(LinuxKernelVersion {
                major: 5,
                minor: 15,
                patch: 0,
            })
        );
        assert_eq!(
            LinuxKernelVersion::parse_release("6.8.12"),
            Some(LinuxKernelVersion {
                major: 6,
                minor: 8,
                patch: 12,
            })
        );
        assert_eq!(LinuxKernelVersion::parse_release("not-a-kernel"), None);
    }

    #[test]
    fn runtime_capability_requires_feature_platform_kernel_and_basic_opcodes() {
        let support = IoUringOpcodeSupport {
            read: true,
            write: true,
            fsync: true,
            ..IoUringOpcodeSupport::default()
        };

        let capability = IoUringRuntimeCapability::classify(
            IoUringBuildSupport::Available,
            Some(LinuxKernelVersion {
                major: 5,
                minor: 1,
                patch: 0,
            }),
            support,
        );

        assert!(capability.basic_path_available());
        assert_eq!(capability.fallback_reason, None);

        let missing_opcode = IoUringRuntimeCapability::classify(
            IoUringBuildSupport::Available,
            Some(LinuxKernelVersion {
                major: 6,
                minor: 1,
                patch: 0,
            }),
            IoUringOpcodeSupport {
                read: true,
                write: true,
                fsync: false,
                ..IoUringOpcodeSupport::default()
            },
        );

        assert!(!missing_opcode.basic_path_available());
        assert_eq!(
            missing_opcode.fallback_reason,
            Some(IoUringFallbackReason::MissingRequiredOpcodes)
        );

        let old_kernel = IoUringRuntimeCapability::classify(
            IoUringBuildSupport::Available,
            Some(LinuxKernelVersion {
                major: 5,
                minor: 0,
                patch: 21,
            }),
            support,
        );

        assert!(!old_kernel.basic_path_available());
        assert_eq!(
            old_kernel.fallback_reason,
            Some(IoUringFallbackReason::KernelTooOld {
                required: LinuxKernelVersion {
                    major: 5,
                    minor: 1,
                    patch: 0,
                },
                actual: Some(LinuxKernelVersion {
                    major: 5,
                    minor: 0,
                    patch: 21,
                }),
            })
        );
    }

    #[test]
    fn opcode_support_is_classified_from_kernel_capability_level() {
        let linux_5_1 = IoUringOpcodeSupport::from_kernel_version(Some(LinuxKernelVersion {
            major: 5,
            minor: 1,
            patch: 0,
        }));
        assert!(linux_5_1.required_supported());
        assert!(linux_5_1.registered_files);
        assert!(linux_5_1.fixed_buffers);
        assert!(!linux_5_1.send_zc);

        let linux_6_0 = IoUringOpcodeSupport::from_kernel_version(Some(LinuxKernelVersion {
            major: 6,
            minor: 0,
            patch: 0,
        }));
        assert!(linux_6_0.required_supported());
        assert!(linux_6_0.send_zc);
    }

    #[test]
    fn runtime_probe_reports_build_support_and_experimental_status() {
        let capability = probe_io_uring_runtime_capability();

        assert_eq!(capability.build_support, io_uring_build_support());
        assert!(capability.experimental);

        if !cfg!(target_os = "linux") {
            assert_eq!(
                capability.fallback_reason,
                Some(IoUringFallbackReason::UnsupportedPlatform)
            );
            assert_eq!(capability.kernel_version, None);
        } else if !cfg!(feature = "io_uring") {
            assert_eq!(capability.fallback_reason, Some(IoUringFallbackReason::FeatureDisabled));
        } else {
            assert!(capability.kernel_version.is_some());
        }
    }
}
