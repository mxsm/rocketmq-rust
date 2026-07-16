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

//! Runtime-neutral outer orchestration for loading CommitLog segments.

/// One Store operation requested by the Local CommitLog load driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitLogLoadStep {
    /// Run the optimized validated native-mmap loader.
    Optimized,
    /// Run the compatibility sequential loader.
    Sequential,
}

/// One externally observable CommitLog load decision.
#[derive(Debug, PartialEq, Eq)]
pub enum CommitLogLoadObservation<E> {
    /// The safe-load environment switch selected the sequential path.
    ForcedSequential,
    /// The optimized path completed successfully.
    OptimizedLoaded,
    /// The optimized path completed but rejected the loaded result.
    OptimizedRejected,
    /// The optimized path failed and the driver will attempt sequential fallback.
    OptimizedFailed(E),
    /// The sequential path failed at its adapter boundary.
    SequentialFailed(E),
}

/// Returns whether the legacy safe-load environment value selects sequential loading.
pub fn safe_load_requested(value: Option<&str>) -> bool {
    value.is_some_and(|value| value == "1" || value.to_lowercase() == "true")
}

/// Drives the optimized/sequential CommitLog load decision and fallback order.
///
/// The optimized path is never attempted when `force_sequential` is true. An optimized `Ok(false)`
/// is a terminal rejection and does not fall back. Only an optimized adapter error selects the
/// sequential fallback. Sequential adapter errors are observed and converted to `false`.
pub fn drive_commit_log_load<E, Execute, Observe>(
    force_sequential: bool,
    mut execute: Execute,
    mut observe: Observe,
) -> bool
where
    Execute: FnMut(CommitLogLoadStep) -> Result<bool, E>,
    Observe: FnMut(CommitLogLoadObservation<E>),
{
    if force_sequential {
        observe(CommitLogLoadObservation::ForcedSequential);
        return match execute(CommitLogLoadStep::Sequential) {
            Ok(loaded) => loaded,
            Err(error) => {
                observe(CommitLogLoadObservation::SequentialFailed(error));
                false
            }
        };
    }

    match execute(CommitLogLoadStep::Optimized) {
        Ok(true) => {
            observe(CommitLogLoadObservation::OptimizedLoaded);
            true
        }
        Ok(false) => {
            observe(CommitLogLoadObservation::OptimizedRejected);
            false
        }
        Err(error) => {
            observe(CommitLogLoadObservation::OptimizedFailed(error));
            match execute(CommitLogLoadStep::Sequential) {
                Ok(loaded) => loaded,
                Err(error) => {
                    observe(CommitLogLoadObservation::SequentialFailed(error));
                    false
                }
            }
        }
    }
}
