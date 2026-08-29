// Copyright 2026 The RocketMQ Rust Authors
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

use super::*;

#[test]
fn canonical_route_permit_tracks_and_releases_exact_target() {
    let coordinator = DeferredGenerationHandoff::new();
    let target = DeferredGenerationTarget::pull(CheetahString::from_static_str("TopicA"), 0);

    let permit = coordinator.acquire_route(target.clone()).expect("canonical route");
    let snapshot = coordinator.snapshot();
    assert_eq!(snapshot.tracked_targets, 1);
    assert_eq!(snapshot.candidates, 1);
    assert_eq!(snapshot.targets[0].target, target);

    drop(permit);
    assert!(coordinator.zero_report().is_zero());
}

#[test]
fn shutdown_seal_rejects_new_routes_and_drains_existing_permits() {
    let coordinator = DeferredGenerationHandoff::new();
    let target = DeferredGenerationTarget::pop_lite(CheetahString::from_static_str("client-a"));
    let permit = coordinator.acquire_route(target.clone()).expect("canonical route");

    assert_eq!(coordinator.seal(), DeferredGenerationSeal::Sealed);
    assert_eq!(coordinator.seal(), DeferredGenerationSeal::AlreadySealed);
    assert!(matches!(
        coordinator.acquire_route(target),
        Err(DeferredGenerationRouteError::ShutdownSealed)
    ));
    assert!(!coordinator.zero_report().is_zero());

    drop(permit);
    assert!(coordinator.zero_report().is_zero());
}
