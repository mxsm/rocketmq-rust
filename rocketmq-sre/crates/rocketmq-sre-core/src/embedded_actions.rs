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

/// Immutable Phase 3 action descriptor sources embedded into every planner
/// and executor process.
///
/// Keeping one list prevents the Control Plane and Executor from silently
/// loading different catalog surfaces. Exact descriptor hashes are still
/// frozen into every sealed plan.
pub const EMBEDDED_ACTION_DESCRIPTOR_YAMLS: &[&str] = &[
    include_str!("../../../config/actions/observability.logger_level_ttl.v1.yaml"),
    include_str!("../../../config/actions/proxy.scale_out_one.v1.yaml"),
    include_str!("../../../config/actions/proxy.restart_one.v1.yaml"),
    include_str!("../../../config/actions/broker.config.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/topic.config.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/subscription_group.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/consumer.request_mode.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/consumer.offset.reset_bounded.v1.yaml"),
    include_str!("../../../config/actions/topic.queue.expand_only.v1.yaml"),
    include_str!("../../../config/actions/namesrv.config.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/controller.config.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/proxy.rollout_image_canary.v1.yaml"),
    include_str!("../../../config/actions/broker.restart_one.v1.yaml"),
    include_str!("../../../config/actions/static_topic.patch_non_remap.v1.yaml"),
    include_str!("../../../config/actions/tiered.cold_data_flow.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/store.readahead.patch_allowlisted.v1.yaml"),
    include_str!("../../../config/actions/security.credential_rotate_overlap.v1.yaml"),
    include_str!("../../../config/actions/telemetry.collector.restart_one.v1.yaml"),
    include_str!("../../../config/actions/consumer.offset.clone_or_reset_broad.v1.yaml"),
    include_str!("../../../config/actions/message.direct_consume.v1.yaml"),
    include_str!("../../../config/actions/message.dlq.resend.v1.yaml"),
    include_str!("../../../config/actions/timer.switch.v1.yaml"),
    include_str!("../../../config/actions/controller.elect.v1.yaml"),
    include_str!("../../../config/actions/static_topic.remap.v1.yaml"),
    include_str!("../../../config/actions/broker.container.add_remove.v1.yaml"),
];
