# Copyright 2023 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class M06TimerPopLocalContractTests(unittest.TestCase):
    def test_timer_delivery_uses_shared_append_boundary(self) -> None:
        timer = source("rocketmq-store/src/timer/timer_message_store.rs").split(
            "#[cfg(test)]\nmod tests", maxsplit=1
        )[0]
        local_store = source(
            "rocketmq-store/src/message_store/local_file_message_store.rs"
        ).split("#[cfg(test)]\nmod tests", maxsplit=1)[0]
        commit_log = source("rocketmq-store/src/log_file/commit_log.rs").split(
            "#[cfg(test)]\nmod tests", maxsplit=1
        )[0]

        self.assertNotIn(".mut_from_ref()", timer)
        self.assertEqual(timer.count(".put_message_shared("), 2)
        self.assertIn("pub(crate) async fn put_message_shared(&self", local_store)
        self.assertIn("pub(crate) async fn put_messages_shared(&self", local_store)
        self.assertIn("self.put_message_shared(msg).await", local_store)
        self.assertIn("self.put_messages_shared(message_ext_batch).await", local_store)
        self.assertIn("pub async fn put_message(&self", commit_log)
        self.assertIn("pub async fn put_messages(&self", commit_log)

    def test_timer_pop_services_stats_filter_and_hooks_have_local_owners(self) -> None:
        local_root = source("rocketmq-store-local/src/lib.rs")
        local_timer_root = source("rocketmq-store-local/src/timer.rs")
        local_timer_service = source("rocketmq-store-local/src/timer/service.rs")
        local_checkpoint = source("rocketmq-store-local/src/timer/checkpoint.rs")
        local_metrics = source("rocketmq-store-local/src/timer/metrics.rs")
        local_pop = source("rocketmq-store-local/src/pop.rs")
        local_ack = source("rocketmq-store-local/src/pop/ack_msg.rs")
        local_batch_ack = source("rocketmq-store-local/src/pop/batch_ack_msg.rs")
        local_checkpoint_pop = source("rocketmq-store-local/src/pop/pop_check_point.rs")
        local_stats = source("rocketmq-store-local/src/stats/store_stats.rs")
        local_filter = source("rocketmq-store-local/src/filter.rs")
        local_hooks = source("rocketmq-store-local/src/hook.rs")
        local_cold_service = source(
            "rocketmq-store-local/src/services/cold_data_check.rs"
        )

        for module in ("filter", "hook", "pop", "services", "stats", "timer"):
            self.assertIn(f"pub mod {module};", local_root)
        for module in ("checkpoint", "metrics", "service", "slot", "timer_log", "timer_wheel"):
            self.assertIn(f"pub mod {module};", local_timer_root)

        for owner in (
            "pub struct TimerLogRecord",
            "pub struct TimerSchedulePolicy",
            "pub struct TimerBacklogMetrics",
            "pub struct TimerTpsCounter",
            "pub fn recover_timer_log_len(",
            "pub fn clamp_queue_offset(",
            "pub fn timer_slot_is_valid(",
        ):
            self.assertIn(owner, local_timer_service)
        for owner in (
            "pub struct TimerCheckpointRecord",
            "pub struct TimerCheckpointState",
            "pub struct TimerCheckpointVersion",
        ):
            self.assertIn(owner, local_checkpoint)
        for owner in ("pub struct TimerMetric", "pub struct TimerMetricsState"):
            self.assertIn(owner, local_metrics)

        self.assertIn("pub trait AckMessage", local_pop)
        self.assertIn("pub struct AckMsg", local_ack)
        self.assertIn("pub struct BatchAckMsg", local_batch_ack)
        self.assertIn("pub struct PopCheckPoint", local_checkpoint_pop)
        self.assertIn("pub struct StoreStatsState", local_stats)
        self.assertIn("pub trait MessageFilter", local_filter)
        self.assertIn("pub struct HookRegistry", local_hooks)
        self.assertIn("pub struct ColdDataCheckService", local_cold_service)

    def test_store_is_a_protocol_lifecycle_and_external_effect_adapter(self) -> None:
        store_timer = source("rocketmq-store/src/timer/timer_message_store.rs")
        store_checkpoint = source("rocketmq-store/src/timer/timer_checkpoint.rs")
        store_metrics = source("rocketmq-store/src/timer/timer_metrics.rs")
        store_stats = source("rocketmq-store/src/base/store_stats_service.rs")
        store_message_store = source(
            "rocketmq-store/src/message_store/local_file_message_store.rs"
        )

        for owner in (
            "TimerSchedulePolicy",
            "TimerLogRecord",
            "TimerBacklogMetrics",
            "TimerTpsCounter",
            "timer_slot_is_valid",
        ):
            self.assertIn(owner, store_timer)
        self.assertIn("MessageExt", store_timer)
        self.assertIn("LocalFileMessageStore", store_timer)
        self.assertNotIn("struct TimerLogRecord", store_timer)
        self.assertNotIn("struct TimerBacklogMetrics", store_timer)

        self.assertIn("TimerCheckpointState", store_checkpoint)
        self.assertIn("DataVersion", store_checkpoint)
        self.assertIn("OpenOptions", store_checkpoint)
        self.assertIn("TimerMetricsState", store_metrics)
        self.assertIn("ConfigManager", store_metrics)
        self.assertIn("DataVersion", store_metrics)

        self.assertIn("StoreStatsState", store_stats)
        self.assertIn("impl Deref for StoreStatsService", store_stats)
        self.assertIn("BrokerIdentity", store_stats)
        self.assertIn("ScheduledTaskGroup", store_stats)
        self.assertNotIn("struct CallSnapshot", store_stats)
        self.assertNotIn("put_message_topic_times_total:", store_stats)

        self.assertIn("HookRegistry<dyn PutMessageHook + Send + Sync>", store_message_store)
        self.assertIn("HookRegistry::new()", store_message_store)

    def test_legacy_leaf_paths_are_compatibility_reexports(self) -> None:
        reexports = {
            "rocketmq-store/src/filter.rs": (
                "pub use rocketmq_store_local::filter::MessageFilter;"
            ),
            "rocketmq-store/src/log_file/cold_data_check_service.rs": (
                "pub use rocketmq_store_local::services::cold_data_check::ColdDataCheckService;"
            ),
            "rocketmq-store/src/pop/ack_msg.rs": (
                "pub use rocketmq_store_local::pop::ack_msg::AckMsg;"
            ),
            "rocketmq-store/src/pop/batch_ack_msg.rs": (
                "pub use rocketmq_store_local::pop::batch_ack_msg::BatchAckMsg;"
            ),
            "rocketmq-store/src/pop/pop_check_point.rs": (
                "pub use rocketmq_store_local::pop::pop_check_point::PopCheckPoint;"
            ),
            "rocketmq-store/src/stats/stats_type.rs": (
                "pub use rocketmq_store_local::stats::stats_type::StatsType;"
            ),
            "rocketmq-store/src/timer/slot.rs": (
                "pub use rocketmq_store_local::timer::slot::Slot;"
            ),
            "rocketmq-store/src/timer/timer_log.rs": (
                "pub use rocketmq_store_local::timer::timer_log::TimerLog;"
            ),
            "rocketmq-store/src/timer/timer_wheel.rs": (
                "pub use rocketmq_store_local::timer::timer_wheel::TimerWheel;"
            ),
        }
        for path, statement in reexports.items():
            self.assertIn(statement, source(path), path)

    def test_local_sources_keep_forbidden_facade_edges_out(self) -> None:
        local_manifest = source("rocketmq-store-local/Cargo.toml")
        local_sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in sorted((ROOT / "rocketmq-store-local/src").rglob("*.rs"))
        )

        self.assertNotIn("rocketmq-common", local_manifest)
        self.assertNotIn("rocketmq-remoting", local_manifest)
        self.assertNotIn("rocketmq-store =", local_manifest)
        self.assertNotIn("rocketmq_common", local_sources)
        self.assertNotIn("rocketmq_remoting", local_sources)
        self.assertNotIn("pub struct LocalFileMessageStore {", local_sources)
        self.assertNotIn("BrokerIdentity", local_sources)
        self.assertNotIn("DataVersion", local_sources)
        self.assertNotIn("tokio::spawn", local_sources)


if __name__ == "__main__":
    unittest.main()
