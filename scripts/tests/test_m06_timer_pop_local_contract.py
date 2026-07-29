# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import source


class M06TimerPopLocalContractTests(unittest.TestCase):
    def test_local_package_owns_timer_and_pop_value_layouts(self) -> None:
        for relative in (
            "rocketmq-store-local/src/timer/checkpoint.rs",
            "rocketmq-store-local/src/timer/timer_log.rs",
            "rocketmq-store-local/src/pop/ack_msg.rs",
            "rocketmq-store-local/src/pop/batch_ack_msg.rs",
            "rocketmq-store-local/src/pop/pop_check_point.rs",
        ):
            self.assertTrue((ROOT / relative).is_file(), relative)

    def test_store_timer_and_pop_paths_use_local_owners(self) -> None:
        timer = source("rocketmq-store/src/timer/timer_message_store.rs")
        checkpoint = source("rocketmq-store/src/timer/timer_checkpoint.rs")
        pop = source("rocketmq-store/src/pop/pop_check_point.rs")

        self.assertIn("rocketmq_store_local::timer::service", timer)
        self.assertIn("rocketmq_store_local::timer::checkpoint", checkpoint)
        self.assertIn("pub use rocketmq_store_local::pop", pop)


if __name__ == "__main__":
    unittest.main()
