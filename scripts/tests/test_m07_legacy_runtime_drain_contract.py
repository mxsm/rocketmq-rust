# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import source


class LegacyRuntimeDrainContractTest(unittest.TestCase):
    def test_removed_workspace_runtime_facade_stays_deleted(self) -> None:
        self.assertFalse((ROOT / "rocketmq").exists())
        self.assertFalse((ROOT / "rocketmq-rust").exists())

    def test_runtime_package_owns_lifecycle_and_shutdown(self) -> None:
        exports = source("rocketmq-runtime/src/lib.rs")
        public_api = source("rocketmq-runtime/src/public_api.rs")

        self.assertIn("pub use public_api::*", exports)
        self.assertIn("pub use crate::owner::RuntimeOwner", public_api)
        self.assertIn("pub use service_context::", exports)
        self.assertTrue((ROOT / "rocketmq-runtime/src/shutdown_deadline.rs").is_file())
        self.assertTrue((ROOT / "rocketmq-runtime/src/task_group.rs").is_file())

    def test_examples_use_fallible_client_runtime_construction(self) -> None:
        for relative in (
            "rocketmq-client/examples/support/mod.rs",
            "rocketmq-example/examples/support/mod.rs",
        ):
            example = source(relative)
            self.assertIn("ClientRuntime::try_new", example)
            self.assertNotIn("ClientRuntime::new(", example)


if __name__ == "__main__":
    unittest.main()
