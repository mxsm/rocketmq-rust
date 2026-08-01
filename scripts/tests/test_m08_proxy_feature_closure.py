# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import load_toml


class ProxyFeatureClosureContractTests(unittest.TestCase):
    def test_default_proxy_enables_both_current_modes(self) -> None:
        features = load_toml("rocketmq-proxy/Cargo.toml")["features"]

        self.assertEqual(["cluster-mode", "local-mode"], features["default"])
        self.assertEqual(["dep:rocketmq-proxy-cluster"], features["cluster-mode"])
        self.assertEqual(["dep:rocketmq-proxy-local"], features["local-mode"])

    def test_observability_and_tiered_features_are_additive(self) -> None:
        features = load_toml("rocketmq-proxy/Cargo.toml")["features"]

        self.assertEqual(
            ["observability", "rocketmq-observability/otlp-metrics"],
            features["otlp-metrics"],
        )
        self.assertEqual(
            ["otel-traces", "rocketmq-observability/otlp-traces"],
            features["otlp-traces"],
        )
        self.assertEqual(
            ["otel-logs", "rocketmq-observability/otlp-logs"],
            features["otlp-logs"],
        )
        self.assertIn("local-mode", features["tieredstore"])
        self.assertIn("rocketmq-proxy-local/tieredstore", features["tieredstore"])

    def test_feature_closure_has_a_real_cargo_metadata_test(self) -> None:
        self.assertTrue((ROOT / "rocketmq-proxy/tests/proxy_feature_closure.rs").is_file())


if __name__ == "__main__":
    unittest.main()
