# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import source


class NamesrvRouteLookupContractTest(unittest.TestCase):
    def test_transport_lookup_uses_one_absolute_request_deadline(self) -> None:
        lookup = source(
            "rocketmq-namesrv/src/processor/cluster_test_request_processor/route_lookup.rs"
        )
        body = lookup[lookup.index("impl ClusterTestRouteLookup for TransportClusterTestRouteLookup") :]

        self.assertEqual(1, body.count("RequestDeadline::after(self.request_timeout)"))
        self.assertIn("self.lookup_topic_route_until(&topic, deadline)", body)
        self.assertIn("self.resolve_endpoints(deadline)", lookup)
        self.assertIn("resolver.resolve(deadline)", lookup)

    def test_lookup_shutdown_is_owned_and_awaited(self) -> None:
        lookup = source(
            "rocketmq-namesrv/src/processor/cluster_test_request_processor/route_lookup.rs"
        )
        bootstrap = source("rocketmq-namesrv/src/bootstrap.rs")

        self.assertIn("shutdown_until(ShutdownDeadline::after", lookup)
        self.assertIn("cluster_test_route_lookup.shutdown()", bootstrap)
        self.assertTrue((ROOT / "rocketmq-runtime/src/task_group.rs").is_file())


if __name__ == "__main__":
    unittest.main()
