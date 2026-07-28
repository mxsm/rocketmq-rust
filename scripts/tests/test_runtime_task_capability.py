import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "scripts" / "runtime-task-escape-policy.json"


class RuntimeTaskCapabilityContract(unittest.TestCase):
    def test_escape_inventory_has_owner_deadline_and_observation_contract(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        self.assertEqual(policy["schema_version"], 1)
        required = {
            "caller",
            "owner",
            "termination",
            "join_or_deadline",
            "metrics",
            "target_capability",
            "reason",
            "remove_by",
        }
        self.assertGreater(len(policy["entries"]), 0)
        for entry in policy["entries"]:
            self.assertEqual(set(entry), required)
            self.assertTrue(all(isinstance(entry[field], str) and entry[field] for field in required))

    def test_business_modules_use_narrow_task_capabilities(self) -> None:
        business_modules = (
            ROOT / "rocketmq-controller" / "src" / "controller" / "broker_role_notifier" / "actor.rs",
            ROOT / "rocketmq-client" / "src" / "factory" / "route_update.rs",
        )
        forbidden = (
            "RuntimeHandle",
            "spawn_detached",
            "tokio::spawn(",
            "tokio::task::spawn_blocking",
            "runtime::Builder",
            ".block_on(",
        )
        for path in business_modules:
            source = path.read_text(encoding="utf-8")
            for token in forbidden:
                self.assertNotIn(token, source, f"{path.relative_to(ROOT)} contains {token}")

    def test_compatibility_task_escape_apis_are_deprecated(self) -> None:
        handle = (ROOT / "rocketmq-runtime" / "src" / "handle.rs").read_text(encoding="utf-8")
        context = (ROOT / "rocketmq-runtime" / "src" / "service_context.rs").read_text(encoding="utf-8")
        task_group = (ROOT / "rocketmq-runtime" / "src" / "task_group.rs").read_text(encoding="utf-8")

        self.assertIn("raw Tokio handles are a compatibility boundary", handle)
        self.assertIn("business modules should use task_spawner", context)
        self.assertGreaterEqual(task_group.count("#[deprecated("), 3)


if __name__ == "__main__":
    unittest.main()
