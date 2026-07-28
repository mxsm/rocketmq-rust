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

    def test_raw_runtime_and_detached_spawn_are_not_public_apis(self) -> None:
        handle = (ROOT / "rocketmq-runtime" / "src" / "handle.rs").read_text(encoding="utf-8")
        context = (ROOT / "rocketmq-runtime" / "src" / "service_context.rs").read_text(encoding="utf-8")
        task_group = (ROOT / "rocketmq-runtime" / "src" / "task_group.rs").read_text(encoding="utf-8")
        crate_root = (ROOT / "rocketmq-runtime" / "src" / "lib.rs").read_text(encoding="utf-8")

        self.assertIn("pub(crate) struct RuntimeHandle", handle)
        self.assertNotIn("pub fn inner(", handle)
        self.assertNotIn("pub fn spawn(", handle)
        self.assertNotIn("pub fn runtime(", context)
        self.assertNotIn("pub fn root(", task_group)
        self.assertNotIn("spawn_detached", task_group)
        self.assertNotIn("pub use handle::RuntimeHandle", crate_root)


if __name__ == "__main__":
    unittest.main()
