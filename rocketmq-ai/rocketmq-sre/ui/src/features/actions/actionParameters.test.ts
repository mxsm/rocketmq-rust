import { describe, expect, it } from "vitest";
import { ACTION_IDS, actionDefinition, type TypedActionId } from "./actionDefinitions";
import { buildActionParameters, initialActionDraft, reviewedActionParameters, validActionResource } from "./actionParameters";

// Synthetic parameter models, not target qualification or an Agent observation.
export const parameterCases: { id: TypedActionId; values: Record<string, string>; expected: object }[] = [
  { id: ACTION_IDS[0], values: { ttl_seconds: "900" }, expected: { component: "broker", logger: "rocketmq_broker::processor", level: "DEBUG", ttl_seconds: 900 } },
  { id: ACTION_IDS[1], values: { broker: "127.0.0.1:10911", expected_generation: "4", "patch.send_message_thread_pool_nums": "16" }, expected: { broker: "127.0.0.1:10911", expected_generation: 4, patch: { send_message_thread_pool_nums: 16 } } },
  { id: ACTION_IDS[2], values: { topic: "orders", expected_version: "0", "patch.order": "false" }, expected: { topic: "orders", expected_version: 0, patch: { order: false } } },
  { id: ACTION_IDS[3], values: { group: "orders-group", expected_version: "2", "patch.consume_timeout_minutes": "60" }, expected: { group: "orders-group", expected_version: 2, patch: { consume_timeout_minutes: 60 } } },
  { id: ACTION_IDS[4], values: { namespace: "messaging", workload: "proxy", expected_replicas: "3" }, expected: { namespace: "messaging", workload: "proxy", expected_replicas: 3 } },
  { id: ACTION_IDS[5], values: { namespace: "messaging", pod: "proxy-1", expected_uid: "uid-1" }, expected: { namespace: "messaging", pod: "proxy-1", expected_uid: "uid-1" } },
  { id: ACTION_IDS[6], values: { namespace: "messaging", workload: "proxy", container: "proxy", expected_generation: "8", image_digest: "sha256:" + "a".repeat(64) }, expected: { namespace: "messaging", workload: "proxy", container: "proxy", expected_generation: 8, image_digest: "sha256:" + "a".repeat(64), canary_replicas: 1 } },
  { id: ACTION_IDS[7], values: { namespace: "monitoring", pod: "collector-1", expected_uid: "uid-2", pipeline: "metrics" }, expected: { namespace: "monitoring", pod: "collector-1", expected_uid: "uid-2", pipeline: "metrics" } },
  { id: ACTION_IDS[8], values: { credential_set: "broker-api", active_version: "v1", candidate_version: "v2", candidate_secret_ref: "vault://messaging/broker-api/v2", overlap_seconds: "300", validation_probe_topic: "SRE_PROBE_ROTATION" }, expected: { credential_set: "broker-api", active_version: "v1", candidate_version: "v2", candidate_secret_ref: "vault://messaging/broker-api/v2", overlap_seconds: 300, validation_probe_topic: "SRE_PROBE_ROTATION" } },
];

describe("closed wired-action parameter models", () => {
  it.each(parameterCases)("builds the exact existing $id payload and reviews the saved payload", ({ id, values, expected }) => {
    const draft = initialActionDraft(id, "declared/target");
    draft.values = { ...draft.values, ...values };
    expect(buildActionParameters(draft)).toEqual(expected);
    expect(reviewedActionParameters(actionDefinition(id)!, expected)).toEqual(expected);
    expect(reviewedActionParameters(actionDefinition(id)!, { ...expected, command: "private-canary" })).toBeUndefined();
    expect(actionDefinition(id, "0.9.0")).toBeUndefined();
  });

  it("requires selected patch fields and prevents unsafe or rounded CAS and unknown selectors", () => {
    const draft = initialActionDraft(ACTION_IDS[1]);
    draft.values = { broker: "127.0.0.1:10911", expected_generation: "1" };
    expect(buildActionParameters(draft)).toBeUndefined();
    draft.values["patch.send_message_thread_pool_nums"] = "512";
    expect(buildActionParameters(draft)).toEqual({ broker: "127.0.0.1:10911", expected_generation: 1, patch: { send_message_thread_pool_nums: 512 } });
    for (const value of ["", "-1", "1.1", "1e3", "9007199254740993"]) {
      expect(buildActionParameters({ ...draft, values: { ...draft.values, expected_generation: value } })).toBeUndefined();
    }
    for (const value of ["0", "513"]) expect(buildActionParameters({ ...draft, values: { ...draft.values, "patch.send_message_thread_pool_nums": value } })).toBeUndefined();
    expect(buildActionParameters({ ...draft, values: { ...draft.values, raw_request_code: "1" } })).toBeUndefined();
    expect(reviewedActionParameters(actionDefinition(ACTION_IDS[1])!, { broker: "127.0.0.1:10911", expected_generation: 1, patch: { auth: false } })).toBeUndefined();
  });

  it("keeps logger privacy restrictions and actual driver TTL bounds", () => {
    const draft = initialActionDraft(ACTION_IDS[0]);
    for (const logger of ["other::logger", ...["message", "body", "security", "auth", "acl", "secret", "payload", "store"].map((part) => "rocketmq_broker::" + part)]) {
      expect(buildActionParameters({ ...draft, values: { ...draft.values, logger } })).toBeUndefined();
    }
    for (const ttl of ["59", "901"]) expect(buildActionParameters({ ...draft, values: { ...draft.values, ttl_seconds: ttl } })).toBeUndefined();
    for (const ttl of ["60", "900"]) expect(buildActionParameters({ ...draft, values: { ...draft.values, ttl_seconds: ttl } })).toBeDefined();
  });

  it("accepts only a fixed one-replica lowercase digest canary", () => {
    const entry = parameterCases[6];
    const draft = initialActionDraft(entry.id);
    draft.values = { ...draft.values, ...entry.values };
    for (const image of ["proxy:latest", "sha256:" + "A".repeat(64)]) expect(buildActionParameters({ ...draft, values: { ...draft.values, image_digest: image } })).toBeUndefined();
    expect(buildActionParameters({ ...draft, values: { ...draft.values, canary_replicas: "2" } })).toBeUndefined();
  });

  it("requires a managed secret reference, distinct versions and a dedicated probe", () => {
    const entry = parameterCases[8];
    const draft = { ...initialActionDraft(entry.id), values: entry.values };
    for (const reference of ["raw-secret-canary", "https://private.example/token", "vault://secret/"]) expect(buildActionParameters({ ...draft, values: { ...draft.values, candidate_secret_ref: reference } })).toBeUndefined();
    expect(buildActionParameters({ ...draft, values: { ...draft.values, candidate_version: "v1" } })).toBeUndefined();
    expect(buildActionParameters({ ...draft, values: { ...draft.values, validation_probe_topic: "orders" } })).toBeUndefined();
    expect(validActionResource("broker/[::1]:10911")).toBe(true);
    for (const target of ["", "https://private.example", "broker/*", "broker/a\n", ...[0, 31, 127].map((code) => "broker/a" + String.fromCharCode(code))]) expect(validActionResource(target)).toBe(false);
  });
});
