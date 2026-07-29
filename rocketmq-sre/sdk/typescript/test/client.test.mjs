// Copyright 2026 The RocketMQ Rust Authors
// Licensed under the Apache License, Version 2.0.

import assert from "node:assert/strict";
import test from "node:test";

import {
  SreApiError,
  SreClient,
  SreClientConfigurationError,
  SreResponseTooLargeError,
  createLocalPlanDraft,
} from "../dist/index.js";

const CLUSTER_A = "11111111-1111-4111-8111-111111111111";
const CLUSTER_B = "22222222-2222-4222-8222-222222222222";
const INCIDENT = "33333333-3333-4333-8333-333333333333";
const REVISION = "44444444-4444-4444-8444-444444444444";
const EVIDENCE = "55555555-5555-4555-8555-555555555555";
const DRAFT = "66666666-6666-4666-8666-666666666666";

const jsonResponse = (value, init = {}) =>
  new Response(JSON.stringify(value), {
    status: init.status ?? 200,
    headers: {
      "content-type": "application/json",
      ...init.headers,
    },
  });

test("status uses a fixed GET path and refuses redirects", async () => {
  let captured;
  const client = new SreClient({
    baseUrl: "https://sre.example.test/control-plane",
    token: async () => "sensitive-token",
    fetch: async (input, init) => {
      captured = { input, init };
      return jsonResponse({ status: "healthy" });
    },
  });

  assert.deepEqual(await client.status(), { status: "healthy" });
  assert.equal(
    captured.input.toString(),
    "https://sre.example.test/control-plane/healthz",
  );
  assert.equal(captured.init.method, "GET");
  assert.equal(captured.init.redirect, "error");
  assert.equal(
    new Headers(captured.init.headers).get("authorization"),
    "Bearer sensitive-token",
  );
  assert.equal("request" in client, false);
});

test("cluster allowlist filters list results and fails closed before I/O", async () => {
  let requests = 0;
  const client = new SreClient({
    baseUrl: "https://sre.example.test",
    allowedClusters: [CLUSTER_A],
    fetch: async () => {
      requests += 1;
      return jsonResponse([
        { id: CLUSTER_A },
        { id: CLUSTER_B },
      ]);
    },
  });

  assert.deepEqual(
    (await client.clusters()).map((cluster) => cluster.id),
    [CLUSTER_A],
  );
  assert.equal(requests, 1);
  await assert.rejects(
    client.cluster(CLUSTER_B),
    SreClientConfigurationError,
  );
  assert.equal(requests, 1);
});

test("bounded responses and malformed errors never expose raw bodies", async () => {
  const oversized = new SreClient({
    baseUrl: "https://sre.example.test",
    maxResponseBytes: 8,
    fetch: async () =>
      new Response("0123456789", {
        headers: { "content-length": "10" },
      }),
  });
  await assert.rejects(
    oversized.status(),
    SreResponseTooLargeError,
  );

  const malformed = new SreClient({
    baseUrl: "https://sre.example.test",
    fetch: async () =>
      new Response("raw-internal-secret", { status: 500 }),
  });
  await assert.rejects(
    malformed.status(),
    (error) => {
      assert.ok(error instanceof SreApiError);
      assert.equal(error.code, "http_error");
      assert.equal(error.message, "request failed with HTTP status 500");
      assert.equal(error.message.includes("raw-internal-secret"), false);
      return true;
    },
  );
});

test("local typed plan draft performs no request and grants no execution authority", () => {
  const draft = createLocalPlanDraft(
    {
      cluster_id: CLUSTER_A,
      incident_id: INCIDENT,
      diagnosis_revision_id: REVISION,
      expires_at: "2026-08-01T01:00:00.000Z",
      steps: [
        {
          action_id: "rocketmq.broker.config.plan",
          descriptor_version: "1.0.0",
          resource: "broker-a",
          parameters: { maxMessageSize: 4_194_304 },
          evidence_ids: [EVIDENCE],
        },
      ],
    },
    {
      id: DRAFT,
      now: new Date("2026-07-29T01:00:00.000Z"),
    },
  );

  assert.equal(draft.schema_version, "rocketmq-sre.local-plan-draft.v1");
  assert.equal(draft.mode, "local_only");
  assert.equal(draft.id, DRAFT);
  assert.equal(draft.steps.length, 1);
  assert.equal("approved" in draft, false);
  assert.equal("execution" in draft, false);
});

test("credentials in base URLs and token header injection are rejected", async () => {
  assert.throws(
    () =>
      new SreClient({
        baseUrl: "https://operator:secret@sre.example.test",
      }),
    SreClientConfigurationError,
  );

  const client = new SreClient({
    baseUrl: "https://sre.example.test",
    token: "secret\r\nx-leak: yes",
    fetch: async () => jsonResponse({ status: "healthy" }),
  });
  await assert.rejects(client.status(), SreClientConfigurationError);
});
