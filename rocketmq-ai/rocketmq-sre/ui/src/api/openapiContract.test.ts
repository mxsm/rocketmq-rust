import specification from "../../../openapi/rocketmq-sre-phase05.openapi.json";

describe("checked-in Phase 5 OpenAPI", () => {
  it("keeps autonomy typed, risk-bounded, and fail-closed", () => {
    expect(
      specification["x-rocketmq-cluster-mutation-supported"],
    ).toBe(true);
    expect(specification["x-rocketmq-effective-access"]).toBe(
      "bounded_autonomy_with_supervised_r2",
    );
    expect(specification["x-rocketmq-bounded-r1-autonomy-supported"]).toBe(
      true,
    );
    expect(specification["x-rocketmq-r2-supervision-required"]).toBe(true);
    expect(specification["x-rocketmq-r3-agent-reachable"]).toBe(false);
    expect(
      specification[
        "x-rocketmq-unattended-arbitrary-mutation-supported"
      ],
    ).toBe(false);
    expect(
      specification["x-rocketmq-production-dr-cutover-supported"],
    ).toBe(false);
    expect(specification["x-rocketmq-cli-boundary"]).toBe(
      "read_only_with_typed_plan_drafts",
    );
    expect(specification["x-rocketmq-arbitrary-mutation-supported"]).toBe(
      false,
    );

    const paths = Object.entries(specification.paths);
    expect(
      paths.some(([path]) =>
        /\/(delete|reset|truncate)(\/|$)/i.test(path),
      ),
    ).toBe(false);
    expect(
      paths.some(([, operations]) => "delete" in operations),
    ).toBe(false);
  });

  it("binds conversational diagnosis revisions without execution authority", () => {
    const turn = specification.components.schemas.ConversationTurnView;
    const diagnosis =
      specification.components.schemas.InvestigationDiagnosisRevision;
    const streamEvent =
      specification.components.schemas.ConversationStreamEvent;
    const streamOperation =
      specification.paths["/v1/conversations/{id}/turns/stream"].post;

    expect(turn.required).toContain("diagnosis_revision");
    expect(
      turn.properties.diagnosis_revision.oneOf[0].$ref,
    ).toBe("#/components/schemas/InvestigationDiagnosisRevision");
    expect(diagnosis.properties.execution_eligible.const).toBe(false);
    expect(diagnosis.required).toEqual(
      expect.arrayContaining([
        "investigation_id",
        "conversation_id",
        "turn_id",
        "answer_revision_id",
        "evidence_ids",
        "correlation_id",
      ]),
    );
    expect(streamEvent.properties.provisional.type).toBe("boolean");
    expect(streamEvent.properties.event_type.enum).toEqual(
      expect.arrayContaining([
        "accepted",
        "evidence_ready",
        "diagnosis_ready",
        "answer_delta",
        "preview_reset",
        "completed",
        "failed",
        "cancelled",
      ]),
    );
    expect(streamEvent.properties).not.toHaveProperty("execution_request");
    expect(streamOperation.security).toEqual([
      { oidc: ["rocketmq:diagnose"] },
    ]);
    expect(
      streamOperation.responses["200"].content["text/event-stream"],
    ).toBeDefined();
  });

  it("publishes typed Phase 2 and supervised Phase 3 contracts", () => {
    expect(specification["x-rocketmq-sre-phase"]).toBe(5);
    expect(specification["x-rocketmq-phase2-contracts"]).toContain(
      "PostmortemRevision",
    );
    expect(specification.components.schemas.AlertEvent).toBeDefined();
    expect(
      specification.components.schemas.ClusterHealthReport,
    ).toBeDefined();
    expect(
      specification.components.schemas.FleetHealthReport,
    ).toBeDefined();
    expect(specification.components.schemas.CapacityForecast).toBeDefined();
    expect(
      specification.components.schemas.ClusterForecastReport,
    ).toBeDefined();
    expect(
      specification.components.schemas.WhatIfSimulationRequest,
    ).toBeDefined();
    expect(specification.components.schemas.WhatIfSimulation).toBeDefined();
    expect(specification.paths["/v1/clusters/{id}/slo"]).toBeDefined();
    expect(specification.paths["/v1/clusters/{id}/health"]).toBeDefined();
    expect(specification.paths["/v1/fleet/health"]).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/forecasts"],
    ).toBeDefined();
    expect(specification.paths["/v1/simulations"]).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/readiness/upgrade"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/clusters/{id}/readiness/dr"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/incidents/{id}/postmortems"],
    ).toBeDefined();
    expect(specification.paths["/v1/postmortems/{id}"]?.patch).toBeDefined();
    expect(
      specification.paths["/v1/postmortems/{id}/publish"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/incidents/{id}/operations"],
    ).toBeDefined();
    expect(specification.paths["/v1/event-entries"]?.post).toBeDefined();
    expect(
      specification.components.schemas.UnifiedEventEntryRequest.oneOf,
    ).toHaveLength(5);
    expect(
      specification.components.schemas.EventEntrySourceKind.enum,
    ).toEqual([
      "alert",
      "manual_issue",
      "scheduled_inspection",
      "change_event",
      "external_integration",
    ]);
    expect(
      specification.paths["/v1/operations/shift-handoff"],
    ).toBeDefined();
    expect(
      specification.paths["/v1/operations/reports"],
    ).toBeDefined();
    expect(specification.paths["/v1/action-items/{id}"]?.patch).toBeDefined();
    expect(specification.components.schemas.ActionPlan).toBeDefined();
    expect(specification.components.schemas.ApprovalGrant).toBeDefined();
    expect(specification.components.schemas.PolicyDecision).toBeDefined();
    expect(specification.components.schemas.ExecutionRequest).toBeDefined();
    expect(specification.components.schemas.ResourceQuarantine).toBeDefined();
    expect(
      specification.components.schemas.PrepareExecutionPreconditionRequest,
    ).toBeDefined();
    expect(
      specification.components.schemas.ExecutionPreconditionEvidenceView,
    ).toBeDefined();
    expect(
      specification.components.schemas.EvidenceSnapshot__EvidenceExposure
        .oneOf[0].enum,
    ).toContain("execution_agent_api");
    expect(
      specification.paths[
        "/v1/incidents/{incident_id}/execution-preconditions"
      ]?.post,
    ).toBeDefined();
    expect(specification.paths["/v1/plans"]?.post).toBeDefined();
    expect(specification.paths["/v1/plans/{id}/approve"]?.post).toBeDefined();
    expect(specification.paths["/v1/executions"]?.post).toBeDefined();
    expect(
      specification.paths["/v1/resource-quarantines/{id}/clear"]?.post,
    ).toBeDefined();
  });

  it("publishes scoped enterprise Fleet, DR, governance, and FinOps contracts", () => {
    expect(specification.paths["/v1/fleet/overview"]?.get).toBeDefined();
    expect(specification.paths["/v1/fleet/assets"]?.get).toBeDefined();
    expect(specification.paths["/v1/fleet/compliance"]?.get).toBeDefined();
    expect(specification.paths["/v1/fleet/inspections"]?.get).toBeDefined();
    expect(specification.paths["/v1/dr/plans"]?.get).toBeDefined();
    expect(specification.paths["/v1/dr/exercises"]?.get).toBeDefined();
    expect(
      specification.paths["/v1/governance/artifacts"]?.get,
    ).toBeDefined();
    expect(
      specification.paths["/v1/governance/compliance"]?.get,
    ).toBeDefined();
    expect(specification.paths["/v1/finops/report"]?.get).toBeDefined();

    expect(
      specification.paths["/v1/fleet/overview"].get.security,
    ).toEqual([{ oidc: ["rocketmq:read"] }]);
    expect(
      specification.paths["/v1/fleet/onboarding/register"].post.security,
    ).toEqual([{ oidc: ["rocketmq:fleet:manage"] }]);
    expect(
      specification.paths["/v1/dr/exercises"].post.security,
    ).toEqual([{ oidc: ["rocketmq:dr:manage"] }]);
    expect(
      specification.paths["/v1/governance/versions/{id}/transition"].post
        .security,
    ).toEqual([{ oidc: ["rocketmq:governance:manage"] }]);
    expect(
      specification.paths["/v1/finops/budgets/evaluate"].post.security,
    ).toEqual([{ oidc: ["rocketmq:finops:manage"] }]);

    expect(
      specification.components.schemas.DrExerciseMode.enum,
    ).toEqual(["readiness", "tabletop", "supervised_test"]);
    expect(
      specification.components.schemas.GovernanceObjectKind.enum,
    ).toContain("action_descriptor");
    expect(
      specification.components.schemas.FinOpsWorkClass.enum,
    ).toEqual(
      expect.arrayContaining([
        "safety_check",
        "audit",
        "verification",
        "rollback",
      ]),
    );
  });

  it("publishes bounded model lifecycle and smoke operations", () => {
    expect(
      specification.paths["/v1/models/profiles/lifecycle"]?.get,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/lifecycle"]?.get,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/lifecycle"]?.post,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/rollback"]?.post,
    ).toBeDefined();
    expect(
      specification.paths["/v1/models/profiles/{id}/smoke"]?.post,
    ).toBeDefined();
    expect(
      "delete" in
        specification.paths["/v1/models/profiles/{id}/lifecycle"],
    ).toBe(false);
  });

  it("publishes typed and sanitized model capability status", () => {
    const operation =
      specification.paths["/v1/models/capabilities"].get;
    const schemas = specification.components.schemas;

    expect(
      operation.responses["200"].content["application/json"].schema.$ref,
    ).toBe("#/components/schemas/ModelCapabilitiesResponse");
    expect(
      specification.paths["/v1/models/status"].get.responses["200"].content[
        "application/json"
      ].schema.$ref,
    ).toBe("#/components/schemas/ModelCapabilitiesResponse");
    expect(operation.security).toEqual([
      { oidc: ["rocketmq:read"] },
      { oidc: ["rocketmq:diagnose"] },
    ]);
    expect(schemas.ModelCapabilitiesResponse.required).toEqual(
      expect.arrayContaining([
        "schema_version",
        "network_calls_supported",
        "network_calls_enabled",
        "rules_only_available",
        "max_fallbacks",
        "profiles",
        "fallback_order",
        "providers",
        "observed_at",
      ]),
    );
    expect(
      schemas.ModelCapabilitiesResponse.properties.schema_version.const,
    ).toBe("rocketmq-sre.model-capabilities.v1");
    expect(
      schemas.ModelCapabilitiesResponse.properties.profiles.items.$ref,
    ).toBe("#/components/schemas/ModelProfileStatus");

    const profile = schemas.ModelProfileStatus;
    expect(profile.additionalProperties).toBe(false);
    expect(profile.required).toEqual(
      expect.arrayContaining([
        "id",
        "profile_name",
        "protocol_family",
        "capabilities",
        "priority",
        "credential_configured",
        "credential_owner",
        "health",
        "last_health_observed_at",
      ]),
    );
    expect(profile.properties.capabilities.$ref).toBe(
      "#/components/schemas/ModelProviderCapabilities",
    );
    expect(profile.properties).not.toHaveProperty("credential_ref");
    expect(profile.properties).not.toHaveProperty("credential");
    expect(profile.properties).not.toHaveProperty("token");
    expect(profile.properties).not.toHaveProperty("secret");
    expect(profile.properties).not.toHaveProperty("endpoint");
    expect(profile.properties).not.toHaveProperty("endpoint_url");

    expect(
      schemas.ModelProviderCapability.enum,
    ).toEqual(
      expect.arrayContaining([
        "chat",
        "json_schema",
        "tool_calling",
        "streaming",
      ]),
    );
    expect(schemas.ModelProviderDescriptor.properties).not.toHaveProperty(
      "credential_ref",
    );
  });

  it("publishes read-only bounded autonomy outcome and report queries", () => {
    const outcomes =
      specification.paths["/v1/autonomy/outcomes"];
    const reports = specification.paths["/v1/autonomy/reports"];

    expect(outcomes?.get).toBeDefined();
    expect(reports?.get).toBeDefined();
    expect("post" in outcomes).toBe(false);
    expect("delete" in outcomes).toBe(false);
    expect("post" in reports).toBe(false);
    expect("delete" in reports).toBe(false);
    expect(outcomes.get.parameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "cluster_id",
          in: "query",
        }),
        expect.objectContaining({
          name: "limit",
          schema: expect.objectContaining({
            minimum: 1,
            maximum: 200,
            default: 100,
          }),
        }),
      ]),
    );
    expect(reports.get.parameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "period",
          schema: expect.objectContaining({
            enum: ["weekly", "monthly"],
            default: "weekly",
          }),
        }),
      ]),
    );
    const analytics =
      specification.paths["/v1/operations/analytics"];
    expect(analytics?.get).toBeDefined();
    expect(analytics.get.parameters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "cluster_id" }),
        expect.objectContaining({ name: "scenario" }),
        expect.objectContaining({ name: "provider_family" }),
        expect.objectContaining({ name: "model_family" }),
        expect.objectContaining({ name: "action_id" }),
      ]),
    );
    expect(
      specification.components.schemas.OperationsAnalyticsReport
        .properties.schema_version.const,
    ).toBe("rocketmq-sre.operations-analytics.v1");
  });

  it("publishes operator-only autonomy lifecycle controls with bounded approval references", () => {
    const scopes = specification.paths["/v1/autonomy/scopes"];
    const transitions =
      specification.paths["/v1/autonomy/transitions"];
    const freezes = specification.paths["/v1/autonomy/freezes"];
    const killSwitches =
      specification.paths["/v1/autonomy/kill-switches"];

    expect(scopes?.get).toBeDefined();
    expect(transitions?.post).toBeDefined();
    expect(freezes?.post).toBeDefined();
    expect(killSwitches?.post).toBeDefined();
    expect(transitions.post.security).toEqual([
      { oidc: ["rocketmq:autonomy:manage"] },
    ]);
    expect(freezes.post.security).toEqual([
      { oidc: ["rocketmq:autonomy:manage"] },
    ]);
    expect(killSwitches.post.security).toEqual([
      { oidc: ["rocketmq:autonomy:manage"] },
    ]);

    const transitionRequest =
      specification.components.schemas.AutonomyTransitionRequest;
    expect(transitionRequest.additionalProperties).toBe(false);
    expect(transitionRequest.required).toEqual(["target_mode"]);
    expect(
      transitionRequest.properties.owner_approval_ref.pattern,
    ).toBe(
      "^approval://(?!.*(?:\\.\\.|//))[a-z0-9](?:[a-z0-9._/-]*[a-z0-9])$",
    );
    expect(
      transitionRequest.properties.owner_approval_ref.maxLength,
    ).toBe(160);
    expect(
      specification.components.schemas.AutonomyMode.enum,
    ).toEqual([
      "disabled",
      "shadow",
      "supervised",
      "autonomous",
      "paused",
    ]);
  });
});
