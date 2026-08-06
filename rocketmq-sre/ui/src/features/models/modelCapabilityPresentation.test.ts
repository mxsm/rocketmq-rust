import type { ModelCapabilitiesResponse } from "@/api/types";

import {
  modelGatewayPresentation,
  modelProfileHealthLabel,
} from "./modelCapabilityPresentation";

describe("model capability presentation", () => {
  it("distinguishes an enabled healthy runtime from rules-only mode", () => {
    const enabled = capabilities({
      network_calls_enabled: true,
      health: "healthy",
      credential_configured: true,
    });
    const rulesOnly = capabilities({
      network_calls_enabled: false,
      health: "unknown",
      credential_configured: false,
    });

    expect(modelGatewayPresentation(enabled)).toEqual({
      label: "Provider 网络已启用",
      success: true,
    });
    expect(modelGatewayPresentation(rulesOnly)).toEqual({
      label: "Rules-only · Provider 网络关闭",
      success: true,
    });
  });

  it("fails closed when runtime capability state is unavailable", () => {
    expect(modelGatewayPresentation(undefined, true)).toEqual({
      label: "运行状态不可用",
      success: false,
    });
    expect(modelProfileHealthLabel("quarantined")).toBe("已隔离");
  });
});

function capabilities({
  network_calls_enabled,
  health,
  credential_configured,
}: {
  network_calls_enabled: boolean;
  health: "healthy" | "unknown";
  credential_configured: boolean;
}): ModelCapabilitiesResponse {
  return {
    schema_version: "rocketmq-sre.model-capabilities.v1",
    network_calls_supported: true,
    network_calls_enabled,
    rules_only_available: true,
    max_fallbacks: 1,
    fallback_order: ["profile-1"],
    profiles: [
      {
        id: "profile-1",
        profile_name: "deepseek-prod",
        provider_family: "openai_compatible",
        protocol_family: "deep_seek_responses",
        model_family: "deepseek",
        model_name: "deepseek-v4-flash",
        model_revision: "v4-flash",
        endpoint_instance: "deepseek:cn",
        region: "cn",
        capabilities: ["chat", "json_schema"],
        priority: 10,
        credential_configured,
        credential_owner: "gateway",
        health,
        last_health_observed_at: null,
      },
    ],
    providers: [],
    observed_at: "2026-08-07T00:00:00Z",
  };
}
